package io.sirix.index.projection;

import it.unimi.dsi.fastutil.HashCommon;

/**
 * Flat open-addressed hash table for per-group aggregates keyed by a single {@code long} group
 * value, with each key INTERLEAVED with its own accumulator in one stripe so that a group's probe
 * and its fold touch the same cache line.
 *
 * <p>
 * Logical bucket {@code b} owns one contiguous stripe within one fixed-size storage chunk:
 *
 * <pre>
 *   lane 0                : key         — {@code
 * 0
 * } is the EMPTY-bucket sentinel
 *   lanes 1 .. slotWidth  : accumulator — [count, firstSeen, then per aggregate column:
 *                                          presentCount, sum, min, max]
 *   lane 1 + slotWidth    : aux         — one source reference, present only when {@code
 * withAux
 * }
 * </pre>
 *
 * so {@code stride == 1 + slotWidth + (withAux ? 1 : 0)}. A count-only table
 * ({@code aggColumns == 0}) therefore occupies 3 or 4 lanes per group — one cache line for the
 * probe AND the fold AND the winner decode.
 *
 * <p>
 * The accumulator block's INTERNAL layout is byte-for-byte
 * {@link ProjectionIndexByteScan#newGroupAggAcc}'s, unchanged by the interleave, which is why
 * {@link #acquire} hands back an encoded accumulator handle. Callers resolve it once through
 * {@link #storageAtAccBase} and {@link #offsetAtAccBase}; the resulting array and offset have the
 * same block layout as a STANDALONE {@code long[]} (the missing-key and constant-group
 * accumulators keep that standalone shape). From a handle, the key is one load BELOW the resolved
 * offset ({@link #keyAtAccBase}) and the aux lane one {@code slotWidth} ABOVE it
 * ({@link #auxAtAccBase}).
 *
 * <h2>Non-humongous storage</h2>
 *
 * The logical open-addressed table is physically split into at-most-128-KiB {@code long[]} chunks,
 * allocated lazily on the first insertion into their bucket range. A low-cardinality group-by can
 * therefore retain a high-cardinality logical sizing hint (and never rehash if it turns out dense)
 * without zeroing or later scanning the untouched ranges.
 * Hashing, probing, load factor and rehash order are unchanged: a probe still walks the same logical
 * bucket sequence, and a stripe never crosses a chunk boundary. The fixed chunk ceiling is below
 * G1's 2-MiB humongous threshold with the canonical 4-MiB regions, including array-header and
 * alignment margin. High-cardinality scans retain their full up-front capacity (and therefore avoid
 * incremental rehashing); only the physical allocation changes.
 *
 * <p>
 * Chunks may come from a {@link LongChunkPool} attached before the first insertion
 * ({@link #attachChunkPool}): a rehash hands its old chunks back, and {@link #release} hands back
 * every chunk of a table that is done — so a grouped scan whose worker tables flush and whose
 * partition tables grow many times per pass promotes each chunk into the old generation ONCE instead
 * of copying a fresh generation of them through every young pause.
 *
 * <p>
 * This replaces {@code Long2ObjectOpenHashMap<long[]>} in the high-cardinality group-by kernel: no
 * boxed accumulator per group and no per-group allocation. The real group value {@code 0} cannot
 * live in a bucket (its key lane would read as empty), so it takes a dedicated side slot.
 *
 * <p>
 * NOT thread-safe by design: the kernel builds one table per worker and merges by hash partition,
 * so no table is ever written from two threads.
 */
public final class NumericGroupAggTable {

  /** Bucket ceiling; the table grows (at a 3/4 load factor) until it would pass this. */
  private static final int MAX_CAPACITY = 1 << 30;

  /** Largest logical lane count addressable by this table. */
  private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

  /** 128 KiB of primitive lanes per backing array. */
  static final int MAX_STORAGE_CHUNK_LANES = 1 << 14;

  /** Spine of a {@link #release released} table: no chunk, so any probe fails loudly. */
  private static final long[][] RELEASED_STORAGE = new long[0][];

  /**
   * Stands in for a probe hash of {@code 0} in identity mode, because {@code 0} in the key lane is
   * the empty-bucket sentinel. Substituting is sound ONLY with identity lanes: a real group whose
   * probe hash equals this constant lands in the same probe chain and is then separated by its
   * identity, exactly as any other same-key pair is. Without identity lanes the zero group still
   * needs {@link #acquireZero}'s dedicated side slot.
   */
  private static final long ZERO_PROBE_SUBSTITUTE = 0x9E3779B97F4A7C15L;

  private final int slotWidth;
  private final int stride;
  private final int aggColumns;
  /**
   * Bit {@code a} set ⇒ aggregate column {@code a}'s SUM lane is read by the query (a {@code sum} or
   * {@code avg}), so it folds and merges under {@link Math#addExact} and an overflow declines. Bit
   * clear ⇒ nothing reads that lane and it is not folded at all: the accumulator block carries
   * {@code [count, sum, min, max]} for every column whatever the query asked for, and making a
   * {@code min}-only query decline because the sum it never requested does not fit a long would
   * refuse an answerable query — the failure mode that took JSONBench Q4/Q5 off the served path at
   * 100M rows, where one busy group's timestamps first exceed {@code 2^63} µs.
   *
   * <p>
   * Columns past 63 are always exact ({@code a >= 64} short-circuits the test), so a wide roster
   * degrades to the old always-exact behaviour rather than to a silently wrapped lane.
   */
  private final long sumExactMask;
  /**
   * Whether every stripe carries a source-reference lane — the string kernel stores
   * {@code (leaf << 20) | dictId} of a group's first sighting there so only WINNING groups ever
   * materialize their string.
   */
  private final boolean withAux;
  /**
   * Identity lanes per stripe, appended AFTER the aux lane so that {@link #offsetAtAccBase} and
   * {@link #auxAtAccBase} keep their arithmetic. {@code 0} restores the pre-identity table exactly:
   * the key lane alone decides group membership.
   *
   * <p>
   * With {@code idWidth > 0} the key lane degrades to a PROBE hash and the identity lanes decide
   * membership, so two distinct groups may legitimately share a key. Every structural invariant the
   * table relies on already tolerates that: {@link #rehash} re-homes each stripe into the first
   * EMPTY bucket rather than deduplicating, and {@link #buildPartitionIndex} partitions on the key,
   * so same-key groups always meet inside one merge partition where identity can separate them.
   */
  private final int idWidth;
  /** Lane distance from a stripe's accumulator base to its first identity lane. */
  private final int idOffsetFromAcc;
  private long[][] storage;
  /** Chunk recycler, or {@code null} for plain allocation. Set once, before the first insertion. */
  private LongChunkPool pool;
  private int bucketsPerChunk;
  private int chunkBucketMask;
  private int chunkBucketShift;
  private int mask;
  private int size;
  private int growAt;
  /** Times {@link #rehash} ran — zero for a table hinted at its final count (see {@link #rehashes}). */
  private int rehashes;
  private boolean hasZeroKey;
  /**
   * Set the moment {@link #acquireExact} walks past a bucket whose key matches but whose identity
   * does not — i.e. the moment two genuinely different groups are proven to share a probe hash.
   * The table itself keeps them apart regardless; the flag exists for the side structures that are
   * still keyed by the probe hash alone (the per-group COUNT(DISTINCT) sets), which have no way to
   * tell the two apart and must decline rather than merge them.
   */
  private boolean probeKeyCollision;
  private final long[] zeroSlot;
  private long zeroAux;

  /**
   * Handle {@link #acquire}/{@link #acquireExact} answer for a key outside the table's pass range:
   * {@link #storageAtAccBase}/{@link #offsetAtAccBase} resolve it to a scratch block the kernels fold
   * into and nobody reads, so a kernel needs no knowledge of passes at all. Negative, so a kernel's
   * per-dictionary handle cache treats it as unresolved and re-probes (a pass-mode cost only).
   */
  public static final int DISCARD_HANDLE = -1;

  /** {@code 64} = every key belongs (no pass filter); else the partition shift of the pass split. */
  private int passShift = 64;
  private int passLo;
  private int passHi;
  private long[] discard;
  private long[] discardZero;

  /**
   * @param aggColumns aggregate columns per group ({@code slotWidth = 2 + 4 * aggColumns})
   * @param expectedEntries sizing hint; the table grows past it without limit
   */
  public NumericGroupAggTable(final int aggColumns, final int expectedEntries) {
    this(aggColumns, expectedEntries, false);
  }

  /** @param withAux carry a per-entry source-reference lane (see {@link #auxAtAccBase}) */
  public NumericGroupAggTable(final int aggColumns, final int expectedEntries, final boolean withAux) {
    this(aggColumns, expectedEntries, withAux, -1L);
  }

  /** @param sumExactMask which columns' SUM lanes the query reads (see {@link #sumsExact}) */
  public NumericGroupAggTable(final int aggColumns, final int expectedEntries, final boolean withAux,
      final long sumExactMask) {
    this(aggColumns, expectedEntries, withAux, sumExactMask, 0);
  }

  /**
   * @param idWidth identity lanes per group; {@code 0} keeps the key lane as the whole identity.
   *        A positive width turns the key lane into a probe hash and makes membership decided by an
   *        EXACT comparison of the identity lanes, which is what a composite group-by needs: its
   *        key hash folds several components into 64 bits, and that fold is invertible enough to be
   *        solved for a collision in closed form rather than searched for.
   */
  public NumericGroupAggTable(final int aggColumns, final int expectedEntries, final boolean withAux,
      final long sumExactMask, final int idWidth) {
    if (aggColumns < 0) {
      throw new IllegalArgumentException("aggColumns must be >= 0");
    }
    if (idWidth < 0) {
      throw new IllegalArgumentException("idWidth must be >= 0");
    }
    this.aggColumns = aggColumns;
    this.sumExactMask = sumExactMask;
    this.slotWidth = 2 + 4 * aggColumns;
    this.withAux = withAux;
    this.idWidth = idWidth;
    this.idOffsetFromAcc = slotWidth + (withAux
        ? 1
        : 0);
    this.stride = 1 + slotWidth + (withAux
        ? 1
        : 0) + idWidth;
    if (stride > MAX_STORAGE_CHUNK_LANES) {
      throw new IllegalArgumentException(
          "aggregate stripe of " + stride + " lanes exceeds the non-humongous chunk ceiling");
    }
    int cap = (int) (Long.highestOneBit(Math.max(16, Math.min(MAX_CAPACITY, (long) expectedEntries * 4 / 3)) - 1) << 1);
    if (cap < 16) {
      cap = 16;
    }
    while ((long) cap * stride > MAX_ARRAY_LENGTH) {
      cap >>>= 1;
    }
    if (cap < 16) {
      throw new IllegalArgumentException("aggColumns too large for one stripe: " + aggColumns);
    }
    installEmptyStorage(cap);
    this.mask = cap - 1;
    this.growAt = cap - (cap >>> 2);
    this.zeroSlot = newAcc(slotWidth);
  }

  private static long[] newAcc(final int slotWidth) {
    final long[] acc = new long[slotWidth];
    acc[1] = Long.MAX_VALUE;
    for (int base = 2; base < slotWidth; base += 4) {
      acc[base + 2] = Long.MAX_VALUE;
      acc[base + 3] = Long.MIN_VALUE;
    }
    return acc;
  }

  /** Aggregate columns this table's accumulator blocks carry. */
  public int aggColumns() {
    return aggColumns;
  }

  /**
   * This table's {@link #sumExactMask} — kernels fold their rows under the same rule the merge uses.
   */
  public long sumExactMask() {
    return sumExactMask;
  }

  /**
   * Whether aggregate column {@code a}'s SUM lane must be folded exactly, i.e. whether the query
   * reads it at all. THE rule, so kernel and merge can never disagree about a lane.
   */
  public static boolean sumsExact(final long sumExactMask, final int a) {
    return a >= 64 || (sumExactMask >>> a & 1L) != 0L;
  }

  /** Longs per accumulator block. */
  public int slotWidth() {
    return slotWidth;
  }

  /** Longs per bucket stripe: {@code 1 + slotWidth() + (aux ? 1 : 0)}. */
  public int stride() {
    return stride;
  }

  /** Identity lanes per group; {@code 0} when the key lane alone decides membership. */
  public int idWidth() {
    return idWidth;
  }

  /**
   * Identity lane {@code lane} of the group whose accumulator starts at {@code accBase}. Valid only
   * when {@link #idWidth()} is positive.
   */
  public long identityAtAccBase(final int accBase, final int lane) {
    final long[] chunk = storageAtAccBase(accBase);
    return chunk[offsetAtAccBase(accBase) + idOffsetFromAcc + lane];
  }

  /** Number of non-humongous backing arrays. */
  public int storageChunkCount() {
    return storage.length;
  }

  /** Lanes per backing array at the moment. */
  public int chunkLanes() {
    return bucketsPerChunk * stride;
  }

  /**
   * Lanes per backing array of any table with {@code stride} lanes per group once its capacity has
   * reached the chunk ceiling — the one length a {@link LongChunkPool} shared by such tables recycles.
   */
  public static int fullChunkLanes(final int stride) {
    if (stride <= 0 || stride > MAX_STORAGE_CHUNK_LANES) {
      throw new IllegalArgumentException("stride out of range: " + stride);
    }
    return Integer.highestOneBit(MAX_STORAGE_CHUNK_LANES / stride) * stride;
  }

  /**
   * Take this table's chunks from (and return them to) {@code pool}. Only before the first insertion:
   * a table that already holds groups would mix pooled and private chunks.
   *
   * @return this table
   * @throws IllegalStateException if the table already holds a group
   */
  public NumericGroupAggTable attachChunkPool(final LongChunkPool pool) {
    if (size != 0 || hasZeroKey) {
      throw new IllegalStateException("chunk pool attached after the first insertion");
    }
    this.pool = pool;
    return this;
  }

  /** The attached chunk recycler, or {@code null}. */
  public LongChunkPool chunkPool() {
    return pool;
  }

  /**
   * Hand every backing array back to the attached pool (or to the collector) and leave the table
   * without storage. The counters ({@link #size}, {@link #hasZeroKey}, the zero group's slot) survive;
   * probing or walking the table afterwards is a contract violation and fails on the empty spine.
   * A caller that still points INTO the storage — a top-k selector holding accumulator references —
   * must copy what it keeps before this call.
   */
  public void release() {
    final long[][] chunks = storage;
    for (int i = 0; i < chunks.length; i++) {
      final long[] chunk = chunks[i];
      if (chunk != null) {
        chunks[i] = null;
        recycle(chunk);
      }
    }
    storage = RELEASED_STORAGE;
  }

  /** Whether {@link #release} has run. */
  public boolean released() {
    return storage == RELEASED_STORAGE;
  }

  private long[] newChunk(final int lanes) {
    final LongChunkPool p = pool;
    return p != null && lanes == p.chunkLanes()
        ? p.take()
        : new long[lanes];
  }

  private void recycle(final long[] chunk) {
    final LongChunkPool p = pool;
    if (p != null) {
      p.give(chunk);
    }
  }

  /**
   * One backing array for a sequential table walk, or {@code null} when its logical bucket range was
   * never touched. Every live stripe is wholly contained in one non-null chunk and starts at an
   * offset divisible by {@link #stride()}.
   */
  public long[] storageChunkOrNull(final int chunk) {
    return storage[chunk];
  }

  /** Backing array that owns {@code accBase}. Re-resolve after any growing {@link #acquire}. */
  public long[] storageAtAccBase(final int accBase) {
    if (accBase < 0) {
      return discard;
    }
    return storage[accBase >>> chunkBucketShift];
  }

  /** Chunk-local accumulator offset encoded in {@code accBase}. */
  public int offsetAtAccBase(final int accBase) {
    if (accBase < 0) {
      return 1;
    }
    return (accBase & chunkBucketMask) * stride + 1;
  }

  /** Bucket count (a power of two). */
  public int capacity() {
    return mask + 1;
  }

  /** Distinct non-zero keys inserted. */
  public int size() {
    return size;
  }

  /**
   * Rehashes since construction: a table hinted at the count it ends up holding reports zero. The
   * witness that a sizing hint TOOK — a spill's shared partition table grows under its partition
   * lock, so every rehash there is a copy the other workers queue behind.
   */
  public int rehashes() {
    return rehashes;
  }

  /**
   * Whether two distinct groups in this table share a probe hash. Always {@code false} outside
   * identity mode, where a shared key IS a shared group.
   */
  public boolean hasProbeKeyCollision() {
    return probeKeyCollision;
  }

  /**
   * Accumulator handle of the ONLY group carrying {@code probeHash}, for side structures that are
   * keyed by the probe hash. Callers must have established {@link #hasProbeKeyCollision()} is
   * {@code false}, which is exactly what makes the key unambiguous.
   *
   * @param probeHash the group's probe hash as the kernel computed it ({@code 0} remapped internally)
   * @return the encoded accumulator handle
   * @throws IllegalStateException if the table holds no group under that key, or if a collision
   *         makes the key ambiguous
   */
  public int handleOfProbeKey(final long probeHash) {
    if (probeKeyCollision) {
      throw new IllegalStateException("probe key is ambiguous: two groups share it");
    }
    final long key = probeHash == 0L
        ? ZERO_PROBE_SUBSTITUTE
        : probeHash;
    final int start = (int) HashCommon.mix(key) & mask;
    int bucket = start;
    do {
      final long[] chunk = storage[bucket >>> chunkBucketShift];
      final int off = (bucket & chunkBucketMask) * stride;
      final long cur = chunk == null
          ? 0L
          : chunk[off];
      if (cur == key) {
        return bucket;
      }
      if (cur == 0L) {
        break;
      }
      bucket = bucket + 1 & mask;
    } while (bucket != start);
    throw new IllegalStateException("no group carries probe key " + probeHash);
  }

  /** Whether the real group value {@code 0} was seen. */
  public boolean hasZeroKey() {
    return hasZeroKey;
  }

  /** The accumulator block for group value {@code 0} — valid only when {@link #hasZeroKey()}. */
  public long[] zeroSlot() {
    return zeroSlot;
  }

  /** Distinct groups including the zero key. */
  public int sizeIncludingZero() {
    return size + (hasZeroKey
        ? 1
        : 0);
  }

  /**
   * Key of bucket {@code bucket}; {@code 0} = empty (the real key 0 lives in {@link #zeroSlot()}).
   */
  public long keyAtBucket(final int bucket) {
    final int chunk = bucket >>> chunkBucketShift;
    final int off = (bucket & chunkBucketMask) * stride;
    final long[] block = storage[chunk];
    return block == null
        ? 0L
        : block[off];
  }

  /** Encoded accumulator handle of {@code bucket} — valid only when its key lane is non-zero. */
  public int accBaseOfBucket(final int bucket) {
    return bucket;
  }

  /**
   * The key owning the accumulator block at {@code accBase} — ONE load, and it lets a caller VALIDATE
   * a cached block base across rehashes: a base is current iff its bucket still holds the same key
   * (keys are unique, so a match can never be a different group).
   */
  public long keyAtAccBase(final int accBase) {
    final long[] chunk = storageAtAccBase(accBase);
    return chunk == null
        ? 0L
        : chunk[offsetAtAccBase(accBase) - 1];
  }

  /**
   * The partition {@link #mergePartition} assigns {@code key} to under {@code shift} — the ONE hash
   * policy, so side structures (per-group distinct sets) split identically.
   */
  public static int partitionOf(final long key, final int shift) {
    return shift >= 64
        ? 0
        : (int) (HashCommon.mix(key) >>> shift);
  }

  /** Source reference of the entry whose accumulator starts at {@code accBase} (aux lane only). */
  public long auxAtAccBase(final int accBase) {
    final long[] chunk = storageAtAccBase(accBase);
    return chunk[offsetAtAccBase(accBase) + slotWidth];
  }

  /**
   * Stamp the source reference of the entry whose accumulator starts at {@code accBase} (aux lane
   * only). A never-occupied stripe reads {@code 0}, exactly like the separate lane it replaces.
   */
  public void setAuxAtAccBase(final int accBase, final long value) {
    final long[] chunk = storageAtAccBase(accBase);
    chunk[offsetAtAccBase(accBase) + slotWidth] = value;
  }

  /** Source reference of the zero-key group (aux lane only). */
  public long zeroAux() {
    return zeroAux;
  }

  /** Stamp the zero-key group's source reference (aux lane only). */
  public void setZeroAux(final long value) {
    zeroAux = value;
  }

  /**
   * Encoded handle of {@code key}'s accumulator block, inserting a fresh block
   * ({@code count 0, firstSeen ordinal, empty aggregates}) on first sight. {@code key} MUST be
   * non-zero — route the zero group through {@link #acquireZero}.
   *
   * <p>
   * The returned handle is invalidated by any later {@code acquire} that grows the table. Resolve
   * both its backing array and its chunk-local offset AFTER the call.
   */
  /**
   * Restrict this table to the groups whose partition ({@link #partitionOf} under {@code shift}) lies in
   * {@code [lo, hi)}: every other key acquires {@link #DISCARD_HANDLE}. A hash-range pass of a group-by
   * scans the whole input P times and keeps 1/P of the groups per pass, so memory is bounded at any
   * cardinality; the partitioning is the post-scan merge's own, so a partition's groups complete
   * within one pass.
   */
  public void setPassRange(final int shift, final int lo, final int hi) {
    if (shift < 0 || shift > 64) {
      throw new IllegalArgumentException("shift out of range: " + shift);
    }
    if (lo < 0 || hi <= lo) {
      throw new IllegalArgumentException("empty pass range [" + lo + ", " + hi + ")");
    }
    this.passShift = shift;
    this.passLo = lo;
    this.passHi = hi;
    if (discard == null) {
      // Room for a full stripe past the acc base the kernels fold at (offset 1).
      discard = new long[stride + 1];
      discardZero = new long[slotWidth];
    }
  }

  /** Whether {@code key} lies outside the pass range (never, without a pass filter). */
  private boolean outOfPass(final long key) {
    if (passShift == 64) {
      return false;
    }
    final int p = (int) (HashCommon.mix(key) >>> passShift);
    return p < passLo || p >= passHi;
  }

  public int acquire(final long key, final long firstSeenOrdinal) {
    if (outOfPass(key)) {
      return DISCARD_HANDLE;
    }
    final long[][] chunks = storage;
    final int st = stride;
    final int bucketShift = chunkBucketShift;
    final int bucketMask = chunkBucketMask;
    int bucket = (int) HashCommon.mix(key) & mask;
    int chunkIndex = bucket >>> bucketShift;
    int off = (bucket & bucketMask) * st;
    long[] chunk = chunks[chunkIndex];
    long cur = chunk == null
        ? 0L
        : chunk[off];
    while (cur != 0L) {
      if (cur == key) {
        return bucket;
      }
      bucket = bucket + 1 & mask;
      chunkIndex = bucket >>> bucketShift;
      off = (bucket & bucketMask) * st;
      chunk = chunks[chunkIndex];
      cur = chunk == null
          ? 0L
          : chunk[off];
    }
    if (chunk == null) {
      chunk = newChunk(bucketsPerChunk * st);
      chunks[chunkIndex] = chunk;
    }
    chunk[off] = key;
    initBlock(chunk, off + 1, firstSeenOrdinal, slotWidth);
    if (++size > growAt) {
      rehash();
      // The stripe moved with its key; re-probe in the grown table (guaranteed present).
      return find(key);
    }
    return bucket;
  }

  /**
   * Encoded handle of the group whose EXACT identity is {@code identity[identityOffset ..
   * identityOffset + idWidth())}, probed under {@code probeHash}, inserting a fresh block on first
   * sight.
   *
   * <p>
   * {@code probeHash} only chooses the probe chain: a bucket whose key lane matches but whose
   * identity lanes do not is a collision, and the probe simply walks on, so the two tuples occupy
   * two buckets and never fold into one another. That is the whole difference from
   * {@link #acquire(long, long)}, which treats a key match as proof of group identity.
   *
   * <p>
   * The returned handle is invalidated by any later acquire that grows the table. Resolve both its
   * backing array and its chunk-local offset AFTER the call.
   *
   * @param probeHash the group's probe hash; {@code 0} is remapped internally
   * @param firstSeenOrdinal source reference stamped into a freshly created block
   * @param identity the group's exact identity lanes
   * @param identityOffset index of the first identity lane within {@code identity}
   * @return the encoded accumulator handle
   */
  public int acquireExact(final long probeHash, final long firstSeenOrdinal, final long[] identity,
      final int identityOffset) {
    final long key = probeHash == 0L
        ? ZERO_PROBE_SUBSTITUTE
        : probeHash;
    if (outOfPass(key)) {
      return DISCARD_HANDLE;
    }
    final long[][] chunks = storage;
    final int st = stride;
    final int bucketShift = chunkBucketShift;
    final int bucketMask = chunkBucketMask;
    final int idOff = idOffsetFromAcc + 1;
    final int width = idWidth;
    int bucket = (int) HashCommon.mix(key) & mask;
    int chunkIndex = bucket >>> bucketShift;
    int off = (bucket & bucketMask) * st;
    long[] chunk = chunks[chunkIndex];
    long cur = chunk == null
        ? 0L
        : chunk[off];
    while (cur != 0L) {
      if (cur == key) {
        if (identityMatches(chunk, off + idOff, identity, identityOffset, width)) {
          return bucket;
        }
        probeKeyCollision = true;
      }
      bucket = bucket + 1 & mask;
      chunkIndex = bucket >>> bucketShift;
      off = (bucket & bucketMask) * st;
      chunk = chunks[chunkIndex];
      cur = chunk == null
          ? 0L
          : chunk[off];
    }
    if (chunk == null) {
      chunk = newChunk(bucketsPerChunk * st);
      chunks[chunkIndex] = chunk;
    }
    chunk[off] = key;
    initBlock(chunk, off + 1, firstSeenOrdinal, slotWidth);
    System.arraycopy(identity, identityOffset, chunk, off + idOff, width);
    if (++size > growAt) {
      rehash();
      return findExact(key, identity, identityOffset);
    }
    return bucket;
  }

  private static boolean identityMatches(final long[] chunk, final int base, final long[] identity,
      final int identityOffset, final int width) {
    for (int i = 0; i < width; i++) {
      if (chunk[base + i] != identity[identityOffset + i]) {
        return false;
      }
    }
    return true;
  }

  /** Accumulator base of an EXISTING identity — the post-rehash re-probe. */
  private int findExact(final long key, final long[] identity, final int identityOffset) {
    final long[][] chunks = storage;
    final int st = stride;
    final int idOff = idOffsetFromAcc + 1;
    final int width = idWidth;
    int bucket = (int) HashCommon.mix(key) & mask;
    while (true) {
      final int chunkIndex = bucket >>> chunkBucketShift;
      final int off = (bucket & chunkBucketMask) * st;
      final long[] chunk = chunks[chunkIndex];
      if (chunk != null && chunk[off] == key && identityMatches(chunk, off + idOff, identity, identityOffset, width)) {
        return bucket;
      }
      bucket = bucket + 1 & mask;
    }
  }

  /** The zero group's accumulator block, stamping its first-seen ordinal on first sight. */
  public long[] acquireZero(final long firstSeenOrdinal) {
    if (idWidth != 0) {
      throw new IllegalStateException("identity-mode tables route the zero probe hash through acquireExact");
    }
    if (passShift < 64 && passLo > 0) {
      return discardZero; // the zero key lives in partition 0, which this pass does not own
    }
    if (!hasZeroKey) {
      hasZeroKey = true;
      zeroSlot[1] = firstSeenOrdinal;
    }
    return zeroSlot;
  }

  /** Accumulator base of an EXISTING non-zero key. */
  private int find(final long key) {
    final long[][] chunks = storage;
    final int st = stride;
    final int bucketShift = chunkBucketShift;
    final int bucketMask = chunkBucketMask;
    int bucket = (int) HashCommon.mix(key) & mask;
    int chunkIndex = bucket >>> bucketShift;
    int off = (bucket & bucketMask) * st;
    long[] chunk = chunks[chunkIndex];
    while (chunk == null || chunk[off] != key) {
      bucket = bucket + 1 & mask;
      chunkIndex = bucket >>> bucketShift;
      off = (bucket & bucketMask) * st;
      chunk = chunks[chunkIndex];
    }
    return bucket;
  }

  private static void initBlock(final long[] t, final int accBase, final long firstSeenOrdinal, final int slotWidth) {
    t[accBase] = 0L;
    t[accBase + 1] = firstSeenOrdinal;
    for (int b = accBase + 2; b < accBase + slotWidth; b += 4) {
      t[b] = 0L;
      t[b + 1] = 0L;
      t[b + 2] = Long.MAX_VALUE;
      t[b + 3] = Long.MIN_VALUE;
    }
  }

  private void rehash() {
    final int oldCap = mask + 1;
    if (oldCap >= MAX_CAPACITY) {
      throw new IllegalStateException("group table exceeds " + MAX_CAPACITY + " buckets");
    }
    final int newCap = oldCap << 1;
    final long newLength = (long) newCap * stride;
    if (newLength > MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("group table exceeds " + MAX_ARRAY_LENGTH + " lanes");
    }
    final int newMask = newCap - 1;
    final int st = stride;
    final long[][] old = storage;
    final int newBucketsPerChunk = bucketsPerChunk(newCap);
    final int newChunkBucketMask = newBucketsPerChunk - 1;
    final int newChunkBucketShift = Integer.numberOfTrailingZeros(newBucketsPerChunk);
    final long[][] grown = allocateStorage(newCap, newBucketsPerChunk);
    // Bucket order is the OLD table's, so the grown table's probe chains are identical to the
    // ones a same-order rebuild would produce.
    for (final long[] oldChunk : old) {
      if (oldChunk == null) {
        continue;
      }
      for (int o = 0; o < oldChunk.length; o += st) {
        final long key = oldChunk[o];
        if (key == 0L) {
          continue;
        }
        int bucket = (int) HashCommon.mix(key) & newMask;
        int chunkIndex = bucket >>> newChunkBucketShift;
        int to = (bucket & newChunkBucketMask) * st;
        long[] targetChunk = grown[chunkIndex];
        while (targetChunk != null && targetChunk[to] != 0L) {
          bucket = bucket + 1 & newMask;
          chunkIndex = bucket >>> newChunkBucketShift;
          to = (bucket & newChunkBucketMask) * st;
          targetChunk = grown[chunkIndex];
        }
        if (targetChunk == null) {
          targetChunk = newChunk(newBucketsPerChunk * st);
          grown[chunkIndex] = targetChunk;
        }
        System.arraycopy(oldChunk, o, targetChunk, to, st);
      }
      recycle(oldChunk);
    }
    storage = grown;
    bucketsPerChunk = newBucketsPerChunk;
    chunkBucketMask = newChunkBucketMask;
    chunkBucketShift = newChunkBucketShift;
    mask = newMask;
    growAt = newCap - (newCap >>> 2);
    rehashes++;
  }

  private void installEmptyStorage(final int capacity) {
    final int chunkBuckets = bucketsPerChunk(capacity);
    storage = allocateStorage(capacity, chunkBuckets);
    bucketsPerChunk = chunkBuckets;
    chunkBucketMask = chunkBuckets - 1;
    chunkBucketShift = Integer.numberOfTrailingZeros(chunkBuckets);
  }

  private int bucketsPerChunk(final int capacity) {
    final int maxBuckets = Integer.highestOneBit(MAX_STORAGE_CHUNK_LANES / stride);
    return Math.min(capacity, maxBuckets);
  }

  private long[][] allocateStorage(final int capacity, final int chunkBuckets) {
    final int chunkCount = capacity / chunkBuckets;
    return new long[chunkCount][];
  }

  /**
   * Accumulator BASES of every live key, grouped by partition — built by the SCAN worker that owns
   * this table (already parallel, so the wall cost is zero) so the partition merge walks straight to
   * its keys instead of rescanning every source's full bucket array once per partition (P× the loads,
   * each key re-mixed P times — measured 13% of hot suite CPU). Entries are encoded accumulator
   * handles: after one chunk resolution, a stripe's key is at {@code offset - 1} and its aux at
   * {@code offset + slotWidth()}.
   */
  public int[][] buildPartitionIndex(final int partitions, final int shift) {
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be > 0");
    }
    final int[] counts = new int[partitions];
    final int st = stride;
    for (final long[] chunk : storage) {
      if (chunk == null) {
        continue;
      }
      for (int o = 0; o < chunk.length; o += st) {
        if (chunk[o] != 0L) {
          counts[shift < 64
              ? (int) (HashCommon.mix(chunk[o]) >>> shift)
              : 0]++;
        }
      }
    }
    final int[][] out = new int[partitions][];
    for (int p = 0; p < partitions; p++) {
      out[p] = new int[counts[p]];
      counts[p] = 0;
    }
    for (int chunkIndex = 0; chunkIndex < storage.length; chunkIndex++) {
      final long[] chunk = storage[chunkIndex];
      if (chunk == null) {
        continue;
      }
      for (int o = 0; o < chunk.length; o += st) {
        if (chunk[o] != 0L) {
          final int p = shift < 64
              ? (int) (HashCommon.mix(chunk[o]) >>> shift)
              : 0;
          out[p][counts[p]++] = chunkIndex * bucketsPerChunk + o / st;
        }
      }
    }
    return out;
  }

  /**
   * {@link #mergePartition} over pre-built {@link #buildPartitionIndex} indexes — identical merge
   * semantics (first-arrival aux, zero group to partition 0), none of the rescans.
   */
  public static void mergePartitionIndexed(final NumericGroupAggTable[] sources, final int[][][] index,
      final int partition, final NumericGroupAggTable into) {
    final int slotWidth = into.slotWidth;
    final boolean aux = into.withAux;
    for (int s = 0; s < sources.length; s++) {
      final NumericGroupAggTable src = sources[s];
      if (src == null || index[s] == null) {
        continue;
      }
      requireMergeable(src, into);
      for (final int srcHandle : index[s][partition]) {
        final long[] srcTable = src.storageAtAccBase(srcHandle);
        final int srcBase = src.offsetAtAccBase(srcHandle);
        // Identity mode carries the source's identity lanes across, so two same-key groups that a
        // single worker kept apart cannot be folded together by the merge that reunites them.
        final int dstHandle = into.idWidth == 0
            ? into.acquire(srcTable[srcBase - 1], srcTable[srcBase + 1])
            : into.acquireExact(srcTable[srcBase - 1], srcTable[srcBase + 1], srcTable,
                srcBase + into.idOffsetFromAcc);
        // AFTER the acquire: growth swaps the storage out from under any earlier resolution.
        final long[] dstTable = into.storageAtAccBase(dstHandle);
        final int dstBase = into.offsetAtAccBase(dstHandle);
        if (aux && dstTable[dstBase] == 0L) {
          dstTable[dstBase + slotWidth] = srcTable[srcBase + slotWidth];
        }
        mergeBlock(dstTable, dstBase, srcTable, srcBase, slotWidth, into.sumExactMask);
      }
      mergeZeroGroup(src, into, slotWidth, partition);
    }
  }

  /**
   * Fold every entry of {@code sources} whose key hashes into {@code partition} into {@code into} —
   * the partition-parallel replacement for the serial whole-map merge: each partition is owned by
   * exactly one worker, so the merge needs no locks and no thread ever folds the full group count.
   * Partition of a key is the TOP {@code 64 - shift} bits of the same mix the buckets use
   * ({@code shift == 64} means a single partition taking everything). The zero group belongs to
   * partition 0.
   *
   * <p>
   * Sum lanes merge with {@link Math#addExact} — the same interpreter-promotes-on-overflow discipline
   * the row fold enforces; the caller treats the {@link ArithmeticException} as a decline.
   */
  public static void mergePartition(final NumericGroupAggTable[] sources, final int partition, final int shift,
      final NumericGroupAggTable into) {
    final int slotWidth = into.slotWidth;
    final boolean aux = into.withAux;
    for (final NumericGroupAggTable src : sources) {
      if (src == null) {
        continue;
      }
      requireMergeable(src, into);
      final int st = src.stride;
      for (final long[] srcTable : src.storage) {
        if (srcTable == null) {
          continue;
        }
        for (int o = 0; o < srcTable.length; o += st) {
          final long key = srcTable[o];
          if (key == 0L) {
            continue;
          }
          if (shift < 64 && (int) (HashCommon.mix(key) >>> shift) != partition) {
            continue;
          }
          final int srcBase = o + 1;
          final int dstHandle = into.idWidth == 0
              ? into.acquire(key, srcTable[srcBase + 1])
              : into.acquireExact(key, srcTable[srcBase + 1], srcTable, srcBase + into.idOffsetFromAcc);
          final long[] dstTable = into.storageAtAccBase(dstHandle);
          final int dstBase = into.offsetAtAccBase(dstHandle);
          if (aux && dstTable[dstBase] == 0L) {
            // Fresh in the destination: carry the source reference (any sighting's bytes are the
            // same group value, so first-arrival is as good as first-seen).
            dstTable[dstBase + slotWidth] = srcTable[srcBase + slotWidth];
          }
          mergeBlock(dstTable, dstBase, srcTable, srcBase, slotWidth, into.sumExactMask);
        }
      }
      mergeZeroGroup(src, into, slotWidth, partition);
    }
  }

  /**
   * The zero group belongs to partition 0 and lives in a side slot, so it merges the same way for
   * both partition walks.
   */
  private static void mergeZeroGroup(final NumericGroupAggTable src, final NumericGroupAggTable into,
      final int slotWidth, final int partition) {
    if (partition != 0 || !src.hasZeroKey) {
      return;
    }
    final boolean fresh = !into.hasZeroKey;
    final long[] dst = into.acquireZero(src.zeroSlot[1]);
    if (fresh && src.withAux) {
      into.zeroAux = src.zeroAux;
    }
    mergeBlock(dst, 0, src.zeroSlot, 0, slotWidth, into.sumExactMask);
  }

  /**
   * Blocks of different widths would fold lane-misaligned, and an aux-less destination has no lane to
   * carry a source reference INTO — its neighbour's key lane sits there instead. A source that folded
   * a lane the destination does not (or the reverse) would merge a real sum into an unread lane, or
   * an unfolded zero into a real one — both silent.
   */
  private static void requireMergeable(final NumericGroupAggTable src, final NumericGroupAggTable into) {
    if (src.slotWidth != into.slotWidth || src.withAux != into.withAux || src.sumExactMask != into.sumExactMask
        || src.idWidth != into.idWidth) {
      throw new IllegalStateException(
          "incompatible group tables: slotWidth " + src.slotWidth + "/" + into.slotWidth + ", aux " + src.withAux + "/"
              + into.withAux + ", sumExactMask " + src.sumExactMask + "/" + into.sumExactMask + ", idWidth "
              + src.idWidth + "/" + into.idWidth);
    }
  }

  private static void mergeBlock(final long[] dst, final int dstBase, final long[] src, final int srcBase,
      final int slotWidth, final long sumExactMask) {
    dst[dstBase] += src[srcBase];
    if (src[srcBase + 1] < dst[dstBase + 1]) {
      dst[dstBase + 1] = src[srcBase + 1];
    }
    for (int off = 2, a = 0; off < slotWidth; off += 4, a++) {
      dst[dstBase + off] += src[srcBase + off];
      // An unread lane holds 0 on both sides (the kernels skip folding it), so the plain add is a
      // no-op that keeps this loop branch-shaped identically whichever columns the query reads.
      dst[dstBase + off + 1] = sumsExact(sumExactMask, a)
          ? Math.addExact(dst[dstBase + off + 1], src[srcBase + off + 1])
          : dst[dstBase + off + 1] + src[srcBase + off + 1];
      if (src[srcBase + off + 2] < dst[dstBase + off + 2]) {
        dst[dstBase + off + 2] = src[srcBase + off + 2];
      }
      if (src[srcBase + off + 3] > dst[dstBase + off + 3]) {
        dst[dstBase + off + 3] = src[srcBase + off + 3];
      }
    }
  }
}
