package io.sirix.index.projection;

import it.unimi.dsi.fastutil.HashCommon;
import org.jspecify.annotations.Nullable;

import java.lang.ref.Reference;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

import static java.util.Objects.requireNonNull;

/**
 * Open-addressed hash table for per-group aggregates keyed by a {@code long} value or an exact
 * tuple identity. Each storage stripe contains the key and its accumulator. By default, stripes
 * occupy hash buckets; {@link #useDenseIndex()} instead stores them sequentially behind a compact
 * hash index, so collisions probe tags and handles before loading a complete stripe.
 *
 * <p>
 * Each record owns one contiguous stripe within one fixed-size storage chunk:
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
 * The ordinary accumulator layout is byte-for-byte
 * {@link ProjectionIndexByteScan#newGroupAggAcc}'s, unchanged by the interleave, which is why
 * {@link #acquire} hands back an encoded accumulator handle. Callers resolve it once through
 * {@link #storageAtAccBase} and {@link #offsetAtAccBase}; the resulting buffer and offset have the
 * same block layout as a standalone accumulator (the missing-key and constant-group accumulators
 * keep that standalone shape). From a handle, the key is one load BELOW the resolved offset
 * ({@link #keyAtAccBase}) and the aux lane one {@code slotWidth} ABOVE it ({@link #auxAtAccBase}).
 * {@link #sumsOnly(int, int, boolean, long, int)} omits operand extrema and uses present-count/sum
 * pairs; callers must use its matching fold and expand selected results with
 * {@link #expandSumsAccumulator(long[], int, int)}.
 *
 * <h2>Chunked native storage</h2>
 *
 * The logical open-addressed table is physically split into chunks of at most 128 KiB of lanes,
 * taken lazily, on the first insertion into their bucket range, from the {@link GroupWorkspace} of
 * the grouped scan the table belongs to ({@link #attachWorkspace}). A low-cardinality group-by can
 * therefore retain a high-cardinality logical sizing hint (and never rehash if it turns out dense)
 * without clearing or later scanning the untouched ranges. Hashing, probing, load factor and rehash
 * order do not depend on the split: a probe walks the same logical bucket sequence, and a stripe
 * never crosses a chunk boundary. High-cardinality scans retain their full up-front capacity (and
 * therefore avoid incremental rehashing). Dense indexing retains the same chunk ceiling and load
 * factor, but takes payload chunks in insertion order. Its index stores a 32-bit hash tag and a
 * record ordinal; full keys and identity lanes still prove equality. Growing that index preserves
 * record handles, while the chunk of a small payload can still move. Callers must always re-resolve
 * the chunk and offset after an acquisition that can grow the table.
 *
 * <p>
 * A chunk is allocator memory, addressed lane by lane through {@link GroupLanes}; the handles and
 * offsets this class hands out are lane numbers, as they were when the chunks were {@code long[]}s.
 * A rehash hands its old chunks back to the workspace and {@link #release} hands back every chunk
 * of a table that is done, so a grouped scan whose worker tables flush and whose partition tables
 * grow many times per pass builds each new table from the chunks the last one returned. The
 * workspace gives all of it back to the allocator when the scan ends.
 *
 * <h2>How the table reads its own memory</h2>
 *
 * Callers reach a group's lanes through checked access ({@link GroupLanes}) or through a record the
 * owning chunk resolved for them ({@link GroupBuffer#accumulator}). The table's own probes do
 * neither: a probe reads the key lane of whichever bucket the hash names, and a check per read at a
 * data-dependent address is the cost the conversion to allocator memory must not add. The probes
 * therefore read at addresses the table derives from its own geometry, kept in an address spine
 * beside the chunk spine ({@link #storageAddress}, {@link #probeAddress}; {@code 0} marks a chunk
 * that was never taken). Those addresses are in bounds by construction:
 * <ul>
 * <li>the bucket count and the buckets per chunk are powers of two, a probe wraps with the bucket
 * mask at every step, and a chunk is always taken with {@code bucketsPerChunk * stride} lanes, so a
 * bucket's complete stripe (key, accumulator, aux, identity) lies inside its chunk;
 * <li>a dense index holds only record ordinals this table wrote from its own {@link #size}, never a
 * handle read from persisted or foreign bytes, and an ordinal below {@code size} names a stripe
 * that was written;
 * <li>an address enters a spine only together with the chunk it belongs to, growth publishes the
 * new spines before it hands the old chunks back, and only the table's one owner probes or changes
 * it.
 * </ul>
 * A table that can no longer be probed (released, or after a growth that ran out of memory) has its
 * address spines cleared: every probe then misses and reaches the insertion path, which is where
 * the table's state is examined, so the row that finds its group pays for none of this. Debug mode
 * ({@link GroupKernelLanes#DEBUG}) checks every such address against its chunk's bounded segment
 * and generation before reading it.
 *
 * <h2>Code shape</h2>
 *
 * What a row costs here is decided less by what the methods do than by whether the lookup of an
 * existing group ends up compiled into the row loop, and HotSpot's rules for that are about method
 * size, twice over.
 * <ul>
 * <li>A method that inserts reaches chunk allocation and growth. Once that has been compiled into
 * it, the method is too large to be inlined into any caller, however hot the call: the row loop of
 * a high-cardinality scan, where insertion is frequent while the loop is being compiled, would call
 * out for every row from then on. So {@link #acquire} and {@link #acquireExact} are a leaf probe
 * ({@link #probe}, {@link #probeExact}) and one call for everything else
 * ({@link #insertInterleaved}, {@link #acquireIndexed}), and those two are single methods above the
 * size (325 bytecodes) at which a compiler stops inlining even a hot callee. They are never part of
 * {@code acquire}'s code, whose size therefore does not depend on what the scan did before the loop
 * was compiled.
 * <li>A call inside a loop is inlined only where it had been executed often by then. A probe's
 * second step is not: a table that is still filling has hardly any collisions. The probes therefore
 * contain no calls; their reads are written out.
 * </ul>
 * {@code GroupAccessorSizeTest} holds both sizes, so that a refactoring which undoes either fails
 * there instead of in a benchmark.
 *
 * <p>
 * This replaces {@code Long2ObjectOpenHashMap<long[]>} in the high-cardinality group-by kernel: no
 * boxed accumulator per group and no per-group allocation. The real group value {@code 0} cannot
 * live in a bucket (its key lane would read as empty), so it takes a dedicated side slot.
 *
 * <p>
 * A worker owned by {@link GroupTableSpill} may opt into {@link #allowPartialGroups()}. Its
 * accumulators then remain exact partial sums, but several records can describe the same group.
 * Such a table is consumed through its storage records and must merge into an ordinary exact table
 * before selecting or emitting groups. Its partial cache is never a complete enumeration of groups.
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

  /** 128 KiB of lanes per storage chunk. */
  static final int MAX_STORAGE_CHUNK_LANES = 1 << 14;

  /** Spine of a {@link #release released} table: no chunk, so any probe fails loudly. */
  private static final GroupBuffer[] RELEASED_STORAGE = new GroupBuffer[0];

  private static final GroupBuffer[] NO_PROBE_CACHE = new GroupBuffer[0];

  private static final long[] NO_ADDRESSES = new long[0];

  /**
   * The partition index's entry for a partition holding no key (see {@link #buildPartitionIndex}).
   */
  private static final int[] NO_HANDLES = new int[0];

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
  private final boolean sumsOnly;
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
   * table relies on already tolerates that: {@link #rehash} re-homes each stripe into the first EMPTY
   * bucket rather than deduplicating, and {@link #buildPartitionIndex} partitions on the key, so
   * same-key groups always meet inside one merge partition where identity can separate them.
   */
  private final int idWidth;
  /** Lane distance from a stripe's accumulator base to its first identity lane. */
  private final int idOffsetFromAcc;
  private GroupBuffer[] storage;
  /**
   * Address of lane {@code 0} of each {@link #storage} chunk, {@code 0} for a chunk never taken: what
   * the probes read through (see the class comment). All zero once the table cannot be probed.
   */
  private long[] storageAddress;
  /** Null for interleaved buckets; otherwise hash-tag/record-handle entries in bounded chunks. */
  private GroupBuffer[] probeIndex;
  /** {@link #storageAddress}'s twin for {@link #probeIndex}; unused for interleaved buckets. */
  private long[] probeAddress = NO_ADDRESSES;

  /** Dense handles survive index growth; repeated probes still prove every identity lane. */
  private long previousDenseKey;
  private int previousDenseHandle = -1;

  /** Initial ordinary acquisitions used to decide whether a worker benefits from partial grouping. */
  private static final int PARTIAL_PROBE_SAMPLE = 8_192;

  /** Negative: exact probing; positive: sampling; zero: partial cache or append-only acquisition. */
  private int partialProbeBudget = -1;

  /** A small direct cache for partial groups; negative when the sample found almost no reuse. */
  private int partialProbeMask;

  /** Only intermediate worker tables may contain multiple records for the same exact group. */
  private boolean partialGroupsAllowed;
  private static final int PROBE_CHUNK_SHIFT = 14;
  private static final int PROBE_CHUNK_MASK = (1 << PROBE_CHUNK_SHIFT) - 1;
  private static final long PROBE_TAG_MASK = 0xFFFF_FFFF_0000_0000L;
  /**
   * The grouped scan's working memory this table's chunks and blocks come from. Attached before the
   * first insertion ({@link #attachWorkspace}); a table nobody attached one to opens a private one on
   * first need, see {@link #workspace()}.
   */
  private @Nullable GroupWorkspace workspace;
  private boolean privateWorkspace;
  private int bucketsPerChunk;
  private int chunkBucketMask;
  private int chunkBucketShift;
  /**
   * Lanes every storage chunk of the current geometry is taken with:
   * {@code bucketsPerChunk * stride}.
   */
  private int chunkLanes;
  private int mask;
  private int size;
  private int growAt;
  /**
   * Times {@link #rehash} ran — zero for a table hinted at its final count (see {@link #rehashes}).
   */
  private int rehashes;
  private boolean hasZeroKey;
  /**
   * Set the moment {@link #acquireExact} walks past a bucket whose key matches but whose identity
   * does not — i.e. the moment two genuinely different groups are proven to share a probe hash. The
   * table itself keeps them apart regardless; the flag exists for the side structures that are still
   * keyed by the probe hash alone (the per-group COUNT(DISTINCT) sets), which have no way to tell the
   * two apart and must decline rather than merge them.
   */
  private boolean probeKeyCollision;
  /** The zero group's accumulator block; carved from the workspace on first use. */
  private @Nullable GroupBuffer zeroSlot;
  private long zeroAux;
  /** Scratch of {@link #identityOf}; {@code null} until this table is first merged into. */
  private long @Nullable [] mergeIdentity;

  /**
   * Handle {@link #acquire}/{@link #acquireExact} answer for a key outside the table's pass range:
   * {@link #storageAtAccBase}/{@link #offsetAtAccBase} resolve it to a write-only scratch block
   * nobody reads, so a kernel that folds into it unconditionally needs no knowledge of passes at all.
   * Since the block is never read, a kernel may equally skip the fold on this handle instead; the
   * three composite loops do, to drop the (P-1)/P folds a pass would otherwise perform for keys it
   * does not own. Negative, so a kernel's per-dictionary handle cache treats it as unresolved and
   * re-probes (a pass-mode cost only).
   */
  public static final int DISCARD_HANDLE = -1;

  /** {@code 64} = every key belongs (no pass filter); else the partition shift of the pass split. */
  private int passShift = 64;
  private int passLo;
  private int passHi;
  private GroupBuffer discard;
  private GroupBuffer discardZero;

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
   * @param idWidth identity lanes per group; {@code 0} keeps the key lane as the whole identity. A
   *        positive width turns the key lane into a probe hash and makes membership decided by an
   *        EXACT comparison of the identity lanes, which is what a composite group-by needs: its key
   *        hash folds several components into 64 bits, and that fold is invertible enough to be
   *        solved for a collision in closed form rather than searched for.
   */
  public NumericGroupAggTable(final int aggColumns, final int expectedEntries, final boolean withAux,
      final long sumExactMask, final int idWidth) {
    this(aggColumns, expectedEntries, withAux, sumExactMask, idWidth, false);
  }

  /**
   * Count-and-sum layout: each operand stores its present count and exact sum, with no extrema.
   * Callers must use the compact fold and expand winners before reading the ordinary layout.
   */
  public static NumericGroupAggTable sumsOnly(final int aggColumns, final int expectedEntries, final boolean withAux,
      final long sumExactMask, final int idWidth) {
    return new NumericGroupAggTable(aggColumns, expectedEntries, withAux, sumExactMask, idWidth, true);
  }

  private NumericGroupAggTable(final int aggColumns, final int expectedEntries, final boolean withAux,
      final long sumExactMask, final int idWidth, final boolean sumsOnly) {
    if (aggColumns < 0) {
      throw new IllegalArgumentException("aggColumns must be >= 0");
    }
    if (idWidth < 0) {
      throw new IllegalArgumentException("idWidth must be >= 0");
    }
    this.aggColumns = aggColumns;
    this.sumExactMask = sumExactMask;
    this.sumsOnly = sumsOnly;
    this.slotWidth = 2 + (sumsOnly
        ? 2
        : 4) * aggColumns;
    this.withAux = withAux;
    this.idWidth = idWidth;
    this.idOffsetFromAcc = slotWidth + (withAux
        ? 1
        : 0);
    this.stride = strideFor(aggColumns, withAux, idWidth, sumsOnly);
    if (stride > MAX_STORAGE_CHUNK_LANES) {
      throw new IllegalArgumentException(
          "aggregate stripe of " + stride + " lanes exceeds the non-humongous chunk ceiling");
    }
    int cap = capacityFor(expectedEntries);
    while ((long) cap * stride > MAX_ARRAY_LENGTH) {
      cap >>>= 1;
    }
    if (cap < 16) {
      throw new IllegalArgumentException("aggColumns too large for one stripe: " + aggColumns);
    }
    installEmptyStorage(cap);
    this.mask = cap - 1;
    this.growAt = cap - (cap >>> 2);
  }

  /**
   * Lanes one group occupies in a table of this shape — {@code 1} key lane, the accumulator block
   * ({@code 2 + 4 * aggColumns}), the optional aux lane and {@code idWidth} identity lanes.
   *
   * <p>
   * Public and static because the SIZING of a grouped pass is decided before any table exists: the
   * per-pass group budget divides its allocator grant by the bytes a group actually costs, and a
   * planner that guessed a width instead of asking would plan passes for a stripe the query does not
   * have. A {@code count(*)} group-by occupies three lanes; two aggregate columns over a composite
   * key occupy thirteen.
   * </p>
   *
   * @param aggColumns aggregate columns per group
   * @param withAux whether a per-entry source-reference lane is carried
   * @param idWidth identity lanes per group ({@code 0} keeps the key lane as the whole identity)
   * @return lanes per group stripe
   * @throws IllegalArgumentException if any argument is negative
   */
  public static int strideFor(final int aggColumns, final boolean withAux, final int idWidth) {
    return strideFor(aggColumns, withAux, idWidth, false);
  }

  /** Storage lanes per group, including the optional count-and-sum layout. */
  public static int strideFor(final int aggColumns, final boolean withAux, final int idWidth, final boolean sumsOnly) {
    if (aggColumns < 0) {
      throw new IllegalArgumentException("aggColumns must be >= 0: " + aggColumns);
    }
    if (idWidth < 0) {
      throw new IllegalArgumentException("idWidth must be >= 0: " + idWidth);
    }
    final long lanes = 3L + (sumsOnly
        ? 2L
        : 4L) * aggColumns + (withAux
            ? 1L
            : 0L)
        + idWidth;
    if (lanes > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("group stripe has too many lanes: " + lanes);
    }
    return (int) lanes;
  }

  /**
   * THE bucket capacity a sizing hint buys: the power of two at or above 4/3 ×
   * {@code expectedEntries} (the table grows at 3/4 load), floored at 16 — before the per-stride
   * array-length clamp. Public so a planner that derives a hint
   * ({@link GroupTableSpill#sharedTableHint}) sees the same boundary the constructor applies: a hint
   * one past 3/4 × 2^k costs a table twice the size of one at it, and with hashed placement every
   * chunk of that capacity is touched, so the doubling is paid in full.
   */
  public static int capacityFor(final int expectedEntries) {
    final int cap =
        (int) (Long.highestOneBit(Math.max(16, Math.min(MAX_CAPACITY, (long) expectedEntries * 4 / 3)) - 1) << 1);
    return cap < 16
        ? 16
        : cap;
  }

  /**
   * Seed an all-zero block as an empty accumulator: first-seen ordinal and minima at the top of the
   * range, maxima at the bottom.
   */
  private static void seedAcc(final GroupBuffer acc, final int slotWidth, final boolean sumsOnly) {
    GroupLanes.set(acc, 1, Long.MAX_VALUE);
    for (int base = 2; !sumsOnly && base < slotWidth; base += 4) {
      GroupLanes.set(acc, base + 2, Long.MAX_VALUE);
      GroupLanes.set(acc, base + 3, Long.MIN_VALUE);
    }
  }

  /** Whether operand blocks contain only a present count and a sum. */
  public boolean sumsOnly() {
    return sumsOnly;
  }

  /**
   * Expand one selected compact accumulator to the ordinary result-emission layout. Heap to heap: a
   * selected winner has been copied out of its table by the time it is emitted.
   */
  public static long[] expandSumsAccumulator(final long[] source, final int base, final int aggColumns) {
    final long expandedWidth = 2L + 4L * aggColumns;
    if (aggColumns < 0 || base < 0 || (long) base + 2L + 2L * aggColumns > source.length
        || expandedWidth > MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException("compact accumulator outside source bounds");
    }
    final long[] expanded = new long[(int) expandedWidth];
    for (int a = 0; a < aggColumns; a++) {
      expanded[2 + 4 * a + 2] = Long.MAX_VALUE;
      expanded[2 + 4 * a + 3] = Long.MIN_VALUE;
    }
    expanded[0] = source[base];
    expanded[1] = source[base + 1];
    for (int a = 0; a < aggColumns; a++) {
      expanded[2 + 4 * a] = source[base + 2 + 2 * a];
      expanded[3 + 4 * a] = source[base + 3 + 2 * a];
    }
    return expanded;
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
    final GroupBuffer chunk = storageAtAccBase(accBase);
    return GroupLanes.get(chunk, offsetAtAccBase(accBase) + idOffsetFromAcc + lane);
  }

  /** Number of storage chunks the table's bucket range spans. */
  public int storageChunkCount() {
    return storage.length;
  }

  /** Lanes per storage chunk at the moment. */
  public int chunkLanes() {
    return bucketsPerChunk * stride;
  }

  /**
   * Lanes per storage chunk of any table with {@code stride} lanes per group once its capacity has
   * reached the chunk ceiling: the length nearly every chunk a large grouped scan takes and gives
   * back has.
   */
  public static int fullChunkLanes(final int stride) {
    if (stride <= 0 || stride > MAX_STORAGE_CHUNK_LANES) {
      throw new IllegalArgumentException("stride out of range: " + stride);
    }
    return Integer.highestOneBit(MAX_STORAGE_CHUNK_LANES / stride) * stride;
  }

  /** Tables that opened a workspace of their own, process-wide: none in a query. */
  private static final LongAdder PRIVATE_WORKSPACES = new LongAdder();

  /**
   * Take this table's chunks and blocks from {@code workspace}: the working memory of the grouped
   * scan the table belongs to, which returns all of it when the scan ends. Only before the first
   * insertion.
   *
   * @return this table
   * @throws IllegalStateException if the table already holds a group or already has a workspace
   */
  public NumericGroupAggTable attachWorkspace(final GroupWorkspace workspace) {
    requireNonNull(workspace, "workspace");
    if (this.workspace == workspace) {
      return this;
    }
    if (size != 0 || hasZeroKey || this.workspace != null) {
      throw new IllegalStateException("workspace attached after the table took memory");
    }
    this.workspace = workspace;
    return this;
  }

  /**
   * The workspace this table takes its memory from. A table without one opens a PRIVATE workspace
   * here: convenient for a test or a tool that builds one table, and wrong for a query, whose tables
   * share their scan's workspace so that one close returns everything. A private workspace is
   * {@linkplain GroupWorkspace#selfReturning() self-returning}: {@link #close()} returns it, and so
   * does dropping the table.
   */
  public GroupWorkspace workspace() {
    GroupWorkspace own = workspace;
    if (own == null) {
      own = GroupWorkspace.selfReturning();
      workspace = own;
      privateWorkspace = true;
      PRIVATE_WORKSPACES.increment();
    }
    return own;
  }

  /** Tables that opened a private workspace, process-wide (test observability). */
  public static long privateWorkspaceCount() {
    return PRIVATE_WORKSPACES.sum();
  }

  /**
   * Returns a private workspace to the allocator; a table in a scan's workspace is merely
   * {@linkplain #release() released}. Nothing of the table may be read afterwards.
   */
  public void close() {
    retire();
    if (privateWorkspace && workspace != null) {
      workspace.close();
    }
  }

  /**
   * Use a compact hash index and append accumulators densely. Each index entry holds a 32-bit hash
   * tag and a record handle; the complete key and identity still prove equality. Packed stripes plus
   * one index lane per bucket hold {@code stride + 4/3} lanes per live group at the growth threshold,
   * against the interleaved layout's {@code (4/3) * stride}, so this saves lanes only for stripes
   * wider than four — see {@link GroupTableSpill#DENSE_INDEX_MIN_STRIDE} for the gate that applies
   * it. Narrower stripes remain correct here and are used by tests. Must be enabled before the first
   * insertion.
   */
  public NumericGroupAggTable useDenseIndex() {
    if (size != 0 || hasZeroKey) {
      throw new IllegalStateException("dense index enabled after table use");
    }
    if (stride < 3) {
      throw new IllegalArgumentException("dense indexing needs a stripe of at least three lanes");
    }
    probeIndex = new GroupBuffer[(int) (((long) mask + 1L + PROBE_CHUNK_MASK) >>> PROBE_CHUNK_SHIFT)];
    probeAddress = new long[probeIndex.length];
    return this;
  }

  /**
   * Allow incomplete local deduplication, before the first insertion into a dense worker table. After
   * a sample with at least one distinct group per two acquisitions, limit each lookup to one bucket
   * in a 32-KiB direct cache. With at least seven distinct groups per eight acquisitions, append
   * without probing. A cache miss appends an accumulator and replaces only the index entry: the
   * displaced record remains in storage. The partition merge MUST consume every storage record into
   * an exact table before selection or emission. No group or aggregate contribution is lost.
   * Low-cardinality samples retain ordinary probing. This mode is not suitable for distinct sinks or
   * consumers requiring one local record per group.
   */
  NumericGroupAggTable allowPartialGroups() {
    if (probeIndex == null || size != 0 || hasZeroKey || released()) {
      throw new IllegalStateException("partial grouping requires an unused dense worker table");
    }
    partialGroupsAllowed = true;
    partialProbeBudget = PARTIAL_PROBE_SAMPLE;
    return this;
  }

  /** Whether the sample selected bounded probing (test observability). */
  boolean usesPartialGroups() {
    return partialProbeBudget == 0;
  }

  private GroupBuffer newProbeChunk(final int capacity) {
    return workspace().takeChunk(Math.min(capacity, 1 << PROBE_CHUNK_SHIFT));
  }

  private void recycleChunks(final GroupBuffer[] index) {
    for (int i = 0; i < index.length; i++) {
      final GroupBuffer chunk = index[i];
      if (chunk != null) {
        index[i] = null;
        workspace().giveChunk(chunk);
      }
    }
  }

  /**
   * Lane {@code lane} of chunk {@code chunk} of a spine, {@code 0} when that chunk was never taken: a
   * probe's read, for the paths that are not a row's (growth, the re-probe after it, a merge's
   * bookkeeping). {@code spine} and {@code chunks} are the two spines of the same storage; the
   * address is in bounds for the reasons the class comment gives.
   *
   * <p>
   * The row paths ({@link #probe}, {@link #probeExact}, {@link #acquireAccumulator},
   * {@link #acquireIndexed}) spell this read out instead of calling it. A compiler inlines a call
   * inside a probe loop only where the loop had already been taken often when it was compiled, and a
   * table that is still filling has hardly any collisions: the second probe would stay a call for the
   * life of the compiled row loop.
   */
  private static long laneOrZero(final long[] spine, final GroupBuffer[] chunks, final int chunk, final int lane) {
    final long base = spine[chunk];
    if (GroupKernelLanes.DEBUG) {
      debugProbe(chunks, chunk, lane, base);
    }
    return base == 0L
        ? 0L
        : (long) GroupKernelLanes.LANE.get(GroupKernelLanes.ADDRESS_SPACE, base + ((long) lane << 3));
  }

  /**
   * Debug mode's check ahead of every probe read: the address about to be read is its chunk's, and
   * that chunk, live and of its generation, holds the lane.
   */
  private static void debugProbe(final GroupBuffer[] chunks, final int chunk, final int lane, final long base) {
    final GroupBuffer bounded = chunks[chunk];
    if (bounded == null
        ? base != 0L
        : bounded.address != base) {
      throw new IllegalStateException("probe address does not belong to chunk " + chunk);
    }
    if (bounded != null) {
      bounded.checkRange(lane, 1);
    }
  }

  private long probeEntry(final int bucket) {
    return laneOrZero(probeAddress, probeIndex, bucket >>> PROBE_CHUNK_SHIFT, bucket & PROBE_CHUNK_MASK);
  }

  /** Clear the address spines: every probe now misses and reaches {@link #requireLiveTable}. */
  private void stopProbing() {
    previousDenseHandle = -1;
    Arrays.fill(storageAddress, 0L);
    Arrays.fill(probeAddress, 0L);
  }

  /**
   * Hand every storage and index chunk back to the table's workspace and leave the table without
   * storage. The counters ({@link #size}, {@link #hasZeroKey}, the zero group's slot) survive;
   * probing or walking the table afterwards is a contract violation and fails on the empty spine. A
   * caller that still points INTO the storage — a top-k selector holding accumulator references —
   * must copy what it keeps before this call.
   */
  public void release() {
    stopProbing();
    final GroupBuffer[] chunks = storage;
    for (int i = 0; i < chunks.length; i++) {
      final GroupBuffer chunk = chunks[i];
      if (chunk != null) {
        chunks[i] = null;
        recycle(chunk);
      }
    }
    storage = RELEASED_STORAGE;
    if (probeIndex != null) {
      recycleChunks(probeIndex);
      probeIndex = RELEASED_STORAGE;
    }
    if (discard != null) {
      recycle(discard);
      discard = null;
    }
    if (discardZero != null) {
      recycle(discardZero);
      discardZero = null;
    }
  }

  public void retire() {
    release();
    final GroupBuffer zero = zeroSlot;
    zeroSlot = null;
    if (zero != null) {
      recycle(zero);
    }
  }

  /** Whether {@link #release} has run. */
  public boolean released() {
    return storage == RELEASED_STORAGE;
  }

  private GroupBuffer newChunk(final int lanes) {
    return workspace().takeRecordChunk(lanes, stride, slotWidth);
  }

  private void recycle(final GroupBuffer chunk) {
    workspace().giveChunk(chunk);
  }

  /**
   * One storage chunk for a sequential table walk, or {@code null} when its logical bucket range was
   * never touched. Every live stripe is wholly contained in one non-null chunk and starts at an
   * offset divisible by {@link #stride()}.
   */
  public @Nullable GroupBuffer storageChunkOrNull(final int chunk) {
    return storage[chunk];
  }

  /**
   * Storage chunk that owns {@code accBase}. A growing {@link #acquire} moves stripes, so resolve the
   * chunk and {@link #offsetAtAccBase} together, after the acquisition. The lanes are then read and
   * written through {@link GroupLanes}, which checks each access against the chunk, or folded through
   * the record {@link GroupBuffer#accumulator} validates. Debug mode narrows the answer to the
   * handle's own stripe, so a checked access that strays into a neighbouring group fails as well.
   *
   * @throws IndexOutOfBoundsException if {@code accBase} is not a handle of this table as it is now
   * @throws IllegalStateException if the handle names storage that was never taken
   */
  public GroupBuffer storageAtAccBase(final int accBase) {
    if (accBase < 0 || accBase >= handleLimit()) {
      return offTable(accBase);
    }
    final GroupBuffer chunk = storage[accBase >>> chunkBucketShift];
    if (chunk == null) {
      throw new IllegalStateException("aggregation handle refers to unallocated storage: " + accBase);
    }
    return GroupKernelLanes.DEBUG
        ? chunk.view((accBase & chunkBucketMask) * stride, stride)
        : chunk;
  }

  /**
   * Resolve the accumulator of {@code accBase} for a fold of {@code columns} aggregate columns of
   * {@code lanesPerColumn} lanes each: {@link #storageAtAccBase}, {@link #offsetAtAccBase} and
   * {@link GroupBuffer#accumulator} in one step, for the kernels that fold a row straight after
   * acquiring its group. The table validates the handle and the fold against its own geometry, which
   * is what its chunks were taken with, so nothing of the chunk has to be read first: the handle is
   * one this table can have handed out, its chunk was taken, the complete stripe lies inside a chunk
   * of the current lane count, and the fold fits the accumulator the layout declares. Like any
   * resolution it must follow the acquisition, and it is valid until the table next grows.
   *
   * @return the record {@link GroupKernelLanes} folds into, lane {@code 0} being the group's count
   * @throws IllegalArgumentException if the column count cannot describe an accumulator
   * @throws IndexOutOfBoundsException if the handle is not this table's or the fold does not fit
   * @throws IllegalStateException if the table is released or the handle names untaken storage
   */
  long accumulatorAt(final int accBase, final int columns, final int lanesPerColumn) {
    if (lanesPerColumn <= 0 || columns < 0 || columns > (Integer.MAX_VALUE - 2) / lanesPerColumn) {
      throw new IllegalArgumentException("invalid aggregation column count: " + columns);
    }
    if (2 + columns * lanesPerColumn > slotWidth) {
      throw new IndexOutOfBoundsException("fold does not fit the aggregation record layout");
    }
    if (GroupKernelLanes.DEBUG || accBase < 0 || accBase >= handleLimit()) {
      return storageAtAccBase(accBase).accumulator(offsetAtAccBase(accBase), columns, lanesPerColumn);
    }
    final long chunk = storageAddress[accBase >>> chunkBucketShift];
    final int stripe = (accBase & chunkBucketMask) * stride;
    if (chunk == 0L || stripe > chunkLanes - stride) {
      // Untaken storage, a table that can no longer be probed, or a geometry that does not hold the
      // stripe: let the checked resolution word the failure.
      return storageAtAccBase(accBase).accumulator(offsetAtAccBase(accBase), columns, lanesPerColumn);
    }
    return chunk + ((long) (stripe + 1) << 3);
  }

  /** One past the largest handle the table can have handed out. */
  private int handleLimit() {
    return probeIndex == null
        ? mask + 1
        : size;
  }

  /** The discard block for {@link #DISCARD_HANDLE} of a table with a pass range; else a failure. */
  private GroupBuffer offTable(final int accBase) {
    if (accBase == DISCARD_HANDLE && discard != null && !released()) {
      return discard;
    }
    throw new IndexOutOfBoundsException("invalid aggregation handle: " + accBase);
  }

  /** Chunk-local accumulator offset encoded in {@code accBase}. */
  public int offsetAtAccBase(final int accBase) {
    if (accBase < 0) {
      if (accBase == DISCARD_HANDLE && discard != null) {
        return 1;
      }
      throw new IndexOutOfBoundsException("invalid aggregation handle: " + accBase);
    }
    return (accBase & chunkBucketMask) * stride + 1;
  }

  /** Bucket count (a power of two). */
  public int capacity() {
    return mask + 1;
  }

  /** Stored non-zero records; partial worker tables can hold several records for one group. */
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
    requireCompleteIndex();
    return probeKeyCollision;
  }

  /**
   * Accumulator handle of the ONLY group carrying {@code probeHash}, for side structures that are
   * keyed by the probe hash. Callers must have established {@link #hasProbeKeyCollision()} is
   * {@code false}, which is exactly what makes the key unambiguous.
   *
   * @param probeHash the group's probe hash as the kernel computed it ({@code 0} remapped internally)
   * @return the encoded accumulator handle
   * @throws IllegalStateException if the table holds no group under that key, or if a collision makes
   *         the key ambiguous
   */
  public int handleOfProbeKey(final long probeHash) {
    requireCompleteIndex();
    if (probeKeyCollision) {
      throw new IllegalStateException("probe key is ambiguous: two groups share it");
    }
    final long key = probeHash == 0L
        ? ZERO_PROBE_SUBSTITUTE
        : probeHash;
    if (probeIndex != null) {
      final long hash = HashCommon.mix(key);
      int bucket = (int) hash & mask;
      while (true) {
        final long entry = probeEntry(bucket);
        if (entry == 0L) {
          throw new IllegalStateException("no group carries probe key " + probeHash);
        }
        final int handle = (int) entry - 1;
        if (((entry ^ hash) & PROBE_TAG_MASK) == 0L && keyAtAccBase(handle) == key) {
          return handle;
        }
        bucket = bucket + 1 & mask;
      }
    }
    final int start = (int) HashCommon.mix(key) & mask;
    int bucket = start;
    do {
      final GroupBuffer chunk = storage[bucket >>> chunkBucketShift];
      final int off = (bucket & chunkBucketMask) * stride;
      final long cur = chunk == null
          ? 0L
          : GroupLanes.get(chunk, off);
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
  public GroupBuffer zeroSlot() {
    return zeroBlock();
  }

  private GroupBuffer zeroBlock() {
    GroupBuffer block = zeroSlot;
    if (block == null) {
      requireLiveTable();
      block = workspace().takeAccumulatorChunk(slotWidth, slotWidth);
      seedAcc(block, slotWidth, sumsOnly);
      zeroSlot = block;
    }
    return block;
  }

  /** Stored records including the zero key; partial worker counts are upper bounds on groups. */
  public int sizeIncludingZero() {
    return size + (hasZeroKey
        ? 1
        : 0);
  }

  /**
   * Key of bucket {@code bucket}; {@code 0} = empty (the real key 0 lives in {@link #zeroSlot()}).
   */
  public long keyAtBucket(final int bucket) {
    requireBucket(bucket);
    requireCompleteIndex();
    if (probeIndex != null) {
      final long entry = probeEntry(bucket);
      return entry == 0L
          ? 0L
          : keyAtAccBase((int) entry - 1);
    }
    final int chunk = bucket >>> chunkBucketShift;
    final int off = (bucket & chunkBucketMask) * stride;
    final GroupBuffer block = storage[chunk];
    return block == null
        ? 0L
        : GroupLanes.get(block, off);
  }

  /** Encoded accumulator handle of {@code bucket} — valid only when its key lane is non-zero. */
  public int accBaseOfBucket(final int bucket) {
    requireBucket(bucket);
    requireCompleteIndex();
    return probeIndex == null
        ? bucket
        : (int) probeEntry(bucket) - 1;
  }

  /**
   * The key owning the accumulator block at {@code accBase} — ONE load, and it lets a caller VALIDATE
   * a cached block base across rehashes: a base is current iff its bucket still holds the same key
   * (keys are unique, so a match can never be a different group).
   */
  public long keyAtAccBase(final int accBase) {
    requireProbeable();
    if (accBase < 0 || accBase >= handleLimit()) {
      if (accBase == DISCARD_HANDLE && discard != null) {
        return 0L;
      }
      throw new IndexOutOfBoundsException("invalid aggregation handle: " + accBase);
    }
    // This is the validation of a memoized handle, before resolution. Rehash may leave the previous
    // bucket in a chunk that was never taken, and a table that can no longer be probed reads empty:
    // either reports a miss, so the caller acquires again.
    return laneOrZero(storageAddress, storage, accBase >>> chunkBucketShift, (accBase & chunkBucketMask) * stride);
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
    final GroupBuffer chunk = storageAtAccBase(accBase);
    return GroupLanes.get(chunk, offsetAtAccBase(accBase) + slotWidth);
  }

  /**
   * Stamp the source reference of the entry whose accumulator starts at {@code accBase} (aux lane
   * only). A never-occupied stripe reads {@code 0}, exactly like the separate lane it replaces.
   */
  public void setAuxAtAccBase(final int accBase, final long value) {
    final GroupBuffer chunk = storageAtAccBase(accBase);
    GroupLanes.set(chunk, offsetAtAccBase(accBase) + slotWidth, value);
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
   * Restrict this table to the groups whose partition ({@link #partitionOf} under {@code shift}) lies
   * in {@code [lo, hi)}: every other key acquires {@link #DISCARD_HANDLE}. A hash-range pass of a
   * group-by scans the whole input P times and keeps 1/P of the groups per pass, so memory is bounded
   * at any cardinality; the partitioning is the post-scan merge's own, so a partition's groups
   * complete within one pass.
   */
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
  public void setPassRange(final int shift, final int lo, final int hi) {
    requireLiveTable();
    if (shift < 0 || shift > 64) {
      throw new IllegalArgumentException("shift out of range: " + shift);
    }
    if (lo < 0 || hi <= lo) {
      throw new IllegalArgumentException("empty pass range [" + lo + ", " + hi + ")");
    }
    if (discard == null) {
      final GroupBuffer nextDiscard = workspace().takeRecordChunk(stride, stride, slotWidth);
      final GroupBuffer nextDiscardZero;
      try {
        nextDiscardZero = workspace().takeAccumulatorChunk(slotWidth, slotWidth);
      } catch (final RuntimeException | Error failure) {
        recycle(nextDiscard);
        throw failure;
      }
      discard = nextDiscard;
      discardZero = nextDiscardZero;
    }
    this.passShift = shift;
    this.passLo = lo;
    this.passHi = hi;
  }

  /** Whether {@code key} lies outside the pass range (never, without a pass filter). */
  private boolean outOfPass(final long key) {
    if (passShift == 64) {
      return false;
    }
    final int p = (int) (HashCommon.mix(key) >>> passShift);
    return p < passLo || p >= passHi;
  }

  /**
   * The one liveness test on the path of a row that finds its group: the workspace this table's
   * memory lives in is still open. Everything else that stops a table from being probed is found on
   * the insertion path (see the class comment), but a probe of memory that already went back to the
   * allocator could read an unmapped page, and no later check can undo that. Debug mode examines the
   * whole state before every probe.
   */
  private void requireProbeable() {
    if (GroupKernelLanes.DEBUG) {
      requireLiveTable();
      return;
    }
    final GroupWorkspace own = workspace;
    if (own != null && own.ended) {
      throw new IllegalStateException("aggregation table is released");
    }
  }

  /**
   * Encoded handle of {@code key}'s accumulator block, inserting a fresh block
   * ({@code count 0, firstSeen ordinal, empty aggregates}) on first sight. {@code key} MUST be
   * non-zero — route the zero group through {@link #acquireZero}.
   *
   * <p>
   * The returned handle is invalidated by any later {@code acquire} that grows the table. Resolve
   * both its storage chunk and its chunk-local offset AFTER the call.
   */
  public int acquire(final long key, final long firstSeenOrdinal) {
    if (probeIndex != null) {
      return acquireIndexed(key, firstSeenOrdinal, null, 0);
    }
    final int found = probe(key);
    return found >= DISCARD_HANDLE
        ? found
        : insertInterleaved(key, firstSeenOrdinal, -2 - found, null, 0);
  }

  /**
   * {@link #acquire(long, long)} and {@link #accumulatorAt} in one step: the record of {@code key}'s
   * accumulator, for a row loop that folds every row into its group and needs nothing else of the
   * handle. A row that finds its group then pays for one probe and nothing more: the probe already
   * holds the stripe's address and has read its key lane, so the handle is never formed and the
   * stripe is not located a second time. The fold is validated against the layout exactly as
   * {@link #accumulatorAt} validates it, and the stripe against its chunk's lane count.
   *
   * @return the record {@link GroupKernelLanes} folds into, lane {@code 0} being the group's count;
   *         for a key outside the pass range, the record of the table's discard block
   * @throws IllegalArgumentException if the column count cannot describe an accumulator
   * @throws IndexOutOfBoundsException if the fold does not fit the layout
   * @throws IllegalStateException if the table is released
   */
  // Sonar S135: the exits of this hot loop are its compiled shape. Funnelling them through one exit
  // adds a flag test for each row.
  @SuppressWarnings("java:S135")
  long acquireAccumulator(final long key, final long firstSeenOrdinal, final int columns, final int lanesPerColumn) {
    if (lanesPerColumn <= 0 || columns < 0 || columns > (Integer.MAX_VALUE - 2) / lanesPerColumn) {
      throw new IllegalArgumentException("invalid aggregation column count: " + columns);
    }
    if (2 + columns * lanesPerColumn > slotWidth) {
      throw new IndexOutOfBoundsException("fold does not fit the aggregation record layout");
    }
    if (GroupKernelLanes.DEBUG || probeIndex != null) {
      return accumulatorAt(acquire(key, firstSeenOrdinal), columns, lanesPerColumn);
    }
    requireProbeable();
    if (outOfPass(key)) {
      return accumulatorAt(DISCARD_HANDLE, columns, lanesPerColumn);
    }
    final long[] spine = storageAddress;
    final int st = stride;
    final int bucketShift = chunkBucketShift;
    final int bucketMask = chunkBucketMask;
    final int buckets = mask;
    final int lastStripe = chunkLanes - st;
    int bucket = (int) HashCommon.mix(key) & buckets;
    while (true) {
      final int lane = (bucket & bucketMask) * st;
      final long base = spine[bucket >>> bucketShift];
      if (base == 0L || lane > lastStripe) {
        break;
      }
      final long cur = (long) GroupKernelLanes.LANE.get(GroupKernelLanes.ADDRESS_SPACE, base + ((long) lane << 3));
      if (cur == key) {
        return base + ((long) (lane + 1) << 3);
      }
      if (cur == 0L) {
        break;
      }
      bucket = bucket + 1 & buckets;
    }
    // An empty bucket (or a geometry that does not hold the stripe: the insertion examines the
    // table). Resolved AFTER the insertion, which may have grown the table.
    return accumulatorAt(insertInterleaved(key, firstSeenOrdinal, bucket, null, 0), columns, lanesPerColumn);
  }

  /**
   * The lookup of {@link #acquire} in a table without a dense index, and all of it a row that finds
   * its group executes: {@code key}'s handle, {@link #DISCARD_HANDLE} for a key outside the pass
   * range, or {@code -2 - bucket} for the empty bucket the probe stopped at. A leaf (see "Code shape"
   * in the class comment), with every read spelled out.
   */
  private int probe(final long key) {
    requireProbeable();
    if (outOfPass(key)) {
      return DISCARD_HANDLE;
    }
    final long[] spine = storageAddress;
    final GroupBuffer[] chunks = storage;
    final int st = stride;
    final int bucketShift = chunkBucketShift;
    final int bucketMask = chunkBucketMask;
    final int buckets = mask;
    int bucket = (int) HashCommon.mix(key) & buckets;
    while (true) {
      final int chunk = bucket >>> bucketShift;
      final int lane = (bucket & bucketMask) * st;
      final long base = spine[chunk];
      if (GroupKernelLanes.DEBUG) {
        debugProbe(chunks, chunk, lane, base);
      }
      if (base == 0L) {
        return -2 - bucket;
      }
      final long cur = (long) GroupKernelLanes.LANE.get(GroupKernelLanes.ADDRESS_SPACE, base + ((long) lane << 3));
      if (cur == key) {
        return bucket;
      }
      if (cur == 0L) {
        return -2 - bucket;
      }
      bucket = bucket + 1 & buckets;
    }
  }

  private void requireLiveTable() {
    // A failed growth leaves a table that can only be released: it may have lost stripes.
    if (growAt < 0) {
      throw new OutOfMemoryError("aggregation table growth previously exhausted its capacity");
    }
    if (released() || (workspace != null && workspace.isClosed())) {
      throw new IllegalStateException("aggregation table is released");
    }
  }

  private void requireBucket(final int bucket) {
    requireLiveTable();
    if (bucket < 0 || bucket > mask) {
      throw new IndexOutOfBoundsException("invalid aggregation bucket: " + bucket);
    }
  }

  /**
   * Encoded handle of the group whose EXACT identity is {@code identity[identityOffset ..
   * identityOffset + idWidth())}, probed under {@code probeHash}, inserting a fresh block on first
   * sight.
   *
   * <p>
   * {@code probeHash} only chooses the probe chain: a bucket whose key lane matches but whose
   * identity lanes do not is a collision, and the probe simply walks on, so the two tuples occupy two
   * buckets and never fold into one another. That is the whole difference from
   * {@link #acquire(long, long)}, which treats a key match as proof of group identity.
   *
   * <p>
   * The returned handle is invalidated by any later acquire that grows the table. Resolve both its
   * storage chunk and its chunk-local offset AFTER the call.
   *
   * @param probeHash the group's probe hash; {@code 0} is remapped internally
   * @param firstSeenOrdinal source reference stamped into a freshly created block
   * @param identity the group's exact identity lanes
   * @param identityOffset index of the first identity lane within {@code identity}
   * @return the encoded accumulator handle
   */
  public int acquireExact(final long probeHash, final long firstSeenOrdinal, final long[] identity,
      final int identityOffset) {
    if (identityOffset < 0 || identityOffset > identity.length - idWidth) {
      throw new IndexOutOfBoundsException("identity does not contain the complete group tuple");
    }
    final long key = probeHash == 0L
        ? ZERO_PROBE_SUBSTITUTE
        : probeHash;
    if (probeIndex != null) {
      return acquireIndexed(key, firstSeenOrdinal, identity, identityOffset);
    }
    final int found = probeExact(key, identity, identityOffset);
    return found >= DISCARD_HANDLE
        ? found
        : insertInterleaved(key, firstSeenOrdinal, -2 - found, identity, identityOffset);
  }

  /**
   * {@link #probe} for an identity-mode table: a bucket whose key lane matches is the group only when
   * its identity lanes match too, and is otherwise a collision the probe walks past. The identity
   * lanes of the stripe just probed are read in place, for the reason its key lane is.
   */
  // Sonar S1119: the exits of this hot loop are its compiled shape. Funnelling them through one exit
  // adds a flag test for each row.
  // Sonar S3776: a hot loop kept as one compiled unit. Splitting it changes what the compiler inlines
  // and holds in registers.
  @SuppressWarnings({"java:S1119", "java:S3776"})
  private int probeExact(final long key, final long[] identity, final int identityOffset) {
    requireProbeable();
    if (outOfPass(key)) {
      return DISCARD_HANDLE;
    }
    final long[] spine = storageAddress;
    final GroupBuffer[] chunks = storage;
    final int st = stride;
    final int bucketShift = chunkBucketShift;
    final int bucketMask = chunkBucketMask;
    final int buckets = mask;
    final int idOff = idOffsetFromAcc + 1;
    final int width = idWidth;
    int bucket = (int) HashCommon.mix(key) & buckets;
    probe: while (true) {
      final int chunk = bucket >>> bucketShift;
      final int lane = (bucket & bucketMask) * st;
      final long base = spine[chunk];
      if (GroupKernelLanes.DEBUG) {
        debugProbe(chunks, chunk, lane, base);
      }
      if (base == 0L) {
        return -2 - bucket;
      }
      final long cur = (long) GroupKernelLanes.LANE.get(GroupKernelLanes.ADDRESS_SPACE, base + ((long) lane << 3));
      if (cur == 0L) {
        return -2 - bucket;
      }
      if (cur == key) {
        final long identityAt = base + ((long) (lane + idOff) << 3);
        for (int i = 0; i < width; i++) {
          if (GroupKernelLanes.DEBUG) {
            debugProbe(chunks, chunk, lane + idOff + i, base);
          }
          if ((long) GroupKernelLanes.LANE.get(GroupKernelLanes.ADDRESS_SPACE,
              identityAt + ((long) i << 3)) != identity[identityOffset + i]) {
            probeKeyCollision = true;
            bucket = bucket + 1 & buckets;
            continue probe;
          }
        }
        return bucket;
      }
      bucket = bucket + 1 & buckets;
    }
  }

  /**
   * The complete miss of a table without a dense index: take the bucket's chunk on its first
   * insertion, write the stripe, grow, and find the stripe again where growth moved it. This is also
   * where a table that can no longer be probed is found out, because its probes all miss.
   *
   * <p>
   * One method on purpose, and not to be split into helpers: it must stay larger than what a compiler
   * inlines into a hot caller (see "Code shape" in the class comment).
   *
   * @param bucket the empty bucket {@code key}'s probe stopped at, in the current geometry
   * @param identity the group's identity lanes, whose bounds the caller has validated; {@code null}
   *        when the key lane is the whole identity
   */
  // Sonar S1119: the exits of this hot loop are its compiled shape. Funnelling them through one exit
  // adds a flag test for each row.
  // Sonar S3776: a hot loop kept as one compiled unit. Splitting it changes what the compiler inlines
  // and holds in registers.
  @SuppressWarnings({"java:S1119", "java:S3776"})
  private int insertInterleaved(final long key, final long firstSeenOrdinal, final int bucket,
      final long @Nullable [] identity, final int identityOffset) {
    // A failed growth leaves a table that can only be released: it may have lost stripes.
    if (growAt < 0) {
      throw new OutOfMemoryError("aggregation table growth previously exhausted its capacity");
    }
    if (released() || (workspace != null && workspace.isClosed())) {
      throw new IllegalStateException("aggregation table is released");
    }
    final int st = stride;
    final int chunkIndex = bucket >>> chunkBucketShift;
    GroupBuffer chunk = storage[chunkIndex];
    if (chunk == null) {
      chunk = newChunk(chunkLanes);
      storage[chunkIndex] = chunk;
      storageAddress[chunkIndex] = chunk.address;
    }
    // The complete stripe is validated against its chunk before the first store. The aux lane keeps
    // the zero the chunk was handed out with.
    final long record = chunk.record((bucket & chunkBucketMask) * st, st);
    GroupKernelLanes.set(record, 0, key);
    GroupKernelLanes.set(record, 1, 0L);
    GroupKernelLanes.set(record, 2, firstSeenOrdinal);
    final int end = 1 + slotWidth;
    if (sumsOnly) {
      for (int lane = 3; lane < end; lane++) {
        GroupKernelLanes.set(record, lane, 0L);
      }
    } else {
      for (int lane = 3; lane < end; lane += 4) {
        GroupKernelLanes.set(record, lane, 0L);
        GroupKernelLanes.set(record, lane + 1, 0L);
        GroupKernelLanes.set(record, lane + 2, Long.MAX_VALUE);
        GroupKernelLanes.set(record, lane + 3, Long.MIN_VALUE);
      }
    }
    final int idOff = 1 + idOffsetFromAcc;
    final int width = idWidth;
    if (identity != null) {
      for (int i = 0; i < width; i++) {
        GroupKernelLanes.set(record, idOff + i, identity[identityOffset + i]);
      }
    }
    Reference.reachabilityFence(chunk);
    if (++size <= growAt) {
      return bucket;
    }
    rehash();
    // The stripe moved with its key: probe for it in the grown table, where it is present.
    final long[] spine = storageAddress;
    final GroupBuffer[] chunks = storage;
    final int bucketShift = chunkBucketShift;
    final int bucketMask = chunkBucketMask;
    final int buckets = mask;
    int moved = (int) HashCommon.mix(key) & buckets;
    probe: while (true) {
      final int grownChunk = moved >>> bucketShift;
      final int lane = (moved & bucketMask) * st;
      if (laneOrZero(spine, chunks, grownChunk, lane) == key) {
        if (identity != null) {
          for (int i = 0; i < width; i++) {
            if (laneOrZero(spine, chunks, grownChunk, lane + idOff + i) != identity[identityOffset + i]) {
              moved = moved + 1 & buckets;
              continue probe;
            }
          }
        }
        return moved;
      }
      moved = moved + 1 & buckets;
    }
  }

  /** The storage chunk {@code chunkIndex} of the current geometry, taken on its first insertion. */
  private GroupBuffer chunkForInsert(final int chunkIndex) {
    GroupBuffer chunk = storage[chunkIndex];
    if (chunk == null) {
      chunk = newChunk(chunkLanes);
      storage[chunkIndex] = chunk;
      storageAddress[chunkIndex] = chunk.address;
    }
    return chunk;
  }

  /**
   * Write a fresh stripe at lane {@code off} of {@code chunk}: the key, an empty accumulator and,
   * when given, the identity. The aux lane keeps the zero the chunk was handed out with. The complete
   * stripe is validated against its chunk before the first store.
   */
  private void initStripe(final GroupBuffer chunk, final int off, final long key, final long firstSeenOrdinal,
      final long @Nullable [] identity, final int identityOffset) {
    final long record = chunk.record(off, stride);
    GroupKernelLanes.set(record, 0, key);
    GroupKernelLanes.set(record, 1, 0L);
    GroupKernelLanes.set(record, 2, firstSeenOrdinal);
    final int end = 1 + slotWidth;
    if (sumsOnly) {
      for (int lane = 3; lane < end; lane++) {
        GroupKernelLanes.set(record, lane, 0L);
      }
    } else {
      for (int lane = 3; lane < end; lane += 4) {
        GroupKernelLanes.set(record, lane, 0L);
        GroupKernelLanes.set(record, lane + 1, 0L);
        GroupKernelLanes.set(record, lane + 2, Long.MAX_VALUE);
        GroupKernelLanes.set(record, lane + 3, Long.MIN_VALUE);
      }
    }
    if (identity != null) {
      final int first = 1 + idOffsetFromAcc;
      for (int i = 0; i < idWidth; i++) {
        GroupKernelLanes.set(record, first + i, identity[identityOffset + i]);
      }
    }
    Reference.reachabilityFence(chunk);
  }

  /**
   * The acquisition of a table with a dense index, hit and miss alike: the pass filter, the bounded
   * probing of a partial worker table and its sample, and the tag probe of the complete index. One
   * call per row of such a table, and like {@link #insertInterleaved} one method on purpose (see
   * "Code shape" in the class comment); the reads of the tag probe are spelled out because this IS
   * the row path of a dense table.
   */
  // Sonar S135, S1119: the exits of this hot loop are its compiled shape. Funnelling them through one
  // exit adds a flag test for each row.
  // Sonar S3776: a hot loop kept as one compiled unit. Splitting it changes what the compiler inlines
  // and holds in registers.
  @SuppressWarnings({"java:S135", "java:S1119", "java:S3776"})
  private int acquireIndexed(final long key, final long firstSeenOrdinal, final long @Nullable [] identity,
      final int identityOffset) {
    requireProbeable();
    if (outOfPass(key)) {
      return DISCARD_HANDLE;
    }
    if (partialProbeBudget == 0) {
      return acquirePartial(key, firstSeenOrdinal, identity, identityOffset);
    }
    final long[] spine = storageAddress;
    final GroupBuffer[] chunks = storage;
    final int bucketShift = chunkBucketShift;
    final int bucketMask = chunkBucketMask;
    final int st = stride;
    final int idOff = 1 + idOffsetFromAcc;
    final int width = idWidth;
    // The group of the previous row, when this row repeats it; else the tag probe finds or inserts.
    int handle = previousDenseHandle;
    if (handle < 0 || previousDenseKey != key || (width != 0 && !identityMatches(spine, chunks, handle >>> bucketShift,
        (handle & bucketMask) * st + idOff, identity, identityOffset, width))) {
      final long[] indexSpine = probeAddress;
      final GroupBuffer[] indexChunks = probeIndex;
      final long hash = HashCommon.mix(key);
      final int buckets = mask;
      int bucket = (int) hash & buckets;
      probe: while (true) {
        final int indexChunk = bucket >>> PROBE_CHUNK_SHIFT;
        final int indexLane = bucket & PROBE_CHUNK_MASK;
        final long indexBase = indexSpine[indexChunk];
        if (GroupKernelLanes.DEBUG) {
          debugProbe(indexChunks, indexChunk, indexLane, indexBase);
        }
        final long entry = indexBase == 0L
            ? 0L
            : (long) GroupKernelLanes.LANE.get(GroupKernelLanes.ADDRESS_SPACE, indexBase + ((long) indexLane << 3));
        if (entry == 0L) {
          handle = insertDense(key, firstSeenOrdinal, identity, identityOffset, hash, bucket);
          break;
        }
        if (((entry ^ hash) & PROBE_TAG_MASK) == 0L) {
          // A record ordinal this table wrote: below size, so its chunk was taken and holds its
          // stripe.
          final int candidate = (int) entry - 1;
          final int chunk = candidate >>> bucketShift;
          final int lane = (candidate & bucketMask) * st;
          final long base = spine[chunk];
          if (GroupKernelLanes.DEBUG) {
            debugProbe(chunks, chunk, lane, base);
          }
          if (base != 0L
              && (long) GroupKernelLanes.LANE.get(GroupKernelLanes.ADDRESS_SPACE, base + ((long) lane << 3)) == key) {
            final long identityAt = base + ((long) (lane + idOff) << 3);
            for (int i = 0; i < width; i++) {
              if (GroupKernelLanes.DEBUG) {
                debugProbe(chunks, chunk, lane + idOff + i, base);
              }
              if ((long) GroupKernelLanes.LANE.get(GroupKernelLanes.ADDRESS_SPACE,
                  identityAt + ((long) i << 3)) != identity[identityOffset + i]) {
                probeKeyCollision = true;
                bucket = bucket + 1 & buckets;
                continue probe;
              }
            }
            previousDenseKey = key;
            previousDenseHandle = candidate;
            handle = candidate;
            break;
          }
        }
        bucket = bucket + 1 & buckets;
      }
    }
    if (partialProbeBudget > 0 && --partialProbeBudget == 0) {
      endPartialSample();
    }
    return handle;
  }

  /**
   * The sample of ordinary acquisitions is complete: keep probing exactly when it found few distinct
   * groups, else replace the index by the small direct cache, or by nothing when almost no group was
   * seen twice.
   */
  private void endPartialSample() {
    if (size < PARTIAL_PROBE_SAMPLE / 2) {
      partialProbeBudget = -1;
      return;
    }
    partialProbeMask = size >= PARTIAL_PROBE_SAMPLE * 7 / 8
        ? -1
        : Math.min(mask, 4_095);
    final GroupBuffer[] replacement = partialProbeMask < 0
        ? NO_PROBE_CACHE
        : new GroupBuffer[] {workspace().takeChunk(partialProbeMask + 1)};
    final GroupBuffer[] previousIndex = probeIndex;
    probeIndex = replacement;
    probeAddress = partialProbeMask < 0
        ? NO_ADDRESSES
        : new long[] {replacement[0].address};
    recycleChunks(previousIndex);
  }

  /** A partial worker table past its sample: one bounded cache probe, else append a record. */
  // Sonar S9435: hot path: the branch layout is the measured shape, rewriting it changes the compiled
  // method.
  @SuppressWarnings("java:S9435")
  private int acquirePartial(final long key, final long firstSeenOrdinal, final long @Nullable [] identity,
      final int identityOffset) {
    final long hash;
    final int bucket;
    if (partialProbeMask >= 0) {
      hash = HashCommon.mix(key);
      bucket = (int) hash & partialProbeMask;
      final long entry = probeEntry(bucket);
      if (entry != 0L && ((entry ^ hash) & PROBE_TAG_MASK) == 0L) {
        final int handle = (int) entry - 1;
        final int chunk = handle >>> chunkBucketShift;
        final int offset = (handle & chunkBucketMask) * stride;
        if (laneOrZero(storageAddress, storage, chunk, offset) == key
            && (idWidth == 0 || identityMatches(storageAddress, storage, chunk, offset + 1 + idOffsetFromAcc, identity,
                identityOffset, idWidth))) {
          return handle;
        }
      }
    } else {
      hash = 0L;
      bucket = -1;
    }
    // Appending: a table that can no longer be probed arrives here too (its cache reads empty).
    requireLiveTable();
    final int handle = size;
    initStripe(chunkForInsert(handle >>> chunkBucketShift), (handle & chunkBucketMask) * stride, key, firstSeenOrdinal,
        idWidth != 0
            ? requireNonNull(identity, "identity")
            : null,
        identityOffset);
    if (bucket >= 0) {
      GroupLanes.set(probeChunkForInsert(bucket >>> PROBE_CHUNK_SHIFT), bucket & PROBE_CHUNK_MASK,
          (hash & PROBE_TAG_MASK) | (handle + 1L));
    }
    if (++size > growAt) {
      rehash();
    }
    return handle;
  }

  /** The index chunk {@code indexChunk} of the current geometry, taken on its first entry. */
  private GroupBuffer probeChunkForInsert(final int indexChunk) {
    GroupBuffer index = probeIndex[indexChunk];
    if (index == null) {
      index = newProbeChunk(mask + 1);
      probeIndex[indexChunk] = index;
      probeAddress[indexChunk] = index.address;
    }
    return index;
  }

  /**
   * {@link #acquireIndexed}'s miss: append the record and enter it in the index at {@code bucket},
   * the empty entry its probe stopped at.
   */
  // Sonar S9435: hot path: the branch layout is the measured shape, rewriting it changes the compiled
  // method.
  @SuppressWarnings("java:S9435")
  private int insertDense(final long key, final long firstSeenOrdinal, final long @Nullable [] identity,
      final int identityOffset, final long hash, final int bucket) {
    requireLiveTable();
    final int handle = size;
    initStripe(chunkForInsert(handle >>> chunkBucketShift), (handle & chunkBucketMask) * stride, key, firstSeenOrdinal,
        idWidth != 0
            ? requireNonNull(identity, "identity")
            : null,
        identityOffset);
    GroupLanes.set(probeChunkForInsert(bucket >>> PROBE_CHUNK_SHIFT), bucket & PROBE_CHUNK_MASK,
        (hash & PROBE_TAG_MASK) | (handle + 1L));
    if (++size > growAt) {
      rehash();
    }
    previousDenseKey = key;
    previousDenseHandle = handle;
    return handle;
  }

  /** Rebuild only the compact index; accumulator handles remain dense record ordinals. */
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
  private void rehashDense(final int capacity) {
    if (partialProbeBudget == 0) {
      // The fixed cache holds record ordinals, which survive payload growth. It is intentionally
      // incomplete, so rebuilding a full index would pay to deduplicate only the next probe again.
      growDenseStorage(capacity);
      return;
    }
    final int newMask = capacity - 1;
    final GroupBuffer[] grownIndex =
        new GroupBuffer[(int) (((long) capacity + PROBE_CHUNK_MASK) >>> PROBE_CHUNK_SHIFT)];
    final long[] grownAddress = new long[grownIndex.length];
    try {
      final long[] spine = storageAddress;
      final GroupBuffer[] chunks = storage;
      for (int handle = 0; handle < size; handle++) {
        final long key = laneOrZero(spine, chunks, handle >>> chunkBucketShift, (handle & chunkBucketMask) * stride);
        final long hash = HashCommon.mix(key);
        int bucket = (int) hash & newMask;
        // An index chunk that was not taken yet reads as an empty entry.
        while (laneOrZero(grownAddress, grownIndex, bucket >>> PROBE_CHUNK_SHIFT, bucket & PROBE_CHUNK_MASK) != 0L) {
          bucket = bucket + 1 & newMask;
        }
        final int chunkIndex = bucket >>> PROBE_CHUNK_SHIFT;
        GroupBuffer index = grownIndex[chunkIndex];
        if (index == null) {
          index = newProbeChunk(capacity);
          grownIndex[chunkIndex] = index;
          grownAddress[chunkIndex] = index.address;
        }
        GroupLanes.set(index, bucket & PROBE_CHUNK_MASK, (hash & PROBE_TAG_MASK) | (handle + 1L));
      }
      growDenseStorage(capacity);
    } catch (final RuntimeException | Error failure) {
      recycleChunks(grownIndex);
      throw failure;
    }
    // Publish the grown index before the old one is handed back.
    final GroupBuffer[] previousIndex = probeIndex;
    probeIndex = grownIndex;
    probeAddress = grownAddress;
    recycleChunks(previousIndex);
  }

  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
  private void growDenseStorage(final int capacity) {
    final int chunkBuckets = bucketsPerChunk(capacity);
    final GroupBuffer[] previous = storage;
    final GroupBuffer[] grown;
    final long[] grownAddress;
    if (chunkBuckets != bucketsPerChunk) {
      grown = new GroupBuffer[1];
      grown[0] = newChunk(chunkBuckets * stride);
      try {
        GroupLanes.copy(previous[0], 0, grown[0], 0, size * stride);
      } catch (final RuntimeException | Error failure) {
        recycleChunks(grown);
        throw failure;
      }
      grownAddress = new long[] {grown[0].address};
    } else {
      grown = Arrays.copyOf(previous, capacity / chunkBuckets);
      grownAddress = Arrays.copyOf(storageAddress, grown.length);
    }
    // Publish complete geometry before retiring any payload the old table still references.
    storage = grown;
    storageAddress = grownAddress;
    final boolean movedPayload = chunkBuckets != bucketsPerChunk;
    bucketsPerChunk = chunkBuckets;
    chunkLanes = chunkBuckets * stride;
    chunkBucketMask = chunkBuckets - 1;
    chunkBucketShift = Integer.numberOfTrailingZeros(chunkBuckets);
    mask = capacity - 1;
    growAt = capacity - (capacity >>> 2);
    rehashes++;
    if (movedPayload) {
      recycleChunks(previous);
    }
  }

  /**
   * The identity lanes of a stripe that is being merged into THIS table, copied out of the stripe:
   * the probe compares against a heap array, which is what the kernels hand it. One small array per
   * merge target, overwritten per stripe.
   *
   * @param stripe the source stripe's record, lane {@code 0} being its key lane
   */
  private long[] identityOf(final long stripe) {
    long[] lanes = mergeIdentity;
    if (lanes == null) {
      lanes = new long[idWidth];
      mergeIdentity = lanes;
    }
    final int first = 1 + idOffsetFromAcc;
    for (int i = 0; i < lanes.length; i++) {
      lanes[i] = GroupKernelLanes.get(stripe, first + i);
    }
    return lanes;
  }

  /**
   * Whether the identity lanes from lane {@code base} of an OCCUPIED stripe equal {@code identity}.
   */
  private static boolean identityMatches(final long[] spine, final GroupBuffer[] chunks, final int chunk,
      final int base, final long[] identity, final int identityOffset, final int width) {
    for (int i = 0; i < width; i++) {
      if (laneOrZero(spine, chunks, chunk, base + i) != identity[identityOffset + i]) {
        return false;
      }
    }
    return true;
  }

  /** The zero group's accumulator block, stamping its first-seen ordinal on first sight. */
  public GroupBuffer acquireZero(final long firstSeenOrdinal) {
    requireLiveTable();
    if (idWidth != 0) {
      throw new IllegalStateException("identity-mode tables route the zero probe hash through acquireExact");
    }
    if (passShift < 64 && passLo > 0) {
      return discardZero; // the zero key lives in partition 0, which this pass does not own
    }
    final GroupBuffer zero = zeroBlock();
    if (!hasZeroKey) {
      hasZeroKey = true;
      GroupLanes.set(zero, 1, firstSeenOrdinal);
    }
    return zero;
  }

  private void rehash() {
    try {
      growTable();
    } catch (final OutOfMemoryError failure) {
      // Whatever the table still owns stays owned until release; no further row may be accepted or
      // probed for.
      growAt = -1;
      stopProbing();
      throw failure;
    }
  }

  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  // Sonar S3776: a hot loop kept as one compiled unit. Splitting it changes what the compiler inlines
  // and holds in registers.
  @SuppressWarnings({"java:S1181", "java:S3776"})
  private void growTable() {
    final int oldCap = mask + 1;
    if (oldCap >= MAX_CAPACITY) {
      throw new IllegalStateException("group table exceeds " + MAX_CAPACITY + " buckets");
    }
    final int newCap = oldCap << 1;
    final long newLength = (long) newCap * stride;
    if (newLength > MAX_ARRAY_LENGTH) {
      throw new IllegalStateException("group table exceeds " + MAX_ARRAY_LENGTH + " lanes");
    }
    if (probeIndex != null) {
      rehashDense(newCap);
      return;
    }
    final int newMask = newCap - 1;
    final int st = stride;
    final GroupBuffer[] old = storage;
    final int newBucketsPerChunk = bucketsPerChunk(newCap);
    final int newChunkBucketMask = newBucketsPerChunk - 1;
    final int newChunkBucketShift = Integer.numberOfTrailingZeros(newBucketsPerChunk);
    final GroupBuffer[] grown = allocateStorage(newCap, newBucketsPerChunk);
    final long[] grownAddress = new long[grown.length];
    final long[] oldAddress = storageAddress;
    try {
      // Bucket order is the OLD table's, so the grown table's probe chains are identical to the
      // ones a same-order rebuild would produce.
      for (int c = 0; c < old.length; c++) {
        final GroupBuffer oldChunk = old[c];
        if (oldChunk == null) {
          continue;
        }
        final int oldLanes = GroupLanes.count(oldChunk);
        // The whole chunk is one record for the walk: resolved once, not once per bucket.
        final long from = oldChunk.record(0, oldLanes);
        for (int o = 0; o < oldLanes; o += st) {
          final long key = GroupKernelLanes.get(from, o);
          if (key == 0L) {
            continue;
          }
          int bucket = (int) HashCommon.mix(key) & newMask;
          // A chunk of the grown table that was not taken yet reads as an empty bucket.
          while (laneOrZero(grownAddress, grown, bucket >>> newChunkBucketShift,
              (bucket & newChunkBucketMask) * st) != 0L) {
            bucket = bucket + 1 & newMask;
          }
          final int chunkIndex = bucket >>> newChunkBucketShift;
          GroupBuffer targetChunk = grown[chunkIndex];
          if (targetChunk == null) {
            targetChunk = newChunk(newBucketsPerChunk * st);
            grown[chunkIndex] = targetChunk;
            grownAddress[chunkIndex] = targetChunk.address;
          }
          copyStripe(oldChunk, from, o, targetChunk, (bucket & newChunkBucketMask) * st, st);
        }
        // Every stripe of this chunk lives in the grown table now. It goes back at once: it is then
        // the chunk the grown table takes next, and the workspace clears it for that table while the
        // walk above still has it in the cache, a fraction of what clearing it cold costs. The table
        // forgets the chunk in the same step, so it is returned exactly once whatever happens next.
        old[c] = null;
        oldAddress[c] = 0L;
        recycle(oldChunk);
      }
    } catch (final RuntimeException | Error failure) {
      // The stripes already moved are lost with the grown chunks, and their old chunks are gone:
      // the table is finished. What it still holds, release() returns.
      growAt = -1;
      stopProbing();
      recycleChunks(grown);
      throw failure;
    }
    storage = grown;
    storageAddress = grownAddress;
    bucketsPerChunk = newBucketsPerChunk;
    chunkLanes = newBucketsPerChunk * st;
    chunkBucketMask = newChunkBucketMask;
    chunkBucketShift = newChunkBucketShift;
    mask = newMask;
    growAt = newCap - (newCap >>> 2);
    rehashes++;
  }

  /**
   * Copy the {@code lanes} lanes of one stripe from a chunk growth is walking, whose whole-chunk
   * record {@code from} the walk resolved, into a chunk of the grown table. The target stripe is
   * validated against its chunk; the copy itself is a lane loop, because a stripe is a dozen lanes
   * and a bulk copy's own entry cost is several of them. Debug mode copies through the bounded
   * segments.
   */
  private static void copyStripe(final GroupBuffer source, final long from, final int first, final GroupBuffer target,
      final int to, final int lanes) {
    if (GroupKernelLanes.DEBUG) {
      GroupLanes.copy(source, first, target, to, lanes);
      return;
    }
    GroupKernelLanes.copy(from, first, target.record(to, lanes), lanes);
    Reference.reachabilityFence(source);
    Reference.reachabilityFence(target);
  }

  private void installEmptyStorage(final int capacity) {
    final int chunkBuckets = bucketsPerChunk(capacity);
    storage = allocateStorage(capacity, chunkBuckets);
    storageAddress = new long[storage.length];
    bucketsPerChunk = chunkBuckets;
    chunkLanes = chunkBuckets * stride;
    chunkBucketMask = chunkBuckets - 1;
    chunkBucketShift = Integer.numberOfTrailingZeros(chunkBuckets);
  }

  private int bucketsPerChunk(final int capacity) {
    final int maxBuckets = Integer.highestOneBit(MAX_STORAGE_CHUNK_LANES / stride);
    return Math.min(capacity, maxBuckets);
  }

  private GroupBuffer[] allocateStorage(final int capacity, final int chunkBuckets) {
    final int chunkCount = capacity / chunkBuckets;
    return new GroupBuffer[chunkCount];
  }

  /**
   * Accumulator BASES of every live key, grouped by partition — built by the SCAN worker that owns
   * this table (already parallel, so the wall cost is zero) so the partition merge walks straight to
   * its keys instead of rescanning every source's full bucket array once per partition (P× the loads,
   * each key re-mixed P times — measured 13% of hot suite CPU). Entries are encoded accumulator
   * handles: after one chunk resolution, a stripe's key is at {@code offset - 1} and its aux at
   * {@code offset + slotWidth()}.
   */
  // Sonar S3776: a hot loop kept as one compiled unit. Splitting it changes what the compiler inlines
  // and holds in registers.
  @SuppressWarnings("java:S3776")
  public int[][] buildPartitionIndex(final int partitions, final int shift) {
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be > 0");
    }
    final int[] counts = new int[partitions];
    final int st = stride;
    for (final GroupBuffer chunk : storage) {
      if (chunk == null) {
        continue;
      }
      for (int o = 0; o < GroupLanes.count(chunk); o += st) {
        if (GroupLanes.get(chunk, o) != 0L) {
          counts[shift < 64
              ? (int) (HashCommon.mix(GroupLanes.get(chunk, o)) >>> shift)
              : 0]++;
        }
      }
    }
    final int[][] out = new int[partitions][];
    for (int p = 0; p < partitions; p++) {
      // Thousands of partitions over a small table leave most of them empty; one shared empty array
      // keeps a flush from allocating a distinct zero-length object per empty partition.
      out[p] = counts[p] == 0
          ? NO_HANDLES
          : new int[counts[p]];
      counts[p] = 0;
    }
    for (int chunkIndex = 0; chunkIndex < storage.length; chunkIndex++) {
      final GroupBuffer chunk = storage[chunkIndex];
      if (chunk == null) {
        continue;
      }
      for (int o = 0; o < GroupLanes.count(chunk); o += st) {
        if (GroupLanes.get(chunk, o) != 0L) {
          final int p = shift < 64
              ? (int) (HashCommon.mix(GroupLanes.get(chunk, o)) >>> shift)
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
    final boolean denseUnfiltered = into.probeIndex != null && into.passShift == 64;
    for (int s = 0; s < sources.length; s++) {
      final NumericGroupAggTable src = sources[s];
      if (src == null || index[s] == null) {
        continue;
      }
      requireMergeable(src, into);
      for (final int srcHandle : index[s][partition]) {
        into.mergeStripe(src.storageAtAccBase(srcHandle), src.offsetAtAccBase(srcHandle) - 1, denseUnfiltered);
      }
      mergeZeroGroup(src, into, slotWidth, partition);
    }
  }

  /**
   * Fold the stripe at lane {@code stripe} of {@code source}, laid out as this table's own stripes
   * are, into this table: first-arrival aux, minimum first-seen ordinal, exact sums under the mask.
   * Identity mode carries the source's identity lanes across, so two same-key groups that a single
   * worker kept apart cannot be folded together by the merge that reunites them.
   *
   * @param denseUnfiltered this table is a dense, unfiltered merge target, which cannot switch to
   *        partial probing: its acquisition path is chosen once per run instead of once per stripe
   */
  // Sonar S3358, S9435: hot path: the branch layout is the measured shape, rewriting it changes the
  // compiled method.
  @SuppressWarnings({"java:S3358", "java:S9435"})
  private void mergeStripe(final GroupBuffer source, final int stripe, final boolean denseUnfiltered) {
    // The complete source stripe, validated once: key, accumulator, aux and identity.
    final long from = source.record(stripe, stride);
    final long key = GroupKernelLanes.get(from, 0);
    final long firstSeen = GroupKernelLanes.get(from, 2);
    final int handle = denseUnfiltered
        ? acquireIndexed(key, firstSeen, idWidth != 0
            ? identityOf(from)
            : null, 0)
        : idWidth != 0
            ? acquireExact(key, firstSeen, identityOf(from), 0)
            : acquire(key, firstSeen);
    // AFTER the acquire: growth swaps the storage out from under any earlier resolution.
    final GroupBuffer chunk = storageAtAccBase(handle);
    final int width = slotWidth;
    final long into = chunk.record(offsetAtAccBase(handle), withAux
        ? width + 1
        : width);
    if (withAux && GroupKernelLanes.get(into, 0) == 0L) {
      // Fresh in the destination: carry the source reference (any sighting's bytes are the same
      // group value, so first-arrival is as good as first-seen).
      GroupKernelLanes.set(into, width, GroupKernelLanes.get(from, 1 + width));
    }
    mergeBlock(into, 0, from, 1, width, sumExactMask, sumsOnly);
    Reference.reachabilityFence(source);
    Reference.reachabilityFence(chunk);
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
  // Sonar S135: the exits of this hot loop are its compiled shape. Funnelling them through one exit
  // adds a flag test for each row.
  // Sonar S3776: a hot loop kept as one compiled unit. Splitting it changes what the compiler inlines
  // and holds in registers.
  @SuppressWarnings({"java:S135", "java:S3776"})
  public static void mergePartition(final NumericGroupAggTable[] sources, final int partition, final int shift,
      final NumericGroupAggTable into) {
    final int slotWidth = into.slotWidth;
    for (final NumericGroupAggTable src : sources) {
      if (src == null) {
        continue;
      }
      requireMergeable(src, into);
      final int st = src.stride;
      for (final GroupBuffer srcTable : src.storage) {
        if (srcTable == null) {
          continue;
        }
        final int lanes = GroupLanes.count(srcTable);
        for (int o = 0; o < lanes; o += st) {
          final long key = GroupLanes.get(srcTable, o);
          if (key == 0L) {
            continue;
          }
          if (shift < 64 && (int) (HashCommon.mix(key) >>> shift) != partition) {
            continue;
          }
          into.mergeStripe(srcTable, o, false);
        }
      }
      mergeZeroGroup(src, into, slotWidth, partition);
    }
  }

  /**
   * Fold a run of RAW STRIPES — {@code stride} lanes each, laid out exactly as this table's own
   * buckets are (key lane, accumulator block, aux, identity) — into this table. This is the build
   * half of a stripe spill ({@link GroupTableSpill}): a worker table is copied out stripe by stripe
   * into a per-partition append buffer instead of being probed into a shared table, and the
   * partition's table is built here, once, from those buffers after the scan. Semantics are those of
   * {@link #mergePartitionIndexed} — first-arrival aux, minimum first-seen ordinal, exact sums under
   * the mask, identity lanes deciding membership — with one source array instead of a table.
   *
   * @param stripes the buffer chunk; every stripe starts at a multiple of {@link #stride()} from
   *        {@code fromLane} and its key lane is non-zero
   * @param fromLane first lane of the first stripe
   * @param toLane one past the last lane of the last stripe; {@code (toLane - fromLane)} is a
   *        multiple of the stride
   */
  public void mergeStripes(final GroupBuffer stripes, final int fromLane, final int toLane) {
    requireExactMergeTarget();
    final int st = stride;
    if (fromLane < 0 || toLane < fromLane || toLane > GroupLanes.count(stripes) || (toLane - fromLane) % st != 0) {
      throw new IllegalArgumentException(
          "stripe run [" + fromLane + ", " + toLane + ") is not whole stripes of " + st + " lanes");
    }
    // Exact, unfiltered dense merge targets cannot switch to partial probing. Select their
    // acquisition path once rather than repeating pass/layout/partial dispatch for every stripe.
    final boolean denseUnfiltered = probeIndex != null && passShift == 64;
    for (int o = fromLane; o < toLane; o += st) {
      if (GroupLanes.get(stripes, o) == 0L) {
        throw new IllegalStateException("empty key lane in a spilled stripe at lane " + o);
      }
      mergeStripe(stripes, o, denseUnfiltered);
    }
  }

  /**
   * Fold {@code src}'s zero group (its side slot) into this table's — a no-op when {@code src} has
   * none. Public so a spill that carries the zero group separately from the stripes (a zero key lane
   * would read as an empty bucket) can land it on the built partition-0 table.
   */
  public void mergeZeroGroupFrom(final NumericGroupAggTable src) {
    requireMergeable(src, this);
    mergeZeroGroup(src, this, slotWidth, 0);
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
    final GroupBuffer srcZero = src.zeroBlock();
    final GroupBuffer dst = into.acquireZero(GroupLanes.get(srcZero, 1));
    if (fresh && src.withAux) {
      into.zeroAux = src.zeroAux;
    }
    mergeBlock(dst.record(0, slotWidth), 0, srcZero.record(0, slotWidth), 0, slotWidth, into.sumExactMask,
        into.sumsOnly);
    Reference.reachabilityFence(dst);
    Reference.reachabilityFence(srcZero);
  }

  /**
   * Blocks of different widths would fold lane-misaligned, and an aux-less destination has no lane to
   * carry a source reference INTO — its neighbour's key lane sits there instead. A source that folded
   * a lane the destination does not (or the reverse) would merge a real sum into an unread lane, or
   * an unfolded zero into a real one — both silent. The compact and ordinary layouts can reach the
   * SAME slotWidth at different column counts, and their operand blocks are two lanes wide against
   * four, so the flag is checked (and reported) in its own right.
   */
  private static void requireMergeable(final NumericGroupAggTable src, final NumericGroupAggTable into) {
    into.requireExactMergeTarget();
    src.requireLiveTable();
    if (src.slotWidth != into.slotWidth || src.withAux != into.withAux || src.sumExactMask != into.sumExactMask
        || src.idWidth != into.idWidth || src.sumsOnly != into.sumsOnly) {
      throw new IllegalStateException("incompatible group tables: slotWidth " + src.slotWidth + "/" + into.slotWidth
          + ", aux " + src.withAux + "/" + into.withAux + ", sumExactMask " + src.sumExactMask + "/" + into.sumExactMask
          + ", idWidth " + src.idWidth + "/" + into.idWidth + ", sumsOnly " + src.sumsOnly + "/" + into.sumsOnly);
    }
  }

  private void requireExactMergeTarget() {
    requireLiveTable();
    if (partialGroupsAllowed) {
      throw new IllegalStateException("partial worker records must merge into an exact group table");
    }
  }

  private void requireCompleteIndex() {
    if (partialGroupsAllowed) {
      throw new IllegalStateException("partial worker groups must be read through their storage records");
    }
  }

  /**
   * Fold the accumulator at lane {@code srcBase} of the record {@code src} into the one at lane
   * {@code dstBase} of the record {@code dst}. Both records were resolved for at least
   * {@code slotWidth} lanes from those bases.
   */
  private static void mergeBlock(final long dst, final int dstBase, final long src, final int srcBase,
      final int slotWidth, final long sumExactMask, final boolean sumsOnly) {
    GroupKernelLanes.add(dst, dstBase, GroupKernelLanes.get(src, srcBase));
    GroupKernelLanes.min(dst, dstBase + 1, GroupKernelLanes.get(src, srcBase + 1));
    if (sumsOnly) {
      for (int off = 2, a = 0; off < slotWidth; off += 2, a++) {
        GroupKernelLanes.add(dst, dstBase + off, GroupKernelLanes.get(src, srcBase + off));
        if (sumsExact(sumExactMask, a)) {
          GroupKernelLanes.set(dst, dstBase + off + 1, Math.addExact(GroupKernelLanes.get(dst, dstBase + off + 1),
              GroupKernelLanes.get(src, srcBase + off + 1)));
        }
      }
      return;
    }
    for (int off = 2, a = 0; off < slotWidth; off += 4, a++) {
      GroupKernelLanes.add(dst, dstBase + off, GroupKernelLanes.get(src, srcBase + off));
      // An unread lane holds 0 on both sides (the kernels skip folding it), so the plain add is a
      // no-op that keeps this loop branch-shaped identically whichever columns the query reads.
      GroupKernelLanes.set(dst, dstBase + off + 1, sumsExact(sumExactMask, a)
          ? Math.addExact(GroupKernelLanes.get(dst, dstBase + off + 1), GroupKernelLanes.get(src, srcBase + off + 1))
          : GroupKernelLanes.get(dst, dstBase + off + 1) + GroupKernelLanes.get(src, srcBase + off + 1));
      GroupKernelLanes.min(dst, dstBase + off + 2, GroupKernelLanes.get(src, srcBase + off + 2));
      GroupKernelLanes.max(dst, dstBase + off + 3, GroupKernelLanes.get(src, srcBase + off + 3));
    }
  }
}
