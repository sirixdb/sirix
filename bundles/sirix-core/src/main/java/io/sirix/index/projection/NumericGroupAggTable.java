package io.sirix.index.projection;

import it.unimi.dsi.fastutil.HashCommon;

/**
 * Flat open-addressed hash table for per-group aggregates keyed by a single {@code long} group
 * value, with each key INTERLEAVED with its own accumulator in one stripe so that a group's probe
 * and its fold touch the same cache line.
 *
 * <p>
 * Bucket {@code b} owns {@code table[b * stride .. b * stride + stride)}:
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
 * {@link #acquire} hands back the block's base rather than the bucket index: folds, merges, order
 * plans and emission address an accumulator identically whether it lives inside a table stripe or
 * in a STANDALONE {@code long[]} (the missing-key and constant-group accumulators keep that
 * standalone shape). From a block base, the key is one load BELOW it ({@link #keyAtAccBase}) and
 * the aux lane one {@code slotWidth} ABOVE it ({@link #auxAtAccBase}) — no division, no second
 * array, no second miss.
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

  /** Largest {@code long[]} length HotSpot will allocate on any collector. */
  private static final int MAX_ARRAY_LENGTH = Integer.MAX_VALUE - 8;

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
  private long[] table;
  private int mask;
  private int size;
  private int growAt;
  private boolean hasZeroKey;
  private final long[] zeroSlot;
  private long zeroAux;

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
    if (aggColumns < 0) {
      throw new IllegalArgumentException("aggColumns must be >= 0");
    }
    this.aggColumns = aggColumns;
    this.sumExactMask = sumExactMask;
    this.slotWidth = 2 + 4 * aggColumns;
    this.withAux = withAux;
    this.stride = 1 + slotWidth + (withAux
        ? 1
        : 0);
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
    this.table = new long[cap * stride];
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

  /** This table's {@link #sumExactMask} — kernels fold their rows under the same rule the merge uses. */
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

  /** The interleaved storage; bucket {@code b}'s stripe starts at {@code b * stride()}. */
  public long[] table() {
    return table;
  }

  /** Bucket count (a power of two). */
  public int capacity() {
    return mask + 1;
  }

  /** Distinct non-zero keys inserted. */
  public int size() {
    return size;
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
    return table[bucket * stride];
  }

  /** Accumulator base of bucket {@code bucket} — valid only when its key lane is non-zero. */
  public int accBaseOfBucket(final int bucket) {
    return bucket * stride + 1;
  }

  /**
   * The key owning the accumulator block at {@code accBase} — ONE load, and it lets a caller VALIDATE
   * a cached block base across rehashes: a base is current iff its bucket still holds the same key
   * (keys are unique, so a match can never be a different group).
   */
  public long keyAtAccBase(final int accBase) {
    return table[accBase - 1];
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
    return table[accBase + slotWidth];
  }

  /**
   * Stamp the source reference of the entry whose accumulator starts at {@code accBase} (aux lane
   * only). A never-occupied stripe reads {@code 0}, exactly like the separate lane it replaces.
   */
  public void setAuxAtAccBase(final int accBase, final long value) {
    table[accBase + slotWidth] = value;
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
   * Base offset of {@code key}'s accumulator block within {@link #table()}, inserting a fresh block
   * ({@code count 0, firstSeen ordinal, empty aggregates}) on first sight. {@code key} MUST be
   * non-zero — route the zero group through {@link #acquireZero}.
   *
   * <p>
   * The returned base is invalidated by any later {@code acquire} that grows the table, and so is the
   * array {@link #table()} returned before it: re-read both AFTER the call.
   */
  public int acquire(final long key, final long firstSeenOrdinal) {
    final long[] t = table;
    final int st = stride;
    final int len = t.length;
    // Probing walks stripe to stripe by ADDITION: t.length is exactly capacity * stride, so
    // wrapping at it is the same bucket sequence as `pos = pos + 1 & mask` — one multiply per
    // lookup instead of one per probe step. The load factor caps at 3/4, so the walk terminates.
    int off = ((int) HashCommon.mix(key) & mask) * st;
    long cur = t[off];
    while (cur != 0L) {
      if (cur == key) {
        return off + 1;
      }
      off += st;
      if (off == len) {
        off = 0;
      }
      cur = t[off];
    }
    t[off] = key;
    initBlock(t, off + 1, firstSeenOrdinal, slotWidth);
    if (++size > growAt) {
      rehash();
      // The stripe moved with its key; re-probe in the grown table (guaranteed present).
      return find(key);
    }
    return off + 1;
  }

  /** The zero group's accumulator block, stamping its first-seen ordinal on first sight. */
  public long[] acquireZero(final long firstSeenOrdinal) {
    if (!hasZeroKey) {
      hasZeroKey = true;
      zeroSlot[1] = firstSeenOrdinal;
    }
    return zeroSlot;
  }

  /** Accumulator base of an EXISTING non-zero key. */
  private int find(final long key) {
    final long[] t = table;
    final int st = stride;
    final int len = t.length;
    int off = ((int) HashCommon.mix(key) & mask) * st;
    while (t[off] != key) {
      off += st;
      if (off == len) {
        off = 0;
      }
    }
    return off + 1;
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
    final long[] old = table;
    final long[] grown = new long[(int) newLength];
    // Bucket order is the OLD table's, so the grown table's probe chains are identical to the
    // ones a same-order rebuild would produce.
    final int grownLength = grown.length;
    for (int o = 0; o < old.length; o += st) {
      final long key = old[o];
      if (key == 0L) {
        continue;
      }
      int to = ((int) HashCommon.mix(key) & newMask) * st;
      while (grown[to] != 0L) {
        to += st;
        if (to == grownLength) {
          to = 0;
        }
      }
      System.arraycopy(old, o, grown, to, st);
    }
    table = grown;
    mask = newMask;
    growAt = newCap - (newCap >>> 2);
  }

  /**
   * Accumulator BASES of every live key, grouped by partition — built by the SCAN worker that owns
   * this table (already parallel, so the wall cost is zero) so the partition merge walks straight to
   * its keys instead of rescanning every source's full bucket array once per partition (P× the loads,
   * each key re-mixed P times — measured 13% of hot suite CPU). Entries address {@link #table()}
   * directly: a stripe's key is at {@code base - 1} and its aux at {@code base + slotWidth()}, so the
   * merge needs no multiply per entry.
   */
  public int[][] buildPartitionIndex(final int partitions, final int shift) {
    if (partitions <= 0) {
      throw new IllegalArgumentException("partitions must be > 0");
    }
    final int[] counts = new int[partitions];
    final long[] t = table;
    final int st = stride;
    for (int o = 0; o < t.length; o += st) {
      if (t[o] != 0L) {
        counts[shift < 64
            ? (int) (HashCommon.mix(t[o]) >>> shift)
            : 0]++;
      }
    }
    final int[][] out = new int[partitions][];
    for (int p = 0; p < partitions; p++) {
      out[p] = new int[counts[p]];
      counts[p] = 0;
    }
    for (int o = 0; o < t.length; o += st) {
      if (t[o] != 0L) {
        final int p = shift < 64
            ? (int) (HashCommon.mix(t[o]) >>> shift)
            : 0;
        out[p][counts[p]++] = o + 1;
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
      final long[] srcTable = src.table;
      for (final int srcBase : index[s][partition]) {
        final int dstBase = into.acquire(srcTable[srcBase - 1], srcTable[srcBase + 1]);
        // AFTER the acquire: growth swaps the array out from under any earlier read.
        final long[] dstTable = into.table;
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
      final long[] srcTable = src.table;
      final int st = src.stride;
      for (int o = 0; o < srcTable.length; o += st) {
        final long key = srcTable[o];
        if (key == 0L) {
          continue;
        }
        if (shift < 64 && (int) (HashCommon.mix(key) >>> shift) != partition) {
          continue;
        }
        final int srcBase = o + 1;
        final int dstBase = into.acquire(key, srcTable[srcBase + 1]);
        final long[] dstTable = into.table;
        if (aux && dstTable[dstBase] == 0L) {
          // Fresh in the destination: carry the source reference (any sighting's bytes are the
          // same group value, so first-arrival is as good as first-seen).
          dstTable[dstBase + slotWidth] = srcTable[srcBase + slotWidth];
        }
        mergeBlock(dstTable, dstBase, srcTable, srcBase, slotWidth, into.sumExactMask);
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
    if (src.slotWidth != into.slotWidth || src.withAux != into.withAux || src.sumExactMask != into.sumExactMask) {
      throw new IllegalStateException("incompatible group tables: slotWidth " + src.slotWidth + "/" + into.slotWidth
          + ", aux " + src.withAux + "/" + into.withAux + ", sumExactMask " + src.sumExactMask + "/"
          + into.sumExactMask);
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
