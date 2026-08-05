/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;

/**
 * SIMD kernels over {@link StringRegion}'s dict-id column.
 *
 * <h2>A string scan never compares strings</h2>
 *
 * <p>The dictionary turns a string predicate into an integer one before the scan starts. For
 * equality that is one dictionary lookup producing one id, after which the scan is a count of that
 * id in a fixed-width bit-packed column — the same shape as {@link NumberRegion}'s bit-packed
 * values, and served by the same {@link BitUnpackSimd} unpack.
 *
 * <h2>Predicates that are not equality</h2>
 *
 * <p>Restricting the fast path to equality leaves {@code IN (...)}, prefix matches, ranges and
 * {@code LIKE} to be answered a string at a time — on a column where the whole point of the
 * dictionary is that there are only a handful of distinct strings. {@link #countDictIdSet} closes
 * that: the caller evaluates the predicate once per <em>dictionary entry</em>, which for a typical
 * page is eight comparisons rather than ninety, and passes the surviving ids as a bitmap. The scan
 * then tests membership of a small set with a vector shift and mask, at the same cost as equality.
 *
 * <p>This is what ClickHouse does for {@code LowCardinality} columns and what Umbra and DuckDB do
 * with dictionary-encoded string filters: push the predicate into the dictionary, scan the codes.
 * The expensive part — actual string comparison — happens a number of times proportional to the
 * column's cardinality, not to its length.
 *
 * <h2>Membership as arithmetic</h2>
 *
 * <p>A dictionary of at most 64 entries is a single {@code long}: id {@code k} is in the set iff
 * bit {@code k} is set, and the whole test is {@code (mask >>> id) & 1}, which vectorizes to one
 * variable shift and one AND across eight lanes. Wider dictionaries fall back to a per-lane probe
 * of a bitmap. Sixty-four covers the low-cardinality columns dictionary encoding is chosen for in
 * the first place; beyond that the encoder's own heuristics have usually stopped choosing it.
 */
public final class StringRegionSimd {

  private static final int LANES = ColumnLoad.LANES;

  private StringRegionSimd() {
  }

  /**
   * Count values in {@code [start, start + n)} whose dict id equals {@code dictId}.
   *
   * @param payload the region payload
   * @param dictIdsOffset byte offset of the packed dict-id column, from the header
   * @param bitWidth bits per dict id
   * @return the match count, or {@code -1} when the width is not supported
   */
  public static long countDictId(final MemorySegment payload, final int dictIdsOffset, final int bitWidth,
      final int start, final int n, final int dictId) {
    final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(bitWidth);
    if (plan == null) {
      return -1L;
    }
    final long target = dictId & plan.mask();
    long count = 0;
    int i = 0;
    if (BitUnpackSimd.vectorProfitable(n)) {
      final LongVector targetV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, target);
      final int lastGroup =
          BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), dictIdsOffset, bitWidth);
      for (; i <= n - LANES && start + i <= lastGroup; i += LANES) {
        count += plan.unpack(payload, dictIdsOffset, start + i)
                     .compare(VectorOperators.EQ, targetV)
                     .trueCount();
      }
    }
    for (; i < n; i++) {
      if (plan.decodeAt(payload, dictIdsOffset, start + i) == target) {
        count++;
      }
    }
    return count;
  }

  /**
   * Set bit {@code i} of {@code rowBits} for each value in {@code [start, start + n)} whose dict id
   * equals {@code dictId} — the selection-vector form of {@link #countDictId}.
   *
   * <p>Exists for the fused multi-column kernel, which intersects one row bitmap per predicate
   * leaf: a count can say how many rows equal the literal, but only the bitmap can be AND-ed with
   * the rows another column produced. Bits are indexed RELATIVE to {@code start}, matching the
   * bitmap convention everywhere else in this package.
   *
   * <p>The vector body extracts the comparison mask as a long and ORs it straight into the word —
   * groups are eight lanes and words are 64 bits, so a group never straddles two words and the
   * shift is the lane offset within the word.
   *
   * @param rowBits destination bitmap, at least {@code ceil(n / 64)} words; only bits
   *        {@code [0, n)} are written, and they are OVERWRITTEN, not OR-ed with prior content
   * @return the number of bits set, or {@code -1} when the width is not supported
   */
  public static long selectDictIdInto(final MemorySegment payload, final int dictIdsOffset,
      final int bitWidth, final int start, final int n, final int dictId, final long[] rowBits) {
    final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(bitWidth);
    if (plan == null) {
      return -1L;
    }
    final int words = (n + 63) >>> 6;
    if (rowBits.length < words) {
      throw new IllegalArgumentException(
          "row bitmap too small: " + rowBits.length + " words for " + n + " values");
    }
    Arrays.fill(rowBits, 0, words, 0L);
    final long target = dictId & plan.mask();
    long count = 0;
    int i = 0;
    if (BitUnpackSimd.vectorProfitable(n)) {
      final LongVector targetV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, target);
      final int lastGroup =
          BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), dictIdsOffset, bitWidth);
      for (; i <= n - LANES && start + i <= lastGroup; i += LANES) {
        final long mask = plan.unpack(payload, dictIdsOffset, start + i)
                              .compare(VectorOperators.EQ, targetV)
                              .toLong();
        rowBits[i >>> 6] |= mask << (i & 63);
        count += Long.bitCount(mask);
      }
    }
    for (; i < n; i++) {
      if (plan.decodeAt(payload, dictIdsOffset, start + i) == target) {
        rowBits[i >>> 6] |= 1L << (i & 63);
        count++;
      }
    }
    return count;
  }

  /**
   * Live-value counterpart of {@link #countDictId}, for a versioned merge in which some values are
   * shadowed by a newer fragment.
   *
   * <p>Shadowed lanes are unpacked and compared like any other and are then simply not counted —
   * the branch that would skip them is what the kernel exists to remove. {@code liveBits} is
   * indexed RELATIVE to {@code start}.
   *
   * @return the number of live matches, or {@code -1} when the width is not supported
   */
  public static long countDictIdMasked(final MemorySegment payload, final int dictIdsOffset,
      final int bitWidth, final int start, final int n, final int dictId, final long[] liveBits) {
    final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(bitWidth);
    if (plan == null) {
      return -1L;
    }
    final long target = dictId & plan.mask();
    long count = 0;
    int i = 0;
    if (BitUnpackSimd.vectorProfitable(n)) {
      final LongVector targetV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, target);
      final int lastGroup =
          BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), dictIdsOffset, bitWidth);
      for (; i <= n - LANES && start + i <= lastGroup; i += LANES) {
        final VectorMask<Long> live =
            VectorMask.fromLong(ColumnLoad.LONG_SPECIES, liveBits[i >>> 6] >>> (i & 63));
        count += plan.unpack(payload, dictIdsOffset, start + i)
                     .compare(VectorOperators.EQ, targetV)
                     .and(live)
                     .trueCount();
      }
    }
    for (; i < n; i++) {
      if ((liveBits[i >>> 6] & (1L << (i & 63))) == 0L) {
        continue;
      }
      if (plan.decodeAt(payload, dictIdsOffset, start + i) == target) {
        count++;
      }
    }
    return count;
  }

  /**
   * Count values in {@code [start, start + n)} whose dict id is a member of {@code idSet}.
   *
   * <p>The caller builds {@code idSet} by evaluating the real predicate — {@code IN}, prefix,
   * range, {@code LIKE} — once per dictionary entry. Bit {@code k} set means dictionary entry
   * {@code k} satisfies it. An empty set short-circuits without reading the column.
   *
   * @param idSet membership bitmap over dict ids, bit {@code k} for id {@code k}
   * @param dictSize number of dictionary entries, i.e. the exclusive upper bound on ids
   * @return the match count, or {@code -1} when the width is not supported
   */
  public static long countDictIdSet(final MemorySegment payload, final int dictIdsOffset,
      final int bitWidth, final int start, final int n, final long[] idSet, final int dictSize) {
    final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(bitWidth);
    if (plan == null) {
      return -1L;
    }
    if (isEmpty(idSet)) {
      return 0L;
    }
    // A dictionary that fits in one word makes membership a shift and a mask, with no memory
    // access at all in the inner loop.
    if (dictSize <= 64) {
      return countBySetWord(payload, dictIdsOffset, plan, start, n, idSet[0]);
    }
    long count = 0;
    for (int i = 0; i < n; i++) {
      final long id = plan.decodeAt(payload, dictIdsOffset, start + i);
      if (id < dictSize && (idSet[(int) (id >>> 6)] & (1L << (id & 63))) != 0L) {
        count++;
      }
    }
    return count;
  }

  /**
   * Membership against a dictionary of at most 64 entries.
   *
   * <p>{@code (setWord >>> id) & 1} per lane: one variable shift, one AND, one compare. Every lane
   * takes the same path regardless of which ids it holds, so a selective predicate and an
   * unselective one cost the same.
   */
  private static long countBySetWord(final MemorySegment payload, final int dictIdsOffset,
      final BitUnpackSimd.Plan plan, final int start, final int n, final long setWord) {
    long count = 0;
    int i = 0;
    if (BitUnpackSimd.vectorProfitable(n)) {
      final LongVector setV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, setWord);
      final LongVector oneV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, 1L);
      final LongVector zeroV = LongVector.zero(ColumnLoad.LONG_SPECIES);
      final LongVector wordBitsV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, Long.SIZE);
      final int lastGroup = BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), dictIdsOffset,
                                                               plan.bitWidth());
      for (; i <= n - LANES && start + i <= lastGroup; i += LANES) {
        final LongVector ids = plan.unpack(payload, dictIdsOffset, start + i);
        // The lane shift is taken modulo 64, exactly as scalar Java does, so an id of 64 would
        // otherwise test bit 0 and be counted as a member of id 0. The scalar tail below rejects
        // such ids outright; without this mask the two halves of one range disagree, and which
        // half a value lands in depends on the warmup budget.
        count += setV.lanewise(VectorOperators.LSHR, ids)
                     .and(oneV)
                     .compare(VectorOperators.NE, zeroV)
                     .and(ids.compare(VectorOperators.LT, wordBitsV))
                     .trueCount();
      }
    }
    for (; i < n; i++) {
      final long id = plan.decodeAt(payload, dictIdsOffset, start + i);
      if (id < 64 && ((setWord >>> (int) id) & 1L) != 0L) {
        count++;
      }
    }
    return count;
  }

  /**
   * Histogram every dict id in {@code [start, start + n)} into {@code counts}.
   *
   * <p>Used by group-by, where the answer is not "how many match" but "how many of each". The
   * unpack vectorizes; the increments cannot, because eight lanes may target the same counter and
   * a vector scatter has no conflict resolution. So the ids are unpacked eight at a time into a
   * register, spilled once, and counted — which still removes the per-value load, shift and mask,
   * leaving only the increment.
   *
   * @param counts destination, indexed by dict id; must cover the tag's dictionary
   * @return {@code true} when the vector path ran, {@code false} when the caller should scalar-scan
   */
  public static boolean histogramDictIds(final MemorySegment payload, final int dictIdsOffset,
      final int bitWidth, final int start, final int n, final long[] counts) {
    final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(bitWidth);
    if (plan == null) {
      return false;
    }
    final long[] staged = HISTOGRAM_STAGING.get();
    int i = 0;
    if (BitUnpackSimd.vectorProfitable(n)) {
      final int lastGroup =
          BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), dictIdsOffset, bitWidth);
      for (; i <= n - LANES && start + i <= lastGroup; i += LANES) {
        plan.unpack(payload, dictIdsOffset, start + i).intoArray(staged, 0);
        for (int lane = 0; lane < LANES; lane++) {
          counts[(int) staged[lane]]++;
        }
      }
    }
    for (; i < n; i++) {
      counts[(int) plan.decodeAt(payload, dictIdsOffset, start + i)]++;
    }
    return true;
  }

  /**
   * Per-thread lane staging for {@link #histogramDictIds}.
   *
   * <p>Thread-local rather than a fresh array per call: the histogram runs once per tag per page on
   * the group-by path, and a scan crossing thousands of pages should not make an array per page —
   * the same reason {@link NumberRegionDeltaSimd} pools its staging block.
   */
  private static final ThreadLocal<long[]> HISTOGRAM_STAGING =
      ThreadLocal.withInitial(() -> new long[LANES]);

  private static boolean isEmpty(final long[] bits) {
    for (final long word : bits) {
      if (word != 0L) {
        return false;
      }
    }
    return true;
  }
}
