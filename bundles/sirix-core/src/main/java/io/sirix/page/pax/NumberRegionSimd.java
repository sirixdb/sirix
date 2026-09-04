/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.page.PageLayout;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;

/**
 * SIMD kernels over {@link NumberRegion}'s value column, computed on the encoded form.
 *
 * <p>
 * Every kernel here reads packed bytes and produces an answer without materializing the column: no
 * decode buffer, no {@code long[]} of values, no per-record object. A {@code PLAIN_LONG} column is
 * compared where it lies; a {@code BIT_PACKED} or {@code COMPACT_ZM} column is unpacked into vector
 * registers by {@link BitUnpackSimd} and compared there. That is the BtrBlocks premise — the scan
 * operates on compressed data, and decompression, where it happens at all, happens in registers.
 *
 * <h2>Three things that make or break these kernels</h2>
 *
 * <p>
 * <b>The load.</b> All loads go through {@link ColumnLoad}. Vector loads against a heap
 * {@code MemorySegment} — which is what a region payload wrapped by {@code MemorySegment.ofArray}
 * is — do not intrinsify and cost about eight times an array load.
 *
 * <p>
 * <b>The operator must be a constant.</b> A {@link VectorOperators.Comparison} reaching
 * {@code compare} as a <em>method parameter</em> cannot be constant-folded, and the whole compare
 * silently degrades to a generic path — measured at 9.6x slower than the same loop with a literal
 * operator. So no hot loop in this file takes an operator. Every predicate is normalized at the
 * entry point into an inclusive {@code [lo, hi]} bound, and the loops compare against that with
 * operators written out literally. The public API still speaks in operators, because callers think
 * that way; the conversion happens once per scan, not once per vector.
 *
 * <p>
 * Normalizing also collapses the two comparisons of a range into one. For {@code lo <= hi} the test
 * {@code lo <= v <= hi} is equivalent to {@code (v - lo) <=u (hi - lo)} in unsigned arithmetic, so
 * a two-sided range costs a subtract and a single unsigned compare rather than two signed compares
 * and an AND. It is the same identity a C compiler applies to a chained range test, and DuckDB and
 * ClickHouse both lean on it for {@code BETWEEN}.
 *
 * <p>
 * <b>The zone map.</b> The cheapest scan is the one that never runs. {@link NumberRegion} stores
 * per-tag min/max, so {@link #pruneCount} can answer a predicate for a whole range from the header
 * alone. On a cold or larger-than-memory working set that is worth more than any amount of lane
 * throughput, because it is the difference between touching the value bytes and not.
 *
 * <h2>Range size</h2>
 *
 * <p>
 * Kernels check {@link BitUnpackSimd#vectorProfitable} before setting up. A query that asks for one
 * row reaches the same code a full scan does, and for a handful of values the broadcasts and the
 * plan lookup cost more than a scalar loop.
 *
 * <h2>Encodings</h2>
 *
 * <ul>
 * <li>{@code PLAIN_LONG} / {@code PLAIN_LONG_ZM} — compared directly out of the payload.</li>
 * <li>{@code BIT_PACKED} / {@code BIT_PACKED_ZM} / {@code COMPACT_ZM} — unpacked in registers by
 * {@link BitUnpackSimd}, frame-of-reference base added as a vector, compared. Widths past
 * {@link BitUnpackSimd#MAX_BIT_WIDTH} decline to the scalar path.</li>
 * <li>{@code DELTA_ZM} — handled by {@link NumberRegionDeltaSimd}, which reconstructs the
 * sequential prefix sum with vector arithmetic rather than declining it.</li>
 * </ul>
 *
 * <p>
 * Kernels that cannot serve a shape return {@code -1} (or {@code false}) rather than throwing, and
 * the caller falls back to {@link NumberRegion#decodeValueAt}. A wrong answer is never among the
 * options; a slower one is.
 */
public final class NumberRegionSimd {

  private static final int LANES = ColumnLoad.LANES;

  /** Ternary outcome of a zone-map check; see {@link #pruneCount}. */
  public static final long PRUNE_UNKNOWN = -1L;

  private NumberRegionSimd() {}

  // ────────────────────────────────────────────────────────── zone-map pruning

  /**
   * Answer a two-sided range predicate for {@code n} values from their min/max alone, or admit that
   * the payload has to be read.
   *
   * <p>
   * This is the pushdown BtrBlocks and DuckDB both lean on, and it is the single most valuable thing
   * in this file when the data does not fit in memory: a pruned range costs a comparison against two
   * header longs and no access to the value bytes at all.
   *
   * @return {@code 0} when nothing can match, {@code n} when everything must, or
   *         {@link #PRUNE_UNKNOWN} when the range straddles a bound and must be scanned
   */
  public static long pruneCount(final long min, final long max, final long lo, final long hi, final long n) {
    if (max < lo || min > hi) {
      return 0L;
    }
    if (min >= lo && max <= hi) {
      return n;
    }
    return PRUNE_UNKNOWN;
  }

  /** {@link #pruneCount} for a one-sided predicate, expressed through its operator. */
  public static long pruneCount(final long min, final long max, final VectorOperators.Comparison op,
      final long threshold, final long n) {
    if (op == VectorOperators.NE) {
      // NE is the one predicate that is not an interval, so containment cannot decide it; only a
      // range collapsed onto a single value can.
      return min == max
          ? (min == threshold
              ? 0L
              : n)
          : PRUNE_UNKNOWN;
    }
    if (isEmptyPredicate(op, threshold)) {
      return 0L;
    }
    return pruneCount(min, max, loBound(op, threshold), hiBound(op, threshold), n);
  }

  // ─────────────────────────────────────────────────────────── count kernels

  /**
   * Count values in {@code [start, end)} satisfying {@code value OP threshold}.
   *
   * @return the match count, or {@code -1} for a shape these kernels decline
   */
  public static long countMatching(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final VectorOperators.Comparison op, final long threshold) {
    return countMatchingRange(payload, h, start, end, op, threshold, op, threshold);
  }

  /**
   * Count values in {@code [start, end)} satisfying both predicates in one pass.
   *
   * <p>
   * Fusing the two sides of a range into a single scan is what keeps {@code age > 30 AND
   * age < 50} from being two passes and an intersection.
   *
   * @return the match count, or {@code -1} for a shape these kernels decline
   */
  public static long countMatchingRange(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final VectorOperators.Comparison op1, final long threshold1, final VectorOperators.Comparison op2,
      final long threshold2) {
    return countNormalized(payload, h, start, end, op1, threshold1, op2, threshold2, null);
  }

  /**
   * Live-value counterpart of {@link #countMatchingRange}, for a range in which some values are
   * shadowed and must not be counted.
   *
   * <p>
   * Exists for the versioned column merge. A page whose slots were touched across commits is read as
   * several fragments, newest first, and a value on an older fragment counts only if no newer
   * fragment already defined its slot — which includes defining it as a deletion. Applying that per
   * value with a branch is what these kernels exist to avoid, so liveness arrives as a bitmap and
   * becomes a lane mask: dead lanes are loaded and compared like any other and are then simply not
   * counted. Dead values cost arithmetic; they cost no control flow.
   *
   * <p>
   * {@code liveBits} is indexed RELATIVE to {@code start} — bit {@code k} governs value
   * {@code start + k}. Relative indexing keeps the mask extraction a single shift: lane groups
   * advance by a lane count that divides 64, so a group never straddles two words.
   *
   * <p>
   * Callers with nothing shadowed must use {@link #countMatchingRange} rather than passing an
   * all-ones bitmap — that is the overwhelmingly common case and it should cost nothing at all.
   *
   * @param liveBits bit {@code k} set when value {@code start + k} is not shadowed
   * @return the number of live values satisfying both predicates, or {@code -1} for a declined shape
   */
  public static long countMatchingRangeMasked(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final VectorOperators.Comparison op1, final long threshold1, final VectorOperators.Comparison op2,
      final long threshold2, final long[] liveBits) {
    return countNormalized(payload, h, start, end, op1, threshold1, op2, threshold2, liveBits);
  }

  /**
   * Turn an operator pair into an inclusive bound and dispatch, handling the one operator that is not
   * an interval.
   *
   * <p>
   * {@code NE} is expressed as a subtraction rather than a third loop: the values a conjunction
   * accepts, minus those where the excluded value actually occurs. Both terms are ordinary interval
   * scans, so nothing leaves the vector path to support it.
   */
  private static long countNormalized(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final VectorOperators.Comparison op1, final long threshold1, final VectorOperators.Comparison op2,
      final long threshold2, final long[] liveBits) {
    if (start >= end) {
      return 0L;
    }
    if (isEmptyPredicate(op1, threshold1) || isEmptyPredicate(op2, threshold2)) {
      return 0L;
    }
    final boolean ne1 = op1 == VectorOperators.NE;
    final boolean ne2 = op2 == VectorOperators.NE;
    if (ne1 && ne2 && threshold1 == threshold2) {
      return countExcluding(payload, h, start, end, Long.MIN_VALUE, Long.MAX_VALUE, threshold1, liveBits);
    }
    if (ne1 != ne2) {
      return countOneExclusionWithRange(payload, h, start, end, ne1, op1, threshold1, op2, threshold2, liveBits);
    }
    if (ne1) {
      return countTwoExclusions(payload, h, start, end, threshold1, threshold2, liveBits);
    }
    final long lo = Math.max(loBound(op1, threshold1), loBound(op2, threshold2));
    final long hi = Math.min(hiBound(op1, threshold1), hiBound(op2, threshold2));
    if (lo > hi) {
      return 0L;
    }
    return count(payload, h, start, end, lo, hi, liveBits);
  }

  /**
   * One exclusion beside one range bound: count what the kept side admits, minus the excluded value.
   * {@code ne1} says which of the two predicates is the exclusion.
   */
  private static long countOneExclusionWithRange(final MemorySegment payload, final NumberRegion.Header h,
      final int start, final int end, final boolean ne1, final VectorOperators.Comparison op1, final long threshold1,
      final VectorOperators.Comparison op2, final long threshold2, final long[] liveBits) {
    final long excluded = ne1
        ? threshold1
        : threshold2;
    final VectorOperators.Comparison keepOp = ne1
        ? op2
        : op1;
    final long keepThreshold = ne1
        ? threshold2
        : threshold1;
    return countExcluding(payload, h, start, end, loBound(keepOp, keepThreshold), hiBound(keepOp, keepThreshold),
        excluded, liveBits);
  }

  /** Two different exclusions: subtract each from the total, the values being distinct. */
  private static long countTwoExclusions(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final long threshold1, final long threshold2, final long[] liveBits) {
    final long all = count(payload, h, start, end, Long.MIN_VALUE, Long.MAX_VALUE, liveBits);
    final long first = count(payload, h, start, end, threshold1, threshold1, liveBits);
    final long second = count(payload, h, start, end, threshold2, threshold2, liveBits);
    return all < 0 || first < 0 || second < 0
        ? -1L
        : all - first - second;
  }

  /** {@code lo <= v <= hi AND v != excluded}, as two interval scans. */
  private static long countExcluding(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final long lo, final long hi, final long excluded, final long[] liveBits) {
    if (lo > hi) {
      return 0L;
    }
    final long kept = count(payload, h, start, end, lo, hi, liveBits);
    if (kept < 0) {
      return -1L;
    }
    if (excluded < lo || excluded > hi) {
      return kept;
    }
    final long removed = count(payload, h, start, end, excluded, excluded, liveBits);
    return removed < 0
        ? -1L
        : kept - removed;
  }

  /** Dispatch an inclusive-bound count to the kernel for this encoding. */
  private static long count(final MemorySegment payload, final NumberRegion.Header h, final int start, final int end,
      final long lo, final long hi, final long[] liveBits) {
    if (h.isPerTag()) {
      return countPerTag(payload, h, start, end, lo, hi, liveBits);
    }
    if (NumberRegion.isDelta(h.encodingKind)) {
      return NumberRegionDeltaSimd.countRange(payload, h, start, end, lo, hi, liveBits);
    }
    if (NumberRegion.isBitPacked(h.encodingKind)) {
      return countPacked(payload, h.valueBytesOffset, h.valueBase, h.valueBitWidth, start, end, lo, hi, liveBits);
    }
    return countPlain(payload, h.valueBytesOffset, start, end, lo, hi, liveBits);
  }

  // ───────────────────────────────────────────────────────── per-tag kernels
  //
  // A per-tag payload is a run of independently framed columns: each tag has its own base, its own
  // width and its own byte-aligned start. That is exactly the shape these kernels already take —
  // (offset, base, width, range) — so the vector path is not lost, it is entered once per tag with
  // that tag's frame. Scans reach here with one tag's window, which is the single-segment case; the
  // loop exists so a caller that hands over a wider window still gets a right answer.

  /** Inclusive-bound count over a per-tag payload, segment by segment. */
  private static long countPerTag(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final long lo, final long hi, final long[] liveBits) {
    final int limit = Math.min(end, h.count);
    int tag = NumberRegion.tagOfIndex(h, start);
    if (tag < 0) {
      return 0L;
    }
    long total = 0L;
    int cursor = start;
    while (cursor < limit && tag < h.dictSize) {
      final int tagStart = h.tagStart[tag];
      final int segmentEnd = Math.min(limit, tagStart + h.tagCount[tag]);
      if (liveBits != null && (cursor != start || segmentEnd != limit)) {
        // The liveness bitmap is indexed relative to `start`, and a lane group must not straddle a
        // word. Re-basing it per segment would break both, so a masked count that spans tags is
        // declined rather than answered against a shifted bitmap.
        return -1L;
      }
      final long counted = countTagSegment(payload, h, tag, cursor - tagStart, segmentEnd - tagStart, lo, hi, liveBits);
      if (counted < 0L) {
        return -1L;
      }
      total += counted;
      cursor = segmentEnd;
      tag++;
    }
    return total;
  }

  /** One tag's window, dispatched on that tag's width. */
  private static long countTagSegment(final MemorySegment payload, final NumberRegion.Header h, final int tag,
      final int from, final int to, final long lo, final long hi, final long[] liveBits) {
    if (from >= to) {
      return 0L;
    }
    final int width = h.tagWidth[tag] & 0xFF;
    if (width == 0) {
      // Constant tag: the header answers it, no value is read at all.
      final long constant = h.tagMin[tag];
      if (constant < lo || constant > hi) {
        return 0L;
      }
      return liveBits == null
          ? to - from
          : liveCount(liveBits, to - from);
    }
    if (width == 64) {
      return countPlain(payload, h.tagValueOffset[tag], from, to, lo, hi, liveBits);
    }
    return countPacked(payload, h.tagValueOffset[tag], h.tagDecodeBase[tag], width, from, to, lo, hi, liveBits);
  }

  /** Live values among the first {@code n} bits of a relative-indexed liveness bitmap. */
  private static long liveCount(final long[] liveBits, final int n) {
    long live = 0L;
    final int fullWords = n >>> 6;
    for (int w = 0; w < fullWords; w++) {
      live += Long.bitCount(liveBits[w]);
    }
    final int tail = n & 63;
    if (tail != 0) {
      live += Long.bitCount(liveBits[fullWords] & ((1L << tail) - 1L));
    }
    return live;
  }

  // ───────────────────────────────────────────────────── plain-long kernels

  /**
   * Inclusive-bound count over {@code PLAIN_LONG} values, compared in place.
   *
   * <p>
   * The loop body is a subtract and one unsigned compare: see the class comment on why the two-sided
   * test collapses, and why no operator crosses this method's boundary.
   */
  static long countPlain(final MemorySegment payload, final int valueBytesOffset, final int start, final int end,
      final long lo, final long hi, final long[] liveBits) {
    long count = 0;
    int i = start;
    if (BitUnpackSimd.vectorProfitable(end - start)) {
      final LongVector loV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, lo);
      final LongVector spanV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, hi - lo);
      for (; i <= end - LANES; i += LANES) {
        final long byteOff = (long) valueBytesOffset + (long) i * Long.BYTES;
        if (!ColumnLoad.canLoad(payload, byteOff)) {
          break;
        }
        VectorMask<Long> m = ColumnLoad.loadWords(payload, byteOff).sub(loV).compare(VectorOperators.ULE, spanV);
        if (liveBits != null) {
          m = m.and(laneMask(liveBits, i - start));
        }
        count += m.trueCount();
      }
    }
    for (; i < end; i++) {
      if (liveBits != null && !isLive(liveBits, i - start)) {
        continue;
      }
      final long v = plainAt(payload, valueBytesOffset, i);
      if (v >= lo && v <= hi) {
        count++;
      }
    }
    return count;
  }

  // ───────────────────────────────────────────────────── bit-packed kernels

  /** Inclusive-bound count over bit-packed values. {@code -1} when the width is out of range. */
  static long countPacked(final MemorySegment payload, final int valueBytesOffset, final long valueBase,
      final int bitWidth, final int start, final int end, final long lo, final long hi, final long[] liveBits) {
    if (!BitUnpackSimd.supports(bitWidth)) {
      return -1L;
    }
    long count = 0;
    int i = start;
    // The plan is built only once the vector path is actually going to run: constructing a width's
    // phase tables costs milliseconds on a cold JVM, which is not a bill a scalar scan should get.
    if (BitUnpackSimd.vectorProfitable(end - start)) {
      final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(bitWidth);
      // Fold the frame-of-reference base into the bound instead of adding it to every value: the
      // comparison is a shift of the same interval, so the base never has to touch the data.
      final LongVector loV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, lo - valueBase);
      final LongVector spanV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, hi - lo);
      final int lastGroup = BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), valueBytesOffset, bitWidth);
      for (; i <= end - LANES && i <= lastGroup; i += LANES) {
        VectorMask<Long> m = plan.unpack(payload, valueBytesOffset, i).sub(loV).compare(VectorOperators.ULE, spanV);
        if (liveBits != null) {
          // Shadowed lanes are unpacked and compared like any other. Skipping them would
          // reintroduce the per-value branch this kernel exists to remove.
          m = m.and(laneMask(liveBits, i - start));
        }
        count += m.trueCount();
      }
    }
    final long mask = BitUnpackSimd.maskFor(bitWidth);
    for (; i < end; i++) {
      if (liveBits != null && !isLive(liveBits, i - start)) {
        continue;
      }
      final long v = valueBase + BitUnpackSimd.decodeAt(payload, valueBytesOffset, bitWidth, mask, i);
      if (v >= lo && v <= hi) {
        count++;
      }
    }
    return count;
  }

  // ─────────────────────────────────── operator-shaped entry points (tests, callers)

  /** Single-predicate count over {@code PLAIN_LONG} values. */
  public static long countPlainLong(final MemorySegment payload, final int valueBytesOffset, final int start,
      final int end, final VectorOperators.Comparison op, final long threshold) {
    return countPlainLongRange(payload, valueBytesOffset, start, end, op, threshold, op, threshold);
  }

  /** Two-predicate AND over {@code PLAIN_LONG} values. */
  public static long countPlainLongRange(final MemorySegment payload, final int valueBytesOffset, final int start,
      final int end, final VectorOperators.Comparison op1, final long threshold1, final VectorOperators.Comparison op2,
      final long threshold2) {
    return countPlainLongRangeMasked(payload, valueBytesOffset, start, end, op1, threshold1, op2, threshold2, null);
  }

  /** Masked two-predicate AND over {@code PLAIN_LONG} values. */
  public static long countPlainLongRangeMasked(final MemorySegment payload, final int valueBytesOffset, final int start,
      final int end, final VectorOperators.Comparison op1, final long threshold1, final VectorOperators.Comparison op2,
      final long threshold2, final long[] liveBits) {
    if (isEmptyPredicate(op1, threshold1) || isEmptyPredicate(op2, threshold2)) {
      return 0L;
    }
    final boolean ne1 = op1 == VectorOperators.NE;
    final boolean ne2 = op2 == VectorOperators.NE;
    if (ne1 || ne2) {
      return countPlainWithExclusions(payload, valueBytesOffset, start, end, ne1, op1, threshold1, ne2, op2, threshold2,
          liveBits);
    }
    final long lo = Math.max(loBound(op1, threshold1), loBound(op2, threshold2));
    final long hi = Math.min(hiBound(op1, threshold1), hiBound(op2, threshold2));
    return lo > hi
        ? 0L
        : countPlain(payload, valueBytesOffset, start, end, lo, hi, liveBits);
  }

  /**
   * Inclusion-exclusion over a plain column, keeping BOTH predicates: the interval the non-NE side
   * admits, minus the values inside it that the NE side excludes.
   *
   * <p>
   * Same decomposition as the bit-packed twin. This arm once dropped to a scalar loop while the other
   * decomposed, so the two encodings of one column could return different answers for one predicate.
   */
  private static long countPlainWithExclusions(final MemorySegment payload, final int valueBytesOffset, final int start,
      final int end, final boolean ne1, final VectorOperators.Comparison op1, final long threshold1, final boolean ne2,
      final VectorOperators.Comparison op2, final long threshold2, final long[] liveBits) {
    long lo = Long.MIN_VALUE;
    long hi = Long.MAX_VALUE;
    if (!ne1) {
      lo = Math.max(lo, loBound(op1, threshold1));
      hi = Math.min(hi, hiBound(op1, threshold1));
    }
    if (!ne2) {
      lo = Math.max(lo, loBound(op2, threshold2));
      hi = Math.min(hi, hiBound(op2, threshold2));
    }
    if (lo > hi) {
      return 0L;
    }
    long result = countPlain(payload, valueBytesOffset, start, end, lo, hi, liveBits);
    if (ne1 && threshold1 >= lo && threshold1 <= hi) {
      result -= countPlain(payload, valueBytesOffset, start, end, threshold1, threshold1, liveBits);
    }
    // Only subtract the second exclusion when it is a different value, or the values excluded by
    // both would come off twice.
    if (ne2 && threshold2 >= lo && threshold2 <= hi && !(ne1 && threshold1 == threshold2)) {
      result -= countPlain(payload, valueBytesOffset, start, end, threshold2, threshold2, liveBits);
    }
    return result;
  }

  /** Single-predicate count over bit-packed values. {@code -1} when the width is out of range. */
  public static long countBitPacked(final MemorySegment payload, final int valueBytesOffset, final long valueBase,
      final int bitWidth, final int start, final int end, final VectorOperators.Comparison op, final long threshold) {
    return countBitPackedRange(payload, valueBytesOffset, valueBase, bitWidth, start, end, op, threshold, op,
        threshold);
  }

  /** Two-predicate AND over bit-packed values. {@code -1} when the width is out of range. */
  public static long countBitPackedRange(final MemorySegment payload, final int valueBytesOffset, final long valueBase,
      final int bitWidth, final int start, final int end, final VectorOperators.Comparison op1, final long threshold1,
      final VectorOperators.Comparison op2, final long threshold2) {
    return countBitPackedRangeMasked(payload, valueBytesOffset, valueBase, bitWidth, start, end, op1, threshold1, op2,
        threshold2, null);
  }

  /** Masked two-predicate AND over bit-packed values. {@code -1} when the width is out of range. */
  public static long countBitPackedRangeMasked(final MemorySegment payload, final int valueBytesOffset,
      final long valueBase, final int bitWidth, final int start, final int end, final VectorOperators.Comparison op1,
      final long threshold1, final VectorOperators.Comparison op2, final long threshold2, final long[] liveBits) {
    if (!BitUnpackSimd.supports(bitWidth)) {
      return -1L;
    }
    if (isEmptyPredicate(op1, threshold1) || isEmptyPredicate(op2, threshold2)) {
      return 0L;
    }
    final boolean ne1 = op1 == VectorOperators.NE;
    final boolean ne2 = op2 == VectorOperators.NE;
    if (ne1 || ne2) {
      return countPackedWithExclusions(payload, valueBytesOffset, valueBase, bitWidth, start, end, ne1, op1, threshold1,
          ne2, op2, threshold2, liveBits);
    }
    final long lo = Math.max(loBound(op1, threshold1), loBound(op2, threshold2));
    final long hi = Math.min(hiBound(op1, threshold1), hiBound(op2, threshold2));
    return lo > hi
        ? 0L
        : countPacked(payload, valueBytesOffset, valueBase, bitWidth, start, end, lo, hi, liveBits);
  }

  /**
   * Inclusion-exclusion over a bit-packed column, keeping BOTH predicates: the interval the non-NE
   * side admits, minus the values inside it that the NE side excludes.
   *
   * <p>
   * An earlier version computed the kept set over the unbounded range and subtracted only the
   * exclusions, which silently discarded the other predicate entirely — {@code v != 5 AND v < 50}
   * answered {@code n - count(== 5)}.
   */
  private static long countPackedWithExclusions(final MemorySegment payload, final int valueBytesOffset,
      final long valueBase, final int bitWidth, final int start, final int end, final boolean ne1,
      final VectorOperators.Comparison op1, final long threshold1, final boolean ne2,
      final VectorOperators.Comparison op2, final long threshold2, final long[] liveBits) {
    long lo = Long.MIN_VALUE;
    long hi = Long.MAX_VALUE;
    if (!ne1) {
      lo = Math.max(lo, loBound(op1, threshold1));
      hi = Math.min(hi, hiBound(op1, threshold1));
    }
    if (!ne2) {
      lo = Math.max(lo, loBound(op2, threshold2));
      hi = Math.min(hi, hiBound(op2, threshold2));
    }
    if (lo > hi) {
      return 0L;
    }
    long result = countPacked(payload, valueBytesOffset, valueBase, bitWidth, start, end, lo, hi, liveBits);
    if (result < 0) {
      return -1L;
    }
    if (ne1 && threshold1 >= lo && threshold1 <= hi) {
      result -=
          countPacked(payload, valueBytesOffset, valueBase, bitWidth, start, end, threshold1, threshold1, liveBits);
    }
    // Only subtract the second exclusion when it is a different value, or the values excluded by
    // both would come off twice.
    if (ne2 && threshold2 >= lo && threshold2 <= hi && !(ne1 && threshold1 == threshold2)) {
      result -=
          countPacked(payload, valueBytesOffset, valueBase, bitWidth, start, end, threshold2, threshold2, liveBits);
    }
    return result;
  }

  // ─────────────────────────────────────────────────────── aggregate kernels

  /**
   * Compute {@code sum}, {@code min} and {@code max} over {@code [start, end)} in one pass.
   *
   * <p>
   * Three results from one pass because they share the same memory traffic: on a memory-bound kernel
   * the extra arithmetic is free and the loads are not.
   *
   * <p>
   * <b>Output contract</b>: writes {@code out[0]=sum, out[1]=min, out[2]=max}. An empty range writes
   * the identity elements {@code 0 / Long.MAX_VALUE / Long.MIN_VALUE} so a caller folding across
   * pages needs no special case.
   *
   * @return {@code true} when the aggregate was computed, {@code false} for a declined shape
   */
  public static boolean aggregateRange(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final long[] out) {
    if (start >= end) {
      out[0] = 0L;
      out[1] = Long.MAX_VALUE;
      out[2] = Long.MIN_VALUE;
      return true;
    }
    if (h.isPerTag()) {
      return aggregatePerTag(payload, h, start, end, out);
    }
    if (NumberRegion.isDelta(h.encodingKind)) {
      return NumberRegionDeltaSimd.aggregateRange(payload, h, start, end, out);
    }
    if (NumberRegion.isBitPacked(h.encodingKind)) {
      return aggregateBitPacked(payload, h.valueBytesOffset, h.valueBase, h.valueBitWidth, start, end, out);
    }
    aggregatePlainLong(payload, h.valueBytesOffset, start, end, out);
    return true;
  }

  /**
   * Sum/min/max over a per-tag payload.
   *
   * <p>
   * {@code out} doubles as the per-segment destination and is only written with the folded result at
   * the end, so a window spanning several tags still allocates nothing.
   */
  private static boolean aggregatePerTag(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final long[] out) {
    final int limit = Math.min(end, h.count);
    int tag = NumberRegion.tagOfIndex(h, start);
    if (tag < 0) {
      out[0] = 0L;
      out[1] = Long.MAX_VALUE;
      out[2] = Long.MIN_VALUE;
      return true;
    }
    long sum = 0L;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    int cursor = start;
    while (cursor < limit && tag < h.dictSize) {
      final int tagStart = h.tagStart[tag];
      final int segmentEnd = Math.min(limit, tagStart + h.tagCount[tag]);
      final int from = cursor - tagStart;
      final int to = segmentEnd - tagStart;
      if (from < to) {
        final int width = h.tagWidth[tag] & 0xFF;
        if (width == 0) {
          final long constant = h.tagMin[tag];
          out[0] = constant * (to - from);
          out[1] = constant;
          out[2] = constant;
        } else if (width == 64) {
          aggregatePlainLong(payload, h.tagValueOffset[tag], from, to, out);
        } else if (!aggregateBitPacked(payload, h.tagValueOffset[tag], h.tagDecodeBase[tag], width, from, to, out)) {
          return false;
        }
        sum += out[0];
        if (out[1] < min) {
          min = out[1];
        }
        if (out[2] > max) {
          max = out[2];
        }
      }
      cursor = segmentEnd;
      tag++;
    }
    out[0] = sum;
    out[1] = min;
    out[2] = max;
    return true;
  }

  /** Vectorized sum/min/max over {@code PLAIN_LONG} values. */
  private static void aggregatePlainLong(final MemorySegment payload, final int valueBytesOffset, final int start,
      final int end, final long[] out) {
    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    int i = start;
    if (BitUnpackSimd.vectorProfitable(end - start)) {
      LongVector sumV = LongVector.zero(ColumnLoad.LONG_SPECIES);
      LongVector minV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, Long.MAX_VALUE);
      LongVector maxV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, Long.MIN_VALUE);
      boolean any = false;
      for (; i <= end - LANES; i += LANES) {
        final long byteOff = (long) valueBytesOffset + (long) i * Long.BYTES;
        if (!ColumnLoad.canLoad(payload, byteOff)) {
          break;
        }
        final LongVector v = ColumnLoad.loadWords(payload, byteOff);
        sumV = sumV.add(v);
        minV = minV.min(v);
        maxV = maxV.max(v);
        any = true;
      }
      if (any) {
        sum = sumV.reduceLanes(VectorOperators.ADD);
        min = minV.reduceLanes(VectorOperators.MIN);
        max = maxV.reduceLanes(VectorOperators.MAX);
      }
    }
    for (; i < end; i++) {
      final long v = plainAt(payload, valueBytesOffset, i);
      sum += v;
      if (v < min) {
        min = v;
      }
      if (v > max) {
        max = v;
      }
    }
    out[0] = sum;
    out[1] = min;
    out[2] = max;
  }

  /** Vectorized sum/min/max over bit-packed values. {@code false} when the width is out of range. */
  private static boolean aggregateBitPacked(final MemorySegment payload, final int valueBytesOffset,
      final long valueBase, final int bitWidth, final int start, final int end, final long[] out) {
    if (!BitUnpackSimd.supports(bitWidth)) {
      return false;
    }
    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    int i = start;
    if (BitUnpackSimd.vectorProfitable(end - start)) {
      final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(bitWidth);
      LongVector sumV = LongVector.zero(ColumnLoad.LONG_SPECIES);
      LongVector minV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, Long.MAX_VALUE);
      LongVector maxV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, Long.MIN_VALUE);
      final int lastGroup = BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), valueBytesOffset, bitWidth);
      int groups = 0;
      for (; i <= end - LANES && i <= lastGroup; i += LANES) {
        // Residuals aggregate without the base; it is folded back once at the end, which keeps an
        // add off every lane of every group.
        final LongVector v = plan.unpack(payload, valueBytesOffset, i);
        sumV = sumV.add(v);
        minV = minV.min(v);
        maxV = maxV.max(v);
        groups++;
      }
      if (groups > 0) {
        sum = sumV.reduceLanes(VectorOperators.ADD) + valueBase * (long) groups * LANES;
        min = minV.reduceLanes(VectorOperators.MIN) + valueBase;
        max = maxV.reduceLanes(VectorOperators.MAX) + valueBase;
      }
    }
    final long mask = BitUnpackSimd.maskFor(bitWidth);
    for (; i < end; i++) {
      final long v = valueBase + BitUnpackSimd.decodeAt(payload, valueBytesOffset, bitWidth, mask, i);
      sum += v;
      if (v < min) {
        min = v;
      }
      if (v > max) {
        max = v;
      }
    }
    out[0] = sum;
    out[1] = min;
    out[2] = max;
    return true;
  }

  // ──────────────────────────────────────────────────────── selection output

  /**
   * Write the indices of matching values into {@code outIndices}, rather than only counting them.
   *
   * <p>
   * A counting kernel can only answer counting queries; everything downstream of a filter — a
   * projection, a join probe, a group-by — needs to know <em>which</em> rows survived. This produces
   * that selection vector directly from the encoded column, using {@link LongVector#compress} so
   * surviving lanes are packed without a per-lane branch. It is the same shape DuckDB and Umbra push
   * between pipeline operators.
   *
   * <p>
   * Indices are absolute and ascending.
   *
   * @param outIndices destination, must hold at least {@code end - start} entries
   * @return the number of matches written, or {@code -1} for a shape these kernels decline
   */
  /** Per-thread index scratch, so the bitmap form of a selection allocates nothing per call. */
  private static final ThreadLocal<int[]> SELECT_INDEX_SCRATCH =
      ThreadLocal.withInitial(() -> new int[PageLayout.SLOT_COUNT]);

  /**
   * Write matching rows into a selection BITMAP rather than an index vector.
   *
   * <p>
   * Two representations coexist deliberately, and this bridges them. An index vector is the better
   * shape for what comes AFTER a filter — a projection, a join probe — and is what DuckDB and Umbra
   * push between pipeline operators, which is why {@link #selectMatching} produces one. A bitmap is
   * the better shape for building the filter itself: {@code AND}, {@code OR} and {@code NOT} are word
   * operations, liveness is already a bitmap, and the boolean column IS one. So predicate trees
   * compose in bitmaps and materialisation converts once, at the end.
   *
   * <p>
   * Delegates to {@link #selectMatching} rather than repeating the unpack loop a fourth time: every
   * encoding it understands — including the delta recurrence that cannot be range-tested in place —
   * is understood here for free, and a shape it declines is declined here too.
   *
   * @param rowBits tag-local destination, zeroed for {@code n} bits before writing
   * @return the number of rows selected, or {@code -1} for a shape these kernels decline
   */
  public static int selectRangeInto(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int n, final VectorOperators.Comparison op1, final long threshold1, final VectorOperators.Comparison op2,
      final long threshold2, final long[] rowBits) {
    final int words = (n + 63) >>> 6;
    if (rowBits.length < words) {
      throw new IllegalArgumentException("target bitmap too small: " + rowBits.length + " words for " + n + " bits");
    }
    java.util.Arrays.fill(rowBits, 0, words, 0L);
    if (n <= 0) {
      return 0;
    }
    final int[] idx = SELECT_INDEX_SCRATCH.get();
    if (idx.length < n) {
      return -1; // a page wider than the scratch: decline rather than allocate on a scan path
    }
    final int matches = selectMatching(payload, h, start, start + n, op1, threshold1, op2, threshold2, idx);
    if (matches < 0) {
      return -1;
    }
    for (int k = 0; k < matches; k++) {
      final int local = idx[k] - start; // selectMatching reports ABSOLUTE indices
      rowBits[local >>> 6] |= 1L << (local & 63);
    }
    return matches;
  }

  public static int selectMatching(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final VectorOperators.Comparison op1, final long threshold1, final VectorOperators.Comparison op2,
      final long threshold2, final int[] outIndices) {
    requireSelectionBuffer(outIndices, end - start);
    if (op1 == VectorOperators.NE || op2 == VectorOperators.NE) {
      return -1;
    }
    if (isEmptyPredicate(op1, threshold1) || isEmptyPredicate(op2, threshold2)) {
      return 0;
    }
    final long lo = Math.max(loBound(op1, threshold1), loBound(op2, threshold2));
    final long hi = Math.min(hiBound(op1, threshold1), hiBound(op2, threshold2));
    if (lo > hi) {
      return 0;
    }
    if (h.isPerTag()) {
      return selectPerTag(payload, h, start, end, lo, hi, outIndices);
    }
    if (NumberRegion.isDelta(h.encodingKind)) {
      // A delta column cannot be range-tested in place — the stored values are a recurrence — but
      // the block replay the counting kernel already performs yields them in order, and the
      // selection falls out of the same pass. Fused plans over delta-encoded columns serve
      // columnar through this instead of declining to the record path.
      return NumberRegionDeltaSimd.selectRange(payload, h, start, end, lo, hi, outIndices);
    }
    final boolean packed = NumberRegion.isBitPacked(h.encodingKind);
    final BitUnpackSimd.Plan plan = packed
        ? BitUnpackSimd.planFor(h.valueBitWidth)
        : null;
    if (packed && plan == null) {
      return -1;
    }
    final long base = packed
        ? h.valueBase
        : 0L;
    return selectWindow(payload, h.valueBytesOffset, h.valueBitWidth & 0xFF, packed, plan, base, lo, hi, start, end,
        outIndices, 0);
  }

  /**
   * Selection over a per-tag payload: one framed window per tag, indices rebased to the region as
   * each segment finishes.
   *
   * <p>
   * Indices come out of the per-tag kernels relative to the tag, and ascending; adding the tag's
   * start restores the region-absolute, ascending order the contract promises, and does it over the
   * slice just written rather than by branching inside the vector loop.
   */
  private static int selectPerTag(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final long lo, final long hi, final int[] outIndices) {
    final int limit = Math.min(end, h.count);
    int tag = NumberRegion.tagOfIndex(h, start);
    if (tag < 0) {
      return 0;
    }
    int out = 0;
    int cursor = start;
    while (cursor < limit && tag < h.dictSize) {
      final int tagStart = h.tagStart[tag];
      final int segmentEnd = Math.min(limit, tagStart + h.tagCount[tag]);
      final int from = cursor - tagStart;
      final int to = segmentEnd - tagStart;
      final int before = out;
      if (from < to) {
        final int width = h.tagWidth[tag] & 0xFF;
        if (width == 0) {
          final long constant = h.tagMin[tag];
          if (constant >= lo && constant <= hi) {
            for (int i = from; i < to; i++) {
              outIndices[out++] = i;
            }
          }
        } else {
          final boolean packed = width != 64;
          final BitUnpackSimd.Plan plan = packed
              ? BitUnpackSimd.planFor(width)
              : null;
          if (packed && plan == null) {
            return -1;
          }
          out = selectWindow(payload, h.tagValueOffset[tag], width, packed, plan, packed
              ? h.tagDecodeBase[tag]
              : 0L, lo, hi, from, to, outIndices, out);
        }
        for (int k = before; k < out; k++) {
          outIndices[k] += tagStart;
        }
      }
      cursor = segmentEnd;
      tag++;
    }
    return out;
  }

  /** Vector body then scalar tail over one framed window, appending after {@code written}. */
  private static int selectWindow(final MemorySegment payload, final int valueBytesOffset, final int bitWidth,
      final boolean packed, final BitUnpackSimd.Plan plan, final long base, final long lo, final long hi,
      final int start, final int end, final int[] outIndices, final int written) {
    int out = written;
    int i = start;
    if (BitUnpackSimd.vectorProfitable(end - start)) {
      final long progress = selectMatchingVector(payload, valueBytesOffset, bitWidth, packed, plan, base, lo, hi, start,
          end, outIndices, written);
      i = (int) (progress >>> 32);
      out = (int) progress;
    }
    return selectMatchingScalar(payload, valueBytesOffset, packed, plan, base, lo, hi, i, end, out, outIndices);
  }

  /** Reject a selection buffer that cannot hold every row of the window before any work is done. */
  private static void requireSelectionBuffer(final int[] outIndices, final int needed) {
    if (outIndices == null || outIndices.length < needed) {
      throw new IllegalArgumentException("selection buffer too small: " + (outIndices == null
          ? -1
          : outIndices.length) + " < " + needed);
    }
  }

  /**
   * Vector body of {@link #selectMatching}: the resume index in the high word, the number of indices
   * written in the low one.
   */
  private static long selectMatchingVector(final MemorySegment payload, final int valueBytesOffset, final int bitWidth,
      final boolean packed, final BitUnpackSimd.Plan plan, final long base, final long lo, final long hi,
      final int start, final int end, final int[] outIndices, final int written) {
    int out = written;
    int i = start;
    final LongVector loV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, lo - base);
    final LongVector spanV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, hi - lo);
    final int lastGroup = packed
        ? BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), valueBytesOffset, bitWidth)
        : Integer.MAX_VALUE;
    // Lane ordinals, added to the group's start index to turn a lane position into a row index.
    final LongVector laneIota = LongVector.zero(ColumnLoad.LONG_SPECIES).addIndex(1);
    for (; i <= end - LANES && i <= lastGroup; i += LANES) {
      final LongVector v;
      if (packed) {
        v = plan.unpack(payload, valueBytesOffset, i);
      } else {
        final long byteOff = (long) valueBytesOffset + (long) i * Long.BYTES;
        if (!ColumnLoad.canLoad(payload, byteOff)) {
          break;
        }
        v = ColumnLoad.loadWords(payload, byteOff);
      }
      final VectorMask<Long> m = v.sub(loV).compare(VectorOperators.ULE, spanV);
      final int matched = m.trueCount();
      if (matched != 0) {
        // compress() packs the set lanes down to lanes 0..matched-1 in order, so the indices land
        // contiguously and ascending with no per-lane test.
        final LongVector idx = laneIota.add((long) i).compress(m);
        for (int lane = 0; lane < matched; lane++) {
          outIndices[out++] = (int) idx.lane(lane);
        }
      }
    }
    return ((long) i << 32) | out;
  }

  /** Scalar tail of {@link #selectMatching}, resuming at {@code i}; answers the total written. */
  private static int selectMatchingScalar(final MemorySegment payload, final int valueBytesOffset, final boolean packed,
      final BitUnpackSimd.Plan plan, final long base, final long lo, final long hi, final int from, final int end,
      final int written, final int[] outIndices) {
    int out = written;
    for (int i = from; i < end; i++) {
      final long v = packed
          ? base + plan.decodeAt(payload, valueBytesOffset, i)
          : plainAt(payload, valueBytesOffset, i);
      if (v >= lo && v <= hi) {
        outIndices[out++] = i;
      }
    }
    return out;
  }

  // ────────────────────────────────────────────────────────────────── helpers

  /**
   * Lowest value the predicate admits.
   *
   * <p>
   * {@code GT} at {@link Long#MAX_VALUE} would overflow; {@link #isEmptyPredicate} takes that case
   * out before this is reached.
   */
  private static long loBound(final VectorOperators.Comparison op, final long threshold) {
    if (op == VectorOperators.GT) {
      return threshold + 1;
    }
    if (op == VectorOperators.GE || op == VectorOperators.EQ) {
      return threshold;
    }
    return Long.MIN_VALUE;
  }

  /** Highest value the predicate admits. */
  private static long hiBound(final VectorOperators.Comparison op, final long threshold) {
    if (op == VectorOperators.LT) {
      return threshold - 1;
    }
    if (op == VectorOperators.LE || op == VectorOperators.EQ) {
      return threshold;
    }
    return Long.MAX_VALUE;
  }

  /** The two predicates no value can satisfy, which are also the two that would overflow a bound. */
  private static boolean isEmptyPredicate(final VectorOperators.Comparison op, final long threshold) {
    return (op == VectorOperators.GT && threshold == Long.MAX_VALUE)
        || (op == VectorOperators.LT && threshold == Long.MIN_VALUE);
  }

  /** One {@code PLAIN_LONG} value, read little-endian out of the payload. */
  private static long plainAt(final MemorySegment payload, final int valueBytesOffset, final int index) {
    return BitUnpackSimd.readWordSafe(payload, (long) valueBytesOffset + (long) index * Long.BYTES);
  }

  /** Whether value {@code start + relativeIndex} is live, per a relative-indexed liveness bitmap. */
  private static boolean isLive(final long[] liveBits, final int relativeIndex) {
    return (liveBits[relativeIndex >>> 6] & (1L << (relativeIndex & 63))) != 0L;
  }

  /**
   * Lane mask for the {@link #LANES} values starting at relative index {@code r}.
   *
   * <p>
   * One shift, no straddle: {@code r} is always a multiple of {@code LANES}, and {@code LANES}
   * divides 64, so the whole group lives in {@code liveBits[r >>> 6]}.
   */
  private static VectorMask<Long> laneMask(final long[] liveBits, final int r) {
    return ColumnLoad.laneMask(liveBits[r >>> 6] >>> (r & 63));
  }

  /** Scalar equivalent of a lane comparison, for the paths that keep an operator. */
  static boolean eval(final long v, final VectorOperators.Comparison op, final long t) {
    if (op == VectorOperators.GT) {
      return v > t;
    }
    if (op == VectorOperators.LT) {
      return v < t;
    }
    if (op == VectorOperators.GE) {
      return v >= t;
    }
    if (op == VectorOperators.LE) {
      return v <= t;
    }
    if (op == VectorOperators.EQ) {
      return v == t;
    }
    if (op == VectorOperators.NE) {
      return v != t;
    }
    throw new IllegalArgumentException("unsupported op: " + op);
  }
}
