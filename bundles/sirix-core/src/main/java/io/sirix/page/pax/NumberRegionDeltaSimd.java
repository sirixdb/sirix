/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;

/**
 * SIMD kernels over {@link NumberRegion}'s delta-of-delta encoding.
 *
 * <h2>Why a sequential codec gets vector kernels at all</h2>
 *
 * <p>Delta-of-delta reconstructs a value from every value before it: {@code delta += dd; value +=
 * delta}. That is a recurrence, and a recurrence is the textbook reason a codec is called
 * "unvectorizable" — which is exactly what this package used to conclude. Every kernel returned
 * {@code -1} for {@code ENC_DELTA_ZM} and the caller fell back to
 * {@link NumberRegion#decodeValueAt}, which for delta replays the prefix sum from the start of the
 * region <em>per value</em>. Scanning a tag's range was therefore quadratic: the encoding that
 * wins the most space — temporal columns, commit timestamps, monotonic ids, precisely the columns
 * a bitemporal store is full of — was the one the scan handled worst.
 *
 * <p>The recurrence is real but it is not an obstacle, because a prefix sum is a parallel-prefix
 * problem, not a serial one. A running sum over {@code L} lanes is computed in
 * {@code log2(L)} shift-and-add steps (Hillis–Steele), and delta-of-delta simply needs it twice:
 * once to turn residuals into deltas, once to turn deltas into values. Both DuckDB and BtrBlocks
 * vectorize their RLE and FOR-delta variants the same way.
 *
 * <h2>The arithmetic</h2>
 *
 * <p>Entering a group of {@code L} residuals with {@code carryValue} = the previous value and
 * {@code carryDelta} = the previous delta, let {@code P} be the running sum of the group's
 * delta-of-deltas and {@code S} the running sum of {@code P}. Then for lane {@code j}:
 *
 * <pre>
 *   delta[j] = carryDelta + P[j]
 *   value[j] = carryValue + (j + 1) * carryDelta + S[j]
 * </pre>
 *
 * <p>so the whole group is two parallel prefix sums, a multiply by the lane ordinal and two adds.
 * The carries for the next group are the last lane of each, which keeps groups strictly ordered
 * but makes each group internally parallel — the recurrence survives only between groups, at
 * {@code 1/L} of its original frequency.
 *
 * <h2>Decode-then-reduce, not decode-per-kernel</h2>
 *
 * <p>Kernels here reconstruct into a small reusable block and run the reduction over that block,
 * rather than each re-implementing the replay. The block is 512 values — 4 KiB, comfortably L1 —
 * so the values are still hot when the reduction reads them, and the replay exists once instead of
 * once per predicate shape. This is the same staging DuckDB uses between a decompression primitive
 * and the operator that consumes it, and the reason it costs nothing is that the intermediate
 * never leaves cache.
 *
 * <p>Unlike the other encodings, a delta scan cannot start in the middle: reaching index
 * {@code start} means replaying everything before it. That replay is vectorized too, so it costs
 * bandwidth rather than the quadratic re-decode it used to.
 */
public final class NumberRegionDeltaSimd {

  private static final int LANES = ColumnLoad.LANES;

  /** Values reconstructed per staging block. 4 KiB — large enough to amortize, small enough for L1. */
  private static final int BLOCK = 512;

  /** Lane ordinals plus one, i.e. how many deltas each lane has accumulated within its group. */
  private static final LongVector LANE_ORDINAL_PLUS_ONE =
      LongVector.zero(ColumnLoad.LONG_SPECIES).addIndex(1).add(1L);

  /**
   * Per-thread staging block for the reconstructed values.
   *
   * <p>Thread-local rather than allocated per call: a scan crosses thousands of pages, and a fresh
   * 4 KiB array per page is garbage a query does not need to make. Rather than a parameter on every
   * kernel, which would push the buffer's lifetime onto every caller.
   */
  private static final ThreadLocal<long[]> STAGING = ThreadLocal.withInitial(() -> new long[BLOCK]);

  private NumberRegionDeltaSimd() {
  }

  /**
   * Replay position: the values already produced, and the recurrence state needed for the next.
   *
   * <p>{@code value} and {@code delta} describe index {@code nextIndex - 1}.
   */
  private static final class Cursor {
    int nextIndex;
    long value;
    long delta;
  }

  // ─────────────────────────────────────────────────────────────── kernels

  /**
   * Count values in {@code [start, end)} within the inclusive bound {@code [lo, hi]}, optionally
   * restricted to the values a liveness bitmap still owns.
   *
   * <p>Takes a bound rather than comparison operators for the reason spelled out in
   * {@link NumberRegionSimd}: an operator arriving as a parameter cannot be constant-folded, and
   * the vector compare degrades to a generic path when it is not.
   *
   * @param liveBits relative-indexed liveness bitmap, or {@code null} when nothing is shadowed
   */
  static long countRange(final MemorySegment payload, final NumberRegion.Header h, final int start,
      final int end, final long lo, final long hi, final long[] liveBits) {
    final NumberRegionDelta.Header dh = h.deltaHeader;
    if (dh == null) {
      return -1L;
    }
    if (start >= end || lo > hi) {
      return 0L;
    }
    final long[] block = STAGING.get();
    final Cursor cursor = new Cursor();
    long count = 0;

    while (cursor.nextIndex < end) {
      final int blockStart = cursor.nextIndex;
      final int produced = fill(payload, dh, cursor, block, Math.min(BLOCK, end - blockStart));
      if (produced <= 0) {
        break;
      }
      // Values before `start` are replayed but not tested — the recurrence needs them, the
      // predicate does not.
      final int from = Math.max(0, start - blockStart);
      count += reduceCount(block, from, produced, blockStart, start, lo, hi, liveBits);
    }
    return count;
  }

  /** Count matches within one reconstructed block, vectorized over the staging buffer. */
  private static long reduceCount(final long[] block, final int from, final int to,
      final int blockStart, final int start, final long lo, final long hi, final long[] liveBits) {
    long count = 0;
    int i = from;
    // Extracting a lane mask with one shift requires the group not to straddle two words of the
    // bitmap, which holds exactly when the relative index is a multiple of the lane count. The
    // first block containing `start` satisfies that at i == from; a later block need not, so walk
    // scalar until it does rather than silently reading the wrong bits.
    if (liveBits != null) {
      while (i < to && (((blockStart + i - start) & (LANES - 1)) != 0)) {
        final int r = blockStart + i - start;
        if ((liveBits[r >>> 6] & (1L << (r & 63))) != 0L) {
          final long v = block[i];
          if (v >= lo && v <= hi) {
            count++;
          }
        }
        i++;
      }
    }
    if (BitUnpackSimd.vectorProfitable(to - i)) {
      final LongVector loV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, lo);
      final LongVector spanV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, hi - lo);
      for (; i <= to - LANES; i += LANES) {
        VectorMask<Long> m = LongVector.fromArray(ColumnLoad.LONG_SPECIES, block, i)
                                       .sub(loV)
                                       .compare(VectorOperators.ULE, spanV);
        if (liveBits != null) {
          final int r = blockStart + i - start;
          m = m.and(VectorMask.fromLong(ColumnLoad.LONG_SPECIES, liveBits[r >>> 6] >>> (r & 63)));
        }
        count += m.trueCount();
      }
    }
    for (; i < to; i++) {
      if (liveBits != null) {
        final int r = blockStart + i - start;
        if ((liveBits[r >>> 6] & (1L << (r & 63))) == 0L) {
          continue;
        }
      }
      final long v = block[i];
      if (v >= lo && v <= hi) {
        count++;
      }
    }
    return count;
  }

  /**
   * Compute {@code sum}, {@code min} and {@code max} over {@code [start, end)}.
   *
   * @return {@code true} on success, {@code false} when the header carries no delta state
   */
  public static boolean aggregateRange(final MemorySegment payload, final NumberRegion.Header h,
      final int start, final int end, final long[] out) {
    if (out == null || out.length < 3) {
      throw new IllegalArgumentException(
          "aggregate output needs 3 slots (sum, min, max), got "
              + (out == null ? "null" : String.valueOf(out.length)));
    }
    final NumberRegionDelta.Header dh = h.deltaHeader;
    if (dh == null) {
      return false;
    }
    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    if (start < end) {
      final long[] block = STAGING.get();
      final Cursor cursor = new Cursor();
      while (cursor.nextIndex < end) {
        final int blockStart = cursor.nextIndex;
        final int produced = fill(payload, dh, cursor, block, Math.min(BLOCK, end - blockStart));
        if (produced <= 0) {
          break;
        }
        final int from = Math.max(0, start - blockStart);
        int i = from;
        if (BitUnpackSimd.vectorProfitable(produced - from)) {
          LongVector sumV = LongVector.zero(ColumnLoad.LONG_SPECIES);
          LongVector minV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, Long.MAX_VALUE);
          LongVector maxV = LongVector.broadcast(ColumnLoad.LONG_SPECIES, Long.MIN_VALUE);
          for (; i <= produced - LANES; i += LANES) {
            final LongVector v = LongVector.fromArray(ColumnLoad.LONG_SPECIES, block, i);
            sumV = sumV.add(v);
            minV = minV.min(v);
            maxV = maxV.max(v);
          }
          sum += sumV.reduceLanes(VectorOperators.ADD);
          min = Math.min(min, minV.reduceLanes(VectorOperators.MIN));
          max = Math.max(max, maxV.reduceLanes(VectorOperators.MAX));
        }
        for (; i < produced; i++) {
          final long v = block[i];
          sum += v;
          if (v < min) {
            min = v;
          }
          if (v > max) {
            max = v;
          }
        }
      }
    }
    out[0] = sum;
    out[1] = min;
    out[2] = max;
    return true;
  }

  /**
   * Reconstruct up to {@code n} consecutive values starting at {@code cursor.nextIndex}.
   *
   * <p>Advances {@code cursor} past what it produced, so successive calls stream the column.
   *
   * @return the number of values written to {@code out}, which is less than {@code n} only at the
   *         end of the column
   */
  private static int fill(final MemorySegment payload, final NumberRegionDelta.Header dh,
      final Cursor cursor, final long[] out, final int n) {
    final int limit = Math.min(n, dh.count - cursor.nextIndex);
    if (limit <= 0) {
      return 0;
    }
    int produced = 0;

    // The two anchors are stored raw and seed the recurrence.
    if (cursor.nextIndex == 0) {
      cursor.value = dh.firstValue;
      cursor.delta = 0L;
      out[produced++] = cursor.value;
      cursor.nextIndex = 1;
    }
    if (produced < limit && cursor.nextIndex == 1) {
      cursor.delta = dh.firstDelta;
      cursor.value += cursor.delta;
      out[produced++] = cursor.value;
      cursor.nextIndex = 2;
    }
    if (produced >= limit) {
      return produced;
    }

    // A zero residual width means every delta-of-delta is zero: the column is a pure arithmetic
    // sequence and needs no body at all. Worth its own arm because it is the case the encoder was
    // built for (one tick per row) and it reduces to a multiply.
    // The reconstruction is gated on the same warmup budget as the reducers below it. Left
    // ungated it was the one vector loop a short-lived process could reach cold, paying the
    // interpreted-vector cliff the budget exists to avoid on the very encoding this class serves.
    final boolean vectorize = BitUnpackSimd.vectorProfitable(limit - produced);
    if (dh.bitWidth == 0) {
      final LongVector strideV = LANE_ORDINAL_PLUS_ONE.mul(cursor.delta);
      while (vectorize && produced + LANES <= limit) {
        final LongVector v = LongVector.broadcast(ColumnLoad.LONG_SPECIES, cursor.value).add(strideV);
        v.intoArray(out, produced);
        cursor.value = v.lane(LANES - 1);
        cursor.nextIndex += LANES;
        produced += LANES;
      }
      while (produced < limit) {
        cursor.value += cursor.delta;
        out[produced++] = cursor.value;
        cursor.nextIndex++;
      }
      return produced;
    }

    final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(dh.bitWidth);
    final int lastGroup = plan == null
        ? -1
        : BitUnpackSimd.lastVectorGroupStart(payload.byteSize(), dh.bodyOffset, dh.bitWidth);

    while (produced < limit) {
      // Residual index trails the value index by the two raw anchors.
      final int residual = cursor.nextIndex - 2;
      if (vectorize && plan != null && produced + LANES <= limit && residual <= lastGroup
          && residual + LANES <= dh.count - 2) {
        final LongVector dd = unZigZag(plan.unpack(payload, dh.bodyOffset, residual));
        final LongVector deltaRun = prefixSum(dd);
        final LongVector valueRun = prefixSum(deltaRun);
        final LongVector v = LongVector.broadcast(ColumnLoad.LONG_SPECIES, cursor.value)
                                       .add(LANE_ORDINAL_PLUS_ONE.mul(cursor.delta))
                                       .add(valueRun);
        v.intoArray(out, produced);
        cursor.value = v.lane(LANES - 1);
        cursor.delta += deltaRun.lane(LANES - 1);
        cursor.nextIndex += LANES;
        produced += LANES;
      } else {
        final long dd = zigZagDecode(
            BitUnpackSimd.decodeAt(payload, dh.bodyOffset, dh.bitWidth,
                                   BitUnpackSimd.maskFor(dh.bitWidth), residual));
        cursor.delta += dd;
        cursor.value += cursor.delta;
        out[produced++] = cursor.value;
        cursor.nextIndex++;
      }
    }
    return produced;
  }

  // ───────────────────────────────────────────────────────────────── helpers

  /**
   * Inclusive running sum across lanes, in {@code log2(LANES)} shift-and-add steps.
   *
   * <p>Each step adds the vector shifted right by a power of two, zero-filled from the left; after
   * {@code log2(LANES)} steps lane {@code j} holds the sum of lanes {@code 0..j}. The shift is
   * expressed as a slice of a zero vector, which lowers to a lane-align instruction.
   */
  private static LongVector prefixSum(final LongVector v) {
    LongVector running = v;
    for (int shift = 1; shift < LANES; shift <<= 1) {
      running = running.add(
          LongVector.zero(ColumnLoad.LONG_SPECIES).slice(LANES - shift, running));
    }
    return running;
  }

  /** Zig-zag decode across lanes: {@code (x >>> 1) ^ -(x & 1)}. */
  private static LongVector unZigZag(final LongVector zigzag) {
    return zigzag.lanewise(VectorOperators.LSHR, 1L)
                 .lanewise(VectorOperators.XOR, zigzag.and(1L).neg());
  }

  /** Scalar zig-zag decode, for the tail. */
  private static long zigZagDecode(final long zigzag) {
    return (zigzag >>> 1) ^ -(zigzag & 1L);
  }
}
