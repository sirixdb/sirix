/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * SIMD kernels for the two encodings used by {@link NumberRegion}:
 * {@code PLAIN_LONG} and {@code BIT_PACKED}. Each kernel takes a contiguous
 * range of values, a threshold, and a comparison op, and returns the count
 * of values satisfying the predicate. No per-record branches; operators are
 * selected at kernel entry, and the hot loop is 8-lane-wide (AVX-512) or
 * 4-lane-wide (AVX-2) per JVM detection.
 *
 * <p>Why: the scalar {@code NumberRegion.decodeValueAt} path spends ~300 ns
 * per record in a pattern that's ideal for SIMD — fixed-width unpack,
 * integer compare, count. With SIMD, we target 30-60 ns per record, which
 * is the gap between Sirix's scan and DuckDB/ClickHouse.
 *
 * <h2>Supported shapes</h2>
 *
 * <ul>
 *   <li><b>PLAIN_LONG</b>: little-endian 8-byte longs. SIMD via
 *       {@link LongVector#fromMemorySegment(VectorSpecies, MemorySegment, long, ByteOrder)}
 *       — one hardware load per vector, direct compare, {@code trueCount()}.</li>
 *   <li><b>BIT_PACKED</b>: values packed at {@code bitWidth} bits per value,
 *       offset by {@code valueBase}. SIMD via per-lane gather of the containing
 *       64-bit word + lane-wise shift + mask. Works for any {@code bitWidth}
 *       from 1 to 56 (above 56 the cross-word straddle needs an extra load).
 *       Above-56 widths fall back to scalar.</li>
 * </ul>
 *
 * <h2>HFT-grade cost budget</h2>
 *
 * <ul>
 *   <li>Plain-long: one aligned vector load + one SIMD compare + one
 *       popcount = ~1 ns per lane. At 8 lanes = ~0.12 ns/value on AVX-512.
 *       Effective rate: 8 G values/s, bound by memory bandwidth.</li>
 *   <li>Bit-packed (per lane): one 64-bit word load (gather on non-aligned
 *       strides) + shift + mask + compare = ~3-5 ns/lane. Effective rate:
 *       ~1-2 G values/s, well above the scalar ~3 M values/s.</li>
 * </ul>
 */
public final class NumberRegionSimd {

  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
  private static final int LANES = LONG_SPECIES.length();

  private NumberRegionSimd() {
  }

  /**
   * Count values in {@code payload[start..end)} that satisfy
   * {@code value OP threshold} using SIMD over a PLAIN_LONG range.
   *
   * @param payload PAX payload as a {@link MemorySegment} for direct SIMD load
   * @param valueBytesOffset start byte offset of values (from header)
   * @param start first value index (inclusive)
   * @param end last value index (exclusive)
   * @param op vector comparison op
   * @param threshold right-hand side of the compare
   * @return number of values satisfying the predicate
   */
  public static long countPlainLong(final MemorySegment payload, final int valueBytesOffset,
      final int start, final int end, final VectorOperators.Comparison op, final long threshold) {
    final LongVector thr = LongVector.broadcast(LONG_SPECIES, threshold);
    long count = 0;
    int i = start;
    // Vectorized body: process LANES values per iteration.
    final long baseOff = (long) valueBytesOffset + (long) start * 8L;
    for (; i <= end - LANES; i += LANES) {
      final long off = baseOff + (long) (i - start) * 8L;
      final LongVector v =
          LongVector.fromMemorySegment(LONG_SPECIES, payload, off, ByteOrder.LITTLE_ENDIAN);
      final VectorMask<Long> mask = v.compare(op, thr);
      count += mask.trueCount();
    }
    // Scalar tail.
    for (; i < end; i++) {
      final long v = payload.get(ValueLayout.JAVA_LONG_UNALIGNED, (long) valueBytesOffset + (long) i * 8L);
      if (eval(v, op, threshold)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Count values in {@code payload[start..end)} that satisfy
   * {@code (valueBase + unpack(value)) OP threshold} using SIMD over a
   * BIT_PACKED range at the given bit width.
   *
   * <p>Supported widths: 1..56. Above 56 the containing-word straddle logic
   * needs a two-word gather; those widths are rare for real data (values
   * that spread > 56 bits usually land in PLAIN_LONG anyway) so the caller
   * should fall back to scalar {@link NumberRegion#decodeValueAt}.
   *
   * @param payload PAX payload
   * @param valueBytesOffset start byte offset of bit-packed values
   * @param valueBase per-page base value to add to each unpacked value
   * @param bitWidth bits per packed value, 1..56
   * @param start first value index (inclusive)
   * @param end last value index (exclusive)
   * @param op vector comparison op
   * @param threshold right-hand side of the compare
   * @return number of values satisfying the predicate; or {@code -1L} if
   *         the bit width exceeds SIMD support (caller falls back)
   */
  public static long countBitPacked(final MemorySegment payload, final int valueBytesOffset,
      final long valueBase, final int bitWidth, final int start, final int end,
      final VectorOperators.Comparison op, final long threshold) {
    if (bitWidth < 1 || bitWidth > 56) {
      return -1L; // out-of-range: caller falls back to scalar
    }
    final long mask = bitWidth == 64 ? ~0L : (1L << bitWidth) - 1L;
    final long[] unpacked = new long[LANES];
    final LongVector thr = LongVector.broadcast(LONG_SPECIES, threshold);
    final LongVector baseV = LongVector.broadcast(LONG_SPECIES, valueBase);
    final long payloadSize = payload.byteSize();
    long count = 0;
    int i = start;
    // Vectorized body: unpack LANES values into a scratch array, load into
    // a LongVector, add base, compare.
    for (; i <= end - LANES; i += LANES) {
      for (int lane = 0; lane < LANES; lane++) {
        final long bitOff = (long) (i + lane) * (long) bitWidth;
        final long byteOff = valueBytesOffset + (bitOff >>> 3);
        final int shift = (int) (bitOff & 7L);
        final long word = readWordSafe(payload, payloadSize, byteOff);
        unpacked[lane] = (word >>> shift) & mask;
      }
      final LongVector v = LongVector.fromArray(LONG_SPECIES, unpacked, 0).add(baseV);
      final VectorMask<Long> m = v.compare(op, thr);
      count += m.trueCount();
    }
    // Scalar tail using the same unpack math.
    for (; i < end; i++) {
      final long bitOff = (long) i * (long) bitWidth;
      final long byteOff = valueBytesOffset + (bitOff >>> 3);
      final int shift = (int) (bitOff & 7L);
      final long word = readWordSafe(payload, payloadSize, byteOff);
      final long v = valueBase + ((word >>> shift) & mask);
      if (eval(v, op, threshold)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Read a little-endian {@code long} from {@code payload} at {@code byteOff}.
   * Fast path: 8 bytes fit fully in the segment — one unaligned load. Tail
   * path: fewer than 8 bytes remain — compose the long byte-by-byte, zeroing
   * the bits past {@code payloadSize}. Safe because the caller masks the
   * result to {@code bitWidth} bits and the last packed value's bits always
   * lie inside {@code [byteOff, payloadSize)}.
   */
  private static long readWordSafe(final MemorySegment payload, final long payloadSize,
      final long byteOff) {
    if (byteOff + 8L <= payloadSize) {
      return payload.get(ValueLayout.JAVA_LONG_UNALIGNED, byteOff);
    }
    long word = 0L;
    final long remaining = Math.max(0L, payloadSize - byteOff);
    for (long k = 0; k < remaining; k++) {
      word |= (payload.get(ValueLayout.JAVA_BYTE, byteOff + k) & 0xFFL) << (k * 8L);
    }
    return word;
  }

  /**
   * Dispatcher: pick the right kernel based on encoding. Returns the count
   * of satisfying values, or {@code -1L} if the caller should fall back to
   * the scalar {@link NumberRegion#decodeValueAt} loop (for unsupported
   * encodings or bit widths).
   *
   * @param payload PAX payload as a MemorySegment (wrap the {@code byte[]}
   *     once per page via {@code MemorySegment.ofArray})
   */
  public static long countMatching(final MemorySegment payload, final NumberRegion.Header h,
      final int start, final int end, final VectorOperators.Comparison op, final long threshold) {
    if (NumberRegion.isBitPacked(h.encodingKind)) {
      return countBitPacked(payload, h.valueBytesOffset, h.valueBase, h.valueBitWidth,
          start, end, op, threshold);
    }
    return countPlainLong(payload, h.valueBytesOffset, start, end, op, threshold);
  }

  /**
   * Two-predicate AND range kernel: counts values satisfying
   * {@code (value OP1 threshold1) AND (value OP2 threshold2)} in one SIMD pass.
   * Used by the vectorized executor's {@code executeFilterCount2} entry point to
   * fuse same-field range queries like {@code age > 30 AND age < 50} into a single
   * scan, eliminating Brackit's post-filter over the first predicate's match set.
   *
   * <p>Returns {@code -1L} if the bit-packed width exceeds SIMD support (caller
   * falls back to scalar via {@link NumberRegion#decodeValueAt}).
   */
  public static long countMatchingRange(final MemorySegment payload, final NumberRegion.Header h,
      final int start, final int end,
      final VectorOperators.Comparison op1, final long threshold1,
      final VectorOperators.Comparison op2, final long threshold2) {
    if (NumberRegion.isBitPacked(h.encodingKind)) {
      return countBitPackedRange(payload, h.valueBytesOffset, h.valueBase, h.valueBitWidth,
          start, end, op1, threshold1, op2, threshold2);
    }
    return countPlainLongRange(payload, h.valueBytesOffset, start, end, op1, threshold1, op2, threshold2);
  }

  /** Two-predicate AND over PLAIN_LONG values. Single SIMD pass, masks AND'd, popcount. */
  public static long countPlainLongRange(final MemorySegment payload, final int valueBytesOffset,
      final int start, final int end,
      final VectorOperators.Comparison op1, final long threshold1,
      final VectorOperators.Comparison op2, final long threshold2) {
    final LongVector thr1 = LongVector.broadcast(LONG_SPECIES, threshold1);
    final LongVector thr2 = LongVector.broadcast(LONG_SPECIES, threshold2);
    long count = 0;
    int i = start;
    final long baseOff = (long) valueBytesOffset + (long) start * 8L;
    for (; i <= end - LANES; i += LANES) {
      final long off = baseOff + (long) (i - start) * 8L;
      final LongVector v =
          LongVector.fromMemorySegment(LONG_SPECIES, payload, off, ByteOrder.LITTLE_ENDIAN);
      final VectorMask<Long> m1 = v.compare(op1, thr1);
      final VectorMask<Long> m2 = v.compare(op2, thr2);
      count += m1.and(m2).trueCount();
    }
    for (; i < end; i++) {
      final long v = payload.get(ValueLayout.JAVA_LONG_UNALIGNED,
          (long) valueBytesOffset + (long) i * 8L);
      if (eval(v, op1, threshold1) && eval(v, op2, threshold2)) {
        count++;
      }
    }
    return count;
  }

  /** Two-predicate AND over BIT_PACKED values. Returns -1 for widths beyond 56. */
  public static long countBitPackedRange(final MemorySegment payload, final int valueBytesOffset,
      final long valueBase, final int bitWidth, final int start, final int end,
      final VectorOperators.Comparison op1, final long threshold1,
      final VectorOperators.Comparison op2, final long threshold2) {
    if (bitWidth < 1 || bitWidth > 56) {
      return -1L;
    }
    final long mask = bitWidth == 64 ? ~0L : (1L << bitWidth) - 1L;
    final long[] unpacked = new long[LANES];
    final LongVector thr1 = LongVector.broadcast(LONG_SPECIES, threshold1);
    final LongVector thr2 = LongVector.broadcast(LONG_SPECIES, threshold2);
    final LongVector baseV = LongVector.broadcast(LONG_SPECIES, valueBase);
    final long payloadSize = payload.byteSize();
    long count = 0;
    int i = start;
    for (; i <= end - LANES; i += LANES) {
      for (int lane = 0; lane < LANES; lane++) {
        final long bitOff = (long) (i + lane) * (long) bitWidth;
        final long byteOff = valueBytesOffset + (bitOff >>> 3);
        final int shift = (int) (bitOff & 7L);
        final long word = readWordSafe(payload, payloadSize, byteOff);
        unpacked[lane] = (word >>> shift) & mask;
      }
      final LongVector v = LongVector.fromArray(LONG_SPECIES, unpacked, 0).add(baseV);
      count += v.compare(op1, thr1).and(v.compare(op2, thr2)).trueCount();
    }
    for (; i < end; i++) {
      final long bitOff = (long) i * (long) bitWidth;
      final long byteOff = valueBytesOffset + (bitOff >>> 3);
      final int shift = (int) (bitOff & 7L);
      final long word = readWordSafe(payload, payloadSize, byteOff);
      final long v = valueBase + ((word >>> shift) & mask);
      if (eval(v, op1, threshold1) && eval(v, op2, threshold2)) {
        count++;
      }
    }
    return count;
  }

  // ─────────────────────────────────────────────────────── aggregate kernels

  /**
   * SIMD aggregation kernel: computes {@code sum}, {@code min}, {@code max} over
   * {@code payload[start..end)} in a single pass using {@link LongVector} reductions.
   * Replaces the scalar {@link NumberRegion#decodeValueAt} loop in
   * {@code SirixVectorizedExecutor.parallelAggregate}.
   *
   * <p><b>Output contract</b>: writes {@code out[0]=sum, out[1]=min, out[2]=max}.
   * If {@code start >= end}, writes {@code 0/Long.MAX_VALUE/Long.MIN_VALUE} so the
   * caller's identity-element fold is respected. Returns {@code true} on success,
   * {@code false} when the bit-packed width exceeds the SIMD-supported range — the
   * caller falls back to scalar in that case.
   *
   * <h2>Why one kernel, three results</h2>
   * Aggregates over the same range share the same memory bandwidth, so doing
   * sum/min/max in a single tight loop is purely additive in arithmetic but free in
   * memory traffic. AVX-512 has dedicated {@code vpaddq} / {@code vpminsq} /
   * {@code vpmaxsq} instructions that retire in one cycle each.
   *
   * @return {@code true} if aggregation completed; {@code false} if caller must fall back
   */
  public static boolean aggregateRange(final MemorySegment payload, final NumberRegion.Header h,
      final int start, final int end, final long[] out) {
    if (start >= end) {
      out[0] = 0L;
      out[1] = Long.MAX_VALUE;
      out[2] = Long.MIN_VALUE;
      return true;
    }
    if (NumberRegion.isBitPacked(h.encodingKind)) {
      return aggregateBitPacked(payload, h.valueBytesOffset, h.valueBase, h.valueBitWidth,
          start, end, out);
    }
    aggregatePlainLong(payload, h.valueBytesOffset, start, end, out);
    return true;
  }

  /** Vectorized sum/min/max over PLAIN_LONG values. Memory-bandwidth bound on AVX-512. */
  private static void aggregatePlainLong(final MemorySegment payload, final int valueBytesOffset,
      final int start, final int end, final long[] out) {
    LongVector sumV = LongVector.zero(LONG_SPECIES);
    LongVector minV = LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE);
    LongVector maxV = LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE);
    int i = start;
    final long baseOff = (long) valueBytesOffset + (long) start * 8L;
    for (; i <= end - LANES; i += LANES) {
      final long off = baseOff + (long) (i - start) * 8L;
      final LongVector v =
          LongVector.fromMemorySegment(LONG_SPECIES, payload, off, ByteOrder.LITTLE_ENDIAN);
      sumV = sumV.add(v);
      minV = minV.min(v);
      maxV = maxV.max(v);
    }
    long sum = sumV.reduceLanes(VectorOperators.ADD);
    long min = minV.reduceLanes(VectorOperators.MIN);
    long max = maxV.reduceLanes(VectorOperators.MAX);
    // Scalar tail.
    for (; i < end; i++) {
      final long v = payload.get(ValueLayout.JAVA_LONG_UNALIGNED,
          (long) valueBytesOffset + (long) i * 8L);
      sum += v;
      if (v < min) min = v;
      if (v > max) max = v;
    }
    out[0] = sum;
    out[1] = min;
    out[2] = max;
  }

  /** Vectorized sum/min/max over BIT_PACKED values. Returns false for widths {@code > 56}. */
  private static boolean aggregateBitPacked(final MemorySegment payload, final int valueBytesOffset,
      final long valueBase, final int bitWidth, final int start, final int end, final long[] out) {
    if (bitWidth < 1 || bitWidth > 56) {
      return false;
    }
    final long mask = bitWidth == 64 ? ~0L : (1L << bitWidth) - 1L;
    final long[] unpacked = new long[LANES];
    final LongVector baseV = LongVector.broadcast(LONG_SPECIES, valueBase);
    final long payloadSize = payload.byteSize();
    LongVector sumV = LongVector.zero(LONG_SPECIES);
    LongVector minV = LongVector.broadcast(LONG_SPECIES, Long.MAX_VALUE);
    LongVector maxV = LongVector.broadcast(LONG_SPECIES, Long.MIN_VALUE);
    int i = start;
    for (; i <= end - LANES; i += LANES) {
      for (int lane = 0; lane < LANES; lane++) {
        final long bitOff = (long) (i + lane) * (long) bitWidth;
        final long byteOff = valueBytesOffset + (bitOff >>> 3);
        final int shift = (int) (bitOff & 7L);
        final long word = readWordSafe(payload, payloadSize, byteOff);
        unpacked[lane] = (word >>> shift) & mask;
      }
      final LongVector v = LongVector.fromArray(LONG_SPECIES, unpacked, 0).add(baseV);
      sumV = sumV.add(v);
      minV = minV.min(v);
      maxV = maxV.max(v);
    }
    long sum = sumV.reduceLanes(VectorOperators.ADD);
    long min = minV.reduceLanes(VectorOperators.MIN);
    long max = maxV.reduceLanes(VectorOperators.MAX);
    // Scalar tail.
    for (; i < end; i++) {
      final long bitOff = (long) i * (long) bitWidth;
      final long byteOff = valueBytesOffset + (bitOff >>> 3);
      final int shift = (int) (bitOff & 7L);
      final long word = readWordSafe(payload, payloadSize, byteOff);
      final long v = valueBase + ((word >>> shift) & mask);
      sum += v;
      if (v < min) min = v;
      if (v > max) max = v;
    }
    out[0] = sum;
    out[1] = min;
    out[2] = max;
    return true;
  }

  private static boolean eval(final long v, final VectorOperators.Comparison op, final long t) {
    if (op == VectorOperators.GT) return v > t;
    if (op == VectorOperators.LT) return v < t;
    if (op == VectorOperators.GE) return v >= t;
    if (op == VectorOperators.LE) return v <= t;
    if (op == VectorOperators.EQ) return v == t;
    if (op == VectorOperators.NE) return v != t;
    throw new IllegalArgumentException("unsupported op: " + op);
  }

  /** Lane width at runtime — exposed for diagnostics / benches. */
  public static int lanes() {
    return LANES;
  }
}
