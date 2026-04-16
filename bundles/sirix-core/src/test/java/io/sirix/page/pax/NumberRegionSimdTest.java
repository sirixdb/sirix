/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.VectorOperators;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Correctness tests for {@link NumberRegionSimd}. Every kernel result must
 * match the scalar reference (count of values satisfying the predicate)
 * across a range of widths, sizes, and operators.
 */
class NumberRegionSimdTest {

  @Test
  void plainLongMatchesScalarAcrossOps() {
    final int count = 1_000;
    final byte[] payload = new byte[count * 8];
    final Random rng = new Random(42);
    final long[] values = new long[count];
    for (int i = 0; i < count; i++) {
      values[i] = rng.nextLong();
      writeLittleEndianLong(payload, i * 8, values[i]);
    }
    final MemorySegment seg = MemorySegment.ofArray(payload);

    final long threshold = 0L;
    for (VectorOperators.Comparison op :
        new VectorOperators.Comparison[] {
            VectorOperators.GT, VectorOperators.LT, VectorOperators.GE,
            VectorOperators.LE, VectorOperators.EQ
        }) {
      final long expected = scalarCountPlain(values, op, threshold);
      final long actual = NumberRegionSimd.countPlainLong(seg, 0, 0, count, op, threshold);
      assertEquals(expected, actual, "PLAIN_LONG mismatch for op=" + op);
    }
  }

  @Test
  void bitPackedMatchesScalarAtVariousWidths() {
    for (int bitWidth : new int[] { 1, 3, 7, 10, 13, 17, 24, 31, 40, 56 }) {
      final int count = 512;
      final long mask = bitWidth == 64 ? ~0L : (1L << bitWidth) - 1L;
      final long base = 1_000_000L;
      final Random rng = new Random(bitWidth);
      final long[] unpacked = new long[count];
      for (int i = 0; i < count; i++) {
        unpacked[i] = rng.nextLong() & mask;
      }
      final byte[] payload = packBits(unpacked, bitWidth);
      final MemorySegment seg = MemorySegment.ofArray(payload);

      final long threshold = base + mask / 2;
      for (VectorOperators.Comparison op :
          new VectorOperators.Comparison[] {
              VectorOperators.GT, VectorOperators.LT, VectorOperators.EQ
          }) {
        long expected = 0;
        for (int i = 0; i < count; i++) {
          if (eval(base + unpacked[i], op, threshold)) expected++;
        }
        final long actual = NumberRegionSimd.countBitPacked(
            seg, 0, base, bitWidth, 0, count, op, threshold);
        assertEquals(expected, actual,
            "BIT_PACKED mismatch at width=" + bitWidth + " op=" + op);
      }
    }
  }

  @Test
  void bitPackedHandlesUnpaddedTail() {
    // Real serialized pages don't pad the bit-packed region — the region ends
    // at the last byte of the last value. A naive 8-byte read at the tail goes
    // out of bounds. Must match scalar across widths without padding.
    for (final int bitWidth : new int[] { 3, 7, 13, 24, 40, 56 }) {
      final int count = 31; // odd count so tail is small
      final long mask = (1L << bitWidth) - 1L;
      final long base = 42L;
      final Random rng = new Random(bitWidth * 101L);
      final long[] unpacked = new long[count];
      for (int i = 0; i < count; i++) {
        unpacked[i] = rng.nextLong() & mask;
      }
      final byte[] payload = packBitsTight(unpacked, bitWidth);
      final MemorySegment seg = MemorySegment.ofArray(payload);

      final long threshold = base + mask / 3;
      long expected = 0;
      for (int i = 0; i < count; i++) {
        if (eval(base + unpacked[i], VectorOperators.GT, threshold)) expected++;
      }
      final long actual = NumberRegionSimd.countBitPacked(
          seg, 0, base, bitWidth, 0, count, VectorOperators.GT, threshold);
      assertEquals(expected, actual,
          "unpadded BIT_PACKED tail mismatch at width=" + bitWidth
              + " payloadSize=" + payload.length);
    }
  }

  @Test
  void bitPackedRejectsUnsupportedWidths() {
    final byte[] payload = new byte[64];
    final MemorySegment seg = MemorySegment.ofArray(payload);
    final long result = NumberRegionSimd.countBitPacked(
        seg, 0, 0L, 57, 0, 8, VectorOperators.GT, 0L);
    assertEquals(-1L, result, "width 57 should return -1 (scalar fallback)");
  }

  @Test
  void plainLongHandlesTailSmallerThanVector() {
    // count < SIMD lanes — exercises scalar-only tail path.
    final int count = 3;
    final byte[] payload = new byte[count * 8];
    writeLittleEndianLong(payload, 0, 10L);
    writeLittleEndianLong(payload, 8, 20L);
    writeLittleEndianLong(payload, 16, 30L);
    final MemorySegment seg = MemorySegment.ofArray(payload);

    assertEquals(2L,
        NumberRegionSimd.countPlainLong(seg, 0, 0, 3, VectorOperators.GT, 15L));
    assertEquals(1L,
        NumberRegionSimd.countPlainLong(seg, 0, 0, 3, VectorOperators.EQ, 20L));
  }

  private static long scalarCountPlain(final long[] values, final VectorOperators.Comparison op,
      final long threshold) {
    long count = 0;
    for (final long v : values) {
      if (eval(v, op, threshold)) count++;
    }
    return count;
  }

  private static boolean eval(final long v, final VectorOperators.Comparison op, final long t) {
    if (op == VectorOperators.GT) return v > t;
    if (op == VectorOperators.LT) return v < t;
    if (op == VectorOperators.GE) return v >= t;
    if (op == VectorOperators.LE) return v <= t;
    if (op == VectorOperators.EQ) return v == t;
    if (op == VectorOperators.NE) return v != t;
    throw new IllegalArgumentException();
  }

  private static void writeLittleEndianLong(final byte[] buf, final int off, final long v) {
    for (int i = 0; i < 8; i++) {
      buf[off + i] = (byte) (v >>> (i * 8));
    }
  }

  /** Pack {@code values} into a {@code byte[]} at {@code bitWidth} bits each, little-endian. */
  private static byte[] packBits(final long[] values, final int bitWidth) {
    // Pad with 8 extra bytes so 64-bit unaligned reads at the last value are safe.
    final int byteLen = (int) (((long) values.length * bitWidth + 7L) / 8L) + 8;
    final byte[] out = new byte[byteLen];
    final long mask = bitWidth == 64 ? ~0L : (1L << bitWidth) - 1L;
    long buf = 0L;
    int bits = 0;
    int pos = 0;
    for (final long v : values) {
      buf |= (v & mask) << bits;
      bits += bitWidth;
      while (bits >= 8) {
        out[pos++] = (byte) buf;
        buf >>>= 8;
        bits -= 8;
      }
    }
    if (bits > 0) out[pos] = (byte) buf;
    return out;
  }

  /** Pack {@code values} into a {@code byte[]} at {@code bitWidth} bits each — no tail padding. */
  private static byte[] packBitsTight(final long[] values, final int bitWidth) {
    final int byteLen = (int) (((long) values.length * bitWidth + 7L) / 8L);
    final byte[] out = new byte[byteLen];
    final long mask = (1L << bitWidth) - 1L;
    long buf = 0L;
    int bits = 0;
    int pos = 0;
    for (final long v : values) {
      buf |= (v & mask) << bits;
      bits += bitWidth;
      while (bits >= 8) {
        out[pos++] = (byte) buf;
        buf >>>= 8;
        bits -= 8;
      }
    }
    if (bits > 0 && pos < out.length) out[pos] = (byte) buf;
    return out;
  }
}
