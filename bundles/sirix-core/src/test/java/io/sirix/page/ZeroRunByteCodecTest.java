/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip + compression-ratio tests for {@link ZeroRunByteCodec} —
 * the zero-run RLE used as a cheap replacement for LZ4 on Sirix page
 * heaps when {@code -Dsirix.compression=none} is in effect.
 */
@DisplayName("ZeroRunByteCodec")
final class ZeroRunByteCodecTest {

  @Test
  @DisplayName("empty input encodes to frame header only, decodes to empty")
  void emptyRoundTrip() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment in = arena.allocate(0);
      final byte[] buf = new byte[ZeroRunByteCodec.maxEncodedSize(0)];
      final int encoded = ZeroRunByteCodec.encode(in, 0, 0, buf, 0);
      assertEquals(2, encoded); // 1 marker + 1 varint(0)
      final MemorySegment out = arena.allocate(1);
      final int decoded = ZeroRunByteCodec.decode(buf, 0, encoded, out, 0);
      assertEquals(0, decoded);
    }
  }

  @Test
  @DisplayName("all-zero input compresses to <1% of original")
  void allZerosCompressesAggressively() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 4096;
      final MemorySegment in = arena.allocate(n);
      final byte[] buf = new byte[ZeroRunByteCodec.maxEncodedSize(n)];
      final int encoded = ZeroRunByteCodec.encode(in, 0, n, buf, 0);
      assertTrue(encoded < 10,
          "all-zero " + n + " should encode to < 10 bytes, got " + encoded);
      final MemorySegment out = arena.allocate(n);
      final int decoded = ZeroRunByteCodec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      for (int i = 0; i < n; i++) {
        assertEquals(0, out.get(ValueLayout.JAVA_BYTE, i), "byte " + i);
      }
    }
  }

  @Test
  @DisplayName("all-non-zero random bytes round-trip with ~1 byte overhead per 128")
  void randomNonZeroRoundTrip() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 1024;
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0xDEADBEEF);
      final byte[] expected = new byte[n];
      for (int i = 0; i < n; i++) {
        int b;
        do {
          b = rng.nextInt(256);
        } while (b == 0);
        expected[i] = (byte) b;
        in.set(ValueLayout.JAVA_BYTE, i, (byte) b);
      }
      final byte[] buf = new byte[ZeroRunByteCodec.maxEncodedSize(n)];
      final int encoded = ZeroRunByteCodec.encode(in, 0, n, buf, 0);
      assertTrue(encoded < n + 32, "overhead too large: encoded=" + encoded + " n=" + n);
      final MemorySegment out = arena.allocate(n);
      final int decoded = ZeroRunByteCodec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      for (int i = 0; i < n; i++) {
        assertEquals(expected[i], out.get(ValueLayout.JAVA_BYTE, i), "byte " + i);
      }
    }
  }

  @Test
  @DisplayName("mixed zero/non-zero bytes round-trip bit-identical")
  void mixedBytesRoundTrip() {
    try (Arena arena = Arena.ofConfined()) {
      // Simulate a realistic Sirix record heap: short varints (1-3 bytes) with
      // high-byte zeros, sporadic zero runs from null sibling pointers.
      final int n = 2048;
      final MemorySegment in = arena.allocate(n);
      final SplittableRandom rng = new SplittableRandom(0xC0FFEE);
      final byte[] expected = new byte[n];
      for (int i = 0; i < n; i++) {
        final int r = rng.nextInt(5);
        // 50% zero, 50% small non-zero — matches varint-heavy distribution.
        final byte b = r == 0 ? 0 : (byte) (r == 1 ? 0 : rng.nextInt(128) + 1);
        expected[i] = b;
        in.set(ValueLayout.JAVA_BYTE, i, b);
      }
      final byte[] buf = new byte[ZeroRunByteCodec.maxEncodedSize(n)];
      final int encoded = ZeroRunByteCodec.encode(in, 0, n, buf, 0);
      final MemorySegment out = arena.allocate(n);
      final int decoded = ZeroRunByteCodec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      final byte[] actual = new byte[n];
      for (int i = 0; i < n; i++) {
        actual[i] = out.get(ValueLayout.JAVA_BYTE, i);
      }
      assertArrayEquals(expected, actual);
    }
  }

  @Test
  @DisplayName("long zero run > 128 bytes uses long-zero form and round-trips")
  void longZeroRunRoundTrip() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 5000;
      final MemorySegment in = arena.allocate(n);
      // fill is already zero.
      final byte[] buf = new byte[ZeroRunByteCodec.maxEncodedSize(n)];
      final int encoded = ZeroRunByteCodec.encode(in, 0, n, buf, 0);
      assertTrue(encoded < 20, "long-zero should compress ~5000 to < 20 bytes, got " + encoded);
      final MemorySegment out = arena.allocate(n);
      final int decoded = ZeroRunByteCodec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      for (int i = 0; i < n; i++) {
        assertEquals(0, out.get(ValueLayout.JAVA_BYTE, i));
      }
    }
  }

  @Test
  @DisplayName("interleaved zero runs and literals round-trip")
  void interleavedRuns() {
    try (Arena arena = Arena.ofConfined()) {
      final int n = 512;
      final MemorySegment in = arena.allocate(n);
      final byte[] expected = new byte[n];
      // Pattern: 10 zeros, 10 non-zero, repeat.
      for (int i = 0; i < n; i++) {
        final byte b = ((i / 10) & 1) == 0 ? 0 : (byte) (0x40 + (i % 20));
        expected[i] = b;
        in.set(ValueLayout.JAVA_BYTE, i, b);
      }
      final byte[] buf = new byte[ZeroRunByteCodec.maxEncodedSize(n)];
      final int encoded = ZeroRunByteCodec.encode(in, 0, n, buf, 0);
      final MemorySegment out = arena.allocate(n);
      final int decoded = ZeroRunByteCodec.decode(buf, 0, encoded, out, 0);
      assertEquals(n, decoded);
      final byte[] actual = new byte[n];
      for (int i = 0; i < n; i++) {
        actual[i] = out.get(ValueLayout.JAVA_BYTE, i);
      }
      assertArrayEquals(expected, actual);
      // Sanity: this pattern has 50% zeros in runs of 10 — should compress.
      assertTrue(encoded < (n * 3) / 4, "expected compression, got " + encoded + " / " + n);
    }
  }

  @Test
  @DisplayName("1-byte input (no-op zero run, single literal)")
  void singleByte() {
    try (Arena arena = Arena.ofConfined()) {
      for (int v = 0; v < 256; v++) {
        final MemorySegment in = arena.allocate(1);
        in.set(ValueLayout.JAVA_BYTE, 0, (byte) v);
        final byte[] buf = new byte[ZeroRunByteCodec.maxEncodedSize(1)];
        final int encoded = ZeroRunByteCodec.encode(in, 0, 1, buf, 0);
        final MemorySegment out = arena.allocate(1);
        ZeroRunByteCodec.decode(buf, 0, encoded, out, 0);
        assertEquals((byte) v, out.get(ValueLayout.JAVA_BYTE, 0), "v=" + v);
      }
    }
  }
}
