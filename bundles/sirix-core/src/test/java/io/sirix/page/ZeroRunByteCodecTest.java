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
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip + compression-ratio tests for {@link ZeroRunByteCodec} — the zero-run RLE used as a
 * cheap replacement for LZ4 on Sirix page heaps when {@code -Dsirix.compression=none} is in effect.
 */
@DisplayName("ZeroRunByteCodec")
final class ZeroRunByteCodecTest {

  @Test
  void heapDecoderMatchesEncodedInputAndSegmentDecoderAcrossRunBoundaries() {
    final SplittableRandom random = new SplittableRandom(0xF17E57);
    for (final int length : new int[] {0, 1, 2, 127, 128, 129, 255, 256, 8193}) {
      for (int pattern = 0; pattern < 4; pattern++) {
        final byte[] expected = new byte[length];
        for (int i = 0; i < length; i++) {
          expected[i] = switch (pattern) {
            case 0 -> 0;
            case 1 -> (byte) random.nextInt(1, 256);
            case 2 -> (byte) (i % 7 == 0
                ? 1
                : 0);
            default -> (byte) (random.nextInt(3) == 0
                ? random.nextInt(256)
                : 0);
          };
        }
        final byte[] encoded = new byte[7 + ZeroRunByteCodec.maxEncodedSize(length)];
        final int encodedLength = ZeroRunByteCodec.encode(expected, 0, length, encoded, 7);
        final byte[] output = new byte[length + 13];
        final byte[] segmentOutput = new byte[output.length];
        Arrays.fill(output, (byte) 0x5a);
        Arrays.fill(segmentOutput, (byte) 0x5a);
        assertEquals(length, ZeroRunByteCodec.decode(encoded, 7, encodedLength, output, 5));
        assertEquals(length,
            ZeroRunByteCodec.decode(encoded, 7, encodedLength, MemorySegment.ofArray(segmentOutput), 5L));
        assertArrayEquals(segmentOutput, output);
        assertArrayEquals(expected, Arrays.copyOfRange(output, 5, 5 + length));
        for (int i = 0; i < 5; i++) {
          assertEquals((byte) 0x5a, output[i]);
        }
        for (int i = 5 + length; i < output.length; i++) {
          assertEquals((byte) 0x5a, output[i]);
        }
      }
    }
  }

  @Test
  void heapDecoderRejectsTruncationAndNeverConsumesTheScratchTail() {
    final byte[] output = new byte[32];
    final byte[] valid = {(byte) 0xff, 6, 1, 7, 8, (byte) 0x82};
    for (int length = 0; length < valid.length; length++) {
      final int truncatedLength = length;
      assertThrows(IllegalStateException.class, () -> ZeroRunByteCodec.decode(valid, 0, truncatedLength, output, 0));
    }
    assertEquals(6, ZeroRunByteCodec.decode(valid, 0, valid.length, output, 0));
    assertArrayEquals(new byte[] {7, 8, 0, 0, 0, 0}, Arrays.copyOf(output, 6));
    for (final byte[] malformed : new byte[][] {{(byte) 0xff, 2, 3, 1, 2, 3, 4}, // literal exceeds decoded extent
        {(byte) 0xff, 1, (byte) 0x80}, // zero run exceeds decoded extent
        {(byte) 0xff, 2, (byte) 0xff, 1}, // long run shorter than two
        {(byte) 0xff, 2, (byte) 0xff, (byte) 0x80}, // truncated long-run varint
        {(byte) 0xff, 0, 0}, // trailing frame bytes
        {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0x7f}}) {
      assertThrows(IllegalStateException.class,
          () -> ZeroRunByteCodec.decode(malformed, 0, malformed.length, output, 0));
    }
    assertThrows(IndexOutOfBoundsException.class, () -> ZeroRunByteCodec.decode(valid, 0, valid.length, output, 30));
    assertThrows(IndexOutOfBoundsException.class, () -> ZeroRunByteCodec.decode(valid, -1, valid.length, output, 0));
  }

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
      assertTrue(encoded < 10, "all-zero " + n + " should encode to < 10 bytes, got " + encoded);
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
        final byte b = r == 0
            ? 0
            : (byte) (r == 1
                ? 0
                : rng.nextInt(128) + 1);
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
  @DisplayName("long zero run clears only its offset output range across copy chunks")
  void longZeroRunPreservesAdjacentBytes() {
    final int length = 8193;
    final int offset = 7;
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment input = arena.allocate(length);
      final byte[] encoded = new byte[ZeroRunByteCodec.maxEncodedSize(length)];
      final int encodedLength = ZeroRunByteCodec.encode(input, 0, length, encoded, 0);
      final MemorySegment output = arena.allocate(length + offset + 9);
      output.fill((byte) 0x5A);

      assertEquals(length, ZeroRunByteCodec.decode(encoded, 0, encodedLength, output, offset));
      for (int i = 0; i < offset; i++) {
        assertEquals((byte) 0x5A, output.get(ValueLayout.JAVA_BYTE, i));
      }
      for (int i = offset; i < offset + length; i++) {
        assertEquals((byte) 0, output.get(ValueLayout.JAVA_BYTE, i));
      }
      for (int i = offset + length; i < output.byteSize(); i++) {
        assertEquals((byte) 0x5A, output.get(ValueLayout.JAVA_BYTE, i));
      }
    }
  }

  @Test
  @DisplayName("invalid long zero lengths are rejected")
  void invalidLongZeroRunLengths() {
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment output = arena.allocate(2);
      final byte[] zeroLength = {(byte) 0xFF, 2, (byte) 0xFF, 0};
      final byte[] negativeLength =
          {(byte) 0xFF, 2, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x0F};
      assertThrows(IllegalStateException.class,
          () -> ZeroRunByteCodec.decode(zeroLength, 0, zeroLength.length, output, 0));
      assertThrows(IllegalStateException.class,
          () -> ZeroRunByteCodec.decode(negativeLength, 0, negativeLength.length, output, 0));
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
        final byte b = ((i / 10) & 1) == 0
            ? 0
            : (byte) (0x40 + (i % 20));
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
