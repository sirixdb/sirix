/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Round-trip + compression-ratio tests for {@link ByteRunCodec}.
 */
public class ByteRunCodecTest {

  @Test
  @DisplayName("Empty input round-trips")
  public void empty() {
    final MemorySegment input = MemorySegment.ofArray(new byte[0]);
    final byte[] encoded = new byte[ByteRunCodec.maxEncodedSize(0)];
    final int encLen = ByteRunCodec.encode(input, 0L, 0, encoded, 0);
    final byte[] decodedArr = new byte[0];
    final MemorySegment decoded = MemorySegment.ofArray(decodedArr);
    final int decLen = ByteRunCodec.decode(encoded, 0, encLen, decoded, 0L);
    assertEquals(0, decLen);
  }

  @Test
  @DisplayName("Literal-only input round-trips")
  public void literalOnly() {
    final byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }; // no adjacent duplicates
    roundTrip(data);
  }

  @Test
  @DisplayName("All-zero input round-trips + compresses heavily")
  public void allZero() {
    final byte[] data = new byte[10_000];
    roundTrip(data);
    final byte[] encoded = new byte[ByteRunCodec.maxEncodedSize(data.length)];
    final int encLen = ByteRunCodec.encode(MemorySegment.ofArray(data), 0L, data.length,
        encoded, 0);
    // 1 marker + 1 varint + 1 long-zero marker + varint(10000) = ~7 bytes.
    if (encLen > 16) throw new AssertionError("Expected heavy compression: got " + encLen);
  }

  @Test
  @DisplayName("All-non-zero constant-byte input compresses with V2 codec")
  public void allNonZeroConstant() {
    final byte[] data = new byte[10_000];
    for (int i = 0; i < data.length; i++) data[i] = 0x42;
    roundTrip(data);
    final byte[] encoded = new byte[ByteRunCodec.maxEncodedSize(data.length)];
    final int encLen = ByteRunCodec.encode(MemorySegment.ofArray(data), 0L, data.length,
        encoded, 0);
    if (encLen > 16) throw new AssertionError("Expected V2 const-run compression: got " + encLen);
  }

  @Test
  @DisplayName("Mixed zero runs + non-zero const runs + literals")
  public void mixedContent() {
    // 100 zeros | 100 0x01 | 5 literals | 200 0xFF | 50 literals | 50 zeros
    final java.io.ByteArrayOutputStream bao = new java.io.ByteArrayOutputStream();
    for (int i = 0; i < 100; i++) bao.write(0);
    for (int i = 0; i < 100; i++) bao.write(1);
    bao.write(new byte[] { (byte) 0xAA, (byte) 0xBB, (byte) 0xCC, (byte) 0xDD, (byte) 0xEE },
        0, 5);
    for (int i = 0; i < 200; i++) bao.write(0xFF);
    for (int i = 0; i < 50; i++) bao.write(0x10 + i % 7); // small-varint literals
    for (int i = 0; i < 50; i++) bao.write(0);
    roundTrip(bao.toByteArray());
  }

  @Test
  @DisplayName("Random data round-trips and never exceeds max-encoded-size bound")
  public void randomLarge() {
    final Random rng = new Random(7);
    for (int trial = 0; trial < 16; trial++) {
      final int n = 1 + rng.nextInt(4096);
      final byte[] data = new byte[n];
      rng.nextBytes(data);
      // Force some runs so the codec exercises run paths.
      for (int i = 0; i + 8 < n; i += rng.nextInt(128) + 16) {
        final byte v = (byte) rng.nextInt(256);
        for (int k = 0; k < 4 + rng.nextInt(32); k++) {
          if (i + k < n) data[i + k] = v;
        }
      }
      roundTrip(data);
    }
  }

  @Test
  @DisplayName("Run at tail boundary (starts near end of buffer) round-trips")
  public void runAtTail() {
    // 10 literal + 50 zeros at end.
    final byte[] data = new byte[60];
    for (int i = 0; i < 10; i++) data[i] = (byte) (0x30 + i);
    // 0x30..0x39 ASCII, no duplicates so literal
    roundTrip(data);
  }

  /**
   * {@link ByteRunCodec#maxEncodedSize} is a contract: callers — {@code PageKind} among them — size
   * the output buffer with it and then encode straight into that buffer, so an encode that exceeds
   * it is a heap overflow, not a bad compression ratio.
   *
   * <p>The bound used to assume one literal header per 128 input bytes, which holds only while the
   * input has no adjacent duplicates. It does not in general: the literal scan breaks as soon as it
   * sees two equal bytes so the next iteration can emit a run, so {@code x y y} repeating forces a
   * one-byte literal token for every two-byte run and produces four output bytes per three input.
   * Random data hides this — it has adjacent duplicates only about once in 256 bytes — which is why
   * the old bound survived the existing round-trip tests.
   */
  @Test
  @DisplayName("Encoding never exceeds maxEncodedSize, including the worst-case pattern")
  public void neverExceedsMaxEncodedSize() {
    for (final int length : new int[] {0, 1, 2, 3, 4, 127, 128, 129, 1023, 4096, 32_768, 65_536}) {
      assertWithinBound(worstCasePattern(length), "worst case x y y");
      assertWithinBound(alternating(length), "alternating");
      assertWithinBound(random(length, 20260728L), "random");
    }
  }

  /**
   * The densest expansion the encoder can be driven to: a lone literal byte, then a two-byte
   * non-zero run, repeating. Four output bytes for every three input bytes.
   */
  private static byte[] worstCasePattern(final int length) {
    final byte[] data = new byte[length];
    for (int i = 0; i + 2 < length; i += 3) {
      data[i] = (byte) (i % 251 + 1);
      data[i + 1] = 7;
      data[i + 2] = 7;
    }
    return data;
  }

  private static byte[] alternating(final int length) {
    final byte[] data = new byte[length];
    for (int i = 0; i < length; i++) {
      data[i] = (byte) (i & 1);
    }
    return data;
  }

  private static byte[] random(final int length, final long seed) {
    final byte[] data = new byte[length];
    new Random(seed).nextBytes(data);
    return data;
  }

  /** Encode into a generously oversized buffer, then assert the advertised bound was respected. */
  private static void assertWithinBound(final byte[] data, final String label) {
    final int bound = ByteRunCodec.maxEncodedSize(data.length);
    // Deliberately larger than the bound: we want to observe the real size, not just an overflow.
    final byte[] encoded = new byte[bound + 4 * data.length + 64];
    final int encLen = ByteRunCodec.encode(MemorySegment.ofArray(data), 0L, data.length, encoded, 0);

    assertTrue(encLen <= bound,
        () -> label + " (" + data.length + " bytes) encoded to " + encLen
            + ", exceeding maxEncodedSize " + bound);

    // The bound is only worth anything if the encoding it sizes still decodes.
    final byte[] decodedArr = new byte[data.length];
    final int decLen = ByteRunCodec.decode(encoded, 0, encLen, MemorySegment.ofArray(decodedArr), 0L);
    assertEquals(data.length, decLen, () -> label + ": decoded length mismatch");
    assertArrayEquals(data, decodedArr, () -> label + ": decoded content mismatch");
  }

  private static void roundTrip(final byte[] data) {
    final MemorySegment input = MemorySegment.ofArray(data);
    final byte[] encoded = new byte[ByteRunCodec.maxEncodedSize(data.length)];
    final int encLen = ByteRunCodec.encode(input, 0L, data.length, encoded, 0);
    final byte[] decodedArr = new byte[data.length];
    final MemorySegment decoded = MemorySegment.ofArray(decodedArr);
    final int decLen = ByteRunCodec.decode(encoded, 0, encLen, decoded, 0L);
    assertEquals(data.length, decLen, "decoded length mismatch");
    assertArrayEquals(data, decodedArr, "decoded content mismatch");
  }
}
