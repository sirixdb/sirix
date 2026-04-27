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
