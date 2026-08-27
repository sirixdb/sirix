/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the FORMAT-STABILITY contract of {@link HashAccesses}: the Unsafe-free accesses must produce
 * hashes bit-identical to the library's default access, because every checksum already persisted in
 * a sirix data file was computed through that default. A single differing bit here means existing
 * resources fail verification on open. The oracle is the library's own {@code hashBytes} (the
 * Unsafe path), exercised across every length stripe the XXH3 implementation switches on (empty,
 * &le;16, 17–128, 129–240, and long inputs spanning multiple blocks) plus unaligned offsets, and
 * across byte[], heap-segment and native-segment shapes.
 */
final class HashAccessesEquivalenceTest {

  private static final LongHashFunction XX3 = LongHashFunction.xx3();

  /** Every XXH3 stripe boundary, its neighbours, and block-spanning sizes. */
  private static final int[] LENGTHS = {0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 175,
      239, 240, 241, 511, 512, 1024, 1025, 4096, 65_536, 65_537, 1 << 20};

  @Test
  @DisplayName("byte[] hashes through the VarHandle access match the library default bit-for-bit")
  void byteArrayAccessMatchesDefault() {
    final Random random = new Random(0x5151C5_1EEDL);
    for (final int length : LENGTHS) {
      for (final int offset : new int[] {0, 1, 7, 13}) {
        final byte[] buffer = new byte[offset + length + 3];
        random.nextBytes(buffer);
        final long expected = XX3.hashBytes(buffer, offset, length);
        assertEquals(expected, XX3.hash(buffer, HashAccesses.BYTES, offset, length),
            "length=" + length + " offset=" + offset);
      }
    }
  }

  @Test
  @DisplayName("heap and native segment hashes match the byte[] hash of the same bytes")
  void segmentAccessMatchesDefault() {
    final Random random = new Random(0xD1C7_F007L);
    try (Arena arena = Arena.ofConfined()) {
      for (final int length : LENGTHS) {
        final byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        final long expected = XX3.hashBytes(bytes);

        final MemorySegment heap = MemorySegment.ofArray(bytes);
        assertEquals(expected, XX3.hash(heap, HashAccesses.SEGMENT, 0, length), "heap length=" + length);

        final MemorySegment nativeSegment = arena.allocate(Math.max(length, 1));
        MemorySegment.copy(bytes, 0, nativeSegment, ValueLayout.JAVA_BYTE, 0, length);
        assertEquals(expected, XX3.hash(nativeSegment.asSlice(0, length), HashAccesses.SEGMENT, 0, length),
            "native length=" + length);
      }
    }
  }

  @Test
  @DisplayName("HashAlgorithm.XXH3 answers identically across all three input shapes")
  void hashAlgorithmShapesAgree() {
    final Random random = new Random(0xA1607_1774L);
    try (Arena arena = Arena.ofConfined()) {
      for (final int length : LENGTHS) {
        final byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        final long fromArray = HashAlgorithm.XXH3.computeHashLong(bytes);
        assertEquals(XX3.hashBytes(bytes), fromArray, "array vs library, length=" + length);
        assertEquals(fromArray, HashAlgorithm.XXH3.computeHashLong(MemorySegment.ofArray(bytes)),
            "heap segment, length=" + length);
        final MemorySegment nativeSegment = arena.allocate(Math.max(length, 1));
        MemorySegment.copy(bytes, 0, nativeSegment, ValueLayout.JAVA_BYTE, 0, length);
        assertEquals(fromArray, HashAlgorithm.XXH3.computeHashLong(nativeSegment.asSlice(0, length)),
            "native segment, length=" + length);
      }
    }
  }
}
