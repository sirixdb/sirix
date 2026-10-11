/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.index.IndexType;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.HashCommon;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The HOT leaf installs a decoded side trailer from interleaved primitive pairs into a map created
 * already sized for the count, atomically and under one lifecycle hold.
 */
final class HOTLeafSideTrailerDecodeTest {
  private static final int BENCHMARK_SHAPE = 129;

  private static long key(final int i) {
    return (i - 256L) * 0x9E3779B97F4A7C15L;
  }

  private static long offset(final int i) {
    return 1000L + 4096L * i;
  }

  private static long[] pairs(final int count) {
    final long[] pairs = new long[2 * count];
    for (int i = 0; i < count; i++) {
      pairs[2 * i] = key(i);
      pairs[2 * i + 1] = offset(i);
    }
    return pairs;
  }

  private static void assertDecoded(final HOTLeafPage leaf, final int count) {
    assertEquals(count, leaf.segmentRefCount());
    final long[] expectedKeys = new long[count];
    for (int i = 0; i < count; i++) {
      final PageReference reference = leaf.getPageReference(key(i));
      assertNotNull(reference, "reference " + i);
      assertEquals(offset(i), reference.getKey());
      assertSame(reference, leaf.getPageReference(key(i)), "stable identity per key");
      expectedKeys[i] = key(i);
    }
    Arrays.sort(expectedKeys);
    assertArrayEquals(expectedKeys, leaf.overflowPageRefKeysSorted());
    assertNull(leaf.getPageReference(key(count)));
  }

  @Test
  void mapIsSizedOnceForTheTrailerAndExact() {
    for (final int count : new int[] {1, 2, 3, 17, BENCHMARK_SHAPE, 512, 4096}) {
      try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
        leaf.readSideReferences(pairs(count), count);
        assertDecoded(leaf, count);
        assertEquals(HashCommon.arraySize(count, Hash.DEFAULT_LOAD_FACTOR), leaf.sideReferenceTableCapacity(),
            "table sized exactly for " + count + " references at install time");
      }
    }
  }

  @Test
  void emptyTrailerKeepsTheConstructorMap() {
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
      final int before = leaf.sideReferenceTableCapacity();
      leaf.readSideReferences(new long[0], 0);
      assertEquals(0, leaf.segmentRefCount());
      assertEquals(before, leaf.sideReferenceTableCapacity());
    }
  }

  @Test
  void rejectedPairLeavesTheLeafUntouched() {
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
      final int before = leaf.sideReferenceTableCapacity();
      final long[] pairs = pairs(BENCHMARK_SHAPE);
      pairs[2 * 70 + 1] = -1L;
      assertThrows(IllegalStateException.class, () -> leaf.readSideReferences(pairs, BENCHMARK_SHAPE));
      assertEquals(0, leaf.segmentRefCount(), "no partial install");
      for (int i = 0; i < 70; i++) {
        assertNull(leaf.getPageReference(key(i)));
      }
      assertEquals(before, leaf.sideReferenceTableCapacity());
      leaf.readSideReferences(pairs(BENCHMARK_SHAPE), BENCHMARK_SHAPE);
      assertDecoded(leaf, BENCHMARK_SHAPE);
    }
  }

  @Test
  void decodingIntoANonEmptyLeafIsRejected() {
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
      final PageReference existing = new PageReference().setKey(7);
      leaf.setPageReference(5, existing);
      assertThrows(IllegalStateException.class, () -> leaf.readSideReferences(pairs(3), 3));
      assertEquals(1, leaf.segmentRefCount());
      assertSame(existing, leaf.getPageReference(5));
    }
  }

  @Test
  @SuppressWarnings("NullAway") // Deliberately pass null to verify rejection by the public API.
  void malformedDimensionsAreRejectedBeforeAnyInstall() {
    try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
      assertThrows(IndexOutOfBoundsException.class, () -> leaf.readSideReferences(new long[2 * 9 - 1], 9));
      assertThrows(IllegalArgumentException.class, () -> leaf.readSideReferences(new long[2], -1));
      assertThrows(NullPointerException.class, () -> leaf.readSideReferences(null, 0));
      assertEquals(0, leaf.segmentRefCount());
    }
  }

  @Test
  void everySourceKindDecodesTheSameMap() {
    final int count = BENCHMARK_SHAPE;
    final byte[] trailer = new byte[2 * count * Long.BYTES];
    final ByteBuffer encoded = ByteBuffer.wrap(trailer).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < count; i++) {
      encoded.putLong(key(i)).putLong(offset(i));
    }
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment nativeTrailer = arena.allocate(trailer.length);
      MemorySegment.copy(trailer, 0, nativeTrailer, ValueLayout.JAVA_BYTE, 0, trailer.length);
      final BytesIn<?>[] sources = {new ByteArrayBytesIn(trailer),
          new MemorySegmentBytesIn(MemorySegment.ofArray(trailer)), new MemorySegmentBytesIn(nativeTrailer)};
      for (final BytesIn<?> source : sources) {
        final long[] pairs = new long[2 * count];
        source.readLongs(pairs, 0, pairs.length);
        try (HOTLeafPage leaf = new HOTLeafPage(1, 1, IndexType.PROJECTION)) {
          leaf.readSideReferences(pairs, count);
          assertDecoded(leaf, count);
        }
      }
    }
  }
}
