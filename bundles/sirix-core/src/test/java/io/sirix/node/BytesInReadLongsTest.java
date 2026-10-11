/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** {@link BytesIn#readLongs(long[], int, int)} on every source kind a page decoder can receive. */
final class BytesInReadLongsTest {
  private static final int PREFIX_BYTES = 5;
  private static final int LONG_COUNT = 258;

  private static byte[] encoded() {
    final byte[] bytes = new byte[PREFIX_BYTES + LONG_COUNT * Long.BYTES + 3];
    final ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).position(PREFIX_BYTES);
    for (int i = 0; i < LONG_COUNT; i++) {
      buffer.putLong(expected(i));
    }
    return bytes;
  }

  private static long expected(final int i) {
    return switch (i % 4) {
      case 0 -> Long.MIN_VALUE + i;
      case 1 -> Long.MAX_VALUE - i;
      case 2 -> -1L - i;
      default -> 0x0102030405060708L * i;
    };
  }

  private static List<Function<byte[], BytesIn<?>>> sources(final Arena arena) {
    return List.of(ByteArrayBytesIn::new, bytes -> new MemorySegmentBytesIn(MemorySegment.ofArray(bytes)), bytes -> {
      final MemorySegment nativeSegment = arena.allocate(bytes.length);
      MemorySegment.copy(bytes, 0, nativeSegment, ValueLayout.JAVA_BYTE, 0, bytes.length);
      return new MemorySegmentBytesIn(nativeSegment);
    });
  }

  @Test
  void readsExactValuesAndAdvancesPositionOnEverySourceKind() {
    final byte[] bytes = encoded();
    try (Arena arena = Arena.ofConfined()) {
      for (final Function<byte[], BytesIn<?>> factory : sources(arena)) {
        final BytesIn<?> source = factory.apply(bytes);
        for (int skip = 0; skip < PREFIX_BYTES; skip++) {
          source.readByte();
        }
        final long[] destination = new long[LONG_COUNT + 7];
        Arrays.fill(destination, 0x5A5A5A5A5A5A5A5AL);
        source.readLongs(destination, 3, LONG_COUNT);
        for (int i = 0; i < LONG_COUNT; i++) {
          assertEquals(expected(i), destination[3 + i], "long " + i + " via " + source.getClass().getSimpleName());
        }
        assertEquals(0x5A5A5A5A5A5A5A5AL, destination[0]);
        assertEquals(0x5A5A5A5A5A5A5A5AL, destination[2]);
        assertEquals(0x5A5A5A5A5A5A5A5AL, destination[LONG_COUNT + 3]);
        assertEquals(PREFIX_BYTES + (long) LONG_COUNT * Long.BYTES, source.position());
        assertEquals(3L, source.remaining());
        source.readLongs(destination, 0, 0);
        assertEquals(3L, source.remaining(), "a zero-length read leaves the source untouched");
      }
    }
  }

  @Test
  void bulkReadMatchesScalarReadsByteForByte() {
    final byte[] bytes = encoded();
    final long[] scalar = new long[LONG_COUNT];
    final BytesIn<?> reference = new MemorySegmentBytesIn(MemorySegment.ofArray(bytes));
    reference.position(PREFIX_BYTES);
    for (int i = 0; i < LONG_COUNT; i++) {
      scalar[i] = reference.readLong();
    }
    try (Arena arena = Arena.ofConfined()) {
      for (final Function<byte[], BytesIn<?>> factory : sources(arena)) {
        final BytesIn<?> source = factory.apply(bytes);
        source.position(PREFIX_BYTES);
        final long[] bulk = new long[LONG_COUNT];
        source.readLongs(bulk, 0, LONG_COUNT);
        assertArrayEquals(scalar, bulk);
      }
    }
  }

  @Test
  void truncatedSourceAndShortDestinationAreRejected() {
    final byte[] bytes = encoded();
    try (Arena arena = Arena.ofConfined()) {
      for (final Function<byte[], BytesIn<?>> factory : sources(arena)) {
        final BytesIn<?> truncated = factory.apply(bytes);
        truncated.position(PREFIX_BYTES);
        assertThrows(IndexOutOfBoundsException.class,
            () -> truncated.readLongs(new long[LONG_COUNT + 1], 0, LONG_COUNT + 1));
        final BytesIn<?> shortDestination = factory.apply(bytes);
        shortDestination.position(PREFIX_BYTES);
        assertThrows(IndexOutOfBoundsException.class,
            () -> shortDestination.readLongs(new long[LONG_COUNT], 1, LONG_COUNT));
        assertThrows(IndexOutOfBoundsException.class, () -> shortDestination.readLongs(new long[4], 0, -1));
      }
    }
  }

  @Test
  void segmentSourceLeavesPositionUntouchedWhenRejected() {
    final byte[] bytes = encoded();
    final BytesIn<?> source = new MemorySegmentBytesIn(MemorySegment.ofArray(bytes));
    source.position(PREFIX_BYTES);
    assertThrows(IndexOutOfBoundsException.class, () -> source.readLongs(new long[LONG_COUNT + 1], 0, LONG_COUNT + 1));
    assertEquals(PREFIX_BYTES, source.position());
    final long[] destination = new long[LONG_COUNT];
    source.readLongs(destination, 0, LONG_COUNT);
    assertEquals(expected(LONG_COUNT - 1), destination[LONG_COUNT - 1]);
  }
}
