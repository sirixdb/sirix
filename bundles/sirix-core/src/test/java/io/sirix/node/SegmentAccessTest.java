/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.node;

import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class SegmentAccessTest {
  private static final List<Reader> READERS = List.of(new Reader(1, SegmentAccess::getByte),
      new Reader(2, SegmentAccess::getShortLE), new Reader(4, SegmentAccess::getIntLE),
      new Reader(8, SegmentAccess::getLongLE), new Reader(4, SegmentAccess::getFloatLE),
      new Reader(8, SegmentAccess::getDoubleLE), new Reader(2, SegmentAccess::getShortBE),
      new Reader(4, SegmentAccess::getIntBE), new Reader(8, SegmentAccess::getLongBE));

  @Test
  void everyScalarMatchesWireBytesForHeapNativeAndReadOnlySegments() {
    final byte[] wire = new byte[32];
    for (int i = 0; i < wire.length; i++) {
      wire[i] = (byte) (101 * (i + 11));
    }
    final ByteBuffer little = ByteBuffer.wrap(wire).order(ByteOrder.LITTLE_ENDIAN);
    final ByteBuffer big = ByteBuffer.wrap(wire).order(ByteOrder.BIG_ENDIAN);
    final MemorySegment heap = MemorySegment.ofArray(wire);
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment nativeSegment = arena.allocate(wire.length);
      nativeSegment.copyFrom(heap);
      for (final MemorySegment segment : List.of(heap, nativeSegment, heap.asReadOnly(), nativeSegment.asReadOnly())) {
        for (int offset = 0; offset < 16; offset++) {
          assertEquals(wire[offset], SegmentAccess.getByte(segment, offset));
          assertEquals(little.getShort(offset), SegmentAccess.getShortLE(segment, offset));
          assertEquals(little.getInt(offset), SegmentAccess.getIntLE(segment, offset));
          assertEquals(little.getLong(offset), SegmentAccess.getLongLE(segment, offset));
          assertEquals(Float.floatToRawIntBits(little.getFloat(offset)),
              Float.floatToRawIntBits(SegmentAccess.getFloatLE(segment, offset)));
          assertEquals(Double.doubleToRawLongBits(little.getDouble(offset)),
              Double.doubleToRawLongBits(SegmentAccess.getDoubleLE(segment, offset)));
          assertEquals(big.getShort(offset), SegmentAccess.getShortBE(segment, offset));
          assertEquals(big.getInt(offset), SegmentAccess.getIntBE(segment, offset));
          assertEquals(big.getLong(offset), SegmentAccess.getLongBE(segment, offset));
        }
      }
    }
  }

  @Test
  void floatingPointReadsPreserveNegativeZeroAndNanPayloads() {
    final byte[] wire = new byte[16];
    final ByteBuffer buffer = ByteBuffer.wrap(wire).order(ByteOrder.LITTLE_ENDIAN);
    final MemorySegment segment = MemorySegment.ofArray(wire);
    for (final int bits : new int[] {0x80000000, 0x7fc12345, 0xff800000}) {
      buffer.putInt(1, bits);
      assertEquals(bits, Float.floatToRawIntBits(SegmentAccess.getFloatLE(segment, 1)));
    }
    for (final long bits : new long[] {0x8000000000000000L, 0x7ff8123456789abcL, 0xfff0000000000000L}) {
      buffer.putLong(1, bits);
      assertEquals(bits, Double.doubleToRawLongBits(SegmentAccess.getDoubleLE(segment, 1)));
    }
  }

  @Test
  void boundsChecksAcceptTheLastValueAndRejectOverflowWithoutReadingAdjacentBytes() {
    final MemorySegment heap = MemorySegment.ofArray(new byte[32]);
    try (Arena arena = Arena.ofConfined()) {
      for (final MemorySegment backing : List.of(heap, arena.allocate(32))) {
        final MemorySegment segment = backing.asSlice(1, 16);
        for (final Reader reader : READERS) {
          reader.access.read(segment, 16L - reader.width);
          assertThrows(IndexOutOfBoundsException.class, () -> reader.access.read(segment, 17L - reader.width));
          assertThrows(IndexOutOfBoundsException.class, () -> reader.access.read(segment, -1));
          assertThrows(IndexOutOfBoundsException.class, () -> reader.access.read(segment, Long.MAX_VALUE));
          assertThrows(NullPointerException.class, () -> reader.access.read(null, 0));
        }
      }
    }
  }

  @Test
  void closedScopesRemainUnreadable() {
    final MemorySegment segment;
    try (Arena arena = Arena.ofConfined()) {
      segment = arena.allocate(16);
    }
    for (final Reader reader : READERS) {
      assertThrows(IllegalStateException.class, () -> reader.access.read(segment, 0));
    }
  }

  @Test
  void confinedScopesStillRejectOtherThreads() throws Exception {
    try (Arena arena = Arena.ofConfined(); var executor = Executors.newSingleThreadExecutor()) {
      final MemorySegment segment = arena.allocate(16);
      for (final Reader reader : READERS) {
        final Throwable failure = executor.submit(() -> {
          try {
            reader.access.read(segment, 0);
            return null;
          } catch (final RuntimeException exception) {
            return exception;
          }
        }).get();
        assertInstanceOf(WrongThreadException.class, failure);
      }
    }
  }

  @Test
  void failedScalarInputReadDoesNotAdvanceTheCursor() {
    final MemorySegmentBytesIn input = new MemorySegmentBytesIn(MemorySegment.ofArray(new byte[16]));
    input.position(9);
    assertThrows(IndexOutOfBoundsException.class, input::readLong);
    assertEquals(9, input.position());
  }

  @FunctionalInterface
  private interface Access {
    Object read(MemorySegment segment, long offset);
  }

  private record Reader(int width, Access access) {
  }
}
