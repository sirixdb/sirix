package io.sirix.io.bytepipe;

import io.sirix.exception.SirixIOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

final class FFILz4BoundedSegmentTest {
  @Test
  void boundedRangesRoundTripWithoutTouchingAdjacentBytes() {
    assumeTrue(FFILz4Compressor.isNativeAvailable());
    final FFILz4Compressor codec = new FFILz4Compressor();
    try (Arena arena = Arena.ofConfined()) {
      final MemorySegment input = arena.allocate(2048);
      final MemorySegment scratch = arena.allocate(2048);
      input.fill((byte) 42);
      scratch.fill((byte) 19);
      final int length = codec.compressSegmentBounded(input, 128, scratch, 256);
      assertTrue(length > 0);
      MemorySegment.copy(scratch, 0, scratch, 512, length);
      scratch.asSlice(0, length).fill((byte) 0);
      assertEquals(128, codec.decompressSegmentRange(scratch, 512, length, scratch, 1024, 128));
      MemorySegment.copy(scratch, 512, scratch, 0, length);
      assertThrows(IllegalArgumentException.class,
          () -> codec.decompressSegmentRange(scratch, 512, length, scratch.asReadOnly(), 1024, 128));
      assertEquals(19, scratch.get(ValueLayout.JAVA_BYTE, 256));
      assertEquals(128, codec.decompressSegmentRange(scratch, 0, length, scratch, 1024, 128));
      assertEquals(19, scratch.get(ValueLayout.JAVA_BYTE, 1023));
      assertEquals(19, scratch.get(ValueLayout.JAVA_BYTE, 1152));
      for (int i = 1024; i < 1152; i++) {
        assertEquals(42, scratch.get(ValueLayout.JAVA_BYTE, i));
      }
      scratch.asSlice(1024, 128).fill((byte) 19);
      int result;
      try {
        result = codec.decompressSegmentRange(scratch, 0, length, scratch, 1024, 127);
      } catch (SirixIOException corruptFrame) {
        result = -1; // The existing Java fallback signals corrupt input with an exception.
      }
      assertTrue(result < 0);
      assertEquals(19, scratch.get(ValueLayout.JAVA_BYTE, 1151));
      assertThrows(IndexOutOfBoundsException.class, () -> codec.compressSegmentBounded(input, -1, scratch, 256));
      assertThrows(IndexOutOfBoundsException.class, () -> codec.compressSegmentBounded(input, 128, scratch, -1));
      assertThrows(IndexOutOfBoundsException.class, () -> codec.compressSegmentBounded(input, 2049, scratch, 256));
      assertThrows(IndexOutOfBoundsException.class, () -> codec.compressSegmentBounded(input, 128, scratch, 2049));
      assertThrows(IndexOutOfBoundsException.class,
          () -> codec.decompressSegmentRange(scratch, 2048, 1, scratch, 1024, 128));
      assertThrows(IndexOutOfBoundsException.class,
          () -> codec.decompressSegmentRange(scratch, 256, length, scratch, 2048, 1));
      assertThrows(IndexOutOfBoundsException.class,
          () -> codec.decompressSegmentRange(scratch, 256, length, scratch, 1024, -1));
    }
  }

  @Test
  void closedSourceIsRejectedBeforeNativeAccess() {
    final FFILz4Compressor codec = new FFILz4Compressor();
    final MemorySegment closed;
    try (Arena arena = Arena.ofConfined()) {
      closed = arena.allocate(128);
    }
    try (Arena arena = Arena.ofConfined()) {
      assertThrows(IllegalStateException.class,
          () -> codec.decompressSegmentRange(closed, 0, 8, arena.allocate(128), 0, 128));
    }
  }
}
