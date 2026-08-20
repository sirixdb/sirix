package io.sirix.node;

import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

final class MemorySegmentBytesOutDestinationTest {

  @Test
  void reusesExactDestinationViewForRecurringPrimitiveNumberLengths() {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(16)) {
      NodeKind.serializeNumber(0, out);
      final MemorySegment twoByteView = out.getDestination();
      assertDestination(twoByteView, new byte[] {2, 0});

      out.clear();
      NodeKind.serializeNumber(64, out);
      final MemorySegment threeByteView = out.getDestination();
      assertDestination(threeByteView, new byte[] {2, (byte) 0x80, 1});

      out.clear();
      NodeKind.serializeNumber(-1, out);
      final MemorySegment reusedTwoByteView = out.getDestination();
      assertSame(twoByteView, reusedTwoByteView,
          "a recurring used length must reuse its exact view instead of allocating another slice");
      assertDestination(reusedTwoByteView, new byte[] {2, 1});

      out.clear();
      NodeKind.serializeNumber(65, out);
      final MemorySegment reusedThreeByteView = out.getDestination();
      assertSame(threeByteView, reusedThreeByteView,
          "the cache must retain more than only the most recently requested length");
      assertDestination(reusedThreeByteView, new byte[] {2, (byte) 0x82, 1});
    }
  }

  @Test
  void growthInvalidatesViewsBackedByTheReplacedSegment() {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(2)) {
      NodeKind.serializeNumber(0, out);
      final MemorySegment beforeGrowth = out.getDestination();
      assertDestination(beforeGrowth, new byte[] {2, 0});

      out.clear();
      out.write(new byte[32]);
      assertEquals(32, out.getDestination().byteSize());

      out.clear();
      NodeKind.serializeNumber(0, out);
      final MemorySegment afterGrowth = out.getDestination();
      assertNotSame(beforeGrowth, afterGrowth,
          "a cached view must never be reused after growth replaces its backing segment");
      assertDestination(afterGrowth, new byte[] {2, 0});
    }
  }

  @Test
  void readableByteBufferReusesOneWrapperAcrossVaryingLogicalLengths() {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(16)) {
      final byte[] firstThreeBytes = {1, 2, 3};
      out.write(firstThreeBytes);
      final ByteBuffer lengthA = out.readableByteBuffer();
      assertReadable(lengthA, firstThreeBytes);

      final byte[] sevenBytes = {4, 5, 6, 7, 8, 9, 10};
      out.clear();
      out.write(sevenBytes);
      final ByteBuffer lengthB = out.readableByteBuffer();
      assertSame(lengthA, lengthB, "changing only the logical length must not allocate a new wrapper");
      assertReadable(lengthB, sevenBytes);

      final byte[] finalThreeBytes = {11, 12, 13};
      out.clear();
      out.write(finalThreeBytes);
      final ByteBuffer lengthAAgain = out.readableByteBuffer();
      assertSame(lengthA, lengthAAgain, "an A/B/A length sequence must keep the base wrapper identity");
      assertReadable(lengthAAgain, finalThreeBytes);
    }
  }

  @Test
  void readableByteBufferRebuildsOnceAfterBackingSegmentGrowth() {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(4)) {
      out.write(new byte[] {1, 2, 3, 4});
      final ByteBuffer beforeGrowth = out.readableByteBuffer();
      assertReadable(beforeGrowth, new byte[] {1, 2, 3, 4});

      final byte[] grownBytes = {5, 6, 7, 8, 9, 10, 11, 12, 13};
      out.clear();
      out.write(grownBytes);
      final ByteBuffer afterGrowth = out.readableByteBuffer();
      assertNotSame(beforeGrowth, afterGrowth,
          "growth must replace the wrapper that still points at the old backing segment");
      assertReadable(afterGrowth, grownBytes);

      out.clear();
      out.write(new byte[] {14, 15});
      final ByteBuffer afterGrowthReuse = out.readableByteBuffer();
      assertSame(afterGrowth, afterGrowthReuse, "the grown backing segment must retain one reusable wrapper");
      assertReadable(afterGrowthReuse, new byte[] {14, 15});
    }
  }

  @Test
  void readableByteBufferOwnershipIsPerWriterInstance() {
    try (MemorySegmentBytesOut foreground = new MemorySegmentBytesOut(8);
        MemorySegmentBytesOut background = new MemorySegmentBytesOut(8)) {
      foreground.write(new byte[] {1, 2, 3});
      background.write(new byte[] {7, 8});

      final ByteBuffer foregroundBuffer = foreground.readableByteBuffer();
      final ByteBuffer backgroundBuffer = background.readableByteBuffer();
      assertNotSame(foregroundBuffer, backgroundBuffer);
      assertReadable(foregroundBuffer, new byte[] {1, 2, 3});
      assertReadable(backgroundBuffer, new byte[] {7, 8});
    }
  }

  @Test
  void destinationRemainsExactlyBoundedAndRejectsInvalidLogicalPositions() {
    try (MemorySegmentBytesOut out = new MemorySegmentBytesOut(8)) {
      out.writeByte((byte) 7);
      final MemorySegment destination = out.getDestination();
      assertEquals(1, destination.byteSize());
      assertEquals((byte) 7, destination.get(JAVA_BYTE, 0));
      assertThrows(IndexOutOfBoundsException.class, () -> destination.get(JAVA_BYTE, 1),
          "spare writer capacity must not be observable through the destination");

      out.position(9);
      assertThrows(IndexOutOfBoundsException.class, out::getDestination,
          "an externally supplied position beyond capacity must not bypass segment bounds checks");
      assertThrows(IndexOutOfBoundsException.class, out::readableByteBuffer,
          "a readable buffer must not expose an invalid logical position through its capacity view");
    }
  }

  private static void assertDestination(final MemorySegment actual, final byte[] expected) {
    assertEquals(expected.length, actual.byteSize());
    assertArrayEquals(expected, actual.toArray(JAVA_BYTE));
  }

  private static void assertReadable(final ByteBuffer actual, final byte[] expected) {
    assertEquals(0, actual.position());
    assertEquals(expected.length, actual.limit());
    final byte[] bytes = new byte[actual.remaining()];
    actual.get(bytes);
    assertArrayEquals(expected, bytes);
  }
}
