package io.sirix.node;

import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ScratchStats;
import io.sirix.cache.ScratchUse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The serialization sink owns no storage: it writes into allocator scratch, grows by borrowing the
 * next capacity, and returns what it borrowed when it is closed. Growth used to go to an arena of
 * the writer's own, outside the allocator's budget.
 *
 * <p>
 * The assertions are deltas of the process allocator's counters for
 * {@link ScratchUse#SERIALIZATION} and of its lent-buffer gauge, so the class runs isolated.
 */
@Isolated
final class PooledGrowingSegmentTest {

  private static final long KIB = 1024L;

  @BeforeAll
  static void initializeAllocator() {
    assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).init(8L * 1024 * 1024 * 1024);
  }

  private static ScratchStats stats() {
    return Allocators.getInstance().scratchStats();
  }

  private static long borrows(final ScratchStats stats) {
    return stats.acquisitions(ScratchUse.SERIALIZATION);
  }

  @Test
  void aPageWriterBorrowsOneBufferAndReturnsItOnClose() {
    final ScratchStats before = stats();
    final long lentBefore = before.totalLentBuffers();
    try (PooledGrowingSegment writer = PooledGrowingSegment.forPage()) {
      assertEquals(PooledGrowingSegment.PAGE_BUFFER_BYTES, writer.capacity());
      assertEquals(lentBefore + 1, stats().totalLentBuffers());
      writer.writeLong(0x1122_3344_5566_7788L);
      writer.writeInt(42);
      assertEquals(12, writer.position());
      assertEquals(0x1122_3344_5566_7788L, writer.getCurrentSegment().get(LE.LONG, 0));
      writer.reset();
      assertEquals(0, writer.position());
      assertEquals(lentBefore + 1, stats().totalLentBuffers(), "a reset keeps the borrowed buffer");
    }
    final ScratchStats after = stats();
    assertEquals(1, borrows(after) - borrows(before));
    assertEquals(PooledGrowingSegment.PAGE_BUFFER_BYTES,
        after.requestedBytes(ScratchUse.SERIALIZATION) - before.requestedBytes(ScratchUse.SERIALIZATION));
    assertEquals(lentBefore, after.totalLentBuffers(), "closing returned the buffer");

    for (int page = 0; page < 50; page++) {
      try (PooledGrowingSegment writer = PooledGrowingSegment.forPage()) {
        writer.writeLong(page);
      }
    }
    final ScratchStats warm = stats();
    assertEquals(50, borrows(warm) - borrows(after));
    assertEquals(0, warm.totalCreated() - after.totalCreated(),
        "every page reuses the buffer the allocator kept resident");
    assertEquals(lentBefore, warm.totalLentBuffers());
  }

  @Test
  void growthBorrowsTheNextCapacityKeepsThePrefixAndHoldsOneBufferAtATime() {
    final ScratchStats before = stats();
    final long lentBefore = before.totalLentBuffers();
    final byte[] chunk = pattern(100 * (int) KIB, 7);
    try (PooledGrowingSegment writer = PooledGrowingSegment.forPage()) {
      writer.write(chunk);
      assertEquals(128 * KIB, writer.capacity());
      writer.write(chunk);
      assertEquals(256 * KIB, writer.capacity(), "200 KiB written: the 128 KiB buffer was exchanged for 256 KiB");
      assertEquals(lentBefore + 1, stats().totalLentBuffers(), "the smaller buffer was returned");
      writer.write(chunk);
      writer.write(chunk);
      writer.write(chunk);
      assertEquals(512 * KIB, writer.capacity());
      writer.skip(600 * KIB);
      assertEquals(2 * 1024 * KIB, writer.capacity(), "at least the required size, at least double");
      assertEquals(1100 * KIB, writer.position());
      assertEquals(lentBefore + 1, stats().totalLentBuffers());
      final MemorySegment written = writer.getWrittenSlice();
      for (int copy = 0; copy < 5; copy++) {
        assertArrayEquals(chunk,
            written.asSlice((long) copy * chunk.length, chunk.length).toArray(ValueLayout.JAVA_BYTE),
            "copy " + copy + " survived every exchange");
      }
    }
    final ScratchStats after = stats();
    assertEquals(4, borrows(after) - borrows(before), "128 KiB, then 256 KiB, 512 KiB and 2 MiB");
    assertEquals(0, after.refusals() - before.refusals());
    assertEquals(lentBefore, after.totalLentBuffers());
  }

  @Test
  void aWriterOverACallersBufferBorrowsOnlyWhenItOutgrowsItAndReturnsToItOnReset() {
    final ScratchStats before = stats();
    final long lentBefore = before.totalLentBuffers();
    final MemorySegment own = MemorySegment.ofArray(new byte[64]);
    final PooledGrowingSegment writer = new PooledGrowingSegment(own);
    writer.writeLong(1L);
    assertSame(own, writer.getCurrentSegment());
    assertEquals(0, borrows(stats()) - borrows(before), "inside the caller's buffer nothing is borrowed");

    final byte[] chunk = pattern(200, 3);
    writer.write(chunk);
    assertEquals(4 * KIB, writer.capacity(), "outgrown: the smallest capacity that holds 208 bytes");
    assertEquals(lentBefore + 1, stats().totalLentBuffers());
    assertEquals(1L, writer.getCurrentSegment().get(LE.LONG, 0), "the prefix written to the caller's buffer moved");
    assertArrayEquals(chunk, writer.getWrittenSlice().asSlice(8, 200).toArray(ValueLayout.JAVA_BYTE));

    writer.reset();
    assertSame(own, writer.getCurrentSegment(), "a reset continues in the caller's buffer");
    assertEquals(lentBefore, stats().totalLentBuffers(), "and returns the scratch");
    writer.write(chunk);
    assertEquals(lentBefore + 1, stats().totalLentBuffers());
    writer.close();
    assertEquals(lentBefore, stats().totalLentBuffers());
    assertSame(own, writer.getCurrentSegment());
    writer.close();
    assertEquals(2, borrows(stats()) - borrows(before));
    assertEquals(0, stats().staleReleases() - before.staleReleases(), "a repeated close returns nothing twice");
  }

  @Test
  void aClosedScratchWriterCannotBeWrittenAndBorrowsNothing() {
    final PooledGrowingSegment writer = PooledGrowingSegment.ofScratch(8 * KIB);
    writer.writeInt(5);
    writer.close();
    final ScratchStats closed = stats();
    assertEquals(0, writer.position());
    assertEquals(0, writer.capacity());
    assertThrows(IllegalStateException.class, () -> writer.writeByte((byte) 1));
    assertThrows(IllegalStateException.class, () -> writer.skip(16));
    assertEquals(0, borrows(stats()) - borrows(closed), "a write after close must not silently borrow again");
    assertThrows(IllegalArgumentException.class, () -> PooledGrowingSegment.ofScratch(0));
    assertThrows(NullPointerException.class, () -> new PooledGrowingSegment(null));
  }

  @Test
  void aSinkThatCreatedItsSegmentReturnsItsScratchOnCloseAndABorrowedOneDoesNot() {
    final ScratchStats before = stats();
    final long lentBefore = before.totalLentBuffers();
    final byte[] chunk = pattern(4096, 1);
    try (PooledBytesOut owning = new PooledBytesOut(32)) {
      owning.write(chunk);
      assertEquals(lentBefore + 1, stats().totalLentBuffers(), "32 heap bytes were outgrown");
      assertArrayEquals(chunk, owning.toByteArray());
    }
    assertEquals(lentBefore, stats().totalLentBuffers(), "the sink owned its segment and closed it");

    try (PooledGrowingSegment segment = PooledGrowingSegment.forPage()) {
      try (PooledBytesOut borrowing = new PooledBytesOut(segment)) {
        borrowing.write(chunk);
      }
      assertEquals(lentBefore + 1, stats().totalLentBuffers(),
          "closing a sink over a segment handed to it leaves the segment to its borrower");
      assertTrue(segment.position() == chunk.length);
    }
    assertEquals(lentBefore, stats().totalLentBuffers());
  }

  private static byte[] pattern(final int length, final int seed) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (i * 31 + seed);
    }
    return bytes;
  }
}
