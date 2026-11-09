package io.sirix.node;

import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ScratchStats;
import io.sirix.cache.ScratchUse;
import net.openhft.hashing.LongHashFunction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A writer created with {@link MemorySegmentBytesOut#ofScratch} is the heap writer's twin over
 * borrowed allocator scratch: the same bytes, the same views, native memory a positional write
 * takes without a copy, and a borrow that {@code close()} returns. The commit's flush buffers are
 * such writers; they used to be heap arrays kept in a per-thread stash.
 *
 * <p>
 * The assertions are deltas of the process allocator's counters for {@link ScratchUse#COMMIT_FLUSH}
 * and of its lent-buffer gauge, so the class runs isolated.
 */
@Isolated
final class MemorySegmentBytesOutScratchTest {

  private static final long KIB = 1024L;

  @BeforeAll
  static void initializeAllocator() {
    assertInstanceOf(FrameSlotAllocator.class, Allocators.getInstance()).init(8L * 1024 * 1024 * 1024);
  }

  private static ScratchStats stats() {
    return Allocators.getInstance().scratchStats();
  }

  private static long borrows(final ScratchStats stats) {
    return stats.acquisitions(ScratchUse.COMMIT_FLUSH);
  }

  @Test
  void aScratchWriterWritesTheSameBytesAsAHeapWriterAndReturnsItsBorrowOnClose() {
    final ScratchStats before = stats();
    final long lentBefore = before.totalLentBuffers();
    final MemorySegmentBytesOut heap = new MemorySegmentBytesOut(64);
    final MemorySegmentBytesOut scratch = MemorySegmentBytesOut.ofScratch(128_000, ScratchUse.COMMIT_FLUSH);
    assertEquals(128 * KIB, scratch.capacity(), "rounded up to the allocator's capacity");
    assertEquals(lentBefore + 1, stats().totalLentBuffers());
    for (final MemorySegmentBytesOut out : new MemorySegmentBytesOut[] {heap, scratch}) {
      out.writeInt(0xCAFE_BABE);
      out.writeLong(-7L);
      out.writeByte((byte) 3);
      out.writeUtf8("flush");
      out.write(pattern(5_000, 9));
      out.writeStopBit(1L << 40);
    }
    assertArrayEquals(heap.toByteArray(), scratch.toByteArray());
    assertEquals(heap.hashDirect(LongHashFunction.xx3()), scratch.hashDirect(LongHashFunction.xx3()),
        "the native range hashes like the heap array");
    final MemorySegment used = scratch.getDestination();
    assertEquals(heap.writePosition(), used.byteSize(), "the used view ends at the write position");
    assertTrue(used.isNative());

    final ByteBuffer readable = scratch.readableByteBuffer();
    assertTrue(readable.isDirect(), "a positional write takes the bytes without an intermediate copy");
    assertEquals(0, readable.position());
    assertEquals(heap.writePosition(), readable.limit());
    assertSame(readable, scratch.readableByteBuffer(), "the file-I/O view is built once per backing buffer");

    scratch.clear();
    assertEquals(0, scratch.writePosition());
    assertEquals(lentBefore + 1, stats().totalLentBuffers(), "a clear keeps the borrow");
    scratch.close();
    scratch.close();
    final ScratchStats after = stats();
    assertEquals(lentBefore, after.totalLentBuffers(), "closing returned the buffer, once");
    assertEquals(1, borrows(after) - borrows(before));
    assertEquals(0, after.staleReleases() - before.staleReleases());
    assertEquals(0, scratch.capacity());
    assertThrows(IllegalStateException.class, () -> scratch.writeInt(1), "a closed writer cannot be written");
    assertEquals(lentBefore, stats().totalLentBuffers(), "and does not borrow again behind its owner's back");
    heap.close();
  }

  @Test
  void growthExchangesTheBorrowAndKeepsWhatWasWritten() {
    final ScratchStats before = stats();
    final long lentBefore = before.totalLentBuffers();
    final byte[] chunk = pattern(60_000, 4);
    try (MemorySegmentBytesOut out = MemorySegmentBytesOut.ofScratch(64_000, ScratchUse.COMMIT_FLUSH)) {
      assertEquals(64 * KIB, out.capacity());
      final ByteBuffer small = out.readableByteBuffer();
      out.write(chunk);
      out.write(chunk);
      assertEquals(128 * KIB, out.capacity(), "120,000 bytes: the next capacity");
      out.write(chunk);
      out.write(chunk);
      out.write(chunk);
      assertEquals(512 * KIB, out.capacity(), "300,000 bytes outgrow 256 KiB");
      assertEquals(lentBefore + 1, stats().totalLentBuffers(), "one buffer at a time: each exchange returned the last");
      final byte[] written = out.toByteArray();
      assertEquals(5 * chunk.length, written.length);
      for (int copy = 0; copy < 5; copy++) {
        final byte[] slice = new byte[chunk.length];
        System.arraycopy(written, copy * chunk.length, slice, 0, chunk.length);
        assertArrayEquals(chunk, slice, "copy " + copy + " survived every exchange");
      }
      final ByteBuffer large = out.readableByteBuffer();
      assertNotSame(small, large, "the file-I/O view follows the backing buffer");
      assertEquals(written.length, large.limit());
    }
    final ScratchStats after = stats();
    assertEquals(4, borrows(after) - borrows(before), "64 KiB, then 128 KiB, 256 KiB and 512 KiB");
    assertEquals(lentBefore, after.totalLentBuffers());
  }

  @Test
  void theGrowingSegmentReportsWhetherItsBorrowIsAlive() {
    final GrowingMemorySegment heap = new GrowingMemorySegment(16);
    assertTrue(heap.isAlive());
    heap.close();
    assertTrue(heap.isAlive(), "heap memory is the collector's: closing changes nothing");

    final GrowingMemorySegment scratch = GrowingMemorySegment.ofScratch(4 * KIB, ScratchUse.COMMIT_FLUSH);
    assertTrue(scratch.isAlive());
    assertTrue(scratch.getSegment().isNative());
    scratch.writeLong(11L);
    assertEquals(8, scratch.position());
    scratch.close();
    assertFalse(scratch.isAlive());
    assertEquals(0, scratch.capacity());
    assertEquals(0, scratch.position());
    assertEquals(0, scratch.getUsedSegment().byteSize());
    assertThrows(IllegalArgumentException.class, () -> GrowingMemorySegment.ofScratch(0, ScratchUse.COMMIT_FLUSH));
    assertThrows(NullPointerException.class, () -> GrowingMemorySegment.ofScratch(16, null));
  }

  private static byte[] pattern(final int length, final int seed) {
    final byte[] bytes = new byte[length];
    for (int i = 0; i < length; i++) {
      bytes[i] = (byte) (i * 31 + seed);
    }
    return bytes;
  }
}
