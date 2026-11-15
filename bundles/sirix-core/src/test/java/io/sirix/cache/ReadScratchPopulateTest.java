/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.sirix.utils.OS;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * A small scratch buffer is resident before the first read lands in it.
 *
 * <p>
 * A positional read copies its bytes out in the kernel, so a page of the buffer that nothing has
 * touched yet costs a fault inside the read. In a fresh process those were the reads of its first
 * query: the same reads at the same offsets took ten more page faults than with the reader's old
 * pool of zeroed direct buffers, nine of them in the read buffer. The pages are counted here
 * through {@code /proc/self/pagemap}, whose "present" bit needs no privilege, so the tests see what
 * the kernel did and not what the allocator believes.
 * </p>
 */
final class ReadScratchPopulateTest {

  private static final long KIB = 1024L;
  private static final long PAGE = 4096L;
  private static final long BUDGET = 32L << 20;
  private static final Path PAGEMAP = Path.of("/proc/self/pagemap");

  /** The switch is static: tests that set it must not interleave. */
  private static final String SWITCH = "sirix.allocator.readScratchPopulate";

  @Test
  @ResourceLock(SWITCH)
  void aBufferIsResidentWhenAReadBorrowsItFirst() throws IOException {
    assumeLinuxWithPopulate();
    final boolean previous = FrameScratch.setReadPagePreparationForTesting(true);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET, 4);
    try (ScratchLease lease = new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ)) {
      assertEquals(128 * KIB / PAGE, residentPages(lease.segment()), "every page, before anything is written");
      assertEquals(128 * KIB, allocator.scratch().populatedReadBytes());
    } finally {
      FrameScratch.setReadPagePreparationForTesting(previous);
      allocator.shutdown();
    }
  }

  @Test
  @ResourceLock(SWITCH)
  void aBufferOfAnotherUseIsPopulatedOnlyOnceAReadBorrowsIt() throws IOException {
    assumeLinuxWithPopulate();
    final boolean previous = FrameScratch.setReadPagePreparationForTesting(true);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET, 4);
    try {
      final long address;
      try (ScratchLease codec = new ScratchLease(allocator).acquire(64 * KIB, ScratchUse.CODEC)) {
        address = codec.segment().address();
        assertEquals(0L, residentPages(codec.segment()), "other uses touch what they need, when they need it");
        assertEquals(0L, allocator.scratch().populatedReadBytes());
      }
      try (ScratchLease read = new ScratchLease(allocator).acquire(64 * KIB, ScratchUse.READ)) {
        assertEquals(address, read.segment().address(), "the returned buffer is the one lent again");
        assertEquals(64 * KIB / PAGE, residentPages(read.segment()));
        assertEquals(64 * KIB, allocator.scratch().populatedReadBytes());
      }
      try (ScratchLease again = new ScratchLease(allocator).acquire(64 * KIB, ScratchUse.READ)) {
        assertEquals(address, again.segment().address());
        assertEquals(64 * KIB, allocator.scratch().populatedReadBytes(), "resident pages are not populated twice");
      }
    } finally {
      FrameScratch.setReadPagePreparationForTesting(previous);
      allocator.shutdown();
    }
  }

  @Test
  @ResourceLock(SWITCH)
  void aRegionAboveTheSmallCapacitiesFaultsAsFarAsItsReadsReach() throws IOException {
    assumeLinuxWithPopulate();
    final boolean previous = FrameScratch.setReadPagePreparationForTesting(true);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET, 4);
    try (ScratchLease lease = new ScratchLease(allocator).acquire(1024 * KIB, ScratchUse.READ)) {
      assertEquals(0L, residentPages(lease.segment()));
      assertEquals(0L, allocator.scratch().populatedReadBytes());
    } finally {
      FrameScratch.setReadPagePreparationForTesting(previous);
      allocator.shutdown();
    }
  }

  @Test
  @ResourceLock(SWITCH)
  void theSwitchLeavesABufferToFaultLazily() throws IOException {
    assumeLinuxWithPopulate();
    final boolean previous = FrameScratch.setReadPagePreparationForTesting(false);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET, 4);
    try (ScratchLease lease = new ScratchLease(allocator).acquire(128 * KIB, ScratchUse.READ)) {
      assertEquals(0L, residentPages(lease.segment()));
      assertEquals(0L, allocator.scratch().populatedReadBytes());
    } finally {
      FrameScratch.setReadPagePreparationForTesting(previous);
      allocator.shutdown();
    }
  }

  private static void assumeLinuxWithPopulate() {
    assumeTrue(OS.isLinux() && Files.isReadable(PAGEMAP), "Linux with /proc/self/pagemap");
    try (Arena arena = Arena.ofConfined()) {
      assumeTrue(VirtualMemory.forCurrentPlatform().populate(arena.allocate(PAGE, PAGE)),
          "the kernel offers MADV_POPULATE_WRITE (Linux 5.14+)");
    }
  }

  /** Pages of {@code segment} the kernel holds in memory: bit 63 of each page's pagemap entry. */
  private static long residentPages(final MemorySegment segment) throws IOException {
    final int pages = Math.toIntExact(segment.byteSize() / PAGE);
    final ByteBuffer entries = ByteBuffer.allocate(pages * Long.BYTES).order(ByteOrder.nativeOrder());
    try (FileChannel pagemap = FileChannel.open(PAGEMAP, StandardOpenOption.READ)) {
      long position = segment.address() / PAGE * Long.BYTES;
      while (entries.hasRemaining()) {
        final int read = pagemap.read(entries, position);
        if (read <= 0) {
          throw new IOException("short read of " + PAGEMAP + " at " + position);
        }
        position += read;
      }
    }
    long resident = 0L;
    for (int page = 0; page < pages; page++) {
      if (entries.getLong(page * Long.BYTES) < 0L) {
        resident++;
      }
    }
    return resident;
  }
}
