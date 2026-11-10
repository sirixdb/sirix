/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.node.SegmentAccess;
import io.sirix.utils.OS;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Cache-owned slabs: charged to the physical budget before mapping, global-scope views, refunded
 * exactly once, releasable from any thread, and never served through page-cache pressure.
 */
final class FrameSlotAllocatorSlabTest {
  private static final long BUDGET = 4L << 20;

  @Test
  void slabsAreChargedBeforeMappingAndRefundedExactlyOnce() throws Exception {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    MemorySegment slab = null;
    try {
      final long committedBefore = allocator.getPhysicalMemoryBytes();
      final long activeBefore = allocator.getActiveMemoryBytes();
      slab = allocator.allocateSlab(100_000);
      assertNotNull(slab);
      assertEquals(102_400, slab.byteSize(), "charged at whole 4 KiB pages: 25 of them");
      assertSame(Arena.global().scope(), slab.scope(), "no per-access liveness check");
      assertEquals(102_400, allocator.getSlabBytes());
      assertEquals(committedBefore + 102_400, allocator.getPhysicalMemoryBytes());
      assertEquals(activeBefore + 102_400, allocator.getActiveMemoryBytes());
      SegmentAccess.putLong(slab, 8, 42L);
      SegmentAccess.putInt(slab, 102_396, 7);
      assertEquals(42L, SegmentAccess.getLong(slab, 8));
      assertEquals(7, SegmentAccess.getInt(slab, 102_396));
      assertThrows(IllegalStateException.class, allocator::shutdown, "live slabs block shutdown");
      final ExecutorService other = Executors.newSingleThreadExecutor();
      final MemorySegment mapped = slab;
      try {
        assertTrue(other.submit(() -> allocator.releaseSlab(mapped)).get(), "release from a foreign thread");
      } finally {
        other.shutdown();
      }
      assertFalse(allocator.releaseSlab(slab), "a repeated release is rejected");
      slab = null;
      assertEquals(0, allocator.getSlabBytes(), "released: no longer live");
      assertEquals(102_400, allocator.getRecycledSlabBytes(), "kept mapped for the next slab of this size");
      assertEquals(committedBefore + 102_400, allocator.getPhysicalMemoryBytes(),
          "a recycled slab's pages stay committed until pressure or shutdown");
      assertEquals(activeBefore, allocator.getActiveMemoryBytes());
      assertThrows(IllegalArgumentException.class, () -> allocator.allocateSlab(0));
      allocator.shutdown();
      assertEquals(0, allocator.getRecycledSlabBytes(), "shutdown drains the recycled slabs");
      assertEquals(committedBefore, allocator.getPhysicalMemoryBytes());
    } finally {
      if (slab != null && allocator.getSlabBytes() != 0) {
        allocator.releaseSlab(slab);
      }
      allocator.shutdown();
    }
  }

  @Test
  void aReleasedSlabIsReusedForTheNextSlabOfItsSizeWithoutRemapping() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final boolean poison = FrameSlotAllocator.setPoisonRecycledSlabsForTesting(true);
    try {
      final MemorySegment first = allocator.allocateSlab(8192);
      assertNotNull(first);
      SegmentAccess.putLong(first, 0, 0x1234L);
      final long committed = allocator.getPhysicalMemoryBytes();
      assertTrue(allocator.releaseSlab(first));
      assertEquals(8192, allocator.getRecycledSlabBytes());
      assertEquals(committed, allocator.getPhysicalMemoryBytes(), "no unmap on release");
      final MemorySegment other = allocator.allocateSlab(4096);
      assertNotNull(other, "a different size does not take the recycled slab");
      assertEquals(8192, allocator.getRecycledSlabBytes());
      final MemorySegment second = allocator.allocateSlab(8000);
      assertNotNull(second);
      assertEquals(first.address(), second.address(), "the same pages serve the next slab of this page size");
      assertNotSame(first, second, "a fresh view: the old instance is not the owner any more");
      assertEquals(8192, second.byteSize());
      assertSame(Arena.global().scope(), second.scope());
      assertEquals(0, allocator.getRecycledSlabBytes());
      assertEquals(committed + 4096, allocator.getPhysicalMemoryBytes(), "only the 4 KiB slab was newly mapped");
      assertEquals(0x5A5A5A5A5A5A5A5AL, SegmentAccess.getLong(second, 0),
          "the test poison replaces the dirty contents: nothing may assume zeroed memory");
      assertFalse(allocator.releaseSlab(first), "the stale instance cannot release the new owner's slab");
      assertEquals(8192 + 4096, allocator.getSlabBytes());
      assertTrue(allocator.releaseSlab(second));
      assertTrue(allocator.releaseSlab(other));
      assertEquals(0, allocator.getSlabBytes());
      assertEquals(8192 + 4096, allocator.getRecycledSlabBytes());
    } finally {
      FrameSlotAllocator.setPoisonRecycledSlabsForTesting(poison);
      allocator.shutdown();
    }
  }

  @Test
  void theRecycleBoundIsExactAndTheExcessIsUnmapped() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    try {
      final long previous = allocator.setSlabRecycleLimitBytesForTesting(3 * 4096);
      assertEquals(BUDGET / 16, previous, "default bound: one sixteenth of the budget");
      final MemorySegment[] slabs = new MemorySegment[5];
      for (int i = 0; i < slabs.length; i++) {
        slabs[i] = allocator.allocateSlab(4096);
        assertNotNull(slabs[i]);
      }
      final long committed = allocator.getPhysicalMemoryBytes();
      for (final MemorySegment slab : slabs) {
        assertTrue(allocator.releaseSlab(slab));
      }
      assertEquals(3 * 4096, allocator.getRecycledSlabBytes(), "exactly the bound is kept");
      assertEquals(committed - 2 * 4096, allocator.getPhysicalMemoryBytes(), "the two beyond it were unmapped");
      allocator.setSlabRecycleLimitBytesForTesting(4096);
      assertEquals(4096, allocator.getRecycledSlabBytes(), "lowering the bound drains the excess");
      allocator.setSlabRecycleLimitBytesForTesting(0);
      assertEquals(0, allocator.getRecycledSlabBytes(), "zero disables recycling");
      final MemorySegment again = allocator.allocateSlab(4096);
      assertNotNull(again);
      assertTrue(allocator.releaseSlab(again));
      assertEquals(0, allocator.getRecycledSlabBytes(), "with the bound at zero a release unmaps");
      assertEquals(committed - 5 * 4096, allocator.getPhysicalMemoryBytes());
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void recycledSlabsAreGivenBackBeforeAnyCommitmentIsRefused() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    try {
      allocator.setSlabRecycleLimitBytesForTesting(BUDGET);
      final MemorySegment half = allocator.allocateSlab(BUDGET / 2);
      assertNotNull(half);
      assertTrue(allocator.releaseSlab(half));
      assertEquals(BUDGET / 2, allocator.getRecycledSlabBytes(), "the whole half is kept mapped");
      final MemorySegment large = allocator.allocateSlab(BUDGET / 2 + 4096);
      assertNotNull(large, "a request that only fits once the recycled half is unmapped is admitted");
      assertEquals(0, allocator.getRecycledSlabBytes(), "the recycled slab was drained for it");
      assertTrue(allocator.releaseSlab(large));
      assertEquals(BUDGET / 2 + 4096, allocator.getRecycledSlabBytes());
      final MemorySegment slot = allocator.allocate(4096);
      assertNotNull(slot, "a frame slot that needs the budget drains recycled slabs too");
      allocator.release(slot);
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  void concurrentReleaseAndReuseKeepTheAccountingExact() throws Exception {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final ExecutorService pool = Executors.newFixedThreadPool(4);
    try {
      allocator.setSlabRecycleLimitBytesForTesting(16 * 4096);
      final long committedBefore = allocator.getPhysicalMemoryBytes();
      final java.util.List<java.util.concurrent.Future<?>> futures = new java.util.ArrayList<>();
      for (int t = 0; t < 4; t++) {
        futures.add(pool.submit(() -> {
          for (int i = 0; i < 2_000; i++) {
            final MemorySegment slab = allocator.allocateSlab(4096 * (1 + i % 3));
            assertNotNull(slab);
            SegmentAccess.putLong(slab, 0, i);
            assertEquals(i, SegmentAccess.getLong(slab, 0));
            assertTrue(allocator.releaseSlab(slab));
          }
        }));
      }
      for (final java.util.concurrent.Future<?> future : futures) {
        future.get();
      }
      assertEquals(0, allocator.getSlabBytes(), "every slab released");
      assertTrue(allocator.getRecycledSlabBytes() <= 16 * 4096, "the bound held under contention");
      assertEquals(committedBefore + allocator.getRecycledSlabBytes(), allocator.getPhysicalMemoryBytes(),
          "committed = what it was before any slab + the recycled slabs, nothing leaked or double-counted");
    } finally {
      pool.shutdownNow();
      allocator.shutdown();
    }
  }

  @Test
  void aSlabBeyondTheBudgetIsRefusedWithoutEvictingPageCaches() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final AtomicInteger pressure = new AtomicInteger();
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(pressure::incrementAndGet);
    try {
      assertNull(allocator.allocateSlab(BUDGET + 1), "a refusal, not an error, and no pressure");
      final MemorySegment first = allocator.allocateSlab(BUDGET / 2);
      assertNotNull(first);
      assertNull(allocator.allocateSlab(BUDGET / 2 + 4096), "retained plus request exceeds the budget");
      final MemorySegment second = allocator.allocateSlab(BUDGET / 2 - BUDGET / 8);
      assertNotNull(second, "exactly the remaining non-execution budget fits");
      assertNull(allocator.allocateSlab(4096), "the protected execution floor cannot be cached");
      assertEquals(0, pressure.get(), "slabs never evict persisted pages");
      assertTrue(allocator.releaseSlab(first));
      assertTrue(allocator.releaseSlab(second));
      assertEquals(0, allocator.getSlabBytes());
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      allocator.shutdown();
    }
  }

  @Test
  void aStaleHandleWhoseAddressWasReusedCannotReleaseTheNewSlab() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    try {
      final MemorySegment first = allocator.allocateSlab(4096);
      assertNotNull(first);
      assertTrue(allocator.releaseSlab(first));
      final MemorySegment second = allocator.allocateSlab(4096);
      assertNotNull(second);
      if (second.address() == first.address()) {
        assertFalse(allocator.releaseSlab(first), "exact-instance identity protects the reused address");
        assertEquals(4096, allocator.getSlabBytes());
      }
      assertTrue(allocator.releaseSlab(second));
    } finally {
      allocator.shutdown();
    }
  }

  /** Budget for the page-preparation tests: several MiB slabs beside the recycle bound. */
  private static final long PAGE_BUDGET = 64L << 20;

  private static final String PAGE_PREPARATION_LOCK = "sirix.allocator.slabPagePreparation";

  @Test
  @ResourceLock(PAGE_PREPARATION_LOCK)
  void aFreshSlabIsPopulatedAndOnlyALargeOneIsAdvisedHugePages() throws IOException {
    assumeLinuxWithPopulate();
    final boolean[] previous = FrameSlotAllocator.setSlabPagePreparationForTesting(true, true);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(PAGE_BUDGET);
    try {
      final MemorySegment large = allocator.allocateSlab(4L << 20);
      assertNotNull(large);
      final Map<String, String> largeMapping = smapsOf(large.address());
      assertTrue(kilobytes(largeMapping, "Rss") >= 4096,
          "a populated slab is resident before its first write: " + largeMapping);
      assertTrue(hasFlag(largeMapping, "hg"),
          "a slab of 2 MiB or more carries the huge-page advice: " + largeMapping.get("VmFlags"));
      assertEquals(4L << 20, allocator.getPopulatedSlabBytes());

      final MemorySegment small = allocator.allocateSlab(64L << 10);
      assertNotNull(small);
      final Map<String, String> smallMapping = smapsOf(small.address());
      assertTrue(kilobytes(smallMapping, "Rss") >= 64, "a small slab is populated too: " + smallMapping);
      assertFalse(hasFlag(smallMapping, "hg"), "a slab below 2 MiB is not advised huge pages");
      assertEquals((4L << 20) + (64L << 10), allocator.getPopulatedSlabBytes());

      assertTrue(allocator.releaseSlab(small));
      assertTrue(allocator.releaseSlab(large));
    } finally {
      FrameSlotAllocator.setSlabPagePreparationForTesting(previous[0], previous[1]);
      allocator.shutdown();
    }
  }

  @Test
  @ResourceLock(PAGE_PREPARATION_LOCK)
  void theDefaultPopulatesFreshSlabsWithoutHugePageAdvice() {
    final boolean[] defaults = FrameSlotAllocator.setSlabPagePreparationForTesting(true, true);
    try {
      assertTrue(defaults[0], "fresh slabs are populated by default");
      assertFalse(defaults[1], "huge-page advice is opt-in: synchronous compaction stalls measured");
    } finally {
      FrameSlotAllocator.setSlabPagePreparationForTesting(defaults[0], defaults[1]);
    }
  }

  @Test
  @ResourceLock(PAGE_PREPARATION_LOCK)
  void aRecycledSlabIsNotPopulatedAgain() {
    assumeLinuxWithPopulate();
    final boolean[] previous = FrameSlotAllocator.setSlabPagePreparationForTesting(true, true);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(PAGE_BUDGET);
    try {
      final MemorySegment first = allocator.allocateSlab(1L << 20);
      assertNotNull(first);
      assertEquals(1L << 20, allocator.getPopulatedSlabBytes());
      assertTrue(allocator.releaseSlab(first));
      assertEquals(1L << 20, allocator.getRecycledSlabBytes(), "the slab is recycled, not unmapped");
      final MemorySegment second = allocator.allocateSlab(1L << 20);
      assertNotNull(second);
      assertEquals(first.address(), second.address(), "the recycled slab is reused");
      assertEquals(1L << 20, allocator.getPopulatedSlabBytes(), "resident pages are not populated twice");
      assertTrue(allocator.releaseSlab(second));
    } finally {
      FrameSlotAllocator.setSlabPagePreparationForTesting(previous[0], previous[1]);
      allocator.shutdown();
    }
  }

  @Test
  @ResourceLock(PAGE_PREPARATION_LOCK)
  void pagePreparationCanBeSwitchedOff() throws IOException {
    assumeLinuxWithPopulate();
    final boolean[] previous = FrameSlotAllocator.setSlabPagePreparationForTesting(false, false);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(PAGE_BUDGET);
    try {
      final MemorySegment slab = allocator.allocateSlab(4L << 20);
      assertNotNull(slab);
      assertEquals(0L, allocator.getPopulatedSlabBytes());
      assertFalse(hasFlag(smapsOf(slab.address()), "hg"));
      assertTrue(allocator.releaseSlab(slab));
    } finally {
      FrameSlotAllocator.setSlabPagePreparationForTesting(previous[0], previous[1]);
      allocator.shutdown();
    }
  }

  private static void assumeLinuxWithPopulate() {
    assumeTrue(OS.isLinux() && Files.isReadable(Path.of("/proc/self/smaps")), "Linux with /proc/self/smaps");
    try (Arena arena = Arena.ofConfined()) {
      assumeTrue(VirtualMemory.forCurrentPlatform().populate(arena.allocate(4096, 4096)),
          "the kernel offers MADV_POPULATE_WRITE (Linux 5.14+)");
    }
  }

  private static final Pattern MAPPING_HEADER = Pattern.compile("^([0-9a-f]+)-([0-9a-f]+) .*");

  /**
   * The {@code /proc/self/smaps} record of the mapping holding {@code address}: field name to value.
   */
  private static Map<String, String> smapsOf(final long address) throws IOException {
    final List<String> lines = Files.readAllLines(Path.of("/proc/self/smaps"));
    Map<String, String> current = null;
    for (final String line : lines) {
      final Matcher header = MAPPING_HEADER.matcher(line);
      if (header.matches()) {
        final long start = Long.parseUnsignedLong(header.group(1), 16);
        final long end = Long.parseUnsignedLong(header.group(2), 16);
        if (current != null) {
          return current;
        }
        current = Long.compareUnsigned(start, address) <= 0 && Long.compareUnsigned(address, end) < 0
            ? new HashMap<>()
            : null;
      } else if (current != null) {
        final int colon = line.indexOf(':');
        current.put(line.substring(0, colon), line.substring(colon + 1).trim());
      }
    }
    assertNotNull(current, "no mapping holds 0x" + Long.toHexString(address));
    return current;
  }

  private static boolean hasFlag(final Map<String, String> mapping, final String flag) {
    final String flags = mapping.get("VmFlags");
    assertNotNull(flags, "VmFlags in " + mapping);
    return (" " + flags + " ").contains(" " + flag + " ");
  }

  private static long kilobytes(final Map<String, String> mapping, final String field) {
    final String value = mapping.get(field);
    assertNotNull(value, field + " in " + mapping);
    return Long.parseLong(value.substring(0, value.indexOf(' ')));
  }
}
