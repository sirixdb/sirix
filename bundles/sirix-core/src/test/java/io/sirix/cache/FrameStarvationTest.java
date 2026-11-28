/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * MEM-108 — no page-frame starvation. Through a real {@link BufferManagerImpl}: one size class is
 * filled through the record-page cache to <em>L_f</em>, then pages needing every other class are
 * read through the same cache. Every read must succeed within the allocator's existing 10 s wait
 * bound, and no {@link OutOfMemoryError} may be thrown while the caches hold unguarded frames that
 * cover the shortfall.
 *
 * <p>
 * The record-page cache is given a weight of <em>L_f</em>, so that today's fixed shares do not stop
 * it from holding one class at the limit; what is under test is that the budget those frames hold
 * can move to another class when the buffer manager evicts them. The allocator is real; the pages
 * are stubs over real frames.
 */
@MemPlanned(step = 1, value = {"MEM-108", "MEM-103"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class FrameStarvationTest {

  private static final long MIB = 1024L * 1024L;
  private static final long BUDGET = 32 * MIB;
  private static final long FILL_FRAME = 64 * 1024L;

  private final HeapCacheBudget heap = new HeapCacheBudget(Runtime.getRuntime().maxMemory(), ignored -> null);
  private FrameSlotAllocator.PressureListener previousListener;
  private FrameSlotAllocator allocator;
  private BufferManagerImpl manager;
  private final List<MemCachePageStub> held = new ArrayList<>();

  @BeforeEach
  void setUp() {
    previousListener = FrameSlotAllocator.pressureListener();
    allocator = new FrameSlotAllocator(BUDGET);
    manager = new BufferManagerImpl(1L << 20, FrameBudgetOracle.frameCommitLimit(allocator), 1L << 20, 4, 4, 4, heap);
  }

  @AfterEach
  void tearDown() {
    try {
      manager.close();
    } finally {
      FrameSlotAllocator.setPressureListener(previousListener);
      allocator.shutdown();
    }
  }

  /** Reads one page of {@code frameBytes} through the record-page cache, as a reader would. */
  private void read(final ShardedPageCache<CacheablePage> cache, final long index, final long frameBytes) {
    final PageReference key = MemCachePageStub.keyOf(index);
    final CacheablePage page =
        cache.getOrLoadAndGuard(key, ignored -> MemCachePageStub.overFrame(index, allocator, frameBytes));
    assertNotNull(page, "MEM-108: the read of a " + frameBytes + "-byte page must produce a page");
    page.releaseGuard();
  }

  /**
   * JUnit aborts the whole run on an {@link OutOfMemoryError}, so a starved read is reported as the
   * test failure it is instead of escaping as the error.
   */
  private void readOrFail(final ShardedPageCache<CacheablePage> cache, final long index, final long frameBytes) {
    try {
      read(cache, index, frameBytes);
    } catch (final OutOfMemoryError starved) {
      fail("MEM-108: a " + frameBytes + "-byte page must be readable while the cache holds unguarded 64 KiB frames: "
          + starved.getMessage(), starved);
    }
  }

  @Test
  @Timeout(90)
  @DisplayName("MEM-108: after one class fills the budget through the cache, every other class is still readable")
  void everyOtherClassIsReadableAfterOneClassFilledTheBudget() {
    final long limit = FrameBudgetOracle.frameCommitLimit(allocator);
    final ShardedPageCache<CacheablePage> cache = MemCacheSeam.recordPages(manager);
    long index = 0;
    for (long charged = 0; charged < limit; charged += FILL_FRAME) {
      read(cache, index++, FILL_FRAME);
    }
    assertEquals(limit, allocator.getPhysicalMemoryBytes(),
        "the fill must reach L_f for the scenario to mean anything");

    for (final long size : FrameSlotAllocator.SIZE_CLASSES) {
      if (size == FILL_FRAME) {
        continue;
      }
      final long readIndex = index++;
      readOrFail(cache, readIndex, size);
      assertTrue(allocator.getPhysicalMemoryBytes() <= limit,
          "MEM-108: frame charges stay within L_f after the " + size + "-byte read");
    }
  }

  @Test
  @Timeout(90)
  @DisplayName("MEM-108: a read of an exhausted class succeeds by recycling one of its own unguarded frames")
  void aReadOfAnExhaustedClassRecyclesOneOfItsOwnUnguardedFrames() {
    final int slots = 64;
    final int fillClass = FrameSlotAllocator.indexForSize(FILL_FRAME);
    allocator.shutdown();
    allocator =
        MemCacheSeam.newBoundedAllocator(BUDGET, RecordingVirtualMemory.withOsPageBytes(4096L), fillClass, slots);
    final long limit = FrameBudgetOracle.frameCommitLimit(allocator);
    final ShardedPageCache<CacheablePage> cache = MemCacheSeam.recordPages(manager);
    long index = 0;
    final long smallFrame = 32 * 1024L;
    for (int i = 0; i < 8; i++) {
      readAndKeep(cache, index++, smallFrame);
    }
    for (int i = 0; i < slots; i++) {
      readAndKeep(cache, index++, FILL_FRAME);
    }
    assertEquals(slots, allocator.liveSlotCount(fillClass), "the class must be exhausted");
    assertTrue(allocator.getPhysicalMemoryBytes() + FILL_FRAME <= limit, "the budget must have room left");

    readOrFail(cache, index, FILL_FRAME);

    assertEquals(slots, allocator.liveSlotCount(fillClass),
        "MEM-108: one frame of the class was recycled for the read");
    for (int i = 0; i < 8; i++) {
      assertTrue(!held.get(i).isTornDown(), "MEM-108: a frame of another class must stay cached, page " + i);
    }
  }

  /** Reads a page through the cache and keeps it unguarded and observable. */
  private void readAndKeep(final ShardedPageCache<CacheablePage> cache, final long index, final long frameBytes) {
    final PageReference key = MemCachePageStub.keyOf(index);
    final MemCachePageStub page = (MemCachePageStub) cache.getOrLoadAndGuard(key,
        ignored -> MemCachePageStub.overFrame(index, allocator, frameBytes));
    held.add(page);
    page.releaseGuard();
  }
}
