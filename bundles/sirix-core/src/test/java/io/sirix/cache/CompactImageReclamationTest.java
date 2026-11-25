/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.cache.CompactImageStore.Block;
import io.sirix.index.IndexType;
import io.sirix.index.hot.cache.HOTFragmentCache;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Compact-image slabs are admitted against the allocator's permanent allowance, which is the same
 * budget page frames commit from. Without reclamation, retained compact images can fill it and a
 * later page frame can never be committed, however many images are closed afterwards: a released
 * block returns to the pool's free list, not its bytes to the budget.
 *
 * <p>
 * These tests pin the relief and its limits: the shortfall is taken and no more, a slab holding a
 * live image is never taken, a pass that cannot help does not run, and a reclaimed block is never
 * leased again. The strategy is
 * {@code firstmate/data/sirix-compact-decode-arena/reclaim-strategy.md}.
 * </p>
 */
final class CompactImageReclamationTest {

  private static final long KIB = 1024L;

  private static final long MIB = 1024 * KIB;

  /** The budget the review finding named. Its permanent allowance is about 52 MiB. */
  private static final long BUDGET = 64 * MIB;

  /** A typical compact image: the pool serves it from its 4 KiB class, four blocks to a slab. */
  private static final int IMAGE_BYTES = 4096;

  /**
   * The image size the two budget-filling arms use. A full leaf fills the 64 MiB budget's permanent
   * allowance in a few hundred images rather than thirteen thousand, which is the same mechanism in
   * a fixture a unit suite can afford; the pool serves it from its largest class, four to a slab.
   */
  private static final int LARGE_IMAGE_BYTES = 64 * 1024;

  /** The frame the starved scan asks for. */
  private static final long FRAME_BYTES = 64 * KIB;

  @Test
  @DisplayName("a frame request succeeds once the pressure listener's eviction frees idle slabs")
  void aFrameRequestSucceedsThroughTheProductionPressureSequence() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    // The store registers itself with this allocator, so its pressure path can reclaim.
    final CompactImageStore pool = new CompactImageStore(allocator);
    final HOTFragmentCache fragments = new HOTFragmentCache(16 * MIB, BUDGET);
    // Exactly what BufferManagerImpl registers: the caches evict synchronously under pressure.
    FrameSlotAllocator.setPressureListener(fragments::evictUnderPressure);
    try {
      final int cached = cacheCompactImages(pool, fragments);
      assertTrue(cached > 1, "the budget admitted more than one cached compact image");
      final long committedWithImages = allocator.getPhysicalMemoryBytes();
      assertEquals(cached, pool.leasedBlockCount(), "every cached image still holds its block");

      // Nothing in this test closes an image: the frame request must get there by itself, through
      // the listener's eviction and the reclamation that follows it.
      final MemorySegment frame = allocator.allocate(FRAME_BYTES);
      assertNotNull(frame);
      assertEquals(FRAME_BYTES, frame.byteSize());
      assertTrue(pool.leasedBlockCount() < cached, "the listener's eviction closed cached images");
      assertTrue(allocator.getPhysicalMemoryBytes() <= committedWithImages,
          "the discarded slabs' commitment did not come back: " + allocator.getPhysicalMemoryBytes() + " > "
              + committedWithImages);
      allocator.release(frame);
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      fragments.clear();
      pool.releaseIdleSlabs();
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("without reclamation the same production sequence still exhausts the budget")
  void withoutReclamationTheProductionSequenceStillExhaustsTheBudget() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    // Same budget, same slabs, same eviction — but the store draws its slabs through a plain
    // allocator, so it does not register and nothing reclaims. Evicting the images returns their
    // blocks and still cannot make a frame allocatable: that is the defect this change removes.
    final CompactImageStore pool = new CompactImageStore(new ForwardingAllocator(allocator));
    final HOTFragmentCache fragments = new HOTFragmentCache(16 * MIB, BUDGET);
    FrameSlotAllocator.setPressureListener(fragments::evictUnderPressure);
    try {
      assertTrue(cacheCompactImages(pool, fragments) > 1);

      // The wait here is the allocator's own retry loop, not an assertion on time: it fires
      // pressure, parks and retries for ten seconds before it reports the class exhausted.
      final OutOfMemoryError failure = assertThrows(OutOfMemoryError.class, () -> allocator.allocate(FRAME_BYTES));
      assertTrue(failure.getMessage().contains("exhausted"), failure.getMessage());
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      fragments.clear();
      pool.releaseIdleSlabs();
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("the shared cache-pressure boundary evicts the compact half, not only the native one")
  void cachePressureEvictsTheCompactHalf() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16 * MIB);
    final CompactImageStore pool = new CompactImageStore(allocator);
    final HOTFragmentCache fragments = new HOTFragmentCache(16 * MIB, 16 * MIB);
    try {
      final Block block = pool.acquire(IMAGE_BYTES);
      assertNotNull(block);
      final HOTLeafPage image = compactImageOver(block, 1);
      fragments.put(reference(1), image);
      assertEquals(1, pool.leasedBlockCount());

      // One pass clears the second-chance flag, the next evicts it; the production loop fires
      // pressure repeatedly, so both happen there.
      fragments.evictUnderPressure();
      fragments.evictUnderPressure();

      assertTrue(image.isClosed(), "a cached compact image must be evictable under allocator pressure");
      assertEquals(0, pool.leasedBlockCount(), "its block went back to the pool");
      fragments.clear();
      assertEquals(0, pool.leasedBlockCount(), "clearing the cache left no lease behind");
      assertTrue(pool.releaseIdleSlabs(), "the pool had nothing in flight");
      assertEquals("leases=0 acquisitions=0 blocks=0", pool.describeInFlight(), "the pool is empty after cleanup");
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("a slab holding one live image is never reclaimed")
  void aSlabWithALiveImageIsNeverReclaimed() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16 * MIB);
    try {
      final CompactImageStore pool = new CompactImageStore(allocator);
      final List<Block> leased = new ArrayList<>();
      // Two slabs' worth of blocks, every block leased: no slab can be idle.
      for (int i = 0; i < 8; i++) {
        final Block block = pool.acquire(IMAGE_BYTES);
        assertNotNull(block);
        block.segment().fill((byte) (i + 1));
        leased.add(block);
      }
      final long pooledWithLeases = pool.pooledBytes();

      assertEquals(0L, pool.reclaimIdleSlabs(Long.MAX_VALUE / 2));
      assertEquals(pooledWithLeases, pool.pooledBytes(), "a leased block's slab stayed mapped");
      for (int i = 0; i < leased.size(); i++) {
        assertEquals((byte) (i + 1), leased.get(i).segment().get(ValueLayout.JAVA_BYTE, 0L),
            "the live image is still readable");
      }
      leased.forEach(Block::run);
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("only the shortfall is released; the rest stays warm")
  void onlyTheShortfallIsReleased() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16 * MIB);
    try {
      final CompactImageStore pool = new CompactImageStore(allocator);
      final List<Block> blocks = new ArrayList<>();
      for (int i = 0; i < 12; i++) {
        final Block block = pool.acquire(IMAGE_BYTES);
        assertNotNull(block);
        blocks.add(block);
      }
      blocks.forEach(Block::run);
      final long pooledWhenIdle = pool.pooledBytes();
      final long slabBytes = 4L * IMAGE_BYTES;
      assertEquals(3 * slabBytes, pooledWhenIdle, "three idle slabs of four 4 KiB blocks");

      final long released = pool.reclaimIdleSlabs(slabBytes);
      assertEquals(slabBytes, released, "one slab covers the request, so one slab is taken");
      assertEquals(pooledWhenIdle - slabBytes, pool.pooledBytes());

      final long rest = pool.reclaimIdleSlabs(Long.MAX_VALUE / 2);
      assertEquals(2 * slabBytes, rest, "the remaining idle slabs are available to a larger request");
      assertEquals(0L, pool.pooledBytes());
      assertEquals(0, pool.blockCount(), "a reclaimed slab's blocks are not counted as held");
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("a pass that cannot free a whole slab does not run")
  void aPassThatCannotFreeAWholeSlabDoesNotRun() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16 * MIB);
    try {
      final CompactImageStore pool = new CompactImageStore(allocator);
      final Block first = pool.acquire(IMAGE_BYTES);
      final Block second = pool.acquire(IMAGE_BYTES);
      assertNotNull(first);
      assertNotNull(second);
      // Two of the slab's four blocks are free: fewer than a slab's worth, so nothing can be idle.
      second.run();
      final long pooled = pool.pooledBytes();

      assertEquals(0L, pool.reclaimIdleSlabs(Long.MAX_VALUE / 2));
      assertEquals(pooled, pool.pooledBytes());
      first.run();
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("a discarded slab is revived rather than re-mapped, and reads back as zeros")
  void aDiscardedSlabIsRevivedAndReadsAsZeros() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16 * MIB);
    final CompactImageStore pool = new CompactImageStore(allocator);
    try {
      final long[] addresses = new long[4];
      for (int i = 0; i < addresses.length; i++) {
        final Block block = pool.acquire(IMAGE_BYTES);
        assertNotNull(block);
        addresses[i] = block.segment().address();
        block.segment().fill((byte) 0x5A);
        block.run();
      }
      final int carvedBeforeDiscard = pool.blockCount();
      assertTrue(pool.reclaimIdleSlabs(Long.MAX_VALUE / 2) > 0L);
      assertEquals(0, pool.blockCount(), "a discarded slab's blocks are no longer charged");

      // Revival, not a new mapping: the same addresses come back, and their contents are gone.
      for (int i = 0; i < addresses.length; i++) {
        final Block revived = pool.acquire(IMAGE_BYTES);
        assertNotNull(revived);
        assertTrue(contains(addresses, revived.segment().address()),
            "a revived block must address the slab that was discarded, not a freshly mapped one");
        assertEquals((byte) 0, revived.segment().get(ValueLayout.JAVA_BYTE, 0L),
            "a discarded slab must read back as zeros on this platform");
        revived.run();
      }
      assertEquals(carvedBeforeDiscard, pool.blockCount(), "the directory did not grow across the cycle");
    } finally {
      pool.releaseIdleSlabs();
      allocator.shutdown();
    }
  }

  private static boolean contains(final long[] addresses, final long address) {
    for (final long candidate : addresses) {
      if (candidate == address) {
        return true;
      }
    }
    return false;
  }

  /** Cache a compact image per leased block, as a projection point read would. */
  private static int cacheCompactImages(final CompactImageStore pool, final HOTFragmentCache fragments) {
    int cached = 0;
    for (;;) {
      final Block block = pool.acquire(LARGE_IMAGE_BYTES);
      if (block == null) {
        return cached;
      }
      fragments.put(reference(cached), compactImageOver(block, cached));
      cached++;
    }
  }

  /**
   * A compact image over {@code block}, built the way {@code PageKind}'s compact branch builds one:
   * the block is the page's storage and its frame owner, so closing the page returns the block.
   */
  private static HOTLeafPage compactImageOver(final Block block, final int key) {
    block.segment().fill((byte) (key + 1));
    return new HOTLeafPage(key, 1, IndexType.PROJECTION, block.segment(), block, new int[] {0}, 0,
        (int) block.segment().byteSize(), new byte[0], 0, true);
  }

  private static PageReference reference(final int key) {
    return new PageReference().setKey(key).setDatabaseId(0).setResourceId(0);
  }

  /** Lease 4 KiB images until the permanent allowance refuses another slab. */
  private static List<Block> fillPermanentAllowance(final CompactImageStore pool) {
    final List<Block> images = new ArrayList<>();
    for (;;) {
      final Block block = pool.acquire(IMAGE_BYTES);
      if (block == null) {
        return images;
      }
      images.add(block);
    }
  }

  /**
   * Forwards every allocator call, while not being a {@link FrameSlotAllocator}: a store built on it
   * never registers, so its slabs are admitted and released exactly as before but nothing reclaims
   * them. The arm this makes possible is the defect the change removes.
   */
  private record ForwardingAllocator(FrameSlotAllocator delegate) implements MemorySegmentAllocator {

    @Override
    public @Nullable MemorySegment allocatePermanentSlab(final long bytes) {
      return delegate.allocatePermanentSlab(bytes);
    }

    @Override
    public @Nullable MemorySegment allocateSlab(final long bytes) {
      return delegate.allocateSlab(bytes);
    }

    @Override
    public boolean releaseSlab(final MemorySegment slab) {
      return delegate.releaseSlab(slab);
    }

    @Override
    public void init(final long maxBufferSize) {
      delegate.init(maxBufferSize);
    }

    @Override
    public boolean isInitialized() {
      return delegate.isInitialized();
    }

    @Override
    public void free() {
      delegate.free();
    }

    @Override
    public MemorySegment allocate(final long size) {
      return delegate.allocate(size);
    }

    @Override
    public void release(final MemorySegment segment) {
      delegate.release(segment);
    }

    @Override
    public long getMaxBufferSize() {
      return delegate.getMaxBufferSize();
    }

    @Override
    public void resetSegment(final MemorySegment segment) {
      delegate.resetSegment(segment);
    }
  }
}
