/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.cache.CompactImageStore.Block;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
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

  /** The frame the starved scan asks for. */
  private static final long FRAME_BYTES = 64 * KIB;

  @Test
  @DisplayName("a frame request succeeds once the idle compact slabs of its shortfall are reclaimed")
  void aFrameRequestSucceedsOnceIdleCompactSlabsAreReclaimed() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(null);
    try {
      // The store registers itself with this allocator, so its pressure path can reclaim.
      final CompactImageStore pool = new CompactImageStore(allocator);
      final List<Block> images = fillPermanentAllowance(pool);
      assertTrue(images.size() > 1, "the budget admitted more than one compact image");
      final long committedWithImages = allocator.getPhysicalMemoryBytes();

      // Every page closes. The blocks are free, but their slabs are still mapped and committed.
      images.forEach(Block::run);
      assertEquals(0, pool.leasedBlockCount());
      assertEquals(committedWithImages, allocator.getPhysicalMemoryBytes(),
          "closing an image returns its block, never its bytes");

      final MemorySegment frame = allocator.allocate(FRAME_BYTES);
      assertNotNull(frame);
      assertEquals(FRAME_BYTES, frame.byteSize());
      // The refund shows after the retried reservation drained what the release parked.
      assertTrue(allocator.getPhysicalMemoryBytes() <= committedWithImages,
          "the reclaimed slabs' commitment did not come back: " + allocator.getPhysicalMemoryBytes() + " > "
              + committedWithImages);
      allocator.release(frame);
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("without reclamation the same frame request exhausts the budget")
  void withoutReclamationTheSameFrameRequestExhaustsTheBudget() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final FrameSlotAllocator.PressureListener previous = FrameSlotAllocator.pressureListener();
    FrameSlotAllocator.setPressureListener(null);
    // Same budget and the same slabs, but the store draws them through a plain allocator, so it
    // does not register and nothing reclaims. This is the state the review finding describes - and
    // the reason this test has to give the slabs back itself at the end.
    final CompactImageStore pool = new CompactImageStore(new ForwardingAllocator(allocator));
    try {
      final List<Block> images = fillPermanentAllowance(pool);
      images.forEach(Block::run);
      assertEquals(0, pool.leasedBlockCount());

      // The wait here is the allocator's own retry loop, not an assertion on time: it fires
      // pressure, parks and retries for ten seconds before it reports the class exhausted.
      final OutOfMemoryError failure = assertThrows(OutOfMemoryError.class, () -> allocator.allocate(FRAME_BYTES));
      assertTrue(failure.getMessage().contains("exhausted"), failure.getMessage());
    } finally {
      FrameSlotAllocator.setPressureListener(previous);
      pool.releaseIdleSlabs();
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
  @DisplayName("a reclaimed block is never leased again")
  void aReclaimedBlockIsNeverLeasedAgain() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(16 * MIB);
    try {
      final CompactImageStore pool = new CompactImageStore(allocator);
      final Map<Block, Boolean> reclaimedBlocks = new IdentityHashMap<>();
      for (int i = 0; i < 4; i++) {
        final Block block = pool.acquire(IMAGE_BYTES);
        assertNotNull(block);
        reclaimedBlocks.put(block, Boolean.TRUE);
        block.run();
      }
      assertTrue(pool.reclaimIdleSlabs(Long.MAX_VALUE / 2) > 0L);

      for (int i = 0; i < 4; i++) {
        final Block fresh = pool.acquire(IMAGE_BYTES);
        assertNotNull(fresh);
        assertNull(reclaimedBlocks.get(fresh), "a block of a released slab was handed out again");
        fresh.segment().fill((byte) 9);
        fresh.run();
      }
    } finally {
      allocator.shutdown();
    }
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
