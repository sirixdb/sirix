/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.cache.CompactImageStore.Block;
import io.sirix.index.IndexType;
import io.sirix.index.hot.cache.HOTFragmentCache;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The compact-image pool has one owner and one budget: the share that already bounds what the
 * fragment cache's compact half retains. It charges none of the allocator's frame commitment, so
 * cached compact images cannot starve a page frame, and a full pool refuses a lease rather than
 * taking bytes from anyone — the decoder then keeps its own GC-owned image.
 *
 * <p>
 * These tests pin that split. The production sequence is the regression witness: a cache full of
 * compact images, nothing closed by the test, then the page frame a scan needs. Its negative arm
 * charges the pool's slabs to the frame allowance, which is what the first version of this pool did,
 * and the same sequence then starves. Memory comes back the way the owning cache's memory always
 * comes back — the cache evicts, the image closes, the block returns and the next decode reuses it.
 * Design and the established shapes it follows:
 * {@code firstmate/data/sirix-compact-decode-arena/reclaim-strategy.md}.
 * </p>
 */
final class CompactImagePoolBudgetTest {

  private static final long KIB = 1024L;

  private static final long MIB = 1024 * KIB;

  /** The budget the review finding named. Its permanent frame allowance is about 52 MiB. */
  private static final long BUDGET = 64 * MIB;

  /**
   * The pool ceiling these tests give the compact half. Eight slabs of the largest class, which is
   * enough for the fill arms to reach it in a few dozen images instead of thousands.
   */
  private static final long POOL_CEILING = 2 * MIB;

  /** A full leaf image: the pool serves it from its largest class, four blocks to a slab. */
  private static final int LARGE_IMAGE_BYTES = 64 * 1024;

  /** The frame the scan asks for after the images are cached. */
  private static final long FRAME_BYTES = 64 * KIB;

  @Test
  @DisplayName("a frame request succeeds although the compact-image pool is full")
  void aFullPoolDoesNotStarveAPageFrame() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final CompactImageStore pool = new CompactImageStore(allocator, POOL_CEILING);
    final HOTFragmentCache fragments = new HOTFragmentCache(16 * MIB, BUDGET);
    try {
      final int cached = cacheCompactImages(pool, fragments);
      assertTrue(cached > 1, "the pool admitted more than one cached compact image");
      assertEquals(cached, pool.leasedBlockCount(), "every cached image still holds its block");
      assertEquals(POOL_CEILING, pool.mappedSlabBytes(), "the pool is at its ceiling");
      assertEquals(0L, allocator.nonExecutionCommittedBytes(),
          "a full pool charged none of the allocator's commitment");

      // Nothing here closes an image, evicts a cache or reclaims anything: the frame request must
      // succeed because the pool was never allowed to hold the allowance it commits from.
      final MemorySegment frame = allocator.allocate(FRAME_BYTES);
      assertNotNull(frame);
      assertEquals(FRAME_BYTES, frame.byteSize());
      assertEquals(cached, pool.leasedBlockCount(), "serving the frame cost no cached image");
      allocator.release(frame);
    } finally {
      fragments.clear();
      assertTrue(pool.releaseIdleSlabs());
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("charging the pool's slabs to the frame allowance starves the same frame request")
  void aPoolChargedToTheFrameAllowanceStarvesThatFrame() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    // The admission the first version of this pool used: every slab against the permanent frame
    // allowance, which is the allowance page frames commit from. Same slabs, same images, same
    // ceiling — only the budget they answer to differs.
    final CompactImageStore pool = new CompactImageStore(new FrameChargingAllocator(allocator), Long.MAX_VALUE / 4);
    final HOTFragmentCache fragments = new HOTFragmentCache(16 * MIB, BUDGET);
    try {
      assertTrue(cacheCompactImages(pool, fragments) > 1);
      assertTrue(allocator.nonExecutionCommittedBytes() > 32 * MIB,
          "the cached images hold the frame allowance: " + allocator.nonExecutionCommittedBytes());

      // The wait inside is the allocator's own retry loop, not an assertion on time: it fires
      // pressure, parks and retries before it reports the class exhausted.
      final OutOfMemoryError failure = assertThrows(OutOfMemoryError.class, () -> allocator.allocate(FRAME_BYTES));
      assertTrue(failure.getMessage().contains("exhausted"), failure.getMessage());
    } finally {
      fragments.clear();
      pool.releaseIdleSlabs();
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("the pool refuses at its own ceiling and never maps past it")
  void thePoolStopsAtItsOwnCeiling() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final CompactImageStore pool = new CompactImageStore(allocator, POOL_CEILING);
    try {
      final List<Block> held = fillPool(pool);
      try {
        assertEquals(POOL_CEILING, pool.mappedSlabBytes());
        assertNull(pool.acquire(LARGE_IMAGE_BYTES), "a full pool refuses rather than mapping more");
        assertTrue(pool.mappedSlabBytes() <= pool.maxSlabBytes());
        assertEquals(POOL_CEILING / LARGE_IMAGE_BYTES, held.size(), "the ceiling admitted exactly its blocks");
      } finally {
        for (final Block block : held) {
          block.run();
        }
      }
    } finally {
      assertTrue(pool.releaseIdleSlabs());
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("a pool with no share of its own refuses every lease")
  void aZeroCeilingRefusesEveryLease() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final CompactImageStore pool = new CompactImageStore(allocator, 0L);
    try {
      assertNull(pool.acquire(LARGE_IMAGE_BYTES));
      assertNull(pool.acquire(64));
      assertEquals(0L, pool.mappedSlabBytes());
      assertEquals(0, pool.blockCount());
    } finally {
      assertTrue(pool.releaseIdleSlabs());
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("a returned block is reused, so churn never grows the pool past its ceiling")
  void areturnedBlockIsReusedInsteadOfGrowingThePool() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final CompactImageStore pool = new CompactImageStore(allocator, POOL_CEILING);
    try {
      final Block first = pool.acquire(LARGE_IMAGE_BYTES);
      assertNotNull(first);
      final long afterFirstSlab = pool.mappedSlabBytes();
      assertTrue(afterFirstSlab > 0L);
      final long address = first.segment().address();
      first.run();
      for (int round = 0; round < 64; round++) {
        final Block block = pool.acquire(LARGE_IMAGE_BYTES);
        assertNotNull(block);
        assertEquals(afterFirstSlab, pool.mappedSlabBytes(), "a reused block maps nothing");
        block.run();
      }
      final Block again = pool.acquire(LARGE_IMAGE_BYTES);
      assertNotNull(again);
      assertEquals(address, again.segment().address(), "the same block came back");
      again.run();
    } finally {
      assertTrue(pool.releaseIdleSlabs());
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("the owning cache's eviction returns the blocks, which is how the pool's memory comes back")
  void evictingTheOwningCacheReturnsItsBlocks() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final CompactImageStore pool = new CompactImageStore(allocator, POOL_CEILING);
    final HOTFragmentCache fragments = new HOTFragmentCache(16 * MIB, BUDGET);
    try {
      final int cached = cacheCompactImages(pool, fragments);
      assertEquals(cached, pool.leasedBlockCount());
      final long mapped = pool.mappedSlabBytes();

      // Two passes: the cache gives a freshly inserted page one reprieve (second chance), so the
      // first pass clears that flag and the second is the one that evicts. Production fires this
      // repeatedly, from the compact half's own clock sweeper.
      fragments.compactImages().evictUnderPressure();
      fragments.compactImages().evictUnderPressure();
      assertTrue(pool.leasedBlockCount() < cached, "eviction closed cached images and returned their blocks");

      final Block reused = pool.acquire(LARGE_IMAGE_BYTES);
      assertNotNull(reused, "a returned block serves the next decode");
      assertEquals(mapped, pool.mappedSlabBytes(), "and it serves it without mapping anything");
      reused.run();
    } finally {
      fragments.clear();
      assertTrue(pool.releaseIdleSlabs());
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("a cleanup refused by a live lease names what still holds the pool")
  void aRefusedCleanupNamesWhatHoldsThePool() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(BUDGET);
    final CompactImageStore pool = new CompactImageStore(allocator, POOL_CEILING);
    try {
      final Block held = pool.acquire(LARGE_IMAGE_BYTES);
      assertNotNull(held);
      assertFalse(pool.releaseIdleSlabs(), "a live lease must refuse cleanup");
      assertEquals("leases=1 acquisitions=0 blocks=4", pool.describeInFlight());
      held.run();
      assertTrue(pool.releaseIdleSlabs(), "cleanup succeeds once the lease is returned");
      assertEquals("leases=0 acquisitions=0 blocks=0", pool.describeInFlight());
    } finally {
      allocator.shutdown();
    }
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

  /** Lease full-leaf images until the pool's own ceiling refuses another slab. */
  private static List<Block> fillPool(final CompactImageStore pool) {
    final List<Block> images = new ArrayList<>();
    for (;;) {
      final Block block = pool.acquire(LARGE_IMAGE_BYTES);
      if (block == null) {
        return images;
      }
      images.add(block);
    }
  }

  /**
   * Charges every pool slab to the permanent frame allowance before mapping it, and refunds it on
   * release: the admission the pool used before it had a budget of its own. The arm this makes
   * possible is the starvation the current admission removes.
   */
  private record FrameChargingAllocator(FrameSlotAllocator delegate) implements MemorySegmentAllocator {

    @Override
    public @Nullable MemorySegment allocateOwnerBudgetedSlab(final long bytes) {
      if (!delegate.reserveSlabCommittedBytes(bytes)) {
        return null;
      }
      final MemorySegment slab = delegate.allocateOwnerBudgetedSlab(bytes);
      if (slab == null) {
        delegate.refundCommittedBytes(bytes);
      }
      return slab;
    }

    @Override
    public @Nullable MemorySegment allocateSlab(final long bytes) {
      return delegate.allocateSlab(bytes);
    }

    @Override
    public boolean releaseSlab(final MemorySegment slab) {
      final long bytes = slab.byteSize();
      final boolean released = delegate.releaseSlab(slab);
      if (released) {
        delegate.refundCommittedBytes(bytes);
      }
      return released;
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
