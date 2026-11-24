/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.sirix.cache.CompactImageStore.Block;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The lease contract of the reusable blocks a compact page image owns: one owner, exact reuse, a
 * loud double release, and a refusal the caller can fall back from.
 */
final class CompactImageStoreTest {

  /** A store over its own slab arena, so a test never disturbs the process-wide one. */
  private static CompactImageStore storeOver(final SlabRecordingAllocator allocator) {
    return new CompactImageStore(allocator);
  }

  @Test
  @DisplayName("every legal image size has a class that holds it with under an eighth of slack")
  void theClassTableHoldsEverySizeWithBoundedSlack() {
    int previousBlock = 0;
    for (int size = 0; size <= CompactImageStore.MAX_BLOCK_BYTES; size++) {
      final int block = CompactImageStore.blockBytesFor(size);
      assertTrue(block >= size, "size " + size + " does not fit block " + block);
      assertTrue(block >= previousBlock, "the class table is monotone at size " + size);
      previousBlock = block;
      final long slack = block - (long) size;
      final long allowed = Math.max(CompactImageStore.MIN_BLOCK_BYTES, size / 8);
      assertTrue(slack <= allowed, "size " + size + " wastes " + slack + " of " + block + " bytes");
      final int index = CompactImageStore.classIndexFor(size);
      assertEquals(block, CompactImageStore.blockBytesOfClass(index),
          "size " + size + " is routed to a class of a different block size");
    }
    assertEquals(CompactImageStore.MIN_BLOCK_BYTES, CompactImageStore.blockBytesFor(0));
    assertEquals(CompactImageStore.MAX_BLOCK_BYTES, previousBlock);
  }

  @ParameterizedTest
  @CsvSource({"0,64", "1,64", "64,64", "65,128", "512,512", "513,576", "1024,1024", "1025,1152", "65536,65536"})
  @DisplayName("a request is served by the smallest class that holds it")
  void blockSizeIsTheSmallestClassThatHolds(final int requested, final int expectedBlockBytes) {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      final Block block = storeOver(allocator).acquire(requested);
      assertNotNull(block);
      assertEquals(expectedBlockBytes, block.segment().byteSize());
      assertTrue(block.segment().isNative());
      assertTrue(block.isLeased());
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  @DisplayName("a request above the largest block is refused rather than served from a fresh mapping")
  void oversizedRequestsAreRefused() {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      assertNull(storeOver(allocator).acquire(CompactImageStore.MAX_BLOCK_BYTES + 1L));
      assertEquals(0, allocator.slabCount());
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  @DisplayName("a negative request is a programming error")
  void negativeRequestsAreRejected() {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      assertThrows(IllegalArgumentException.class, () -> storeOver(allocator).acquire(-1L));
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  @DisplayName("a released block is leased again instead of a new one being carved")
  void releasedBlocksAreReused() {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      final CompactImageStore store = storeOver(allocator);
      final Block first = store.acquire(300);
      assertNotNull(first);
      final int carved = store.blockCount();
      assertEquals(1, store.leasedBlockCount());
      first.run();
      assertFalse(first.isLeased());
      assertEquals(0, store.leasedBlockCount());
      final Block second = store.acquire(300);
      assertSame(first, second, "the same block answers the next request of its class");
      assertEquals(carved, store.blockCount(), "no block is carved while one is free");
      assertEquals(1, allocator.slabCount(), "and no further slab is mapped");
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  @DisplayName("releasing a block that is not leased is refused instead of freeing it twice")
  void doubleReleaseIsRefused() {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      final CompactImageStore store = storeOver(allocator);
      final Block block = store.acquire(1000);
      assertNotNull(block);
      block.run();
      assertThrows(IllegalStateException.class, block::run);
      assertEquals(0, store.leasedBlockCount(), "the refused release did not double-count the block");
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  @DisplayName("a store whose allocator admits no slab refuses the lease, leaving the caller its fallback")
  void aRefusedSlabRefusesTheLease() {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    allocator.refuseSlabs(true);
    try {
      final CompactImageStore store = storeOver(allocator);
      assertNull(store.acquire(512));
      assertEquals(0, store.blockCount());
      allocator.refuseSlabs(false);
      assertNotNull(store.acquire(512), "a later request succeeds once the budget admits a slab");
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  @DisplayName("the test-only refusal switch makes every lease refuse")
  void theRefusalSwitchRefusesEveryLease() {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      final CompactImageStore store = storeOver(allocator);
      store.setRefuseLeasesForTesting(true);
      assertNull(store.acquire(512));
      store.setRefuseLeasesForTesting(false);
      assertNotNull(store.acquire(512));
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  @DisplayName("a poisoned release overwrites the block, so a stale reader cannot find its image")
  void poisonedReleaseOverwritesTheBlock() {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      final CompactImageStore store = storeOver(allocator);
      store.setPoisonOnReleaseForTesting(true);
      final Block block = store.acquire(300);
      assertNotNull(block);
      block.segment().fill((byte) 7);
      block.run();
      assertEquals((byte) 0x5A, block.segment().get(ValueLayout.JAVA_BYTE, 0L));
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  @DisplayName("idle slabs go back to the allocator; a leased block keeps them")
  void idleSlabsAreReturnedAndLeasedOnesAreNot() {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      final CompactImageStore store = storeOver(allocator);
      final Block block = store.acquire(300);
      assertNotNull(block);
      assertFalse(store.releaseIdleSlabs(), "a leased block must keep its slab mapped");
      assertEquals(0, allocator.releasedSlabCount());
      block.run();
      assertTrue(store.releaseIdleSlabs());
      assertEquals(allocator.slabCount(), allocator.releasedSlabCount());
      assertEquals(0, store.blockCount());
      assertEquals(0L, store.pooledBytes());
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  void shutdownCannotUnmapAnAcquisitionBeforeHandout() throws Exception {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    final CompactImageStore store = storeOver(allocator);
    final ExecutorService worker = Executors.newSingleThreadExecutor();
    final CountDownLatch popped = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    try {
      final Block warm = store.acquire(4096);
      assertNotNull(warm);
      warm.run();
      store.setLeasePublicationHookForTesting(() -> pause(popped, proceed));
      final Future<Block> pending = worker.submit(() -> store.acquire(4096));
      assertTrue(popped.await(10, TimeUnit.SECONDS));
      assertEquals(0, store.leasedBlockCount());
      assertFalse(store.releaseIdleSlabs());
      assertEquals(0, allocator.releasedSlabCount());
      proceed.countDown();
      final Block acquired = pending.get(10, TimeUnit.SECONDS);
      assertSame(warm, acquired);
      acquired.segment().fill((byte) 7);
      acquired.run();
      assertTrue(store.releaseIdleSlabs());
      assertNull(store.acquire(4096));
    } finally {
      proceed.countDown();
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
      allocator.closeArena();
    }
  }

  @Test
  void shutdownStopsWarmAndColdAdmissionBeforeUnmapping() throws Exception {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    final CompactImageStore store = storeOver(allocator);
    final ExecutorService worker = Executors.newSingleThreadExecutor();
    final CountDownLatch checked = new CountDownLatch(1);
    final CountDownLatch proceed = new CountDownLatch(1);
    try {
      final Block warm = store.acquire(4096);
      assertNotNull(warm);
      warm.run();
      store.setSlabReleaseHookForTesting(() -> pause(checked, proceed));
      final Future<Boolean> shutdown = worker.submit(store::releaseIdleSlabs);
      assertTrue(checked.await(10, TimeUnit.SECONDS));
      assertNull(store.acquire(4096));
      assertNull(store.acquire(8192));
      assertEquals(1, allocator.slabCount());
      assertEquals(0, allocator.releasedSlabCount());
      proceed.countDown();
      assertTrue(shutdown.get(10, TimeUnit.SECONDS));
      assertEquals(1, allocator.releasedSlabCount());
      assertEquals(0, store.blockCount());
      assertEquals(0L, store.pooledBytes());
      assertNull(store.acquire(4096));
    } finally {
      proceed.countDown();
      worker.shutdownNow();
      assertTrue(worker.awaitTermination(10, TimeUnit.SECONDS));
      allocator.closeArena();
    }
  }

  @ParameterizedTest
  @CsvSource({"0,false", "0,true", "4,false", "4,true", "12,false", "12,true"})
  void unpublishedSlabIsReturnedWithoutReplacingTheFailure(final int heldBlocks,
      final boolean cleanupFails) {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      final CompactImageStore store = storeOver(allocator);
      final List<Block> held = new ArrayList<>();
      for (int i = 0; i < heldBlocks; i++) {
        final Block block = store.acquire(4096);
        assertNotNull(block);
        held.add(block);
      }
      final OutOfMemoryError failure = new OutOfMemoryError("publication failed");
      final IllegalStateException cleanupFailure = new IllegalStateException("cleanup failed");
      allocator.releaseFailure = cleanupFails ? cleanupFailure : null;
      store.setSlabPublicationHookForTesting(() -> {
        throw failure;
      });
      assertSame(failure, assertThrows(OutOfMemoryError.class, () -> store.acquire(4096)));
      assertEquals(heldBlocks / 4 + 1, allocator.slabCount());
      assertEquals(1, allocator.releasedSlabCount());
      assertEquals(heldBlocks, store.blockCount());
      assertEquals((long) heldBlocks * 4096, store.pooledBytes());
      assertEquals(cleanupFails ? 1 : 0, failure.getSuppressed().length);
      if (cleanupFails) {
        assertSame(cleanupFailure, failure.getSuppressed()[0]);
      }
      allocator.releaseFailure = null;
      store.setSlabPublicationHookForTesting(null);
      final Block recovered = store.acquire(4096);
      assertNotNull(recovered);
      recovered.segment().fill((byte) 9);
      recovered.run();
      for (final Block block : held) {
        block.segment().fill((byte) 8);
        block.run();
      }
      assertTrue(store.releaseIdleSlabs());
      assertEquals(allocator.slabCount(), allocator.releasedSlabCount());
    } finally {
      allocator.closeArena();
    }
  }

  @Test
  void coldPopulationHasLinearDirectoryCopyCost() {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    try {
      final CompactImageStore store = storeOver(allocator);
      final int count = 32768;
      final List<Block> held = new ArrayList<>(count);
      for (int i = 0; i < count; i++) {
        final Block block = store.acquire(4096);
        assertNotNull(block);
        held.add(block);
        if (i == 8) {
          assertEquals(12, store.blockCount());
          assertEquals(49152L, store.pooledBytes());
        }
      }
      assertEquals(count, store.blockCount());
      assertEquals((long) count * 4096, store.pooledBytes());
      assertTrue(store.directoryGrowthCount() <= 14,
          "32768 blocks need at most fourteen geometric directory allocations, not 8192");
      for (final Block block : held) {
        block.run();
      }
      assertTrue(store.releaseIdleSlabs());
      assertEquals(0, store.blockCount());
      assertEquals(0L, store.pooledBytes());
    } finally {
      allocator.closeArena();
    }
  }

  private static void pause(final CountDownLatch arrived, final CountDownLatch proceed) {
    arrived.countDown();
    try {
      if (!proceed.await(10, TimeUnit.SECONDS)) {
        throw new IllegalStateException("timed out waiting for the test operation");
      }
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(interrupted);
    }
  }

  @Test
  @DisplayName("concurrent leases never hand the same block to two owners")
  void concurrentLeasesAreExclusive() throws InterruptedException {
    final SlabRecordingAllocator allocator = new SlabRecordingAllocator();
    final int threads = 8;
    final int rounds = 4_000;
    final CompactImageStore store = storeOver(allocator);
    final ConcurrentHashMap<Block, Thread> held = new ConcurrentHashMap<>();
    final AtomicReference<String> conflict = new AtomicReference<>();
    final AtomicInteger refusals = new AtomicInteger();
    final CountDownLatch start = new CountDownLatch(1);
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      for (int t = 0; t < threads; t++) {
        pool.execute(() -> {
          try {
            start.await();
          } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
          }
          for (int i = 0; i < rounds && conflict.get() == null; i++) {
            final Block block = store.acquire(300 + (i & 7) * 200);
            if (block == null) {
              refusals.incrementAndGet();
              continue;
            }
            final Thread previous = held.putIfAbsent(block, Thread.currentThread());
            if (previous != null) {
              conflict.compareAndSet(null, "block leased twice, also to " + previous.getName());
              return;
            }
            // Writing the whole block would detect an overlap of two blocks' bytes as well.
            block.segment().fill((byte) 1);
            held.remove(block);
            block.run();
          }
        });
      }
      start.countDown();
      pool.shutdown();
      assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "the lease loops finished");
      assertNull(conflict.get());
      assertEquals(0, refusals.get(), "the stub allocator admits every slab");
      assertEquals(0, store.leasedBlockCount(), "every lease was returned");
    } finally {
      allocator.closeArena();
    }
  }

  /** Slabs from one shared arena, so the blocks are real native memory with no global budget. */
  private static final class SlabRecordingAllocator implements MemorySegmentAllocator {

    private final Arena arena = Arena.ofShared();

    private final List<MemorySegment> slabs = new ArrayList<>();

    private final List<MemorySegment> released = new ArrayList<>();

    private volatile boolean refuse;

    private @Nullable RuntimeException releaseFailure;

    void refuseSlabs(final boolean value) {
      refuse = value;
    }

    synchronized int slabCount() {
      return slabs.size();
    }

    synchronized int releasedSlabCount() {
      return released.size();
    }

    void closeArena() {
      arena.close();
    }

    @Override
    public synchronized @Nullable MemorySegment allocatePermanentSlab(final long bytes) {
      if (refuse) {
        return null;
      }
      final MemorySegment slab = arena.allocate(bytes, 8L);
      slabs.add(slab);
      return slab;
    }

    @Override
    public synchronized boolean releaseSlab(final MemorySegment slab) {
      released.add(slab);
      if (releaseFailure != null) {
        throw releaseFailure;
      }
      return true;
    }

    @Override
    public void init(final long maxBufferSize) {
      // Nothing to initialise: the arena above is the whole budget.
    }

    @Override
    public boolean isInitialized() {
      return true;
    }

    @Override
    public void free() {
      closeArena();
    }

    @Override
    public MemorySegment allocate(final long size) {
      throw new UnsupportedOperationException("the compact-image store only takes slabs");
    }

    @Override
    public void release(final MemorySegment segment) {
      throw new UnsupportedOperationException("the compact-image store only takes slabs");
    }

    @Override
    public long getMaxBufferSize() {
      return Long.MAX_VALUE;
    }

    @Override
    public void resetSegment(final MemorySegment segment) {
      segment.fill((byte) 0);
    }
  }
}
