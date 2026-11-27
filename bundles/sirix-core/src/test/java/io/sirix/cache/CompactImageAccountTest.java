/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-301 and MEM-302: the compact-image pool, its fallback images and its native metadata are one
 * account, reserved from the allocator budget <em>B</em>, never above the owner's share; a full
 * account evicts compact images up to the shortfall and then decodes uncached into a scratch lease;
 * idle slabs stay mapped and charged until shutdown and MEM-103 transfers never take budget from
 * them.
 *
 * <p>
 * Planned (step 3): the pool does not exist yet, so every member is bound through
 * {@link MemCompactSeam}; the allocator is the real {@link FrameSlotAllocator}. No timing is
 * asserted.
 */
@MemPlanned(step = 3, value = {"MEM-301", "MEM-302"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class CompactImageAccountTest {

  private static final long MIB = 1024L * 1024L;
  private static final int SHARE_UNITS = 4;
  private static final int LARGEST_CLASS = FrameSlotAllocator.SIZE_CLASSES.length - 1;

  /**
   * An allocator below the 64 MiB headroom threshold, so its frame limit and content limit coincide.
   */
  private static FrameSlotAllocator newAllocator(final long unit) {
    return new FrameSlotAllocator(Math.max(32L * MIB, 8L * unit));
  }

  private static void quiesce(final FrameSlotAllocator allocator) {
    allocator.scratch().reclaimAllIdle();
  }

  /**
   * The account never exceeds the share, grows in whole slab units and is always inside
   * committedBytes.
   */
  private static void assertAccount(final FrameSlotAllocator allocator, final MemCompactSeam.Pool pool, final long unit,
      final String after) {
    final long account = pool.accountBytes();
    assertTrue(pool.slabBytes() >= 0 && pool.fallbackImageBytes() >= 0 && pool.metadataBytes() >= 0,
        "MEM-301 no negative part of the account after " + after);
    assertTrue(account <= pool.shareBytes(),
        "MEM-301 the account " + account + " exceeds the share " + pool.shareBytes() + " after " + after);
    assertEquals(0L, pool.slabBytes() % unit,
        "MEM-301 the account is reserved in slab-sized units, got " + pool.slabBytes() + " after " + after);
    assertTrue(allocator.getPhysicalMemoryBytes() >= account,
        "MEM-301 every byte of the account is charged to committedBytes: committed="
            + allocator.getPhysicalMemoryBytes() + " account=" + account + " after " + after);
  }

  private static void closeLeaseKeepingSlabs(final FrameSlotAllocator allocator, final MemCompactSeam.Pool pool,
      final Object lease) {
    final long mapped = pool.slabBytes();
    final long charged = allocator.getPhysicalMemoryBytes();
    pool.close(lease);
    assertEquals(mapped, pool.slabBytes(), "MEM-302 releasing a lease keeps every mapped slab");
    assertEquals(charged, allocator.getPhysicalMemoryBytes(), "MEM-302 releasing a lease keeps its charge");
  }

  private static void closeDecodeKeepingSlabs(final FrameSlotAllocator allocator, final MemCompactSeam.Pool pool,
      final Object decoded) {
    final long mapped = pool.slabBytes();
    final long charged = allocator.getPhysicalMemoryBytes() - allocator.scratch().regionBytes()
        - pool.fallbackImageBytes() - pool.metadataBytes();
    pool.closeDecoded(decoded);
    assertEquals(mapped, pool.slabBytes(), "MEM-302 closing a decode keeps every mapped slab");
    assertEquals(charged, allocator.getPhysicalMemoryBytes() - allocator.scratch().regionBytes()
        - pool.fallbackImageBytes() - pool.metadataBytes(), "MEM-302 closing a decode keeps its slab charge");
    assertTrue(allocator.getPhysicalMemoryBytes() >= mapped, "MEM-302 every mapped slab stays charged");
  }

  @ParameterizedTest(name = "seed {0}")
  @ValueSource(longs = {1L, 7L, 42L, 1_000_003L, 20_261_126L})
  @DisplayName("MEM-301: account <= share and inside committedBytes after every step of a seeded sequence; shutdown returns it to 0")
  void accountInvariantHoldsOverASeededSequence(final long seed) {
    final long unit = MemCompactSeam.slabUnitBytes();
    final FrameSlotAllocator allocator = newAllocator(unit);
    final MemCompactSeam.Pool pool = MemCompactSeam.create(allocator, SHARE_UNITS * unit);
    final List<Object> leases = new ArrayList<>();
    final List<Object> decodes = new ArrayList<>();
    final Random random = new Random(seed);
    try {
      assertEquals(SHARE_UNITS * unit, pool.shareBytes(), "the pool reports the share it was given");
      for (int step = 0; step < 300; step++) {
        final int operation = random.nextInt(6);
        final String name;
        switch (operation) {
          case 0, 1 -> {
            name = "lease";
            final Object lease = pool.lease(Math.max(64L, random.nextLong(unit / 2)));
            if (lease != null) {
              leases.add(lease);
            }
          }
          case 2 -> {
            name = "release";
            if (!leases.isEmpty()) {
              closeLeaseKeepingSlabs(allocator, pool, leases.remove(random.nextInt(leases.size())));
            }
          }
          case 3, 4 -> {
            name = "decode";
            decodes.add(pool.decode(random.nextInt(48), Math.max(64L, random.nextLong(unit / 4))));
          }
          default -> {
            name = "close decode";
            if (!decodes.isEmpty()) {
              closeDecodeKeepingSlabs(allocator, pool, decodes.remove(random.nextInt(decodes.size())));
            }
          }
        }
        assertAccount(allocator, pool, unit, name + " (step " + step + ", seed " + seed + ")");
      }
      leases.forEach(lease -> closeLeaseKeepingSlabs(allocator, pool, lease));
      decodes.forEach(decoded -> closeDecodeKeepingSlabs(allocator, pool, decoded));
      leases.clear();
      decodes.clear();
      quiesce(allocator);
      assertAccount(allocator, pool, unit, "closing everything");
      assertEquals(pool.accountBytes(), allocator.getPhysicalMemoryBytes(),
          "MEM-301 with nothing else live, committedBytes is exactly the account");
    } finally {
      leases.forEach(lease -> closeLeaseKeepingSlabs(allocator, pool, lease));
      decodes.forEach(decoded -> closeDecodeKeepingSlabs(allocator, pool, decoded));
      pool.shutdown();
      assertEquals(0L, pool.accountBytes(), "MEM-301 shutdown returns the account to 0");
      quiesce(allocator);
      assertEquals(0L, allocator.getPhysicalMemoryBytes(), "MEM-301 shutdown refunds every byte of the account");
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-301: a refused reservation makes the pool refuse the lease and leaves the account untouched")
  void refusedReservationMakesThePoolRefuseTheLease() {
    final long unit = MemCompactSeam.slabUnitBytes();
    final FrameSlotAllocator allocator = newAllocator(unit);
    final MemCompactSeam.Pool pool = MemCompactSeam.create(allocator, SHARE_UNITS * unit);
    final List<FrameSlotAllocator.FrameSlot> held = new ArrayList<>();
    try {
      // Owned frames (nothing a transfer can claim) take the whole frame limit.
      FrameSlotAllocator.FrameSlot frame;
      while ((frame = allocator.allocateSlot(FrameSlotAllocator.SIZE_CLASSES[LARGEST_CLASS])) != null) {
        held.add(frame);
      }
      final long committed = allocator.getPhysicalMemoryBytes();
      assertFalse(held.isEmpty(), "the fixture must fill the budget with owned frames");
      assertNull(pool.lease(unit / 4), "MEM-301 a refused reservation makes the pool refuse the lease");
      assertEquals(0L, pool.slabBytes(), "no slab is mapped for a refused reservation");
      assertEquals(0L, pool.accountBytes(), "a refused lease leaves the account untouched");
      assertEquals(committed, allocator.getPhysicalMemoryBytes(), "a refused lease charges nothing");
      assertAccount(allocator, pool, unit, "a refused lease");

      // Once the frames are recycled, the same request is admitted: the budget moved (MEM-103).
      held.forEach(FrameSlotAllocator.FrameSlot::close);
      held.clear();
      final Object lease = pool.lease(unit / 4);
      assertNotNull(lease, "MEM-301 the lease is admitted once the reservation can be served");
      assertTrue(pool.slabBytes() > 0, "the admitted lease mapped a slab");
      assertAccount(allocator, pool, unit, "the admitted lease");
      closeLeaseKeepingSlabs(allocator, pool, lease);
    } finally {
      held.forEach(FrameSlotAllocator.FrameSlot::close);
      pool.shutdown();
      quiesce(allocator);
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-301: a full account evicts compact images up to the shortfall, then caches the new decode")
  void fullAccountEvictsUpToTheShortfallBeforeDecoding() {
    final long unit = MemCompactSeam.slabUnitBytes();
    final long imageBytes = unit / 4;
    final FrameSlotAllocator allocator = newAllocator(unit);
    final MemCompactSeam.Pool pool = MemCompactSeam.create(allocator, SHARE_UNITS * unit);
    try {
      // Fill the account with unguarded (closed) cached images until the first eviction proves it is
      // full.
      long key = 0;
      long evictedBefore = 0;
      long cachedBefore = 0;
      long evictedAfter = 0;
      Object last = null;
      for (; key < 4096 && pool.evictedImageBytes() == 0L; key++) {
        evictedBefore = pool.evictedImageBytes();
        cachedBefore = pool.cachedImageBytes();
        last = pool.decode(key, imageBytes);
        assertTrue(pool.retainedByCache(last), "MEM-301 an image the account admits is retained by the cache");
        assertFalse(pool.scratchBacked(last), "an admitted image is not decoded into scratch");
        closeDecodeKeepingSlabs(allocator, pool, last);
        assertAccount(allocator, pool, unit, "decode of key " + key);
      }
      evictedAfter = pool.evictedImageBytes();
      assertTrue(evictedAfter > 0L,
          "MEM-301 a full account must evict compact images instead of growing past the share");
      final long shortfall = imageBytes;
      assertTrue(evictedAfter - evictedBefore >= shortfall, "MEM-301 eviction covers the shortfall of " + shortfall
          + " bytes, evicted " + (evictedAfter - evictedBefore));
      assertTrue(evictedAfter - evictedBefore <= shortfall + imageBytes,
          "MEM-301 eviction stops at the shortfall (plus one image), evicted " + (evictedAfter - evictedBefore));
      assertTrue(pool.cachedImageBytes() > 0L, "the cache is not wiped to admit one image");
      assertTrue(pool.cachedImageBytes() <= cachedBefore + imageBytes,
          "the cache did not grow by more than the one admitted image");
    } finally {
      pool.shutdown();
      quiesce(allocator);
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-301: when the account still refuses, the decode lands uncached in a scratch lease, never in GC-owned memory")
  void accountStillRefusingDecodesUncachedIntoAScratchLease() {
    final long unit = MemCompactSeam.slabUnitBytes();
    final long imageBytes = unit / 4;
    final FrameSlotAllocator allocator = newAllocator(unit);
    final MemCompactSeam.Pool pool = MemCompactSeam.create(allocator, SHARE_UNITS * unit);
    final List<Object> pinned = new ArrayList<>();
    try {
      // Open decodes guard their cache entries: nothing can be evicted, so the account fills for good.
      Object fallback = null;
      ScratchStats scratchBeforeFallback = null;
      for (long key = 0; key < 4096 && fallback == null; key++) {
        final ScratchStats beforeDecode = allocator.scratch().stats();
        final Object decoded = pool.decode(key, imageBytes);
        if (pool.scratchBacked(decoded)) {
          fallback = decoded;
          scratchBeforeFallback = beforeDecode;
        } else {
          assertTrue(pool.retainedByCache(decoded), "an image served from the pool is retained by the cache");
          pinned.add(decoded);
        }
      }
      assertNotNull(fallback, "MEM-301 a full account of guarded images must serve the next decode from scratch");
      final long account = pool.accountBytes();
      final long cachedCount = pool.cachedImageCount();
      final long cachedBytes = pool.cachedImageBytes();
      final long evicted = pool.evictedImageBytes();
      assertNotNull(scratchBeforeFallback);
      final ScratchStats scratchHeld = allocator.scratch().stats();
      assertFalse(pool.retainedByCache(fallback), "MEM-301 the scratch decode is not retained by the cache");
      assertTrue(pool.decodedBytes(fallback) >= imageBytes, "the scratch decode holds the whole image");
      final MemorySegment segment = pool.decodedSegment(fallback);
      assertTrue(segment.isNative(), "MEM-301 the uncached decode is native scratch, not GC-owned memory");
      assertTrue(segment.byteSize() >= imageBytes, "the scratch segment holds the whole image");
      assertEquals(scratchBeforeFallback.totalLentBuffers() + 1L, scratchHeld.totalLentBuffers(),
          "the uncached decode holds one scratch lease");
      assertTrue(scratchHeld.lentBytes() >= scratchBeforeFallback.lentBytes() + imageBytes,
          "the scratch lease holds the whole image and is charged to the allocator");
      assertTrue(account <= pool.shareBytes(), "MEM-301 the account stays within the share");
      assertAccount(allocator, pool, unit, "an uncached decode");
      closeDecodeKeepingSlabs(allocator, pool, fallback);
      assertEquals(account, pool.accountBytes(), "an uncached decode does not touch the account");
      assertEquals(cachedCount, pool.cachedImageCount(), "the cache does not retain the uncached decode");
      assertEquals(cachedBytes, pool.cachedImageBytes(), "the cache bytes are those of before the uncached decode");
      assertEquals(evicted, pool.evictedImageBytes(), "guarded images are never evicted");
      final ScratchStats scratchReturned = allocator.scratch().stats();
      assertEquals(scratchBeforeFallback.totalLentBuffers(), scratchReturned.totalLentBuffers(),
          "closing the decode returns its scratch lease");
      assertEquals(scratchBeforeFallback.lentBytes(), scratchReturned.lentBytes(),
          "closing the decode returns all borrowed scratch bytes");
    } finally {
      pinned.forEach(decoded -> closeDecodeKeepingSlabs(allocator, pool, decoded));
      pool.shutdown();
      quiesce(allocator);
      allocator.shutdown();
    }
  }

  @Test
  @DisplayName("MEM-302: idle slabs stay mapped and charged until shutdown; MEM-103 transfers never take budget from them")
  void idleSlabsStayMappedAndChargedAndTransfersLeaveThemAlone() {
    final long unit = MemCompactSeam.slabUnitBytes();
    final FrameSlotAllocator allocator = newAllocator(unit);
    final MemCompactSeam.Pool pool = MemCompactSeam.create(allocator, SHARE_UNITS * unit);
    final List<Object> leases = new ArrayList<>();
    final List<FrameSlotAllocator.FrameSlot> frames = new ArrayList<>();
    try {
      for (int i = 0; i < 64; i++) {
        final Object lease = pool.lease(unit / 8);
        if (lease == null) {
          break;
        }
        leases.add(lease);
      }
      assertFalse(leases.isEmpty(), "the fixture must map at least one slab");
      final long mapped = pool.slabBytes();
      final long charged = allocator.getPhysicalMemoryBytes();
      leases.forEach(lease -> closeLeaseKeepingSlabs(allocator, pool, lease));
      leases.clear();
      assertEquals(charged, allocator.getPhysicalMemoryBytes(), "MEM-302 releasing leases refunds no slab charge");
      assertTrue(mapped > 0L, "slabs are mapped");
      assertEquals(mapped, pool.slabBytes(), "MEM-302 idle slabs stay mapped after every lease was released");
      assertTrue(allocator.getPhysicalMemoryBytes() >= mapped, "MEM-302 idle slabs stay charged");

      // Recycle a budget's worth of frames, then ask for more than the transfer can supply.
      FrameSlotAllocator.FrameSlot frame;
      while ((frame = allocator.allocateSlot(FrameSlotAllocator.SIZE_CLASSES[LARGEST_CLASS])) != null) {
        frames.add(frame);
      }
      frames.forEach(FrameSlotAllocator.FrameSlot::close);
      frames.clear();
      final long recycled = FrameBudgetOracle.recycledBytes(allocator, LARGEST_CLASS);
      assertTrue(recycled > 0L, "the fixture must leave recycled frames for a transfer to claim");
      final long free = FrameBudgetOracle.scratchCommitLimit(allocator) - allocator.getPhysicalMemoryBytes();
      final long beyond = free + recycled + 1L;
      assertFalse(allocator.reserveScratchCommittedBytes(beyond),
          "a request only the idle slabs could serve is refused, not served from them");
      assertEquals(mapped, pool.slabBytes(), "MEM-302 a transfer never takes budget from a mapped slab");
      assertTrue(allocator.getPhysicalMemoryBytes() >= mapped,
          "MEM-302 the slabs are still charged after the transfer");
      final long servable = FrameBudgetOracle.scratchCommitLimit(allocator) - allocator.getPhysicalMemoryBytes();
      assertTrue(allocator.reserveScratchCommittedBytes(servable), "the recycled frames did serve what they could");
      allocator.refundCommittedBytes(servable);
      assertEquals(mapped, pool.slabBytes(), "the slabs are untouched after the served request too");
      assertAccount(allocator, pool, unit, "the transfers");

      pool.shutdown();
      assertEquals(0L, pool.slabBytes(), "MEM-302 shutdown unmaps the idle slabs");
      quiesce(allocator);
      assertEquals(0L, allocator.getPhysicalMemoryBytes() - FrameBudgetOracle.chargedFrameBytes(allocator),
          "MEM-302 shutdown refunds the slabs, leaving only frame charges");
    } finally {
      leases.forEach(lease -> closeLeaseKeepingSlabs(allocator, pool, lease));
      frames.forEach(FrameSlotAllocator.FrameSlot::close);
      pool.shutdown();
      quiesce(allocator);
      allocator.shutdown();
    }
  }
}
