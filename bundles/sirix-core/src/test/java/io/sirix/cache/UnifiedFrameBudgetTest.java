/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.access.Databases;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-201 — one frame budget, optional caps. The frame-holding caches (record pages, record
 * fragments, HOT leaves, the native half of the HOT fragment cache) have no default weight cap;
 * their joint frame use is bounded only by <em>L_f</em>. {@code sirix.cache.recordPage} and
 * {@code sirix.cache.recordPageFragment}, when set, are maximum caps for their cache; the HOT caps
 * derive from {@code sirix.cache.recordPage} as today when, and only when, it is set.
 *
 * <p>
 * Driven through the {@code HeapCacheBudget} property seam and the explicit-policy constructor seam
 * of {@link MemCacheSeam}; the caches are filled with weight-only pages, so the test measures
 * capacity, not the allocator.
 */
@MemPlanned(step = 2, value = {"MEM-201"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class UnifiedFrameBudgetTest {

  private static final long KIB = 1024L;
  private static final long MIB = 1024L * KIB;
  private static final long PAGE = 64 * KIB;
  private static final long BUDGET = 32 * MIB;

  /** The frame-holding caches of MEM-201. */
  enum Tier {
    RECORD_PAGES, RECORD_FRAGMENTS, HOT_LEAVES, NATIVE_HOT_FRAGMENTS;

    ShardedPageCache<CacheablePage> of(final BufferManagerImpl manager) {
      return switch (this) {
        case RECORD_PAGES -> MemCacheSeam.recordPages(manager);
        case RECORD_FRAGMENTS -> MemCacheSeam.recordFragments(manager);
        case HOT_LEAVES -> MemCacheSeam.hotLeaves(manager);
        case NATIVE_HOT_FRAGMENTS -> MemCacheSeam.nativeHotFragments(manager);
      };
    }
  }

  private final HeapCacheBudget heap = new HeapCacheBudget(Runtime.getRuntime().maxMemory(), ignored -> null);
  private FrameSlotAllocator.PressureListener previousListener;
  private FrameSlotAllocator allocator;
  private BufferManagerImpl manager;
  private long frameLimit;

  @BeforeEach
  void setUp() {
    previousListener = FrameSlotAllocator.pressureListener();
    allocator = new FrameSlotAllocator(BUDGET);
    frameLimit = FrameBudgetOracle.frameCommitLimit(allocator);
  }

  @AfterEach
  void tearDown() {
    try {
      if (manager != null) {
        manager.close();
      }
    } finally {
      FrameSlotAllocator.setPressureListener(previousListener);
      allocator.shutdown();
    }
  }

  private void openManager(final Map<String, String> properties) {
    manager = MemCacheSeam.newBufferManager(frameLimit, properties, heap);
  }

  private List<MemCachePageStub> fill(final ShardedPageCache<CacheablePage> cache, final long frameBytes) {
    final List<MemCachePageStub> pages = new ArrayList<>();
    for (long index = 0; index * PAGE < frameBytes; index++) {
      pages.add(MemCacheSeam.putWeighing(cache, index, PAGE));
    }
    return pages;
  }

  @ParameterizedTest
  @EnumSource(Tier.class)
  @DisplayName("MEM-201: with no cap property set, each frame cache alone holds frames up to L_f")
  void withNoCapPropertyEachFrameCacheAloneHoldsFramesUpToTheFrameCommitLimit(final Tier tier) {
    openManager(Map.of());
    final ShardedPageCache<CacheablePage> cache = tier.of(manager);

    final List<MemCachePageStub> pages = fill(cache, frameLimit);

    assertEquals(frameLimit, cache.getCurrentWeightBytes(), "MEM-201: " + tier + " must hold the whole frame budget");
    assertEquals(frameLimit / PAGE, cache.size(), "MEM-201: no " + tier + " entry may be shed below L_f");
    assertFalse(pages.stream().anyMatch(MemCachePageStub::isTornDown), "MEM-201: nothing evicted by a default cap");
    assertTrue(MemCacheSeam.isUncapped(cache.getMaxWeightBytes(), frameLimit),
        "MEM-201: " + tier + " must carry no default cap below L_f, max weight " + cache.getMaxWeightBytes());
  }

  @Test
  @DisplayName("MEM-201: sirix.cache.recordPage, when set, is a maximum cap for the record-page cache")
  void theRecordPageCapBoundsTheRecordPageCache() {
    final long cap = 8 * MIB;
    openManager(Map.of(Databases.PROP_RECORD_PAGE_CACHE, Long.toString(cap)));
    final ShardedPageCache<CacheablePage> cache = MemCacheSeam.recordPages(manager);

    fill(cache, 2 * cap);

    assertTrue(cache.getCurrentWeightBytes() <= cap,
        "MEM-201: the record-page cache must not hold more than its cap " + cap + ": " + cache.getCurrentWeightBytes());
    assertTrue(cache.getMaxWeightBytes() > 0 && cache.getMaxWeightBytes() <= cap,
        "MEM-201: the cap is the cache's maximum weight: " + cache.getMaxWeightBytes());
  }

  @Test
  @DisplayName("MEM-201: sirix.cache.recordPageFragment, when set, caps only the record-fragment cache")
  void theFragmentCapBoundsOnlyTheRecordFragmentCache() {
    final long cap = 4 * MIB;
    openManager(Map.of(Databases.PROP_RECORD_PAGE_FRAGMENT_CACHE, Long.toString(cap)));
    final ShardedPageCache<CacheablePage> fragments = MemCacheSeam.recordFragments(manager);
    final ShardedPageCache<CacheablePage> pages = MemCacheSeam.recordPages(manager);

    fill(fragments, 2 * cap);
    final List<MemCachePageStub> held = fill(pages, frameLimit);

    assertTrue(fragments.getCurrentWeightBytes() <= cap, "MEM-201: the fragment cache must not hold more than its cap "
        + cap + ": " + fragments.getCurrentWeightBytes());
    assertEquals(frameLimit, pages.getCurrentWeightBytes(),
        "MEM-201: a fragment cap must not cap the record-page cache");
    assertFalse(held.stream().anyMatch(MemCachePageStub::isTornDown));
  }

  @Test
  @DisplayName("MEM-201: the HOT caps derive from sirix.cache.recordPage as today when it is set")
  void theHotCapsAreDerivedFromTheRecordPageCapWhenItIsSet() {
    final long recordPageCap = 1024 * MIB;
    openManager(Map.of(Databases.PROP_RECORD_PAGE_CACHE, Long.toString(recordPageCap)));

    final long hot = manager.getHOTLeafPageCacheMaxWeightBytes() + manager.getNativeHOTLeafFragmentCacheMaxWeightBytes()
        + manager.getHOTMiniPageCacheMaxWeightBytes() + manager.getProjectionBlobCache().getMaxWeightBytes();

    assertEquals(recordPageCap / 4, hot,
        "MEM-201: the HOT allowance is a quarter of the record-page cap, split between leaves, fragments, slots and blobs");
  }

  @Test
  @DisplayName("MEM-201: the HOT caps are not derived from a record-page cap that is not set")
  void theHotCapsAreNotDerivedWhenTheRecordPageCapIsNotSet() {
    openManager(Map.of(Databases.PROP_RECORD_PAGE_FRAGMENT_CACHE, Long.toString(4 * MIB)));

    assertTrue(MemCacheSeam.isUncapped(manager.getHOTLeafPageCacheMaxWeightBytes(), frameLimit),
        "MEM-201: HOT leaves: " + manager.getHOTLeafPageCacheMaxWeightBytes());
    assertTrue(MemCacheSeam.isUncapped(manager.getNativeHOTLeafFragmentCacheMaxWeightBytes(), frameLimit),
        "MEM-201: native HOT fragments: " + manager.getNativeHOTLeafFragmentCacheMaxWeightBytes());
    assertTrue(MemCacheSeam.isUncapped(manager.getRecordPageCacheMaxWeightBytes(), frameLimit),
        "MEM-201: record pages: " + manager.getRecordPageCacheMaxWeightBytes());
  }

  @Test
  @MemPlanned(step = 1, value = {"MEM-201", "MEM-109"})
  @DisplayName("MEM-201: every size class has address space for the whole frame budget (MEM-109)")
  void everySizeClassHasAddressSpaceForTheWholeFrameBudget() {
    final FrameSlotAllocator large = new FrameSlotAllocator(64L * 1024 * MIB);
    try {
      final long limit = FrameBudgetOracle.frameCommitLimit(large);
      for (int classIdx = 0; classIdx < FrameSlotAllocator.SIZE_CLASSES.length; classIdx++) {
        final long capacity = (long) large.slotCount(classIdx) * FrameSlotAllocator.SIZE_CLASSES[classIdx];
        assertTrue(capacity >= limit,
            "MEM-201: class " + classIdx + " holds " + capacity + " bytes of slots, less than L_f " + limit);
      }
    } finally {
      large.shutdown();
    }
  }
}
