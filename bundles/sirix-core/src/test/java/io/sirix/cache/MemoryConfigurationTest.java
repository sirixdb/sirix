/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * MEM-401 — configuration surface. The one budget setting stays {@code sirix.allocator.maxSize};
 * the execution floor (<em>B</em>/8), the oversized headroom (<em>B</em>/16, at most 1 GiB), the
 * scratch reserve and the slab-recycle bound keep their values and properties; the only optional
 * caps are those of MEM-201; there is no property that restores permanent frames, and the
 * worst-case switch of MEM-104 is a test seam, not a property (a property named for releasing,
 * returning to the OS or permanence would be a new public setting and fails the first test).
 */
@MemPlanned(step = 1, value = {"MEM-401"})
@ResourceLock("FrameSlotAllocator.pressureListener")
final class MemoryConfigurationTest {

  private static final long KIB = 1024L;
  private static final long MIB = 1024L * KIB;
  private static final long GIB = 1024L * MIB;

  /**
   * Every public {@code sirix.*} property constant of the memory classes at the baseline. A property
   * added by the pool work, such as a switch that restores permanent frames or a new cache share, is
   * a new public setting and fails this test (decision Q4: no new public property).
   */
  private static final Set<String> BASELINE_PROPERTIES = Set.of("sirix.allocator.maxSize",
      "sirix.allocator.readScratchPopulate", "sirix.allocator.scratchReserveBytes", "sirix.allocator.slabPopulate",
      "sirix.allocator.slabRecycleBytes", "sirix.cache.page", "sirix.cache.page.max.entries", "sirix.cache.recordPage",
      "sirix.cache.recordPageFragment", "sirix.cache.revisionRoot.max.entries", "sirix.heapCache.hotLookupBytes",
      "sirix.heapCache.hotMiniBytes", "sirix.heapCache.maxBytes", "sirix.heapCache.projectionBlobBytes",
      "sirix.hotHeapCache.maxBytes", "sirix.hotLookupCache.maxEntries", "sirix.projection.cacheBytes",
      "sirix.projection.eagerMaterializeBytes", "sirix.projection.globalDict.recordCacheBytes",
      "sirix.projection.globalDict.verdictCacheBytes", "sirix.projection.segmentDict.verdictCacheBytes");

  private static final List<String> RESERVE_PROPERTIES = List.of("sirix.allocator.oversizedHeadroomBytes",
      FrameSlotAllocator.SCRATCH_RESERVE_BYTES_PROPERTY, FrameSlotAllocator.SLAB_RECYCLE_BYTES_PROPERTY);

  private FrameSlotAllocator.PressureListener previousListener;

  @BeforeEach
  void setUp() {
    previousListener = FrameSlotAllocator.pressureListener();
    for (final String property : RESERVE_PROPERTIES) {
      assertNull(System.getProperty(property), "the reserves are asserted at their defaults: " + property + " is set");
    }
  }

  @AfterEach
  void tearDown() {
    FrameSlotAllocator.setPressureListener(previousListener);
  }

  private static Set<String> publicSirixProperties() {
    final Set<String> properties = new TreeSet<>();
    for (final Class<?> owner : List.of(FrameSlotAllocator.class, BufferManagerImpl.class, HeapCacheBudget.class,
        PageCache.class, MemorySegmentAllocator.class, AggregationBudget.class, Databases.class,
        DatabaseConfiguration.class)) {
      for (final Field field : owner.getDeclaredFields()) {
        final int modifiers = field.getModifiers();
        if (Modifier.isPublic(modifiers) && Modifier.isStatic(modifiers) && Modifier.isFinal(modifiers)
            && field.getType() == String.class) {
          try {
            final Object value = field.get(null);
            if (value instanceof String name && name.startsWith("sirix.")) {
              properties.add(name);
            }
          } catch (final IllegalAccessException e) {
            throw new AssertionError(e);
          }
        }
      }
    }
    for (final HeapCacheBudget.Pool pool : HeapCacheBudget.Pool.values()) {
      properties.add(pool.property());
    }
    return properties;
  }

  @Test
  @DisplayName("MEM-401: the pool work adds no public property, and none that restores permanent frames")
  void noNewPublicPropertyExists() {
    final Set<String> actual = publicSirixProperties();

    final Set<String> added = new TreeSet<>(actual);
    added.removeAll(BASELINE_PROPERTIES);
    assertEquals(Set.<String>of(), added, "MEM-401: new public memory properties");
    for (final String property : actual) {
      final String lower = property.toLowerCase(Locale.ROOT);
      assertFalse(lower.contains("permanent") || lower.contains("release") || lower.contains("returntoos"),
          "MEM-401: no property may restore permanent frames or switch release behavior: " + property);
    }
  }

  @ParameterizedTest
  @ValueSource(longs = {256 * MIB, 16 * GIB, 64 * GIB})
  @DisplayName("MEM-401: the execution floor, oversized headroom, scratch reserve and slab bound keep their values")
  void theReservesKeepTheirValues(final long budget) {
    final long headroom = budget >= 64 * MIB
        ? Math.min(budget / 16, GIB)
        : 0L;
    final FrameSlotAllocator allocator = new FrameSlotAllocator(budget);
    try {
      assertEquals(budget / 8, allocator.aggregationBudget().capacity(), "MEM-401: the execution floor is B/8");
      assertEquals(headroom, allocator.oversizedHeadroomBytes(), "MEM-401: the oversized headroom is B/16 up to 1 GiB");
      assertEquals(Math.min(headroom / 4, 256 * MIB), allocator.scratchReserveBytes(),
          "MEM-401: the scratch reserve is a quarter of the headroom up to 256 MiB");
      assertEquals(budget - budget / 8 - headroom, FrameBudgetOracle.frameCommitLimit(allocator),
          "MEM-401: L_f = B - B/8 - oversized headroom");
      final long previousBound = allocator.setSlabRecycleLimitBytesForTesting(0L);
      allocator.setSlabRecycleLimitBytesForTesting(previousBound);
      assertEquals(Math.min(budget / 16, 512 * MIB), previousBound, "MEM-401: the slab-recycle bound");
    } finally {
      allocator.shutdown();
    }
  }

  @Test
  @MemPlanned(step = 2, value = {"MEM-401", "MEM-201"})
  @DisplayName("MEM-401: the caps of MEM-201 apply only when set")
  void theCapsApplyOnlyWhenSet() {
    final HeapCacheBudget heap = new HeapCacheBudget(Runtime.getRuntime().maxMemory(), ignored -> null);
    final long frameLimit = 28 * MIB;
    try (BufferManagerImpl unset = MemCacheSeam.newBufferManager(frameLimit, Map.of(), heap)) {
      assertTrue(MemCacheSeam.isUncapped(unset.getRecordPageCacheMaxWeightBytes(), frameLimit),
          "MEM-401: no record-page cap by default: " + unset.getRecordPageCacheMaxWeightBytes());
      assertTrue(MemCacheSeam.isUncapped(unset.getRecordPageFragmentCacheMaxWeightBytes(), frameLimit),
          "MEM-401: no fragment cap by default: " + unset.getRecordPageFragmentCacheMaxWeightBytes());
    }
    try (BufferManagerImpl capped = MemCacheSeam.newBufferManager(frameLimit, Map.of(Databases.PROP_RECORD_PAGE_CACHE,
        Long.toString(4 * MIB), Databases.PROP_RECORD_PAGE_FRAGMENT_CACHE, Long.toString(2 * MIB)), heap)) {
      assertEquals(4 * MIB, capped.getRecordPageCacheMaxWeightBytes(), "MEM-401: the record-page cap applies when set");
      assertEquals(2 * MIB, capped.getRecordPageFragmentCacheMaxWeightBytes(),
          "MEM-401: the fragment cap applies when set");
    }
  }
}
