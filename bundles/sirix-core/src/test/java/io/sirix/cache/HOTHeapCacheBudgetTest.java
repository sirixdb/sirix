/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The HOT caches that retain Java-heap images must be bounded by a heap-derived ceiling, not by the
 * record-page budget, which the allocator's off-heap budget sizes.
 */
final class HOTHeapCacheBudgetTest {
  /** Far above any JVM max heap a test runs under, so an off-heap-derived share cannot be honest. */
  private static final long RECORD_PAGE_BUDGET = 256L << 30;

  /** What {@code BufferManagerImpl} grants the HOT caches together. */
  private static final long HOT_BUDGET = RECORD_PAGE_BUDGET / 4;

  /** The share the raw-fragment cache took before a heap ceiling applied. */
  private static final long OFF_HEAP_DERIVED_FRAGMENT_SHARE = HOT_BUDGET / 4;

  private static final long CEILING = 8L << 20;

  private static BufferManagerImpl manager() {
    return new BufferManagerImpl(1L << 20, RECORD_PAGE_BUDGET, 1L << 20, 4, 4, 4);
  }

  private void withCeiling(final String value, final Runnable body) {
    final String previous = System.getProperty(BufferManagerImpl.HOT_HEAP_CACHE_BYTES_PROPERTY);
    if (value == null) {
      System.clearProperty(BufferManagerImpl.HOT_HEAP_CACHE_BYTES_PROPERTY);
    } else {
      System.setProperty(BufferManagerImpl.HOT_HEAP_CACHE_BYTES_PROPERTY, value);
    }
    try {
      body.run();
    } finally {
      if (previous == null) {
        System.clearProperty(BufferManagerImpl.HOT_HEAP_CACHE_BYTES_PROPERTY);
      } else {
        System.setProperty(BufferManagerImpl.HOT_HEAP_CACHE_BYTES_PROPERTY, previous);
      }
    }
  }

  @Test
  void aConfiguredCeilingBoundsBothHeapResidentCachesAndReturnsTheRestToCompleteLeaves() {
    withCeiling(Long.toString(CEILING), () -> {
      try (BufferManagerImpl manager = manager()) {
        assertTrue(manager.getHOTLeafFragmentCacheMaxWeightBytes() <= CEILING,
            "compact raw images are heap-resident and must respect the heap ceiling: "
                + manager.getHOTLeafFragmentCacheMaxWeightBytes());
        assertTrue(manager.getHOTMiniPageCacheMaxWeightBytes() <= CEILING,
            "resolved slots are heap-resident and must respect the heap ceiling: "
                + manager.getHOTMiniPageCacheMaxWeightBytes());
        assertTrue(manager.getHOTLeafFragmentCacheMaxWeightBytes() > 0,
            "a positive ceiling must not leave the cache unbounded");
        assertEquals(HOT_BUDGET,
            manager.getHOTLeafPageCacheMaxWeightBytes() + manager.getHOTLeafFragmentCacheMaxWeightBytes()
                + manager.getHOTMiniPageCacheMaxWeightBytes(),
            "capacity the heap ceiling declines belongs to the off-heap complete-leaf cache");
      }
    });
  }

  @Test
  void theDefaultCeilingKeepsHeapResidentCachesWellBelowTheOffHeapDerivedShare() {
    withCeiling(null, () -> {
      try (BufferManagerImpl manager = manager()) {
        final long fragment = manager.getHOTLeafFragmentCacheMaxWeightBytes();
        assertTrue(fragment > 0, "the default must still bound rather than disable the cache");
        assertTrue(fragment < OFF_HEAP_DERIVED_FRAGMENT_SHARE,
            "the raw-fragment cache still takes its off-heap-derived share of " + OFF_HEAP_DERIVED_FRAGMENT_SHARE
                + " bytes: " + fragment);
        assertTrue(fragment <= Runtime.getRuntime().maxMemory(),
            "a heap-resident cache cannot be allowed to retain more than the whole heap: " + fragment);
        assertTrue(manager.getHOTMiniPageCacheMaxWeightBytes() <= Runtime.getRuntime().maxMemory(),
            "the resolved-slot cache cannot be allowed to retain more than the whole heap");
      }
    });
  }

  @Test
  void anInvalidCeilingFallsBackToTheDefaultInsteadOfUnboundingTheCache() {
    for (final String invalid : new String[] {"0", "-1", "not-a-number"}) {
      withCeiling(invalid, () -> {
        try (BufferManagerImpl manager = manager()) {
          final long fragment = manager.getHOTLeafFragmentCacheMaxWeightBytes();
          assertTrue(fragment > 0, "a rejected ceiling must not read as unbounded");
          assertTrue(fragment < OFF_HEAP_DERIVED_FRAGMENT_SHARE,
              "a rejected ceiling must land where no ceiling was configured: " + fragment);
        }
      });
    }
  }
}
