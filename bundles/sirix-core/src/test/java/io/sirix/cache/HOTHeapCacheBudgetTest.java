/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.index.IndexType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Each HOT fragment residency must be bounded by the pool it actually consumes: allocator frames by
 * the off-heap-derived fragment share, compact decoded images by the retained-heap ceiling.
 */
final class HOTHeapCacheBudgetTest {
  /** Far above any JVM max heap a test runs under, so a heap ceiling must be the smaller number. */
  private static final long RECORD_PAGE_BUDGET = 256L << 30;

  /** What {@code BufferManagerImpl} grants the HOT caches together. */
  private static final long HOT_BUDGET = RECORD_PAGE_BUDGET / 4;

  /** The off-heap share the raw-fragment cache has always taken. */
  private static final long OFF_HEAP_FRAGMENT_SHARE = HOT_BUDGET / 4;

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
  void theHeapCeilingBoundsCompactImagesAndLeavesNativeFragmentCapacityUntouched() {
    withCeiling(Long.toString(CEILING), () -> {
      try (BufferManagerImpl manager = manager()) {
        assertEquals(OFF_HEAP_FRAGMENT_SHARE, manager.getNativeHOTLeafFragmentCacheMaxWeightBytes(),
            "an allocator-frame image costs no heap, so the heap ceiling must not shrink its share");
        assertEquals(CEILING, manager.getHeapHOTLeafFragmentCacheMaxWeightBytes(),
            "compact decoded images are heap-resident and take the heap ceiling");
        assertTrue(manager.getHOTMiniPageCacheMaxWeightBytes() <= CEILING,
            "resolved slots are heap-resident and take the heap ceiling: "
                + manager.getHOTMiniPageCacheMaxWeightBytes());
        assertEquals(HOT_BUDGET,
            manager.getHOTLeafPageCacheMaxWeightBytes() + manager.getNativeHOTLeafFragmentCacheMaxWeightBytes()
                + manager.getHOTMiniPageCacheMaxWeightBytes(),
            "the off-heap HOT allowance is split between complete leaves, native fragments and slots");
      }
    });
  }

  @Test
  void theDefaultCeilingIsHeapDerivedWhileTheNativeShareStaysOffHeapDerived() {
    withCeiling(null, () -> {
      try (BufferManagerImpl manager = manager()) {
        assertEquals(OFF_HEAP_FRAGMENT_SHARE, manager.getNativeHOTLeafFragmentCacheMaxWeightBytes(),
            "the native fragment share is the pre-existing off-heap-derived number");
        final long heap = manager.getHeapHOTLeafFragmentCacheMaxWeightBytes();
        assertTrue(heap > 0, "the default must still bound rather than disable the compact half");
        assertTrue(heap < OFF_HEAP_FRAGMENT_SHARE,
            "the compact half must not inherit the off-heap-derived share of " + OFF_HEAP_FRAGMENT_SHARE + ": " + heap);
        assertTrue(heap <= Runtime.getRuntime().maxMemory(),
            "a heap-resident cache cannot be allowed to retain more than the whole heap: " + heap);
      }
    });
  }

  @Test
  void anInvalidCeilingFallsBackToTheDefaultInsteadOfUnboundingTheCompactHalf() {
    for (final String invalid : new String[] {"0", "-1", "not-a-number"}) {
      withCeiling(invalid, () -> {
        try (BufferManagerImpl manager = manager()) {
          final long heap = manager.getHeapHOTLeafFragmentCacheMaxWeightBytes();
          assertTrue(heap > 0, "a rejected ceiling must not read as unbounded");
          assertTrue(heap < OFF_HEAP_FRAGMENT_SHARE,
              "a rejected ceiling must land where no ceiling was configured: " + heap);
        }
      });
    }
  }

  /**
   * A sparse delta fragment is a small packed heap image. Filling the compact half with many of them
   * must leave the cache inside its stated bound rather than growing with the fragment count.
   */
  @Test
  void manySmallSparseDeltaFragmentsStayInsideTheStatedHeapBound() {
    final long bound = 512L * 1024L;
    final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, bound);
    try {
      for (int fragment = 0; fragment < 4_096; fragment++) {
        final HOTLeafPage delta = sparseDeltaFragment(fragment);
        cache.put(new PageReference().setKey(fragment).setDatabaseId(1).setResourceId(2), delta);
      }
      // Admission second-chances a page it has just touched, so a tight fill loop overshoots until
      // a sweep clears those HOT bits; two passes are what the clock sweeper does every cycle.
      cache.heapImages().evictUnderPressure();
      cache.heapImages().evictUnderPressure();
      assertTrue(cache.heapImages().getCurrentWeightBytes() <= bound,
          "4096 sparse delta fragments retained more heap than the stated bound of " + bound + ": "
              + cache.heapImages().getCurrentWeightBytes());
      assertEquals(0L, cache.nativeImages().getCurrentWeightBytes(),
          "a heap-resident image must not be charged against the off-heap half");
      assertTrue(cache.heapImages().size() > 0, "eviction must not empty the cache it is bounding");
    } finally {
      cache.clear();
    }
  }

  /**
   * The charge a compact image takes in the cache must track what it actually retains. The expected
   * footprint here is measured from the image's own live structures — the packed slot bytes, the
   * exact-size offset directory and the fixed per-page objects — at the JVM's documented object and
   * array layout, independently of how {@code HOTLeafPage} computes its estimate.
   */
  @Test
  void theChargeOfACompactImageTracksItsMeasuredFootprint() {
    final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, 1L << 20);
    final PageReference key = new PageReference().setKey(7).setDatabaseId(1).setResourceId(2);
    try {
      final HOTLeafPage delta = sparseDeltaFragment(0);
      final long packedBytes = delta.slots().byteSize();
      final long measured = arrayBytes(packedBytes, 1) // packed slot bytes
          + arrayBytes(delta.size(), Integer.BYTES) // exact-size offset directory
          + arrayBytes(64, 1) // dirty bitmap
          + 64 // page object header and fields
          + 48; // empty side-reference map
      cache.put(key, delta);
      final long charged = cache.heapImages().getCurrentWeightBytes();
      assertTrue(charged >= measured,
          "the charge must not understate the retained footprint: charged " + charged + " < measured " + measured);
      assertTrue(charged <= measured + 8L * 1024L, "the charge must stay within 8 KiB of the measured footprint of "
          + measured + " bytes; conservative headroom is fine, an unbounded overshoot is not: " + charged);
    } finally {
      cache.clear();
    }
  }

  /**
   * Object and array layout with 8-byte alignment: 16-byte array header, padded to a multiple of 8.
   */
  private static long arrayBytes(final long length, final int elementBytes) {
    return (16L + length * elementBytes + 7L) / 8L * 8L;
  }

  /**
   * A heap-resident image shaped like a sparse delta fragment: a handful of entries in packed bytes
   * with an exact-size directory, which is what the compact decoder produces.
   */
  private static HOTLeafPage sparseDeltaFragment(final int ordinal) {
    final int entries = 3;
    final int[] offsets = new int[entries];
    final byte[] packed = new byte[entries * 8];
    for (int entry = 0; entry < entries; entry++) {
      final int offset = entry * 8;
      offsets[entry] = offset;
      packed[offset] = 2; // suffix length, little endian
      packed[offset + 2] = (byte) ordinal;
      packed[offset + 3] = (byte) entry;
      packed[offset + 4] = 2; // value length, little endian
    }
    return new HOTLeafPage(ordinal, 1, IndexType.PROJECTION, MemorySegment.ofArray(packed), null, offsets, entries,
        packed.length, new byte[0], 0);
  }
}
