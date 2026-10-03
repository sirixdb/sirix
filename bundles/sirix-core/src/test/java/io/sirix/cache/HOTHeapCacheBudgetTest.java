/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.api.Database;
import io.sirix.api.HOTReadIntent;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexType;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.io.StorageType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.foreign.MemorySegment;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

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
   * must leave the cache inside its stated bound, and the clock sweeper the real
   * {@link BufferManagerImpl} lifecycle starts is what brings it back inside — nothing in production
   * sweeps it by hand.
   */
  @Test
  void manySmallSparseDeltaFragmentsStayInsideTheStatedHeapBoundUnderTheRealLifecycle() throws Exception {
    final long bound = 512L * 1024L;
    withCeiling(Long.toString(bound), () -> {
      final RevisionEpochTracker tracker = new RevisionEpochTracker(RevisionEpochTracker.defaultSlotCount());
      try (BufferManagerImpl manager = new BufferManagerImpl(1L << 20, 1L << 30, 1L << 20, 4, 4, 4)) {
        assertEquals(bound, manager.getHeapHOTLeafFragmentCacheMaxWeightBytes());
        manager.startClockSweepers(tracker);
        final Cache<PageReference, HOTLeafPage> fragments = manager.getHOTLeafFragmentCache();
        for (int fragment = 0; fragment < 4_096; fragment++) {
          fragments.put(new PageReference().setKey(fragment).setDatabaseId(1).setResourceId(2),
              sparseDeltaFragment(fragment));
        }
        final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        long retained = manager.getHeapHOTLeafFragmentCacheCurrentWeightBytes();
        while (retained > bound && System.nanoTime() < deadline) {
          try {
            Thread.sleep(50L);
          } catch (final InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            break;
          }
          retained = manager.getHeapHOTLeafFragmentCacheCurrentWeightBytes();
        }
        assertTrue(retained <= bound,
            "the background sweeper never brought 4096 sparse delta fragments back inside the stated bound of " + bound
                + ": " + retained);
        assertEquals(0L, manager.getNativeHOTLeafFragmentCacheCurrentWeightBytes(),
            "a heap-resident image must not be charged against the off-heap half");
      }
    });
  }

  /**
   * A native-only backend keeps the whole off-heap fragment share and puts nothing in the compact
   * half, which is what makes the heap ceiling safe to apply to the compact half alone.
   */
  @Test
  void aMemoryMappedResourceKeepsTheOffHeapShareAndLeavesTheCompactHalfEmpty(@TempDir final Path directory) {
    final Path path = directory.resolve("memory-mapped");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(path)));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder("resource")
                                                              .storageType(StorageType.MEMORY_MAPPED)
                                                              .versioningApproach(VersioningType.SLIDING_SNAPSHOT)
                                                              .maxNumberOfRevisionsToRestore(32)
                                                              .build()));
      try (JsonResourceSession session = database.beginResourceSession("resource")) {
        for (int revision = 1; revision <= 6; revision++) {
          try (JsonNodeTrx writer = session.beginNodeTrx()) {
            new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).putBlob(5_000_000L + revision,
                new byte[64]);
            writer.commit();
          }
        }
      }
      Databases.clearGlobalCaches();
      final BufferManager buffers = Databases.getGlobalBufferManager();
      try (JsonResourceSession session = database.beginResourceSession("resource");
          JsonNodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx(6)) {
        for (int revision = 1; revision <= 6; revision++) {
          ProjectionIndexHOTStorage.readBlob(reader.getStorageEngineReader(), 0, 5_000_000L + revision,
              HOTReadIntent.POINT);
        }
      }
      final BufferManagerImpl manager = (BufferManagerImpl) buffers;
      // Re-derive the pre-change formula from the allowance the manager was actually built with.
      final long hotBudget = manager.getHOTLeafPageCacheMaxWeightBytes()
          + manager.getNativeHOTLeafFragmentCacheMaxWeightBytes() + manager.getHOTMiniPageCacheMaxWeightBytes();
      final long preChangeShare = Math.min(hotBudget / 2, Math.max(hotBudget / 4, 32L * HOTLeafPage.DEFAULT_SIZE));
      assertEquals(preChangeShare, manager.getNativeHOTLeafFragmentCacheMaxWeightBytes(),
          "a native-only backend must keep exactly the pre-change off-heap fragment share");
      assertEquals(0L, manager.getHeapHOTLeafFragmentCacheCurrentWeightBytes(),
          "a MEMORY_MAPPED resource decodes native frames, so the compact half must stay empty");
      assertTrue(manager.getNativeHOTLeafFragmentCacheCurrentWeightBytes() > 0,
          "a versioned MEMORY_MAPPED chain read must populate the native half");
    }
  }

  /**
   * Retained heap of 10,000 decoder-only compact images, built through the compact constructor and
   * held live across a GC-settled heap delta, measured once at {@code -Xmx2g}: 7,528,936 bytes, i.e.
   * 752.9 bytes per image. {@value #MEASURED_FOOTPRINT_BYTES} is that number rounded up, and the
   * bound the cache charge must never fall below.
   */
  private static final long MEASURED_FOOTPRINT_BYTES = 1_024L;

  /**
   * The charge must be a conservative UPPER bound on what an image retains, never an understatement:
   * the budget is only honest if the cache over-counts rather than under-counts. Asserted against the
   * recorded measurement above rather than against the formula that produces the charge.
   */
  @Test
  void theChargeOfACompactImageIsAConservativeUpperBoundOnItsMeasuredFootprint() {
    final HOTFragmentCache cache = new HOTFragmentCache(1L << 20, 1L << 20);
    final PageReference key = new PageReference().setKey(7).setDatabaseId(1).setResourceId(2);
    try {
      cache.put(key, sparseDeltaFragment(0));
      final long charged = cache.heapImages().getCurrentWeightBytes();
      assertTrue(charged >= MEASURED_FOOTPRINT_BYTES, "the charge understates the measured retained footprint of "
          + MEASURED_FOOTPRINT_BYTES + " bytes: " + charged);
    } finally {
      cache.clear();
    }
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
