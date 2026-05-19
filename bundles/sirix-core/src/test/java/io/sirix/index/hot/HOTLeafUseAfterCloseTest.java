/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot;

import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.cache.BufferManager;
import io.sirix.cache.Cache;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Regression test for the HOT reader's navigate-then-guard eviction race — the intermittent
 * "contiguous 256/512-key run vanishes" miss seen on
 * {@code HOTMicrobenchmark.smallCombinedMicrobench}.
 *
 * <p>The HOT reader resolves a leaf, then acquires its guard in a separate step. A
 * guard-respecting evictor can close the leaf in that gap. The reader then observed
 * {@code acquireGuard() == false} and reported the key as <em>absent</em> (or iterated a
 * closed page) — but an evicted leaf is transient, not missing. The fix re-navigates and
 * reloads instead of concluding the key is gone.
 *
 * <p>The miss is memory-pressure-gated: eviction only fires under pressure, and
 * {@code FrameSlotAllocator.releaseSlot} keeps a freed slot's bytes intact, so a use-after-
 * eviction reads stale-but-valid data on an idle machine and corrupts only under load.
 *
 * <p>This test removes the timing dependence:
 * {@link FrameSlotAllocator#setPoisonOnReleaseForTesting} zero-fills every freed slot so any
 * use-after-eviction is deterministic, and a background thread evicts every unguarded cached
 * HOT leaf at maximum rate during readback. A correct reader re-resolves a transiently
 * evicted leaf from storage, so the scan must survive with zero key loss.
 */
@DisplayName("HOT reader survives eviction racing the navigate-then-guard window")
final class HOTLeafUseAfterCloseTest {

  private static final int N = 500_000;
  private static final long PATH_NODE_KEY = 5L;

  private static String originalHOTSetting;

  @BeforeAll
  static void enableHOT() {
    originalHOTSetting = System.getProperty("sirix.index.useHOT");
    System.setProperty("sirix.index.useHOT", "true");
  }

  @AfterAll
  static void restoreHOT() {
    if (originalHOTSetting != null) {
      System.setProperty("sirix.index.useHOT", originalHOTSetting);
    } else {
      System.clearProperty("sirix.index.useHOT");
    }
  }

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    FrameSlotAllocator.setPoisonOnReleaseForTesting(false);
    JsonTestHelper.deleteEverything();
  }

  @Test
  @DisplayName("every inserted CAS key survives readback under concurrent cache eviction")
  @Timeout(value = 300, unit = TimeUnit.SECONDS)
  void everyKeySurvivesReadbackUnderEviction() throws InterruptedException {
    final String prevStrictBinna = System.getProperty("hot.strict.binna");
    System.setProperty("hot.strict.binna", "true");
    // Disable the O(total-entries) leaf-walk fallback: it masks a cursor that wrongly
    // reports a key absent (turning a fast miss into an hours-long full-scan storm). With
    // it off, a regression of the eviction-race bug surfaces as a fast, clean miss count.
    final String prevFallback = System.getProperty("hot.cas.leftmostfallback.disable");
    System.setProperty("hot.cas.leftmostfallback.disable", "true");
    final AtomicBoolean stopEvictor = new AtomicBoolean(false);
    Thread evictor = null;
    try {
      final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
      final IndexDef casIndexDef;

      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var trx = session.beginNodeTrx()) {
        final var ic = session.getWtxIndexController(trx.getRevisionNumber());
        final var pathToValue = Path.parse("/x/[]/v", PathParser.Type.JSON);
        casIndexDef = IndexDefs.createCASIdxDef(false, Type.INR,
            Collections.singleton(pathToValue), 0, IndexDef.DbType.JSON);
        ic.createIndexes(Set.of(casIndexDef), trx);
        final var writer = HOTIndexWriter.create(trx.getStorageEngineWriter(),
            CASKeySerializer.INSTANCE, IndexType.CAS, casIndexDef.getID());
        final NodeReferences scratch = new NodeReferences();

        final int warmupBase = N + 1_000_000;
        for (int i = 0; i < 5_000; i++) {
          scratch.getNodeKeys().clear();
          scratch.getNodeKeys().add(warmupBase + i);
          writer.index(new CASValue(new Int32(warmupBase + i), Type.INR, PATH_NODE_KEY), scratch,
              null);
        }
        for (int i = 0; i < N; i++) {
          scratch.getNodeKeys().clear();
          scratch.getNodeKeys().add(i);
          writer.index(new CASValue(new Int32(i), Type.INR, PATH_NODE_KEY), scratch, null);
        }
        trx.commit();
      }

      int misses = 0;
      final StringBuilder firstMisses = new StringBuilder();
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var rtx = session.beginNodeReadOnlyTrx()) {
        final var reader = HOTIndexReader.create(rtx.getStorageEngineReader(),
            CASKeySerializer.INSTANCE, IndexType.CAS, casIndexDef.getID());
        final BufferManager bufferManager = rtx.getStorageEngineReader().getBufferManager();

        // Maximum-rate eviction concurrent with the scan, modelling severe memory pressure.
        // The hammer closes every *unguarded* cached HOT leaf — exactly what
        // ShardedPageCache.evictUnderPressure does, just without the budget gate — so a leaf
        // the reader resolved but has not yet guarded gets yanked from under it. Poison
        // zero-fills the freed slot, so reading a leaf after it was evicted is unambiguous.
        FrameSlotAllocator.setPoisonOnReleaseForTesting(true);
        final Thread hammer = new Thread(() -> {
          while (!stopEvictor.get()) {
            evictUnguardedLeaves(bufferManager.getHOTLeafPageCache());
            bufferManager.getPageCache().clear();
          }
        }, "evict-hammer");
        hammer.setDaemon(true);
        evictor = hammer;
        hammer.start();

        for (int v = 0; v < N; v++) {
          if (reader.get(new CASValue(new Int32(v), Type.INR, PATH_NODE_KEY), SearchMode.EQUAL)
              == null) {
            if (misses < 20) {
              if (firstMisses.length() > 0) {
                firstMisses.append(',');
              }
              firstMisses.append(v);
            }
            misses++;
          }
        }
        stopEvictor.set(true);
        hammer.join();
      }

      System.out.println("[hot-use-after-close] readback under eviction: misses=" + misses + "/"
          + N + " first20=[" + firstMisses + "]");
      assertEquals(0, misses,
          "readback under concurrent eviction lost " + misses + " keys — a HOT page was used"
              + " after it was evicted/closed; first missing: [" + firstMisses + "]");
    } finally {
      stopEvictor.set(true);
      if (evictor != null) {
        try {
          evictor.join(5_000);
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      }
      if (prevStrictBinna == null) {
        System.clearProperty("hot.strict.binna");
      } else {
        System.setProperty("hot.strict.binna", prevStrictBinna);
      }
      if (prevFallback == null) {
        System.clearProperty("hot.cas.leftmostfallback.disable");
      } else {
        System.setProperty("hot.cas.leftmostfallback.disable", prevFallback);
      }
    }
  }

  /**
   * Evict cached HOT leaves exactly as {@code ShardedPageCache.evictUnderPressure} does —
   * guard-respecting, with the {@code isHot()} second-chance — but without the budget gate
   * so it sheds at maximum rate. A guarded or recently-touched (hot) leaf is spared, so the
   * reproduction exercises the genuine navigate-then-guard race that real memory pressure
   * produces, not an unfaithfully harsh close of leaves the reader is actively using.
   */
  private static void evictUnguardedLeaves(final Cache<PageReference, HOTLeafPage> cache) {
    final var entries = new ArrayList<>(cache.asMap().entrySet());
    for (final var entry : entries) {
      final HOTLeafPage leaf = entry.getValue();
      if (leaf == null || leaf.isClosed() || leaf.getGuardCount() != 0) {
        continue;
      }
      // Second chance: acquireGuard() marks a leaf hot, so a leaf the reader is actively
      // using survives one cycle — mirrors evictUnderPressure / the ClockSweeper.
      if (leaf.isHot()) {
        leaf.clearHot();
        continue;
      }
      cache.remove(entry.getKey());
      entry.getKey().setPage(null);
      if (!leaf.isClosed() && leaf.getGuardCount() == 0 && !leaf.isHot()) {
        leaf.close();
      }
    }
  }
}
