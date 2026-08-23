/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import io.sirix.JsonTestHelper;
import io.sirix.JsonTestHelper.PATHS;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AfterCommitState;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.io.StorageType;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression gate for the transaction intent log's bookkeeping COST across an async-flush import.
 *
 * <p>
 * The official 100M-row ClickBench load died with an {@code OutOfMemoryError} inside
 * {@code Long2LongOpenHashMap.rehash}, called from the log's forwarding map. The mechanism was not
 * a leak in the ordinary sense: only {@link io.sirix.page.KeyValueLeafPage}s can be written by a
 * background snapshot flush, so every other page — the whole trie spine of an uncommitted
 * transaction — was frozen by {@code snapshot()} and handed straight back by
 * {@code cleanupSnapshot()} under a NEW identity, one permanent forwarding link per page per flush.
 * The resident structural set grows with the corpus, so the map grew with the SQUARE of the flush
 * count: measured on a ClickBench load, {@code forwardedEntries(F) = 1.70 * F^2}, which is 17.4
 * million entries after 4M rows and an extrapolated 160 million by 12M rows — where the 16 GB heap
 * died, inside a rehash, exactly as observed.
 *
 * <p>
 * The fix gives those pages a pinned slot whose identity never changes again. These tests pin the
 * three properties that keeps true, in a form that does not depend on corpus size:
 *
 * <ul>
 * <li>the generation-scoped region does NOT accumulate across epochs (it held the re-promoted spine
 * before, which is the quadratic term's source);</li>
 * <li>forwarding links accrue at a rate that does not grow with the flush count;</li>
 * <li>and the records are all still there afterwards — pinning must not lose a page, which is the
 * failure mode any change to this machinery risks (#1076, #1077).</li>
 * </ul>
 *
 * <p>
 * Verified non-vacuous by restoring the pre-fix behaviour with
 * {@code -Dsirix.til.disablePinning=true}: the promotion assertion then fails with 384 containers
 * promoted over 194 flushes (and forwarding links accruing at 10.9 per flush rather than 3.0).
 *
 * <p>
 * Note what this workload can and cannot show. Its resident structural set is small and roughly
 * CONSTANT — a flat array does not grow a trie spine — so the pre-fix bookkeeping here is merely
 * linear with a bad constant, not quadratic. The promotion count is therefore the assertion that
 * carries the guard: it names the mechanism directly and holds at any size, whereas an assertion on
 * entries or bytes only bites once the corpus is large enough to grow the spine. On the ClickBench
 * load that actually died, the same mechanism produced {@code forwardedEntries(F) = 1.70 * F^2}.
 */
final class AsyncFlushLogBookkeepingTest {

  private static final String RESOURCE = "async-flush-bookkeeping-resource";

  /**
   * Auto-commit threshold. Deliberately small: the defect is per-FLUSH, so the test needs many
   * flushes rather than many records.
   */
  private static final int MAX_NODES_BEFORE_FLUSH = 256;

  /** Enough to rotate the log ~200 times at the threshold above. */
  private static final int INSERTED_RECORDS = 50_000;

  /** Samples of the log's state, one per snapshot flush. */
  private record FlushSample(int liveEntries, int pinnedEntries, int forwardedLinks, int completedOffsets,
      int completedHashes, long structuralPromotions) {
  }

  private final List<FlushSample> samples = new ArrayList<>();

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
    samples.clear();
  }

  @AfterEach
  void tearDown() {
    NodeStorageEngineWriter.asyncFlushFaultHook = null;
    JsonTestHelper.deleteEverything();
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("An async-flush import keeps its log bookkeeping bounded, and loses no record doing it")
  void asyncFlushImport_doesNotAccumulateLogBookkeepingPerFlush() {
    // The hook is used here purely as a per-flush observer — it never throws. "prepare" fires
    // inside asyncFlush() after the previous snapshot has been cleaned up, which is the moment the
    // pre-fix code had just finished re-promoting the entire resident structural set.
    final AtomicInteger flushes = new AtomicInteger();
    NodeStorageEngineWriter.asyncFlushFaultHook = (engineWriter, site) -> {
      if (!"prepare".equals(site)) {
        return;
      }
      flushes.incrementAndGet();
      final TransactionIntentLog log = engineWriter.getLog();
      samples.add(new FlushSample(log.liveEntryCount(), log.pinnedSize(), log.forwardedEntryCount(),
          log.completedDiskOffsetCount(), log.completedDiskHashCount(), log.structuralPromotionCount()));
    };

    final long readBack = runAsyncFlushImport();

    assertEquals(INSERTED_RECORDS, readBack,
        "every inserted record must survive the import — a page dropped by the pinning pass would "
            + "show up here and nowhere else");

    assertTrue(samples.size() >= 20, "the import rotated the log only " + samples.size()
        + " times — too few to say anything " + "about per-flush growth; raise INSERTED_RECORDS");

    final FlushSample last = samples.get(samples.size() - 1);
    System.out.printf("[bookkeeping] flushes=%d first=%s quarter=%s last=%s%n", samples.size(), samples.get(0),
        samples.get(samples.size() / 4), last);

    // 1. THE mechanism. A promotion mints a fresh identity for a page that did not change and
    // leaves a permanent forwarding link; the whole resident structural set used to be promoted on
    // every flush, which is the O(flushes * residents) term. Pinned pages are never promoted, so
    // this is exactly zero — an assertion that needs no scale to be meaningful, unlike anything
    // measured in bytes or entries, and the one the corpus-sized defect is made of.
    assertEquals(0L, last.structuralPromotions(),
        "cleanupSnapshot promoted " + last.structuralPromotions() + " containers back into the "
            + "generation-scoped region over " + samples.size() + " flushes — every one of them "
            + "costs a permanent forwarding link, and the count scales with the resident structural "
            + "set, so on a corpus-sized import this is quadratic in the flush count");

    // 2. The generation-scoped region holds ONE epoch's record pages and must not grow with the
    // flush count — growth there IS the re-promoted structural set.
    final FlushSample early = samples.get(samples.size() / 4);
    assertTrue(last.liveEntries() <= early.liveEntries() * 2,
        "the live log region grew from " + early.liveEntries() + " to " + last.liveEntries()
            + " entries across the import — pages that no flush can write are accumulating in it " + "again");

    // 3. Forwarding links must accrue at a rate that does not grow. Quadratic growth raises the
    // per-flush increment as the run proceeds; a bounded rate keeps it flat.
    final int mid = samples.size() / 2;
    final int firstHalf = samples.get(mid).forwardedLinks() - samples.get(0).forwardedLinks();
    final int secondHalf = last.forwardedLinks() - samples.get(mid).forwardedLinks();
    assertTrue(secondHalf <= Math.max(64, firstHalf * 2),
        "forwarding links accrued " + firstHalf + " over the first half of the import and " + secondHalf
            + " over the second — the rate is growing with the flush count");

    // 4. A 100M-row load is one transaction. Retaining one durable resolution per flushed page
    // therefore grows until the final commit, even though almost every reference that could use it
    // is already unreachable. Current PageReference copies carry a reachability-scoped resolution
    // handle; these maps are compatibility fallbacks only and must remain empty.
    assertEquals(0, last.completedOffsets(), "retained " + last.completedOffsets() + " historical disk offsets after "
        + samples.size() + " flushes — bookkeeping is accumulating with every page written");
    assertEquals(0, last.completedHashes(), "retained " + last.completedHashes() + " historical page hashes after "
        + samples.size() + " flushes — compatibility metadata is accumulating in a corpus-sized import");

    // 5. Non-vacuity, checked LAST on purpose: a build where pinning never ran should be caught
    // failing the properties above, not tripping here first.
    assertTrue(last.pinnedEntries() > 0,
        "no entry was ever pinned — the test exercised none of the machinery it is guarding");
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("Repeated updates of one live page do not create page-bound async prewrites")
  void repeatedUpdatesReuseOneLiveLogEntry() {
    final AtomicInteger flushes = new AtomicInteger();
    NodeStorageEngineWriter.asyncFlushFaultHook = (engineWriter, site) -> {
      if ("prepare".equals(site)) {
        flushes.incrementAndGet();
      }
    };

    final int updates = 10_000;
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        final long valueNodeKey = wtx.insertStringValueAsFirstChild("value-0").getNodeKey();
        for (int update = 1; update <= updates; update++) {
          assertTrue(wtx.moveTo(valueNodeKey));
          wtx.setStringValue("value-" + update);
        }

        final TransactionIntentLog log = ((NodeStorageEngineWriter) wtx.getStorageEngineWriter()).getLog();
        assertEquals(0, flushes.get(), "one write-hot page must not cross the distinct-page boundary");
        assertEquals(0, log.getCurrentGeneration(), "no async snapshot generation should be created");
        assertTrue(log.liveEntryCount() < NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT,
            "replacing one live TIL identity must not consume a new slot per update");
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToFirstChild());
      assertEquals("value-" + updates, rtx.getValue());
      assertEquals(1, session.getMostRecentRevisionNumber());
    }
  }

  @Test
  @Timeout(value = 2, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("One hot record rotates at the node-count bound in every epoch")
  void repeatedUpdatesUseNodeCountBoundInEveryEpoch() {
    final AtomicInteger flushes = new AtomicInteger();
    NodeStorageEngineWriter.asyncFlushFaultHook = (engineWriter, site) -> {
      if ("prepare".equals(site)) {
        flushes.incrementAndGet();
      }
    };

    final int nodeCount = AfterCommitState.MAX_ASYNC_FLUSH_NODE_COUNT;
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                   .storeDiffs(false)
                                                   .hashKind(HashType.NONE)
                                                   .buildPathSummary(false)
                                                   .versioningApproach(VersioningType.FULL)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .build());
      try (final JsonResourceSession session = database.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx = session.beginNodeTrx(Integer.MAX_VALUE, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        final long valueNodeKey = wtx.insertStringValueAsFirstChild("first-a").getNodeKey();
        final NodeStorageEngineWriter writer = (NodeStorageEngineWriter) wtx.getStorageEngineWriter();
        final TransactionIntentLog log = writer.getLog();

        // The insert is modification one. Stop exactly at the bound: rotation is deliberately
        // checked only at the next compound-operation-safe mutation boundary.
        for (int mutation = 1; mutation < nodeCount; mutation++) {
          assertTrue(wtx.moveTo(valueNodeKey));
          wtx.setStringValue((mutation & 1) == 0
              ? "first-a"
              : "first-b");
        }
        assertEquals(0, flushes.get());
        assertEquals(0, log.getCurrentGeneration());
        assertTrue(log.liveEntryCount() < NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT,
            "one write-hot page must leave the live-TIL page bound below the threshold");

        assertTrue(wtx.moveTo(valueNodeKey));
        wtx.setStringValue("second-a");
        assertEquals(1, flushes.get(), "the next mutation must rotate the completed first epoch");
        assertEquals(1, log.getCurrentGeneration());
        writer.awaitPendingAsyncFlush();

        // The triggering update is modification one of the second epoch. Again stop exactly at the
        // threshold, then prove that only the following mutation rotates it.
        for (int mutation = 1; mutation < nodeCount; mutation++) {
          assertTrue(wtx.moveTo(valueNodeKey));
          wtx.setStringValue((mutation & 1) == 0
              ? "second-a"
              : "second-b");
        }
        assertEquals(1, flushes.get());
        assertEquals(1, log.getCurrentGeneration());
        assertTrue(log.liveEntryCount() < NodeStorageEngineWriter.MAX_ASYNC_FLUSH_LOG_ENTRY_COUNT,
            "one write-hot page must leave the live-TIL page bound below the threshold");

        assertTrue(wtx.moveTo(valueNodeKey));
        wtx.setStringValue("final-value");
        assertEquals(2, flushes.get(), "the next mutation must rotate the completed second epoch");
        assertEquals(2, log.getCurrentGeneration());
        wtx.commit();
      }
    }

    Databases.clearGlobalCaches();
    try (final Database<JsonResourceSession> database = Databases.openJsonDatabase(PATHS.PATH1.getFile());
        final JsonResourceSession session = database.beginResourceSession(RESOURCE);
        final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveToFirstChild());
      assertEquals("final-value", rtx.getValue());
      assertEquals(1, session.getMostRecentRevisionNumber());
    }
  }

  @Test
  @Timeout(value = 5, unit = TimeUnit.MINUTES, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
  @DisplayName("An import killed by a failed flush strands no pinned entries once the writer is torn down")
  void failedImport_strandsNoPinnedEntries() {
    // Pinned containers are owned by the log exactly like the generation-scoped ones, and nothing
    // releases them before the transaction ends. This fix's territory overlaps the async-flush
    // failure path, where a fault poisons the writer and the session close drives
    // rollback() -> log.clear() and then close(). A pinned region not drained there would strand
    // every structural page a failed bulk load had pinned, off-heap frames included, with no owner
    // left to free them.
    //
    // WHAT THIS DOES AND DOES NOT PIN DOWN (measured by mutation, not assumed): removing the drain
    // from clear() ALONE does not trip this test, because close() drains too and the assertion runs
    // after teardown. Removing it from BOTH sites does trip it ("left 6 pinned entries behind").
    // So the property proven here is the end state — a failed import leaks nothing — not that any
    // one of the two teardown paths is the drainer. That is the property that matters for memory;
    // anyone tightening it to a single path has to observe the region between rollback and close.
    final AtomicReference<TransactionIntentLog> capturedLog = new AtomicReference<>();
    final AtomicInteger pinnedHighWater = new AtomicInteger();
    NodeStorageEngineWriter.asyncFlushFaultHook = (engineWriter, site) -> {
      // This observer owns only flush preparation/execution. Close now exposes independent fault
      // sites after the TIL has already been closed, where getLog() is intentionally unusable.
      if (!"prepare".equals(site) && !"write".equals(site)) {
        return;
      }
      final TransactionIntentLog log = engineWriter.getLog();
      capturedLog.compareAndSet(null, log);
      pinnedHighWater.accumulateAndGet(log.pinnedSize(), Math::max);
      // Fail a LATER flush, so pages have already been pinned when the failure lands.
      if ("write".equals(site) && log.pinnedSize() > 0) {
        throw new IllegalStateException("injected async-flush fault (pinned-region rollback test)");
      }
    };

    assertThrows(Throwable.class, this::runAsyncFlushImport, "a flush whose worker died must fail the import");

    // Non-vacuity: if nothing had been pinned before the fault, an empty region afterwards would
    // say nothing at all.
    assertTrue(pinnedHighWater.get() > 0, "no entry was pinned before the injected failure — the test proved nothing");

    final TransactionIntentLog log = capturedLog.get();
    assertNotNull(log, "the hook must have captured the transaction log it fired on");
    assertEquals(0, log.pinnedSize(), "teardown left " + log.pinnedSize() + " pinned entries behind — a failed "
        + "load must release everything the log owns, pinned pages included");
  }

  /**
   * A {@code KEEP_OPEN_ASYNC_FLUSH} import large enough to rotate the transaction log many times,
   * then a read-back of every record through a fresh session.
   *
   * @return how many array children the committed revision actually holds
   */
  private long runAsyncFlushImport() {
    Databases.createJsonDatabase(new DatabaseConfiguration(PATHS.PATH1.getFile()));
    try (final Database<JsonResourceSession> db = Databases.openJsonDatabase(PATHS.PATH1.getFile())) {
      db.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                             .storeDiffs(false)
                                             .hashKind(HashType.NONE)
                                             .buildPathSummary(false)
                                             .versioningApproach(VersioningType.FULL)
                                             .storageType(StorageType.FILE_CHANNEL)
                                             .build());

      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeTrx wtx =
              session.beginNodeTrx(MAX_NODES_BEFORE_FLUSH, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
        final long arrayNodeKey = wtx.insertArrayAsFirstChild().getNodeKey();
        for (int i = 0; i < INSERTED_RECORDS; i++) {
          wtx.moveTo(arrayNodeKey);
          wtx.insertStringValueAsFirstChild("item-" + i);
        }
        wtx.commit();
      }

      // Read back through a NEW session, so the answer comes from what was durably committed
      // rather than from any state the writing transaction still held.
      try (final JsonResourceSession session = db.beginResourceSession(RESOURCE);
          final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        rtx.moveToFirstChild();
        return rtx.getChildCount();
      }
    }
  }
}
