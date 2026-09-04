/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.page.ChunkedBodyConfig;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Task #78 gate: a projection whose whole-leaf materialization would exceed the eager budget is
 * served through WINDOWED payload loads — and the windowed view answers byte-for-byte what the
 * eager whole-column list answers.
 *
 * <p>
 * Both arms run against the same persisted store, and the witness demands positives on each side
 * (the guards-must-demand-a-positive-witness rule): the eager arm must record an eager
 * materialization and no windowed engagement; the windowed arm must record the engagement and one
 * materialization per window; and a kernel answer computed over each list must agree with the
 * corpus arithmetic, not merely with the other arm.
 */
final class ProjectionWindowedPayloadServeTest {

  private static final java.nio.file.Path DATABASE_PATH = JsonTestHelper.PATHS.PATH1.getFile();
  private static final int INDEX_NUMBER = 0;
  private static final int RECORDS = 4000;
  /** Two leaves per window at 1024 rows per leaf and 4000 records: three windows, one partial. */
  private static final int TEST_WINDOW_LEAVES = 2;

  private long priorEagerBudget = -1;
  private int priorWindowLeaves = -1;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
  }

  @AfterEach
  void tearDown() {
    if (priorEagerBudget >= 0) {
      ProjectionIndexCatalog.setEagerMaterializeBytesForTesting(priorEagerBudget);
    }
    if (priorWindowLeaves >= 0) {
      ProjectionIndexCatalog.setWindowLeavesForTesting(priorWindowLeaves);
    }
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
    Databases.getGlobalBufferManager().clearAllCaches();
  }

  @Test
  void aWindowedListAnswersExactlyLikeTheEagerList() {
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        final int rowGroupCount = rowGroupCount(session, revision);
        assertTrue(rowGroupCount >= 3, "the corpus must span several leaves; got " + rowGroupCount);
        final long projectedWeight = 1L << 20; // any positive figure routes; the budget decides

        // ==== eager arm ====
        priorEagerBudget = ProjectionIndexCatalog.setEagerMaterializeBytesForTesting(Long.MAX_VALUE);
        priorWindowLeaves = ProjectionIndexCatalog.setWindowLeavesForTesting(TEST_WINDOW_LEAVES);
        ChunkedBodyConfig.resetDiag();
        final Supplier<List<byte[]>> eagerSupplier = ProjectionIndexCatalog.rowGroupMaterializer(session, revision,
            INDEX_NUMBER, rowGroupCount, projectedWeight);
        final List<byte[]> eager = eagerSupplier.get();
        assertEquals(rowGroupCount, eager.size());
        assertEquals(0, ChunkedBodyConfig.lazyLoads(), "the eager arm must not engage the windowed route");
        assertEquals(0, ChunkedBodyConfig.chunkMaterializations(), "the eager arm must materialize no windows");
        assertEquals(1, ChunkedBodyConfig.eagerFallbacks(),
            "a lazy handle materialized eagerly must record exactly one eager materialization");

        // ==== windowed arm ====
        ProjectionIndexCatalog.setEagerMaterializeBytesForTesting(1L);
        ChunkedBodyConfig.resetDiag();
        final Supplier<List<byte[]>> windowedSupplier = ProjectionIndexCatalog.rowGroupMaterializer(session, revision,
            INDEX_NUMBER, rowGroupCount, projectedWeight);
        final List<byte[]> windowed = windowedSupplier.get();
        assertEquals(rowGroupCount, windowed.size());
        assertNotNull(ProjectionWindowedRowGroupPayloads.cacheOf(windowed),
            "over budget, the supplier must return a windowed view; got " + windowed.getClass().getSimpleName());

        // Byte-for-byte identical payloads at every index — sequential AND shuffled access order,
        // because the windowed view materializes and may evict as it goes.
        for (int i = 0; i < rowGroupCount; i++) {
          assertArrayEquals(eager.get(i), windowed.get(i), "payload of row group " + i + " differs");
        }
        for (int i = rowGroupCount - 1; i >= 0; i--) {
          assertArrayEquals(eager.get(i), windowed.get(i), "payload of row group " + i + " differs on re-access");
        }
        assertEquals(1, ChunkedBodyConfig.lazyLoads(), "the windowed route must record its engagement");
        final int expectedWindows = 1 + (rowGroupCount - 1) / TEST_WINDOW_LEAVES;
        assertTrue(ChunkedBodyConfig.chunkMaterializations() >= expectedWindows,
            "every window must have materialized at least once: " + ChunkedBodyConfig.chunkMaterializations() + " < "
                + expectedWindows);
        assertEquals(0, ChunkedBodyConfig.eagerFallbacks(), "the windowed arm must not fall back to eager");

        // A real kernel answer over each list, checked against the corpus arithmetic — a defect
        // that empties or inflates both arms identically cannot pass.
        assertEquals(RECORDS, ProjectionIndexByteScan.countRows(eager));
        assertEquals(RECORDS, ProjectionIndexByteScan.countRows(windowed));
      }
    }
  }

  @Test
  void anOverBudgetColumnFillDeclinesWithoutMemoizingCorruption() {
    // The second eager site of task #78: the sliced string-predicate route fills a WHOLE column's
    // BODY+DICT segments at once. Over the budget it must decline through the established
    // IllegalStateException door (callers fall back to the whole-leaf windowed route) — and the
    // decline must NOT poison the store: with the budget restored, the very same store fills.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = rtx.getStorageEngineReader();
          final ProjectionIndexMetadata metadata =
              ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, 0L));
          assertNotNull(metadata);
          final int[] physicalOrder =
              ProjectionIndexFences.readPhysicalOrder(reader, INDEX_NUMBER, metadata.rowGroupCount());
          final List<ProjectionIndexHOTStorage.RowGroupDirectory> directories =
              ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(reader, INDEX_NUMBER,
                  metadata.rowGroupCount(), physicalOrder, worker -> worker.accept(reader));
          assertNotNull(directories, "the store must expose row-group directories");
          final ProjectionColumnStore store = new ProjectionColumnStore(directories);
          final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
              offsets -> ProjectionIndexHOTStorage.readSegmentBytesBatch(reader, offsets);
          final int nameColumn = 0;
          final long projected = store.projectedColumnFillBytes(nameColumn);
          assertTrue(projected > 0, "the name column must project a positive fill size");

          final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(projected - 1);
          try {
            final IllegalStateException decline = org.junit.jupiter.api.Assertions.assertThrows(
                IllegalStateException.class, () -> store.column(nameColumn, fetcher));
            assertTrue(decline.getMessage().contains("budget"),
                "the decline must name the budget, got: " + decline.getMessage());
            assertTrue(store.projectedColumnFillBytes(nameColumn) == projected,
                "the projected size must be stable across declines");
            // NOT memoized as corruption: restoring the budget lets the SAME store fill.
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE);
            final ProjectionColumnStore.ColumnSlice[] slices = store.column(nameColumn, fetcher);
            assertEquals(metadata.rowGroupCount(), slices.length);
          } finally {
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
          }
        }
      }
    }
  }

  @Test
  void theWindowedViewBoundsResidencyAndSurvivesEviction() {
    // Direct unit coverage of the view: a synthetic fetcher, a tiny resident cap, and an access
    // pattern that forces eviction and re-materialization. Content must stay stable and the
    // resident count bounded.
    final int rowGroups = 16;
    final int windowSize = 2;
    final AtomicInteger fetches = new AtomicInteger();
    final AtomicInteger sourcesSeen = new AtomicInteger();
    final AtomicInteger sourcesA = new AtomicInteger();
    final AtomicInteger sourcesB = new AtomicInteger();
    final java.util.Set<ProjectionWindowedRowGroupPayloads.ReaderSource> sourcesOfA =
        java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());
    final ProjectionWindowedRowGroupPayloads cache =
        new ProjectionWindowedRowGroupPayloads(rowGroups, windowSize, 2, (source, from, toExclusive) -> {
          fetches.incrementAndGet();
          if (source != null) {
            sourcesSeen.incrementAndGet();
            if (sourcesOfA.contains(source)) {
              sourcesA.incrementAndGet();
            } else {
              sourcesB.incrementAndGet();
            }
          }
          final byte[][] window = new byte[toExclusive - from][];
          for (int i = from; i < toExclusive; i++) {
            window[i - from] = new byte[] {(byte) i, (byte) (i >> 8)};
          }
          return window;
        });
    assertEquals(rowGroups, cache.size());
    assertEquals(8, cache.windowCount());

    // TWO callers, each with its OWN source. A shared mutable source field would make every fetch
    // use whichever caller bound last — the dead-session failure with the arrow reversed — so each
    // view's fetches must arrive carrying that view's own source and no other.
    final ProjectionWindowedRowGroupPayloads.ReaderSource sourceA = () -> {
      throw new UnsupportedOperationException("the synthetic fetcher never opens a reader");
    };
    sourcesOfA.add(sourceA);
    final ProjectionWindowedRowGroupPayloads.ReaderSource sourceB = () -> {
      throw new UnsupportedOperationException("the synthetic fetcher never opens a reader");
    };
    final List<byte[]> viewA = cache.boundTo(sourceA);
    final List<byte[]> viewB = cache.boundTo(sourceB);
    assertNotSame(viewA, viewB, "each consult gets its own thin view");
    assertSame(cache, ProjectionWindowedRowGroupPayloads.cacheOf(viewA));
    assertSame(cache, ProjectionWindowedRowGroupPayloads.cacheOf(viewB),
        "both views must share ONE window cache, or the resident cap is multiplied");
    assertSame(sourceA, ProjectionWindowedRowGroupPayloads.sourceOf(viewA));
    assertSame(sourceB, ProjectionWindowedRowGroupPayloads.sourceOf(viewB));

    // Two full passes plus a scatter: every access must see its own row group's bytes. The passes
    // alternate views, so a shared-source implementation would be caught by the per-source tally.
    for (int pass = 0; pass < 2; pass++) {
      final List<byte[]> view = pass == 0
          ? viewA
          : viewB;
      for (int i = 0; i < rowGroups; i++) {
        assertArrayEquals(new byte[] {(byte) i, (byte) (i >> 8)}, view.get(i));
      }
    }
    IntStream.of(15, 0, 7, 3, 12, 1, 14, 2)
             .forEach(i -> assertArrayEquals(new byte[] {(byte) i, (byte) (i >> 8)}, viewA.get(i)));
    assertTrue(fetches.get() > cache.windowCount(),
        "a 2-window resident cap over 8 windows must have re-materialized evicted windows; fetches=" + fetches.get());
    assertTrue(cache.residentWindows() <= 3,
        "residency must stay near the cap (cap 2, plus one in-flight); resident=" + cache.residentWindows());
    assertEquals(fetches.get(), sourcesSeen.get(), "every fetch must be handed a reader source");
    assertTrue(sourcesA.get() > 0 && sourcesB.get() > 0,
        "both callers' sources must have been used; A=" + sourcesA.get() + " B=" + sourcesB.get());
    assertEquals(fetches.get(), sourcesA.get() + sourcesB.get(),
        "every fetch must carry the source of the view that triggered it, never another caller's");
  }

  @Test
  void aWindowedViewIsOwnedByTheHandleAndSurvivesTheBuildingSessionsClose() {
    // A handle lives in the catalog's PROCESS-WIDE cache, keyed by (resource, def, build revision)
    // — which is exactly the identity of the leaves a windowed view reads, so the handle is the
    // view's natural owner. What makes that safe is that the view captures NO session: each consult
    // rebinds it to the CALLER's own live reader source. The property this pins is the one that
    // originally forced the view off the handle: after the session that built it has closed, the
    // very same memoized view still serves, through its successor's transactions.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      final IndexDef def = projectionDef();
      final int revision;
      final int rowGroupCount;
      final ProjectionIndexRegistry.Handle sharedHandle;
      final List<byte[]> firstSessionView;

      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(def), wtx);
          wtx.commit();
        }
        revision = session.getMostRecentRevisionNumber();
        rowGroupCount = rowGroupCount(session, revision);
        priorWindowLeaves = ProjectionIndexCatalog.setWindowLeavesForTesting(TEST_WINDOW_LEAVES);
        priorEagerBudget = ProjectionIndexCatalog.setEagerMaterializeBytesForTesting(1L);
        ChunkedBodyConfig.resetDiag();

        sharedHandle = ProjectionIndexCatalog.load(session, revision, def);
        assertNotNull(sharedHandle, "the persisted projection must load");
        assertFalse(sharedHandle.payloadsMaterialized(), "a freshly loaded column-lazy handle holds no leaves");

        final AtomicInteger consulted = new AtomicInteger();
        final var delegate =
            (ProjectionWindowedRowGroupPayloads.BoundMaterializer) ProjectionIndexCatalog.rowGroupMaterializer(session,
                revision, INDEX_NUMBER, rowGroupCount, 1L << 20);
        final Supplier<List<byte[]>> callerMaterializer = new ProjectionWindowedRowGroupPayloads.BoundMaterializer() {
          @Override
          public List<byte[]> get() {
            consulted.incrementAndGet();
            return delegate.get();
          }

          @Override
          public ProjectionWindowedRowGroupPayloads.ReaderSource readerSource() {
            return delegate.readerSource();
          }
        };

        firstSessionView = sharedHandle.rowGroupPayloads(callerMaterializer);
        assertNotNull(ProjectionWindowedRowGroupPayloads.cacheOf(firstSessionView),
            "over budget the handle must serve a windowed view; got " + firstSessionView.getClass().getSimpleName());
        assertFalse(sharedHandle.payloadsMaterialized(),
            "a windowed view is NOT resident — reporting it as materialized steers later queries off the "
                + "sliced route into a byte-kernel scan that re-reads windows from disk");

        // Memoized ON THE HANDLE: a second consult neither rebuilds the view nor re-walks the
        // physical order, so the materializer is not consulted again.
        final List<byte[]> again = sharedHandle.rowGroupPayloads(callerMaterializer);
        assertEquals(1, consulted.get(), "a memoized cache must not be rebuilt per consult");
        assertNotSame(firstSessionView, again, "each consult gets its OWN thin view, never a shared bound one");
        assertSame(ProjectionWindowedRowGroupPayloads.cacheOf(firstSessionView),
            ProjectionWindowedRowGroupPayloads.cacheOf(again), "one window cache per handle");
        assertFalse(sharedHandle.payloadsMaterialized());
        assertEquals(1, ChunkedBodyConfig.lazyLoads(),
            "the windowed route must be ENGAGED once per handle, not once per caller");

        assertEquals(RECORDS, ProjectionIndexByteScan.countRows(firstSessionView));
      }

      // ==== the reported failure: a NEW session on the SAME cached handle ====
      // The first session is closed. Its windows are evicted below the cap as the scan walks, so
      // serving here REQUIRES opening fresh transactions — through the successor, never the corpse.
      try (JsonResourceSession reopened = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final ProjectionIndexRegistry.Handle again = ProjectionIndexCatalog.load(reopened, revision, def);
        assertSame(sharedHandle, again, "the catalog's DATA cache must still serve the same handle instance");
        assertFalse(again.payloadsMaterialized());

        final List<byte[]> reopenedView = again.rowGroupPayloads(
            ProjectionIndexCatalog.rowGroupMaterializer(reopened, revision, INDEX_NUMBER, rowGroupCount, 1L << 20));
        assertSame(ProjectionWindowedRowGroupPayloads.cacheOf(firstSessionView),
            ProjectionWindowedRowGroupPayloads.cacheOf(reopenedView),
            "the handle's memoized CACHE is the successor's too — it captured no session to go stale");
        assertNotSame(firstSessionView, reopenedView, "the successor reads through its OWN source");
        assertEquals(RECORDS, ProjectionIndexByteScan.countRows(reopenedView),
            "the new session must serve its windows through ITS OWN live transactions");

        // A windowed handle keeps serving windowed while callers hand it windowed materializers.
        // It is PROMOTED only when a caller's materializer actually produces resident leaves — the
        // handle never decides on its own to pay for a whole-leaf materialization.
        assertFalse(again.payloadsMaterialized());
        ProjectionIndexCatalog.setEagerMaterializeBytesForTesting(Long.MAX_VALUE);
        final List<byte[]> promoted = again.rowGroupPayloads(
            ProjectionIndexCatalog.rowGroupMaterializer(reopened, revision, INDEX_NUMBER, rowGroupCount, 1L << 20));
        assertNull(ProjectionWindowedRowGroupPayloads.cacheOf(promoted),
            "a materializer that produced resident leaves must promote the handle off the windowed route");
        assertTrue(again.payloadsMaterialized(), "promotion must flip the resident predicate");
        assertEquals(RECORDS, ProjectionIndexByteScan.countRows(promoted));

        // The eager arm is the contrast, on a handle that never went windowed: under the budget the
        // leaves ARE inert bytes, so the handle memoizes them and reports resident.
        ProjectionIndexCatalog.clearCache();
        final ProjectionIndexRegistry.Handle fresh = ProjectionIndexCatalog.load(reopened, revision, def);
        assertNotSame(sharedHandle, fresh, "clearCache must yield a handle that has served nothing yet");
        final Supplier<List<byte[]>> eagerMaterializer =
            ProjectionIndexCatalog.rowGroupMaterializer(reopened, revision, INDEX_NUMBER, rowGroupCount, 1L << 20);
        final List<byte[]> eager = fresh.rowGroupPayloads(eagerMaterializer);
        assertNull(ProjectionWindowedRowGroupPayloads.cacheOf(eager));
        assertTrue(fresh.payloadsMaterialized(), "resident leaves must flip the predicate");
        assertSame(eager, fresh.rowGroupPayloads(eagerMaterializer),
            "resident leaves must be memoized and served without re-consulting the materializer");
      }
    }
  }

  @Test
  void oneWindowedViewPerHANDLE_NotPerQueryRevision() {
    // A handle is cached on the BUILD revision, so ONE handle serves every query revision since the
    // build. Keying the session memo on the query revision instead builds a fresh view — its own
    // resident-window cap, its own full physical-order walk — per revision, over byte-identical
    // leaves. That is the exact multiplication the session memo exists to prevent, surviving along
    // the revision axis.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        final IndexDef def = projectionDef();
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(def), wtx);
          wtx.commit();
        }
        final int buildRevision = session.getMostRecentRevisionNumber();
        // A later revision that does NOT rebuild the projection: the same handle must serve it.
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.commit();
        }
        final int laterRevision = session.getMostRecentRevisionNumber();
        assertTrue(laterRevision > buildRevision, "the second commit must produce a later revision");

        priorWindowLeaves = ProjectionIndexCatalog.setWindowLeavesForTesting(TEST_WINDOW_LEAVES);
        priorEagerBudget = ProjectionIndexCatalog.setEagerMaterializeBytesForTesting(1L);
        ChunkedBodyConfig.resetDiag();

        final ProjectionIndexRegistry.Handle atBuild = ProjectionIndexCatalog.load(session, buildRevision, def);
        final ProjectionIndexRegistry.Handle atLater = ProjectionIndexCatalog.load(session, laterRevision, def);
        assertNotNull(atBuild);
        assertSame(atBuild, atLater, "one handle, cached on the BUILD revision, serves both query revisions");

        final int rowGroupCount = atBuild.rowGroupCount();
        final List<byte[]> viewAtBuild = atBuild.rowGroupPayloads(
            ProjectionIndexCatalog.rowGroupMaterializer(session, buildRevision, INDEX_NUMBER, rowGroupCount, 1L << 20));
        final List<byte[]> viewAtLater = atLater.rowGroupPayloads(
            ProjectionIndexCatalog.rowGroupMaterializer(session, laterRevision, INDEX_NUMBER, rowGroupCount, 1L << 20));
        assertNotNull(ProjectionWindowedRowGroupPayloads.cacheOf(viewAtBuild));
        assertSame(ProjectionWindowedRowGroupPayloads.cacheOf(viewAtBuild),
            ProjectionWindowedRowGroupPayloads.cacheOf(viewAtLater),
            "the memo owner is the HANDLE; a second query revision on the same handle must not build a second cache");
        assertEquals(1, ChunkedBodyConfig.lazyLoads(),
            "the windowed route must be ENGAGED once per handle, not once per query revision");
        assertEquals(RECORDS, ProjectionIndexByteScan.countRows(viewAtLater));
      }
    }
  }

  @Test
  void aFatDictionaryColumnIsViableInDISTINCT_IDENTITYModeWhenItsFullFillIsNot() {
    // The COUNT(DISTINCT) operand is filled in distinct-identity mode: BODY plus the ~8 B/entry
    // hash chain, with NO dictionary bytes. Judging that operand by the whole-column projection
    // (BODY + DICTIONARY) rejects the sliced arm over bytes its fill never fetches.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(fatDictionaryCorpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = rtx.getStorageEngineReader();
          final ProjectionIndexMetadata metadata =
              ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, 0L));
          assertNotNull(metadata);
          final int[] physicalOrder =
              ProjectionIndexFences.readPhysicalOrder(reader, INDEX_NUMBER, metadata.rowGroupCount());
          final ProjectionColumnStore store =
              new ProjectionColumnStore(ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(
                  reader, INDEX_NUMBER, metadata.rowGroupCount(), physicalOrder, worker -> worker.accept(reader)));
          final int nameColumn = 0;
          final long full = store.projectedColumnFillBytes(nameColumn);
          final long identity = store.projectedColumnIdentityFillBytes(nameColumn);
          assertTrue(identity < full, "a fat dictionary must project a SMALLER identity fill than a whole-column fill: "
              + identity + " vs " + full);

          // A budget between the two modes: the whole-column fill is refused, the identity fill is
          // affordable — so the route gate must disagree with itself across the two predicates.
          final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(identity);
          try {
            assertFalse(store.columnFillable(nameColumn), "the whole-column fill is over budget here");
            assertTrue(store.columnIdentityFillable(nameColumn),
                "the identity fill fits, so a distinct operand on this column is still route-viable");
          } finally {
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
          }
        }
      }
    }
  }

  @Test
  void aMaskedFillIsPricedByWhatTheMaskLeavesStanding() {
    // The budget used to guard only the UNMASKED door. A keep mask returns non-null as soon as ONE
    // leaf drops, so a prune that eliminates almost nothing still routed the whole multi-GB column
    // through columnMasked — the residency the budget exists to refuse. Pricing has to follow the
    // mask: a prune that proves the fetch small proceeds, one that does not declines through the
    // same non-memoizing door.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = rtx.getStorageEngineReader();
          final ProjectionIndexMetadata metadata =
              ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, 0L));
          assertNotNull(metadata);
          final int leaves = metadata.rowGroupCount();
          assertTrue(leaves >= 3, "the corpus must span several leaves; got " + leaves);
          final int[] physicalOrder = ProjectionIndexFences.readPhysicalOrder(reader, INDEX_NUMBER, leaves);
          final ProjectionColumnStore store =
              new ProjectionColumnStore(ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(
                  reader, INDEX_NUMBER, leaves, physicalOrder, worker -> worker.accept(reader)));
          final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
              offsets -> ProjectionIndexHOTStorage.readSegmentBytesBatch(reader, offsets);
          final int nameColumn = 0;

          final long[] keepAll = new long[(leaves + 63) >>> 6];
          for (int leaf = 0; leaf < leaves; leaf++) {
            keepAll[leaf >>> 6] |= 1L << (leaf & 63);
          }
          final long[] keepOne = new long[(leaves + 63) >>> 6];
          keepOne[0] |= 1L;

          final long allBytes = store.projectedMaskedFillBytes(nameColumn, keepAll);
          final long oneBytes = store.projectedMaskedFillBytes(nameColumn, keepOne);
          assertEquals(store.projectedColumnFillBytes(nameColumn), allBytes,
              "a mask that drops nothing must price exactly like the whole-column fill");
          assertTrue(oneBytes > 0 && oneBytes < allBytes,
              "a one-leaf mask must price strictly below the whole column: " + oneBytes + " vs " + allBytes);

          // A budget the one-leaf prune fits and the keep-everything mask does not.
          final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(allBytes - 1);
          try {
            final ProjectionColumnStore.FillBudgetExceededException declined =
                assertThrows(ProjectionColumnStore.FillBudgetExceededException.class,
                    () -> store.columnMasked(nameColumn, fetcher, keepAll));
            assertTrue(declined.getMessage().contains("budget"), declined.getMessage());

            // Same budget, a mask that proves the fetch small: it proceeds.
            final ProjectionColumnStore.ColumnSlice[] pruned = store.columnMasked(nameColumn, fetcher, keepOne);
            assertEquals(leaves, pruned.length, "a masked fill still yields one slice per leaf");

            // The decline is NOT memoized as corruption: with the budget restored the same store
            // fills the same mask.
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE);
            assertEquals(leaves, store.columnMasked(nameColumn, fetcher, keepAll).length);
          } finally {
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
          }
        }
      }
    }
  }

  @Test
  void columnFillsArePricedCUMULATIVELY_NotOnePerColumn() {
    // The fill budget is a per-column figure, but a published fill is RETAINED for the store's whole
    // cache lifetime — so a handle's residency is the SUM over the columns a query mix touches. With
    // only a per-column check, a projection whose columns each sit just under the budget retains
    // several budgets' worth while the cache weigher charges one, and the 8 G envelope the windowed
    // route exists to hold is gone.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = rtx.getStorageEngineReader();
          final ProjectionIndexMetadata metadata =
              ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, 0L));
          assertNotNull(metadata);
          final int leaves = metadata.rowGroupCount();
          final int[] physicalOrder = ProjectionIndexFences.readPhysicalOrder(reader, INDEX_NUMBER, leaves);
          final ProjectionColumnStore store =
              new ProjectionColumnStore(ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(
                  reader, INDEX_NUMBER, leaves, physicalOrder, worker -> worker.accept(reader)));
          final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
              offsets -> ProjectionIndexHOTStorage.readSegmentBytesBatch(reader, offsets);
          final int nameColumn = 0;
          final int scoreColumn = 1;
          final long nameBytes = store.projectedColumnFillBytes(nameColumn);
          final long scoreBytes = store.projectedColumnFillBytes(scoreColumn);

          // A budget each column fits ALONE but the two together do not.
          final long budget = Math.max(nameBytes, scoreBytes) + Math.min(nameBytes, scoreBytes) / 2;
          assertTrue(budget >= nameBytes && budget >= scoreBytes && budget < nameBytes + scoreBytes,
              "the budget must admit either column alone and neither pair: " + budget + " vs " + nameBytes + "+"
                  + scoreBytes);
          final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(budget);
          // ONE open query reads both columns: its scope pins the first fill, so the second cannot
          // be admitted by evicting it (a fit door evicts only what no open query holds).
          try (ProjectionResidencyScope query = ProjectionResidencyScope.open()) {
            assertNotNull(query);
            assertEquals(0L, store.retainedFillBytes(), "a fresh store retains nothing");
            assertTrue(store.columnFillWithinBudget(nameColumn), "the first column alone must fit");
            assertEquals(leaves, store.column(nameColumn, fetcher).length);
            assertTrue(store.retainedFillBytes() >= nameBytes, "a published fill must be charged");
            assertEquals(1, store.residencyPins(nameColumn), "the open query pins its fill");

            // The second column's OWN projection still fits the budget; what does not fit is the
            // pair, and that is the question the store has to be asking.
            assertTrue(scoreBytes <= budget, "the second column alone would fit");
            assertFalse(store.columnFillWithinBudget(scoreColumn),
                "beside what is already retained, the second column must not fit");
            final ProjectionColumnStore.FillBudgetExceededException declined = assertThrows(
                ProjectionColumnStore.FillBudgetExceededException.class, () -> store.column(scoreColumn, fetcher));
            assertTrue(declined.getMessage().contains("already retained"), declined.getMessage());
            assertEquals(leaves, store.column(nameColumn, fetcher).length, "the pinned fill was not evicted");

            // Not memoized as corruption: with room restored the same store fills the same column.
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE);
            assertEquals(leaves, store.column(scoreColumn, fetcher).length);
          } finally {
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
          }
        }
      }
    }
  }

  @Test
  void theRecordKeyFillIsPricedAndChargedLikeEveryOtherRetainedFill() {
    // The KEYS chain is fetched unmasked across every leaf and DECODES into a long[] the store
    // retains for its whole cache lifetime — 8 B/row, which at 100M rows is ~800 MB. The cache
    // weigher's flat charge is justified by the cumulative ledger bounding every retained fill, so
    // a door that retains a row-count-scaled array without pricing or charging it makes that
    // justification false on exactly the workload the windowed route targets.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = rtx.getStorageEngineReader();
          final ProjectionIndexMetadata metadata =
              ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, 0L));
          assertNotNull(metadata);
          final int leaves = metadata.rowGroupCount();
          final int[] physicalOrder = ProjectionIndexFences.readPhysicalOrder(reader, INDEX_NUMBER, leaves);
          final ProjectionColumnStore store =
              new ProjectionColumnStore(ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(
                  reader, INDEX_NUMBER, leaves, physicalOrder, worker -> worker.accept(reader)));
          final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
              offsets -> ProjectionIndexHOTStorage.readSegmentBytesBatch(reader, offsets);

          final long keysBytes = store.projectedRecordKeysFillBytes();
          // The decoded long[] alone is 8 B per row, so the projection must exceed it — a figure
          // that counted only the raw chain would leave the retained half unpriced.
          assertTrue(keysBytes >= 8L * RECORDS,
              "the projection must include the decoded 8 B/row keys: " + keysBytes + " for " + RECORDS + " rows");

          final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(keysBytes - 1);
          try {
            final ProjectionColumnStore.FillBudgetExceededException declined =
                assertThrows(ProjectionColumnStore.FillBudgetExceededException.class, () -> store.recordKeys(fetcher));
            assertTrue(declined.getMessage().contains("record-key"), declined.getMessage());
            assertEquals(0L, store.retainedFillBytes(), "a declined fill must charge nothing");

            // With room, the fill proceeds AND is charged — the ledger is what the weigher's flat
            // charge rests on, so an uncharged retained fill is the same defect as an unpriced one.
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE);
            assertEquals(leaves, store.recordKeys(fetcher).length);
            assertTrue(store.retainedFillBytes() >= keysBytes,
                "a published record-key fill must be charged: " + store.retainedFillBytes() + " < " + keysBytes);
          } finally {
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
          }
        }
      }
    }
  }

  @Test
  void aFillIsPricedByWhatItDECODESInto_NotOnlyItsPackedBytes() {
    // One question — what does this column cost resident — used to have two answers: the cache
    // weigher counted the decoded 8 B/row lane, the fill doors counted only packed segment bytes.
    // A bit-packed NUMERIC_LONG column packs to a byte or two per row and decodes to eight, so a
    // budget priced on packed bytes admits several times what it believes it is admitting. The two
    // sides now share one pricing function, and the observable is that the door's own figure
    // accounts for the lane it will decode into.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = rtx.getStorageEngineReader();
          final ProjectionIndexMetadata metadata =
              ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, 0L));
          assertNotNull(metadata);
          final int leaves = metadata.rowGroupCount();
          final int[] physicalOrder = ProjectionIndexFences.readPhysicalOrder(reader, INDEX_NUMBER, leaves);
          final ProjectionColumnStore store =
              new ProjectionColumnStore(ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(
                  reader, INDEX_NUMBER, leaves, physicalOrder, worker -> worker.accept(reader)));
          final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
              offsets -> ProjectionIndexHOTStorage.readSegmentBytesBatch(reader, offsets);
          final int scoreColumn = 1;
          assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, store.columnKind(scoreColumn),
              "the fixture's score column must be the bit-packed long lane this prices");

          final long decodedLane = 8L * RECORDS;
          final long projected = store.projectedColumnFillBytes(scoreColumn);
          assertTrue(projected >= decodedLane,
              "a long-lane fill must be priced for the 8 B/row it decodes into: " + projected + " < " + decodedLane);

          // And the figure is the one the DOOR enforces, not a number computed beside it: a budget
          // just under it refuses the fill, and one at it admits.
          final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(projected - 1);
          try {
            assertFalse(store.columnFillWithinBudget(scoreColumn));
            assertThrows(ProjectionColumnStore.FillBudgetExceededException.class,
                () -> store.column(scoreColumn, fetcher));
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(projected);
            assertTrue(store.columnFillWithinBudget(scoreColumn));
            assertEquals(leaves, store.column(scoreColumn, fetcher).length);
            assertTrue(store.retainedFillBytes() >= decodedLane,
                "the ledger must carry the decoded lane too: " + store.retainedFillBytes());
          } finally {
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
          }
        }
      }
    }
  }

  @Test
  void aStringColumnFillIsPricedForItsDecodedIdLane_NotOnlyItsSegmentBytes() {
    // The shared pricing function priced STRING_DICT at zero decoded residency — the one kind the
    // windowed route exists to serve. A filled string slice retains a 4 B/row id lane, presence
    // words and a DECOMPRESSED dictionary; a low-cardinality column makes the id lane the dominant
    // term, so a figure that counts only stored segment bytes falls far below what the fill holds.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(lowCardinalityCorpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final ProjectionColumnStore store = storeOf(rtx.getStorageEngineReader());
          final int nameColumn = 0;
          assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, store.columnKind(nameColumn),
              "the fixture's name column must be the dictionary-coded string kind this prices");

          final long idLane = 4L * RECORDS;
          assertTrue(store.projectedColumnFillBytes(nameColumn) >= idLane,
              "a string fill must be priced for the 4 B/row ids it decodes into: "
                  + store.projectedColumnFillBytes(nameColumn) + " < " + idLane);
          // The identity mode retains the same id lane beside its hash chain — the arithmetic that
          // used to contribute exactly zero for a column that is STRING_DICT by construction.
          assertTrue(store.projectedColumnIdentityFillBytes(nameColumn) >= idLane,
              "an identity fill must be priced for its id lane too: "
                  + store.projectedColumnIdentityFillBytes(nameColumn) + " < " + idLane);
        }
      }
    }
  }

  @Test
  void aFilledColumnIsChargedToTheLedgerExactlyOnce() {
    // A slice fill runs through the raw-BODY door, which prices and charges that chain on its own
    // account. Charging the outer projection on top counted one retained copy of the BODY arrays
    // twice, and the ledger is monotonic and feeds the decline predicates — so the over-charge
    // steers later servable queries off the sliced route for residency that is not there.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = rtx.getStorageEngineReader();
          final ProjectionColumnStore store = storeOf(reader);
          final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
              offsets -> ProjectionIndexHOTStorage.readSegmentBytesBatch(reader, offsets);
          final int scoreColumn = 1;
          final long projected = store.projectedColumnFillBytes(scoreColumn);
          final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE);
          try {
            assertEquals(0L, store.retainedFillBytes());
            store.column(scoreColumn, fetcher);
            assertEquals(projected, store.retainedFillBytes(),
                "one fill must charge its projection exactly once, not once per door it passes through");

            // Re-entering the inner raw-BODY door for the same column adds nothing: those bytes are
            // already retained and already charged.
            store.columnBytes(scoreColumn, fetcher);
            assertEquals(projected, store.retainedFillBytes(), "an already-retained chain must not be charged again");
          } finally {
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
          }
        }
      }
    }
  }

  @Test
  void aBodyChainAlreadyRetainedIsNotChargedAgainstTheNextFillOfTheSameColumn() {
    // The fused fold route fills a column's raw BODY bytes directly; a later query on the same
    // cached handle takes the sliced route on that column. The gate priced the second fill at its
    // GROSS projection against a ledger that already held those very body arrays, so it refused
    // fills whose true residency fit the budget — and the group arms answer a refusal by re-entering
    // whole-leaf, permanently steering a servable query off the sliced route for phantom bytes.
    Databases.createJsonDatabase(new DatabaseConfiguration(DATABASE_PATH));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(DATABASE_PATH)) {
      db.createResource(ResourceConfiguration.newBuilder(JsonTestHelper.RESOURCE)
                                             .useDeweyIDs(false)
                                             .hashKind(HashType.NONE)
                                             .storeNodeHistory(false)
                                             .buildPathSummary(true)
                                             .build());
      try (JsonResourceSession session = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()),
              InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
        }
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(projectionDef()), wtx);
          wtx.commit();
        }
        final int revision = session.getMostRecentRevisionNumber();
        try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
          final var reader = rtx.getStorageEngineReader();
          final ProjectionColumnStore store = storeOf(reader);
          final ProjectionColumnStore.ColumnSegmentFetcher fetcher =
              offsets -> ProjectionIndexHOTStorage.readSegmentBytesBatch(reader, offsets);
          final int scoreColumn = 1;
          final long projected = store.projectedColumnFillBytes(scoreColumn);

          // The fold route goes first, exactly as a plain sum() over this column would.
          final long priorBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(projected);
          try {
            store.columnBytes(scoreColumn, fetcher);
            final long afterBody = store.retainedFillBytes();
            assertTrue(afterBody > 0, "the raw BODY fill must be charged");
            assertTrue(afterBody < projected, "the BODY chain alone must be cheaper than the whole fill");

            // A budget that exactly fits the column's true residency. The slice fill reuses those
            // very body arrays, so what it ADDS still fits — the gate must price the increment.
            assertTrue(store.columnFillWithinBudget(scoreColumn),
                "the planner predicate must price what the fill adds, not bytes already retained");
            store.column(scoreColumn, fetcher);
            assertEquals(projected, store.retainedFillBytes(),
                "the store's residency is the column's projection — the body chain is one set of arrays");
          } finally {
            ProjectionColumnStore.setColumnFillBudgetBytesForTesting(priorBudget);
          }
        }
      }
    }
  }

  /** The store behind the fixture's persisted projection. */
  private static ProjectionColumnStore storeOf(final io.sirix.api.StorageEngineReader reader) {
    final ProjectionIndexMetadata metadata =
        ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readBlob(reader, INDEX_NUMBER, 0L));
    assertNotNull(metadata);
    final int leaves = metadata.rowGroupCount();
    final int[] physicalOrder = ProjectionIndexFences.readPhysicalOrder(reader, INDEX_NUMBER, leaves);
    return new ProjectionColumnStore(ProjectionIndexHOTStorage.readAllRowGroupDirectoriesFromColumnSegmentSlots(reader,
        INDEX_NUMBER, leaves, physicalOrder, worker -> worker.accept(reader)));
  }

  /** THREE distinct names over every record: the decoded id lane dwarfs the stored dictionary. */
  private static String lowCardinalityCorpus() {
    final StringBuilder json = new StringBuilder(RECORDS * 32);
    json.append('[');
    for (int record = 0; record < RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"name\":\"n").append(record % 3).append('"');
      json.append(",\"score\":").append(record * 3L).append('}');
    }
    return json.append(']').toString();
  }

  /** Long, highly distinct names: the dictionary dwarfs the 8 B/entry hash chain beside it. */
  private static String fatDictionaryCorpus() {
    final StringBuilder json = new StringBuilder(RECORDS * 256);
    json.append('[');
    for (int record = 0; record < RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"name\":\"n").append(record).append('-').append("abcdefghij".repeat(20)).append('"');
      json.append(",\"score\":").append(record * 3L).append('}');
    }
    return json.append(']').toString();
  }

  private static int rowGroupCount(final JsonResourceSession session, final int revision) {
    try (var rtx = session.beginNodeReadOnlyTrx(revision)) {
      final byte[] raw = ProjectionIndexHOTStorage.readBlob(rtx.getStorageEngineReader(), INDEX_NUMBER, 0L);
      assertNotNull(raw, "the projection must have metadata at slot 0");
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(raw);
      assertNotNull(metadata);
      return metadata.rowGroupCount();
    }
  }

  private static IndexDef projectionDef() {
    return IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
        List.of(Path.parse("/[]/name", PathParser.Type.JSON), Path.parse("/[]/score", PathParser.Type.JSON)),
        List.of(Type.STR, Type.LON), INDEX_NUMBER, IndexDef.DbType.JSON);
  }

  private static String corpus() {
    final StringBuilder json = new StringBuilder(RECORDS * 48);
    json.append('[');
    for (int record = 0; record < RECORDS; record++) {
      if (record > 0) {
        json.append(',');
      }
      json.append("{\"name\":\"n").append(record).append("-").append("x".repeat(record % 13)).append('"');
      json.append(",\"score\":").append(record * 3L).append('}');
    }
    return json.append(']').toString();
  }
}
