/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
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
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
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
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()), InsertPosition.AS_FIRST_CHILD)
              .commitAfterwards().build().call();
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
        final Supplier<List<byte[]>> eagerSupplier =
            ProjectionIndexCatalog.rowGroupMaterializer(session, revision, INDEX_NUMBER, rowGroupCount,
                projectedWeight);
        final List<byte[]> eager = eagerSupplier.get();
        assertEquals(rowGroupCount, eager.size());
        assertEquals(0, ChunkedBodyConfig.lazyLoads(), "the eager arm must not engage the windowed route");
        assertEquals(0, ChunkedBodyConfig.chunkMaterializations(), "the eager arm must materialize no windows");
        assertEquals(1, ChunkedBodyConfig.eagerFallbacks(),
            "a lazy handle materialized eagerly must record exactly one eager materialization");

        // ==== windowed arm ====
        ProjectionIndexCatalog.setEagerMaterializeBytesForTesting(1L);
        ChunkedBodyConfig.resetDiag();
        final Supplier<List<byte[]>> windowedSupplier =
            ProjectionIndexCatalog.rowGroupMaterializer(session, revision, INDEX_NUMBER, rowGroupCount,
                projectedWeight);
        final List<byte[]> windowed = windowedSupplier.get();
        assertEquals(rowGroupCount, windowed.size());
        assertTrue(windowed instanceof ProjectionWindowedRowGroupPayloads,
            "over budget, the supplier must return the windowed view; got " + windowed.getClass().getSimpleName());

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
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()), InsertPosition.AS_FIRST_CHILD)
              .commitAfterwards().build().call();
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
            final IllegalStateException decline =
                org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException.class,
                    () -> store.column(nameColumn, fetcher));
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
    final ProjectionWindowedRowGroupPayloads view =
        new ProjectionWindowedRowGroupPayloads(rowGroups, windowSize, 2, (from, toExclusive) -> {
          fetches.incrementAndGet();
          final byte[][] window = new byte[toExclusive - from][];
          for (int i = from; i < toExclusive; i++) {
            window[i - from] = new byte[] { (byte) i, (byte) (i >> 8) };
          }
          return window;
        });
    assertEquals(rowGroups, view.size());
    assertEquals(8, view.windowCount());
    // Two full passes plus a scatter: every access must see its own row group's bytes.
    for (int pass = 0; pass < 2; pass++) {
      for (int i = 0; i < rowGroups; i++) {
        assertArrayEquals(new byte[] { (byte) i, (byte) (i >> 8) }, view.get(i));
      }
    }
    IntStream.of(15, 0, 7, 3, 12, 1, 14, 2).forEach(i -> assertArrayEquals(new byte[] { (byte) i, (byte) (i >> 8) },
        view.get(i)));
    assertTrue(fetches.get() > view.windowCount(),
        "a 2-window resident cap over 8 windows must have re-materialized evicted windows; fetches=" + fetches.get());
    assertTrue(view.residentWindows() <= 3,
        "residency must stay near the cap (cap 2, plus one in-flight); resident=" + view.residentWindows());
  }

  @Test
  void aWindowedViewIsSessionScopedAndNeverPinnedOnTheSharedHandle() {
    // A handle lives in the catalog's PROCESS-WIDE cache, keyed by (resource, def, build revision)
    // and dropped only when the resource is removed — so anything it memoizes outlives every
    // session that touched it. The eager list is inert bytes and is safe to share; a windowed view
    // is not, because it fetches each window through the session its materializer was built from.
    // The view's owner is therefore the SESSION: not the handle (it would outlive the session and
    // fetch through a closed one) and not the caller (N views over one session permit N times the
    // residency each view's window cap was sized for).
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
          new JsonShredder.Builder(wtx, JsonShredder.createStringReader(corpus()), InsertPosition.AS_FIRST_CHILD)
              .commitAfterwards().build().call();
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
        final Supplier<List<byte[]>> callerMaterializer = () -> {
          consulted.incrementAndGet();
          return ProjectionIndexCatalog.rowGroupMaterializer(session, revision, INDEX_NUMBER, rowGroupCount, 1L << 20)
                                       .get();
        };

        firstSessionView = sharedHandle.rowGroupPayloads(callerMaterializer);
        assertTrue(firstSessionView instanceof ProjectionWindowedRowGroupPayloads,
            "over budget the handle must serve the windowed view; got " + firstSessionView.getClass().getSimpleName());
        assertFalse(sharedHandle.payloadsMaterialized(),
            "a windowed view is NOT resident — reporting it as materialized steers later queries off the "
                + "sliced route and pins a session-bound view on the shared handle");

        // The handle re-consults every caller's own materializer (it memoizes nothing session-bound)
        // and the SESSION hands back the one view it already owns.
        final List<byte[]> again = sharedHandle.rowGroupPayloads(callerMaterializer);
        assertEquals(2, consulted.get(), "the handle must not memoize a session-bound view");
        assertSame(firstSessionView, again, "one windowed view per session, not per consult");
        assertFalse(sharedHandle.payloadsMaterialized());

        // Two INDEPENDENT materializers — what two concurrent compile chains over one session build
        // — must resolve to that same single view, or each brings its own resident-window budget.
        final List<byte[]> chainA =
            ProjectionIndexCatalog.rowGroupMaterializer(session, revision, INDEX_NUMBER, rowGroupCount, 1L << 20).get();
        final List<byte[]> chainB =
            ProjectionIndexCatalog.rowGroupMaterializer(session, revision, INDEX_NUMBER, rowGroupCount, 1L << 20).get();
        assertSame(firstSessionView, chainA, "a second chain over the same session must reuse the session's view");
        assertSame(chainA, chainB, "a third chain over the same session must reuse the session's view");
        assertEquals(1, ChunkedBodyConfig.lazyLoads(),
            "the windowed route must be ENGAGED once per session, not once per caller");

        assertEquals(RECORDS, ProjectionIndexByteScan.countRows(firstSessionView));
      }

      // ==== the reported failure: a NEW session on the SAME cached handle ====
      try (JsonResourceSession reopened = db.beginResourceSession(JsonTestHelper.RESOURCE)) {
        final ProjectionIndexRegistry.Handle again = ProjectionIndexCatalog.load(reopened, revision, def);
        assertSame(sharedHandle, again, "the catalog's DATA cache must still serve the same handle instance");
        assertFalse(again.payloadsMaterialized());

        final List<byte[]> reopenedView = again.rowGroupPayloads(
            ProjectionIndexCatalog.rowGroupMaterializer(reopened, revision, INDEX_NUMBER, rowGroupCount, 1L << 20));
        assertNotSame(firstSessionView, reopenedView,
            "the closed session's view must not be handed to its successor");
        assertEquals(RECORDS, ProjectionIndexByteScan.countRows(reopenedView),
            "the new session must serve its windows through ITS OWN live transactions");

        // The eager arm is the contrast: under the budget the leaves ARE inert bytes, so the handle
        // memoizes them and reports resident.
        ProjectionIndexCatalog.setEagerMaterializeBytesForTesting(Long.MAX_VALUE);
        final Supplier<List<byte[]>> eagerMaterializer =
            ProjectionIndexCatalog.rowGroupMaterializer(reopened, revision, INDEX_NUMBER, rowGroupCount, 1L << 20);
        final List<byte[]> eager = again.rowGroupPayloads(eagerMaterializer);
        assertFalse(eager instanceof ProjectionWindowedRowGroupPayloads);
        assertTrue(again.payloadsMaterialized(), "resident leaves must flip the predicate");
        assertSame(eager, again.rowGroupPayloads(eagerMaterializer),
            "resident leaves must be memoized and served without re-consulting the materializer");
      }
    }
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
