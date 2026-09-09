/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fail-closed lifecycle coverage for an intermediate load-time row-group publication fault. */
final class ProjectionBulkLoadFailureTest {

  private static final byte[] STRING_KIND = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
  private static final int INDEX_NUMBER = 0;

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    ProjectionBulkLoad.clearActive();
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      new JsonShredder.Builder(wtx,
          JsonShredder.createStringReader("[{\"value\":1,\"text\":\"one\"},{\"value\":2,\"text\":\"two\"}]"),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
  }

  @AfterEach
  void tearDown() {
    ProjectionBulkLoad.clearActive();
    JsonTestHelper.deleteEverything();
  }

  @Test
  void failedDuplicateArmDoesNotAbortThePreexistingOwner() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      // The fixture has /[] records, so use a genuinely empty root that satisfies the load-time
      // precondition while still exercising the real controller/listener binding path.
      final IndexDef indexDef = IndexDefs.createProjectionIdxDef(parse("/missing/[]", PathParser.Type.JSON),
          List.of(parse("/missing/[]/value", PathParser.Type.JSON)), List.of(Type.LON), INDEX_NUMBER,
          IndexDef.DbType.JSON);
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final JsonIndexController controller =
          (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
      final ProjectionBulkLoad first = controller.createProjectionIndexAtLoadStart(indexDef, wtx, -1L);

      try {
        assertThrows(IllegalStateException.class,
            () -> controller.createProjectionIndexAtLoadStart(indexDef, wtx, -1L));
        assertSame(first, ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER),
            "a failed putIfAbsent must not resolve and abort the winning ACTIVE owner");
        assertFalse(first.isFinished(), "the preexisting load was poisoned by somebody else's failed arm");
        assertNotNull(controller.getIndexes().getIndexDef(indexDef.getID(), indexDef.getType()),
            "duplicate-arm rollback removed the preexisting catalogue definition");
      } finally {
        first.abort();
      }
      assertNull(ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER));
    }
  }

  @Test
  void arrayChildCountRejectsCompletelyMissedRecordAttribution() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final IndexDef indexDef = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
          List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), INDEX_NUMBER, IndexDef.DbType.JSON);
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final ProjectionBulkLoad load =
          ProjectionBulkLoad.begin(indexDef, resourceKey, wtx, wtx.getPathSummary(), wtx.getStorageEngineWriter(), 2L);
      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild());
      load.noteArrayRootInstance(wtx.getNodeKey(), wtx);
      load.drain(wtx.getStorageEngineWriter(), wtx.getPathSummary(), wtx);

      wtx.insertObjectAsFirstChild();
      load.drain(wtx.getStorageEngineWriter(), wtx.getPathSummary(), wtx);

      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> load.finish(wtx.getStorageEngineWriter(), wtx.getPathSummary(), wtx, wtx.getRevisionNumber()));
      assertTrue(failure.getMessage().contains("emitted 0 rows for 3 records"));
      assertTrue(load.isFinished());
      assertNull(ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));
      wtx.rollback();
    }
  }

  @Test
  void neverDictionaryModeBypassesTheLeadingSample() throws ReflectiveOperationException {
    assertSampleBypassed("never", "/[]/text", Type.STR,
        "NEVER mode must stream row groups instead of retaining a useless decision sample");
  }

  @Test
  void autoModeBypassesTheLeadingSampleWhenNoColumnCanUseIt() throws ReflectiveOperationException {
    assertSampleBypassed("auto", "/[]/value", Type.LON,
        "a numeric-only AUTO projection must stream instead of retaining a useless sample");
  }

  private static void assertSampleBypassed(final String mode, final String fieldPath, final Type fieldType,
      final String message) throws ReflectiveOperationException {
    final String priorMode = System.getProperty("sirix.projection.globalDict");
    System.setProperty("sirix.projection.globalDict", mode);
    try {
      final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
      try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
          final var wtx = session.beginNodeTrx()) {
        final IndexDef indexDef = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
            List.of(parse(fieldPath, PathParser.Type.JSON)), List.of(fieldType), INDEX_NUMBER, IndexDef.DbType.JSON);
        final String resourceKey = session.getResourceConfig().getResource().toString();
        final ProjectionBulkLoad load =
            ProjectionBulkLoad.begin(indexDef, resourceKey, wtx.getPathSummary(), wtx.getStorageEngineWriter());
        try {
          final Field builderField = ProjectionBulkLoad.class.getDeclaredField("builder");
          builderField.setAccessible(true);
          final ProjectionIndexBuilder builder = (ProjectionIndexBuilder) builderField.get(load);
          final Field sampleField = ProjectionIndexBuilder.class.getDeclaredField("sample");
          sampleField.setAccessible(true);
          assertNull(sampleField.get(builder), message);
        } finally {
          load.abort();
        }
      }
    } finally {
      if (priorMode == null) {
        System.clearProperty("sirix.projection.globalDict");
      } else {
        System.setProperty("sirix.projection.globalDict", priorMode);
      }
    }
  }

  @Test
  void intermediateStorageFaultPoisonsLoadAndLeavesPartialLeafBehindStaleTombstone() throws Exception {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final IndexDef indexDef = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
          List.of(parse("/[]/text", PathParser.Type.JSON)), List.of(Type.STR), INDEX_NUMBER, IndexDef.DbType.JSON);
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final RuntimeException injected = new RuntimeException("injected row-group publication failure");
      final AtomicInteger publicationAttempts = new AtomicInteger();
      final ProjectionBulkLoad load = ProjectionBulkLoad.begin(indexDef, resourceKey, wtx.getPathSummary(),
          wtx.getStorageEngineWriter(), -1L, (storage, rowGroupId, encoded) -> {
            publicationAttempts.incrementAndGet();
            // Leave a genuinely written row group behind, then fail before bloom/fence metadata can
            // be published. Slot 0 must keep this physical partial state unreachable to readers.
            storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
            throw injected;
          });

      // Prime the exact reachable state immediately before the 16-leaf dictionary sample drains:
      // 15 full pages are held in the sample and the current 16th page is full. The next real record
      // closes that page during an ordinary intermediate drain and enters the borrowed callback.
      primeBeforeSampleDrain(load);
      wtx.moveToDocumentRoot();
      assertTrue(wtx.moveToFirstChild(), "document must contain the top-level array");
      assertTrue(wtx.moveToFirstChild(), "array must contain the first record");
      final long firstRecordKey = wtx.getNodeKey();
      assertTrue(wtx.moveToRightSibling(), "array must contain the second record");
      final long secondRecordKey = wtx.getNodeKey();
      load.observeRecord(firstRecordKey);
      load.observeRecord(secondRecordKey);

      final RuntimeException thrown = assertThrows(RuntimeException.class,
          () -> load.drain(wtx.getStorageEngineWriter(), wtx.getPathSummary(), wtx));
      assertSame(injected, thrown, "cleanup must preserve the original storage failure");
      assertTrue(load.isFinished(), "a partially published builder must be irreversibly poisoned");
      assertNull(ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER), "poisoned load leaked in ACTIVE");
      assertEquals(1, publicationAttempts.get(), "the failing page was published more than once");

      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      assertNotNull(storage.getRowGroupFromColumnSegmentSlots(1),
          "fixture must leave a real partial row group, not fail before storage");
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(storage.getBlob(0));
      assertNotNull(metadata);
      assertTrue(metadata.isStale(), "partial row group became reachable through live metadata");

      // Neither an explicit retry nor repeated caller cleanup may resume or duplicate publication.
      load.drain(wtx.getStorageEngineWriter(), wtx.getPathSummary(), wtx);
      load.abort();
      load.abort();
      assertEquals(1, publicationAttempts.get());
      assertThrows(IllegalStateException.class, () -> load.observeRecord(secondRecordKey + 1));
    }
  }

  @Test
  void finalCommitPublicationFaultMakesTheOwningTransactionRollbackOnly() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH2.getFile());
    try (final var session = database.beginResourceSession(JsonTestHelper.RESOURCE);
        final var wtx = session.beginNodeTrx()) {
      final IndexDef indexDef = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
          List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), INDEX_NUMBER, IndexDef.DbType.JSON);
      final String resourceKey = session.getResourceConfig().getResource().toString();
      final RuntimeException injected = new RuntimeException("injected final row-group publication failure");
      final AtomicInteger publicationAttempts = new AtomicInteger();
      final ProjectionBulkLoad load = ProjectionBulkLoad.begin(indexDef, resourceKey, wtx, wtx.getPathSummary(),
          wtx.getStorageEngineWriter(), -1L, (storage, rowGroupId, encoded) -> {
            publicationAttempts.incrementAndGet();
            storage.putRowGroupAsColumnSegmentSlots(rowGroupId, encoded);
            throw injected;
          });
      final JsonIndexController controller =
          (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
      controller.getIndexes().add(indexDef);
      controller.createIndexListeners(Set.of(indexDef), wtx);

      new JsonShredder.Builder(wtx, JsonShredder.createStringReader("[{\"value\":7}]"),
          InsertPosition.AS_FIRST_CHILD).build().call();

      final RuntimeException thrown = assertThrows(RuntimeException.class, wtx::commit);
      assertSame(injected, thrown, "the final commit must preserve the publication failure");
      assertEquals(1, publicationAttempts.get());
      assertTrue(load.isFinished(), "a finalization fault must poison the streaming builder");
      assertNull(ProjectionBulkLoad.active(resourceKey, INDEX_NUMBER, wtx));

      final ProjectionIndexHOTStorage storage =
          ProjectionIndexHOTStorage.forBulkBuild(wtx.getStorageEngineWriter(), INDEX_NUMBER);
      assertNotNull(storage.getRowGroupFromColumnSegmentSlots(1), "the fixture must fail after a real row-group write");
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(storage.getBlob(0));
      assertNotNull(metadata);
      assertTrue(metadata.isStale(), "a partial finalization became reachable through live metadata");

      assertEquals(0, session.getMostRecentRevisionNumber(),
          "a failed pre-publication commit advanced the durable revision");
      try (final var committed = session.beginNodeReadOnlyTrx(0)) {
        assertTrue(committed.moveToDocumentRoot());
        assertFalse(committed.moveToFirstChild(), "the uncommitted JSON tree leaked into the preceding revision");
        assertNull(ProjectionIndexHOTStorage.readBlob(committed.getStorageEngineReader(), INDEX_NUMBER, 0L),
            "the uncommitted projection valve leaked into the preceding revision");
      }

      assertThrows(SirixUsageException.class, wtx::commit,
          "retrying a failed atomic publication without rollback must be rejected");
      assertThrows(SirixUsageException.class, wtx::insertArrayAsFirstChild,
          "mutating a transaction after partial index publication must be rejected");

      wtx.rollback();
      final JsonIndexController rolledBackController =
          (JsonIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
      assertNull(rolledBackController.getIndexes().getIndexDef(INDEX_NUMBER, IndexType.PROJECTION),
          "rollback retained an uncommitted projection definition");
      assertFalse(rolledBackController.hasProjectionIndex(),
          "rollback retained the uncommitted projection capability flag");
      assertTrue(wtx.moveToDocumentRoot());
      wtx.insertArrayAsFirstChild();
      wtx.rollback();
      assertFalse(ProjectionBulkLoad.anyActive());
    }
  }

  private static void primeBeforeSampleDrain(final ProjectionBulkLoad load) throws ReflectiveOperationException {
    final Field builderField = ProjectionBulkLoad.class.getDeclaredField("builder");
    builderField.setAccessible(true);
    final ProjectionIndexBuilder builder = (ProjectionIndexBuilder) builderField.get(load);

    final List<ProjectionIndexRowGroupPage> sample = new ArrayList<>(15);
    for (int pageIndex = 0; pageIndex < 15; pageIndex++) {
      sample.add(fullStringPage((long) pageIndex * ProjectionIndexRowGroupPage.MAX_ROWS));
    }
    final Field sampleField = ProjectionIndexBuilder.class.getDeclaredField("sample");
    sampleField.setAccessible(true);
    sampleField.set(builder, sample);

    final Field currentLeafField = ProjectionIndexBuilder.class.getDeclaredField("currentLeaf");
    currentLeafField.setAccessible(true);
    currentLeafField.set(builder, fullStringPage(15L * ProjectionIndexRowGroupPage.MAX_ROWS));

    final Field rowsEmittedField = ProjectionIndexBuilder.class.getDeclaredField("rowsEmitted");
    rowsEmittedField.setAccessible(true);
    rowsEmittedField.setLong(builder, 16L * ProjectionIndexRowGroupPage.MAX_ROWS);
  }

  private static ProjectionIndexRowGroupPage fullStringPage(final long keyBase) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(STRING_KIND);
    final long[] values = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    for (int row = 0; row < ProjectionIndexRowGroupPage.MAX_ROWS; row++) {
      strings[0] = "sample-" + row;
      assertTrue(page.appendRow(keyBase + row + 1, values, bools, strings));
    }
    return page;
  }
}
