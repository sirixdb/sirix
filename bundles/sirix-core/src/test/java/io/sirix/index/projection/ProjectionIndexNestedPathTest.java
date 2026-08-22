/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.JsonTestHelper;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.json.FusedStringCursor;
import io.sirix.access.trx.node.json.ForwardingJsonNodeReadOnlyTrx;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.objectvalue.ArrayValue;
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
import io.sirix.access.trx.node.json.objectvalue.ObjectValue;
import io.sirix.api.Axis;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Path coverage for projection indexes beyond the flat {@code /[]} bench
 * shape:
 * <ul>
 * <li><b>nested roots</b> — the record set lives under object steps
 * ({@code /east/records/[]});</li>
 * <li><b>nested field columns</b> — a column path descends below the record
 * root ({@code .../[]/nested/city});</li>
 * <li><b>multi-PCR roots</b> — one descendant path pattern
 * ({@code //records/[]}) resolving to record sets under SIBLING subtrees;
 * historically this threw ("only single-path roots are supported"), now
 * every matching PCR is a record root;</li>
 * <li><b>multi-PCR field paths</b> — the same field pattern resolving under
 * each root; historically only the first PCR was (silently) matched, which
 * made every record under the second root report the field as missing.</li>
 * </ul>
 */
final class ProjectionIndexNestedPathTest {

  private static final String JSON = """
      {
        "east": {"records": [
          {"age": 30, "name": "a"},
          {"age": 45, "name": "b"}
        ]},
        "west": {"records": [
          {"age": 50, "name": "c"},
          {"age": 20, "nested": {"city": "NYC"}}
        ]},
        "other": [1, 2]
      }""";

  @BeforeEach
  void setUp() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(JSON),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
  }

  @AfterEach
  void tearDown() {
    JsonTestHelper.deleteEverything();
  }

  private static List<byte[]> buildLeaves(final String rootPath, final String agePath,
      final String namePath, final String cityPath) {
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {
      final IndexDef def = IndexDefs.createProjectionIdxDef(
          parse(rootPath, PathParser.Type.JSON),
          List.of(parse(agePath, PathParser.Type.JSON),
                  parse(namePath, PathParser.Type.JSON),
                  parse(cityPath, PathParser.Type.JSON)),
          List.of(Type.LON, Type.STR, Type.STR),
          0,
          IndexDef.DbType.JSON);
      final List<byte[]> leaves = new ArrayList<>();
      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(rtx);
      return leaves;
    }
  }

  private static String stringAt(final ProjectionIndexRowGroupPage leaf, final int column, final int row) {
    final byte[][] dict = leaf.stringDictionary(column);
    final int id = leaf.stringDictIdColumn(column)[row];
    return new String(dict[id], StandardCharsets.UTF_8);
  }

  private static boolean presentAt(final ProjectionIndexRowGroupPage leaf, final int column, final int row) {
    return (leaf.presenceColumnBits(column)[row >>> 6] & (1L << (row & 63))) != 0;
  }

  // ==================== nested single-PCR root ====================

  @Test
  void selfNestedRootPcrsFailFast() {
    // A record set nested inside another matched record's subtree must be
    // rejected loudly — the builder cannot emit correct rows for it (the
    // inner record's fields would overwrite the outer row's columns).
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(
          "{\"records\":[{\"age\":30,\"records\":[{\"age\":99}]}]}"),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {
      final IndexDef def = IndexDefs.createProjectionIdxDef(
          parse("//records/[]", PathParser.Type.JSON),
          List.of(parse("//records/[]/age", PathParser.Type.JSON)),
          List.of(Type.LON), 0, IndexDef.DbType.JSON);
      assertThrows(IllegalStateException.class,
          () -> new ProjectionIndexBuilder(def, pathSummary, payload -> { }));
    }
  }

  @Test
  void liveDescendantProjectionRejectsNewSelfNestedRootWithoutPublishing() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        parse("//records/[]", PathParser.Type.JSON),
        List.of(parse("//records/[]/age", PathParser.Type.JSON)),
        List.of(Type.LON), 0, IndexDef.DbType.JSON);
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      manager.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
      wtx.commit();
    }

    final int baselineRevision;
    final long outerRecordKey;
    final List<byte[]> baselinePayloads;
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx()) {
      baselineRevision = manager.getMostRecentRevisionNumber();
      final Axis descendants = new DescendantAxis(rtx);
      long firstRecordsArray = -1L;
      while (descendants.hasNext()) {
        descendants.nextLong();
        if (isNamedArray(rtx, "records")) {
          firstRecordsArray = rtx.getNodeKey();
          break;
        }
      }
      assertTrue(firstRecordsArray >= 0);
      assertTrue(rtx.moveTo(firstRecordsArray));
      assertTrue(rtx.moveToFirstChild());
      outerRecordKey = rtx.getNodeKey();
      baselinePayloads = projectionPayloads(manager, definition, baselineRevision);
      assertEquals(List.of(30L, 45L, 50L, 20L), numericValues(baselinePayloads));
    }

    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      assertTrue(wtx.moveTo(outerRecordKey));
      final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> {
        wtx.insertObjectRecordAsFirstChild("records", new ArrayValue());
        wtx.insertObjectAsFirstChild()
            .insertObjectRecordAsFirstChild("age", new NumberValue(99));
        wtx.commit();
      });
      assertTrue(hasFailureMessage(failure, "overlapping nested root matches"));
      wtx.rollback();
      assertEquals(baselineRevision, manager.getMostRecentRevisionNumber());
    }

    database.close();
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (final var reopened = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var manager = reopened.beginResourceSession(JsonTestHelper.RESOURCE)) {
      assertEquals(baselineRevision, manager.getMostRecentRevisionNumber());
      final List<byte[]> reopenedPayloads = projectionPayloads(manager, definition, baselineRevision);
      assertEquals(baselinePayloads.size(), reopenedPayloads.size());
      for (int index = 0; index < baselinePayloads.size(); index++) {
        assertArrayEquals(baselinePayloads.get(index), reopenedPayloads.get(index));
      }
    }
  }

  @Test
  void structuralMoveStreamsMoreThanOneBoundedRecordBatchAndColdReopens() {
    JsonTestHelper.deleteEverything();
    final int movedRecords = 300;
    final StringBuilder json = new StringBuilder(16 << 10);
    json.append("[{\"records\":[");
    for (int value = 0; value < movedRecords; value++) {
      if (value > 0) {
        json.append(',');
      }
      json.append("{\"age\":").append(value).append('}');
    }
    json.append("]},{\"records\":[{\"age\":1000}]}]");

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(json.toString()),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        parse("//records/[]", PathParser.Type.JSON),
        List.of(parse("//records/[]/age", PathParser.Type.JSON)),
        List.of(Type.LON), 0, IndexDef.DbType.JSON);
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      manager.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
      wtx.commit();
    }

    final long leftKey;
    final long rightKey;
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx()) {
      rtx.moveToDocumentRoot();
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.moveToFirstChild());
      leftKey = rtx.getNodeKey();
      assertTrue(rtx.moveToRightSibling());
      rightKey = rtx.getNodeKey();
    }
    ProjectionIndexChangeListener.resetMaintenanceTelemetry();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      assertTrue(wtx.moveTo(rightKey));
      wtx.moveSubtreeToRightSibling(leftKey);
      wtx.commit();
    }
    assertEquals(0L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds());

    database.close();
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (final var reopened = Databases.openJsonDatabase(JsonTestHelper.PATHS.PATH1.getFile());
         final var manager = reopened.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final List<Long> values = numericValues(projectionPayloads(
          manager, definition, manager.getMostRecentRevisionNumber()));
      assertEquals(movedRecords + 1, values.size());
      assertEquals(1000L, values.getFirst());
      for (int value = 0; value < movedRecords; value++) {
        assertEquals((long) value, values.get(value + 1));
      }
    }
  }


  @Test
  void nestedRootAndNestedFieldColumn() {
    final List<byte[]> leaves = buildLeaves(
        "/east/records/[]",
        "/east/records/[]/age",
        "/east/records/[]/name",
        "/east/records/[]/nested/city");
    assertEquals(1, leaves.size());
    final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.get(0));
    assertEquals(2, leaf.getRowCount());
    assertEquals(30, leaf.numericColumn(0)[0]);
    assertEquals(45, leaf.numericColumn(0)[1]);
    assertEquals("a", stringAt(leaf, 1, 0));
    assertEquals("b", stringAt(leaf, 1, 1));
    // No east record carries nested/city — column all-missing.
    assertFalse(presentAt(leaf, 2, 0));
    assertFalse(presentAt(leaf, 2, 1));
  }

  @Test
  void repeatedJsonDescendantScalarIsUnrepresentableInsteadOfLastWins() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(
          "{\"records\":[{\"left\":{\"value\":\"a\"},\"right\":{\"value\":\"b\"}}]}"),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {
      final IndexDef def = IndexDefs.createProjectionIdxDef(
          parse("/records/[]", PathParser.Type.JSON),
          List.of(parse("//value", PathParser.Type.JSON)),
          List.of(Type.STR), 0, IndexDef.DbType.JSON);
      final List<byte[]> leaves = new ArrayList<>();
      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(rtx);

      assertEquals(1, leaves.size());
      final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.getFirst());
      assertEquals(1, leaf.getRowCount());
      assertTrue(presentAt(leaf, 0, 0));
      assertTrue(leaf.columnUnrepresentable(0),
          "a scalar descendant path matching two values must force generic fallback");
    }
  }

  @Test
  void repeatedJsonDescendantStringSetsAreUnioned() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(
          "{\"records\":[{\"left\":{\"tags\":[\"a\"]},\"right\":{\"tags\":[\"b\"]}}]}"),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {
      final IndexDef def = IndexDefs.createProjectionIdxDef(
          parse("/records/[]", PathParser.Type.JSON),
          List.of(parse("//tags/[]", PathParser.Type.JSON)),
          List.of(Type.STR), 0, IndexDef.DbType.JSON);
      final List<byte[]> leaves = new ArrayList<>();
      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(rtx);

      final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.getFirst());
      assertEquals(2, leaf.stringSetCountColumn(0)[0]);
      assertFalse(leaf.columnUnrepresentable(0));
      assertEquals(1L, ProjectionIndexByteScan.conjunctiveCount(leaves,
          new ProjectionIndexScan.ColumnPredicate[] {
              ProjectionIndexScan.ColumnPredicate.stringEq(0, "a".getBytes(StandardCharsets.UTF_8))
          }));
      assertEquals(1L, ProjectionIndexByteScan.conjunctiveCount(leaves,
          new ProjectionIndexScan.ColumnPredicate[] {
              ProjectionIndexScan.ColumnPredicate.stringEq(0, "b".getBytes(StandardCharsets.UTF_8))
          }));
    }
  }

  @Test
  void oversizedStringSetFailsClosedWithinItsBoundedLane() {
    JsonTestHelper.deleteEverything();
    final StringBuilder json = new StringBuilder(160 << 10);
    json.append("{\"records\":[{\"tags\":[");
    for (int index = 0; index <= ProjectionIndexRowExtractor.MAX_STRING_SET_ELEMENTS_PER_ROW; index++) {
      if (index != 0) {
        json.append(',');
      }
      json.append("\"x\"");
    }
    json.append("]}]}");
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(json.toString()),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var rtx = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {
      final IndexDef def = IndexDefs.createProjectionIdxDef(
          parse("/records/[]", PathParser.Type.JSON),
          List.of(parse("/records/[]/tags/[]", PathParser.Type.JSON)),
          List.of(Type.STR), 0, IndexDef.DbType.JSON);
      final List<byte[]> leaves = new ArrayList<>();
      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(rtx);
      final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.getFirst());
      assertTrue(leaf.columnUnrepresentable(0));
      assertEquals(0, leaf.stringSetCountColumn(0)[0]);
    }
  }

  @Test
  void indexCreationPreservesPreexistingNonMonotoneRecordOrder() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(
          "{\"records\":[{\"age\":1},{\"age\":2}]}"), InsertPosition.AS_FIRST_CHILD)
          .commitAfterwards()
          .build()
          .call();
    }
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      assertTrue(wtx.moveToDocumentRoot());
      assertTrue(wtx.moveToFirstChild());
      assertTrue(wtx.moveToFirstChild());
      wtx.insertObjectAsFirstChild()
          .insertObjectRecordAsFirstChild("age", new NumberValue(0));
      wtx.commit();
    }

    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        parse("/records/[]", PathParser.Type.JSON),
        List.of(parse("/records/[]/age", PathParser.Type.JSON)),
        List.of(Type.LON), 0, IndexDef.DbType.JSON);
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      manager.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
      wtx.commit();
    }

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final int revision = manager.getMostRecentRevisionNumber();
      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.load(manager, revision, definition);
      assertNotNull(handle, "the cold-reopened projection route must remain catalog-servable");
      final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(
          manager, revision, definition.getID(), handle.rowGroupCount()));
      final long[] actual = new long[3];
      int offset = 0;
      for (final byte[] leafBytes : leaves) {
        final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leafBytes);
        final int rowCount = leaf.getRowCount();
        System.arraycopy(leaf.numericColumn(0), 0, actual, offset, rowCount);
        offset += rowCount;
      }
      assertEquals(actual.length, offset);
      assertArrayEquals(new long[] { 0L, 1L, 2L }, actual);
    }
  }

  @Test
  void firstRecordInsertionUsesPersistedOrderAcrossLargeSiblingRun() {
    JsonTestHelper.deleteEverything();
    final StringBuilder json = new StringBuilder(128 << 10);
    json.append("{\"left\":{\"records\":[{\"age\":1}]}");
    for (int value = 0; value < 20_000; value++) {
      json.append(",\"gap").append(value).append("\":").append(value);
    }
    json.append(",\"right\":{\"records\":[]}}");

    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      new JsonShredder.Builder(wtx, JsonShredder.createStringReader(json.toString()),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }

    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        parse("//records/[]", PathParser.Type.JSON),
        List.of(parse("//records/[]/age", PathParser.Type.JSON)),
        List.of(Type.LON), 0, IndexDef.DbType.JSON);
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      manager.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
      wtx.commit();
    }

    ProjectionIndexChangeListener.resetMaintenanceTelemetry();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      final Axis descendants = new DescendantAxis(wtx);
      int recordsArrays = 0;
      long rightRecordsKey = -1L;
      while (descendants.hasNext()) {
        descendants.nextLong();
        if (isNamedArray(wtx, "records") && ++recordsArrays == 2) {
          rightRecordsKey = wtx.getNodeKey();
          break;
        }
      }
      assertTrue(rightRecordsKey >= 0, "the fixture must contain two record-set arrays");
      assertTrue(wtx.moveTo(rightRecordsKey));
      wtx.insertObjectAsFirstChild()
          .insertObjectRecordAsFirstChild("age", new NumberValue(2));
      wtx.commit();
    }
    assertEquals(0,
        ProjectionIndexChangeListener.lastMaintenanceLocality().documentNeighborNodesRead(),
        "persisted order routing must not walk unrelated document siblings");
    assertTrue(ProjectionIndexChangeListener.lastMaintenanceLocality().keySegmentsRead() < 64,
        "persisted order routing must not inspect the 20,000 unrelated siblings");

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final int revision = manager.getMostRecentRevisionNumber();
      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.load(manager, revision, definition);
      assertNotNull(handle);
      final List<Long> values = new ArrayList<>();
      for (final byte[] payload : handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(
          manager, revision, definition.getID(), handle.rowGroupCount()))) {
        final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(payload);
        for (int row = 0; row < leaf.getRowCount(); row++) {
          values.add(leaf.numericColumn(0)[row]);
        }
      }
      assertEquals(List.of(1L, 2L), values);
    }
  }

  @Test
  void loadTimeSiblingArraysSurviveAsyncEpochsAndColdReopen() {
    JsonTestHelper.deleteEverything();
    final var database = JsonTestHelper.getDatabase(JsonTestHelper.PATHS.PATH1.getFile());
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        parse("//records/[]", PathParser.Type.JSON),
        List.of(parse("//records/[]/age", PathParser.Type.JSON)),
        List.of(Type.LON), 0, IndexDef.DbType.JSON);
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      final JsonIndexController controller =
          (JsonIndexController) manager.getWtxIndexController(wtx.getRevisionNumber());
      final ProjectionBulkLoad load =
          controller.createProjectionIndexAtLoadStart(definition, wtx, 4L);

      final long rootObjectKey = wtx.insertObjectAsFirstChild().getNodeKey();
      final long leftArrayKey = wtx.insertObjectRecordAsFirstChild("left", new ObjectValue())
          .insertObjectRecordAsFirstChild("records", new ArrayValue())
          .getNodeKey();
      assertTrue(wtx.moveTo(leftArrayKey));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"age\":1}"),
          JsonNodeTrx.Commit.NO);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"age\":2}"),
          JsonNodeTrx.Commit.NO);
      controller.notifyBeforePageFlush();
      assertEquals(1L, load.rowsEmitted());
      wtx.getStorageEngineWriter().asyncFlush();
      wtx.getStorageEngineWriter().awaitPendingAsyncFlush();

      assertTrue(wtx.moveTo(rootObjectKey));
      final long rightArrayKey = wtx.insertObjectRecordAsLastChild("right", new ObjectValue())
          .insertObjectRecordAsFirstChild("records", new ArrayValue())
          .getNodeKey();
      assertTrue(wtx.moveTo(rightArrayKey));
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"age\":3}"),
          JsonNodeTrx.Commit.NO);
      wtx.insertSubtreeAsRightSibling(JsonShredder.createStringReader("{\"age\":4}"),
          JsonNodeTrx.Commit.NO);
      controller.notifyBeforePageFlush();
      assertEquals(3L, load.rowsEmitted());
      wtx.getStorageEngineWriter().asyncFlush();
      wtx.getStorageEngineWriter().awaitPendingAsyncFlush();
      wtx.commit();
      assertTrue(load.isFinished());
    }

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE)) {
      final int revision = manager.getMostRecentRevisionNumber();
      final ProjectionIndexRegistry.Handle handle =
          ProjectionIndexCatalog.load(manager, revision, definition);
      assertNotNull(handle);
      final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(
          manager, revision, definition.getID(), handle.rowGroupCount()));
      final long[] values = new long[4];
      int offset = 0;
      for (final byte[] leafBytes : leaves) {
        final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leafBytes);
        System.arraycopy(leaf.numericColumn(0), 0, values, offset, leaf.getRowCount());
        offset += leaf.getRowCount();
      }
      assertEquals(values.length, offset);
      assertArrayEquals(new long[] {1L, 2L, 3L, 4L}, values);
    }
  }

  private static boolean isNamedArray(final JsonNodeReadOnlyTrx rtx, final String name) {
    if (rtx.getKind() == NodeKind.OBJECT_NAMED_ARRAY) {
      return name.equals(rtx.getName().getLocalName());
    }
    if (rtx.getKind() != NodeKind.ARRAY) {
      return false;
    }
    final long nodeKey = rtx.getNodeKey();
    final boolean named = rtx.moveToParent() && rtx.getKind().playsObjectKeyRole()
        && name.equals(rtx.getName().getLocalName());
    rtx.moveTo(nodeKey);
    return named;
  }

  private static List<byte[]> projectionPayloads(final JsonResourceSession manager,
      final IndexDef definition, final int revision) {
    ProjectionIndexCatalog.clearCache();
    final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.load(manager, revision, definition);
    assertNotNull(handle);
    return handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(
        manager, revision, definition.getID(), handle.rowGroupCount()));
  }

  private static List<Long> numericValues(final List<byte[]> payloads) {
    final List<Long> values = new ArrayList<>();
    for (final byte[] payload : payloads) {
      final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(payload);
      for (int row = 0; row < page.getRowCount(); row++) {
        values.add(page.numericColumn(0)[row]);
      }
    }
    return values;
  }

  private static boolean hasFailureMessage(final Throwable failure, final String expected) {
    Throwable current = failure;
    while (current != null) {
      if (current.getMessage() != null && current.getMessage().contains(expected)) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }

  @Test
  void scalarExtractionUsesValueBytesWithoutMaterializingStrings() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var delegate = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {
      final IndexDef def = IndexDefs.createProjectionIdxDef(
          parse("/east/records/[]", PathParser.Type.JSON),
          List.of(parse("/east/records/[]/name", PathParser.Type.JSON)),
          List.of(Type.STR),
          0,
          IndexDef.DbType.JSON);
      final int[] valueByteReads = new int[1];
      final JsonNodeReadOnlyTrx byteOnlyCursor = new ForwardingJsonNodeReadOnlyTrx() {
        @Override
        public JsonNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
          return delegate;
        }

        @Override
        public SirixDeweyID getDeweyID() {
          return delegate.getDeweyID();
        }

        @Override
        public String getValue() {
          throw new AssertionError("scalar projection extraction must not materialize a String");
        }

        @Override
        public int readFusedStringUtf8(final byte[] valueOut) {
          return FusedStringCursor.UNAVAILABLE;
        }

        @Override
        public byte[] getValueBytes() {
          valueByteReads[0]++;
          return delegate.getValueBytes();
        }
      };
      final List<byte[]> leaves = new ArrayList<>();

      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(byteOnlyCursor);

      assertEquals(2, valueByteReads[0], "each projected fused string should be read exactly once as UTF-8");
      assertEquals(1, leaves.size());
      final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.get(0));
      assertEquals("a", stringAt(leaf, 0, 0));
      assertEquals("b", stringAt(leaf, 0, 1));
    }
  }

  @Test
  void scalarExtractionUsesCallerOwnedFusedUtf8Scratch() {
    final var database = JsonTestHelper.getDatabaseWithDeweyIdsEnabled(JsonTestHelper.PATHS.PATH1.getFile());
    try (final var manager = database.beginResourceSession(JsonTestHelper.RESOURCE);
         final var delegate = manager.beginNodeReadOnlyTrx();
         final var pathSummary = manager.openPathSummary()) {
      final IndexDef def = IndexDefs.createProjectionIdxDef(
          parse("/east/records/[]", PathParser.Type.JSON),
          List.of(parse("/east/records/[]/name", PathParser.Type.JSON)),
          List.of(Type.STR),
          0,
          IndexDef.DbType.JSON);
      final int[] fusedReads = new int[1];
      final JsonNodeReadOnlyTrx cursor = new ForwardingJsonNodeReadOnlyTrx() {
        @Override
        public JsonNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
          return delegate;
        }

        @Override
        public SirixDeweyID getDeweyID() {
          return delegate.getDeweyID();
        }

        @Override
        public int readFusedStringUtf8(final byte[] valueOut) {
          fusedReads[0]++;
          return ForwardingJsonNodeReadOnlyTrx.super.readFusedStringUtf8(valueOut);
        }

        @Override
        public byte[] getValueBytes() {
          throw new AssertionError("production fused-string extraction must not materialize a byte array");
        }
      };
      final List<byte[]> leaves = new ArrayList<>();

      new ProjectionIndexBuilder(def, pathSummary, leaves::add).build(cursor);

      // The first one-byte value asks for capacity and retries; the second reuses the grown buffer.
      assertEquals(3, fusedReads[0]);
      assertEquals(1, leaves.size());
      final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.getFirst());
      assertEquals("a", stringAt(leaf, 0, 0));
      assertEquals("b", stringAt(leaf, 0, 1));
    }
  }

  // ==================== multi-PCR root + multi-PCR fields ====================

  @Test
  void descendantPatternRootSpansSiblingSubtrees() {
    final List<byte[]> leaves = buildLeaves(
        "//records/[]",
        "//records/[]/age",
        "//records/[]/name",
        "//records/[]/nested/city");
    assertEquals(1, leaves.size());
    final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.get(0));
    // Both sibling record sets contribute rows, in document order.
    assertEquals(4, leaf.getRowCount());
    assertEquals(30, leaf.numericColumn(0)[0]);
    assertEquals(45, leaf.numericColumn(0)[1]);
    assertEquals(50, leaf.numericColumn(0)[2]);
    assertEquals(20, leaf.numericColumn(0)[3]);
    // Field PCRs resolve under BOTH roots — the west rows must not report
    // age/name as missing (the historical first-PCR-only behavior).
    assertTrue(presentAt(leaf, 0, 2), "west row must match the age field's second PCR");
    assertEquals("c", stringAt(leaf, 1, 2));
    assertFalse(presentAt(leaf, 1, 3), "row without name stays missing");
    // Nested column below the record root, present on exactly one row.
    assertFalse(presentAt(leaf, 2, 0));
    assertTrue(presentAt(leaf, 2, 3));
    assertEquals("NYC", stringAt(leaf, 2, 3));

    // The scan stack agrees: age > 25 matches rows 0,1,2.
    final long matches = ProjectionIndexByteScan.conjunctiveCount(leaves,
        new ProjectionIndexScan.ColumnPredicate[] {
            ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 25L)
        });
    assertEquals(3, matches);
  }

  @Test
  void unresolvableFieldPathYieldsAllMissingColumn() {
    final List<byte[]> leaves = buildLeaves(
        "/west/records/[]",
        "/west/records/[]/age",
        "/west/records/[]/no_such_field",
        "/west/records/[]/nested/city");
    final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leaves.get(0));
    assertEquals(2, leaf.getRowCount());
    assertFalse(presentAt(leaf, 1, 0));
    assertFalse(presentAt(leaf, 1, 1));
    assertTrue(presentAt(leaf, 2, 1));
    assertEquals("NYC", stringAt(leaf, 2, 1));
  }
}
