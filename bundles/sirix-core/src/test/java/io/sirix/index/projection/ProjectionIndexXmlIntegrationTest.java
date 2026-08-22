/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.XmlTestHelper;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.xml.XmlIndexController;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ProjectionIndexXmlIntegrationTest {

  private static final String VERSIONED_RESOURCE = "projection-versioning";

  @BeforeEach
  void setUp() {
    XmlTestHelper.deleteEverything();
  }

  @AfterEach
  void tearDown() {
    XmlTestHelper.closeEverything();
  }

  @Test
  void buildsRowsFromNestedElementsAndAttributes() {
    final var database = XmlTestHelper.getDatabaseWithDeweyIDsEnabled(XmlTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      final String document = "<records><record id=\"1\"><name>A</name><score>7</score></record>"
          + "<record id=\"2\"><name>B</name><score>11</score></record></records>";
      new XmlShredder.Builder(wtx, XmlShredder.createStringReader(document), InsertPosition.AS_FIRST_CHILD)
          .commitAfterwards()
          .build()
          .call();
    }

    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx();
         final var pathSummary = session.openPathSummary()) {
      final IndexDef definition = IndexDefs.createProjectionIdxDef(
          Path.parse("/records/record", PathParser.Type.XML),
          List.of(Path.parse("/records/record/@id", PathParser.Type.XML),
              Path.parse("/records/record/name", PathParser.Type.XML),
              Path.parse("/records/record/score", PathParser.Type.XML)),
          List.of(Type.INT, Type.STR, Type.INT), 0, IndexDef.DbType.XML);
      final List<byte[]> leaves = new ArrayList<>();
      final ProjectionIndexBuilder builder = new ProjectionIndexBuilder(definition, pathSummary, leaves::add);

      builder.build(rtx);

      assertEquals(2L, builder.rowsEmitted());
      assertEquals(1L, ProjectionIndexScan.conjunctiveCount(leaves,
          new ProjectionIndexScan.ColumnPredicate[] {
              ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.EQ, 2L),
              ProjectionIndexScan.ColumnPredicate.numeric(2, ProjectionIndexScan.Op.EQ, 11L)
          }));
    }
  }

  @Test
  void repeatedScalarAndWildcardMatchesAreUnrepresentableInsteadOfLastWins() {
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        Path.parse("/records/record", PathParser.Type.XML),
        List.of(Path.parse("/records/record/score", PathParser.Type.XML),
            Path.parse("/records/record/*", PathParser.Type.XML)),
        List.of(Type.INT, Type.INT), 0, IndexDef.DbType.XML);

    final ProjectionIndexRowGroupPage page = buildSingleLeaf(
        "<records><record><score>7</score><score>11</score></record></records>", definition);

    assertEquals(1, page.getRowCount());
    assertTrue(page.columnUnrepresentable(0), "a repeated exact scalar must force generic fallback");
    assertTrue(page.columnUnrepresentable(1), "a wildcard matching multiple scalars must force generic fallback");
  }

  @Test
  void projectionCreationRejectsDeweyDisabledXmlResourceWithoutResidue() {
    final String disabled = "projection-xml-dewey-disabled";
    final Database<XmlResourceSession> database =
        XmlTestHelper.getDatabaseWithDeweyIDsEnabled(XmlTestHelper.PATHS.PATH1.getFile());
    assertTrue(database.createResource(ResourceConfiguration.newBuilder(disabled)
        .useDeweyIDs(false)
        .build()));
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        Path.parse("/records/record", PathParser.Type.XML),
        List.of(Path.parse("/records/record/score", PathParser.Type.XML)),
        List.of(Type.INT), 0, IndexDef.DbType.XML);

    try (final var session = database.beginResourceSession(disabled);
         final var wtx = session.beginNodeTrx()) {
      final XmlIndexController controller =
          (XmlIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> controller.createProjectionIndexAtLoadStart(definition, wtx, -1L));
      assertTrue(failure.getMessage().contains("Dewey IDs"));
      assertNull(ProjectionBulkLoad.active(
          session.getResourceConfig().getResource().toString(), definition.getID()));
      assertNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()));
      assertFalse(controller.hasProjectionIndex());
      new XmlShredder.Builder(wtx,
          XmlShredder.createStringReader("<records><record><score>1</score></record></records>"),
          InsertPosition.AS_FIRST_CHILD).build().call();
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(disabled);
         final var wtx = session.beginNodeTrx()) {
      final XmlIndexController controller =
          (XmlIndexController) session.getWtxIndexController(wtx.getRevisionNumber());
      final IllegalStateException failure = assertThrows(IllegalStateException.class,
          () -> controller.createIndexes(Set.of(definition), wtx));
      assertTrue(failure.getMessage().contains("Dewey IDs"));
      assertNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()));
      assertFalse(controller.hasProjectionIndex());
      wtx.commit();
    }

    try (final var session = database.beginResourceSession(disabled);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      final var controller = session.getRtxIndexController(rtx.getRevisionNumber());
      assertNull(controller.getIndexes().getIndexDef(definition.getID(), definition.getType()));
      assertFalse(controller.hasProjectionIndex());
    }
  }

  @Test
  void xmlNumericSourceTypesNeverClaimFalsePureDoubleEvidence() {
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        Path.parse("/records/record", PathParser.Type.XML),
        List.of(Path.parse("/records/record/decimal", PathParser.Type.XML),
            Path.parse("/records/record/float", PathParser.Type.XML),
            Path.parse("/records/record/double", PathParser.Type.XML)),
        List.of(Type.DEC, Type.FLO, Type.DBL), 0, IndexDef.DbType.XML);

    final ProjectionIndexRowGroupPage page = buildSingleLeaf(
        "<records><record><decimal>9007199254740993</decimal><float>16777217</float>"
            + "<double>1.5</double></record></records>", definition);

    assertTrue(page.columnUnrepresentable(0), "a lossy decimal-to-double conversion must decline value serving");
    assertFalse(page.columnPureDoubleSource(0));
    assertFalse(page.columnPureDoubleSource(1));
    assertTrue(page.columnPureDoubleSource(2));
  }

  @Test
  void indexCreationPreservesPreexistingNonMonotoneDocumentOrder() {
    final Database<XmlResourceSession> database =
        XmlTestHelper.getDatabaseWithDeweyIDsEnabled(XmlTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      new XmlShredder.Builder(wtx,
          XmlShredder.createStringReader("<records><record><score>1</score></record>"
              + "<record><score>2</score></record></records>"), InsertPosition.AS_FIRST_CHILD)
          .commitAfterwards()
          .build()
          .call();
    }
    final long firstRecordKey;
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      firstRecordKey = elementKey(rtx, "record", 0);
    }
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(firstRecordKey));
      wtx.insertElementAsLeftSibling(new QNm("record"))
          .insertElementAsFirstChild(new QNm("score"))
          .insertTextAsFirstChild("0");
      wtx.commit();
    }

    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        Path.parse("/records/record", PathParser.Type.XML),
        List.of(Path.parse("/records/record/score", PathParser.Type.XML)),
        List.of(Type.INT), 0, IndexDef.DbType.XML);
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
      wtx.commit();
    }
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    assertPersistedOrder(database, definition, 0L, 1L, 2L);
  }

  @Test
  void recordRootMovesAndArbitraryInsertionsPreserveDocumentOrder() {
    final Database<XmlResourceSession> database =
        XmlTestHelper.getDatabaseWithDeweyIDsEnabled(XmlTestHelper.PATHS.PATH1.getFile());
    final String document = "<root><records><record><score>1</score><left/><right/></record>"
        + "<record><score>2</score></record></records>"
        + "<archive><record><score>9</score></record></archive></root>";
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      new XmlShredder.Builder(wtx, XmlShredder.createStringReader(document), InsertPosition.AS_FIRST_CHILD)
          .commitAfterwards()
          .build()
          .call();
    }

    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        Path.parse("/root/records/record", PathParser.Type.XML),
        List.of(Path.parse("/root/records/record/score", PathParser.Type.XML)),
        List.of(Type.INT), 0, IndexDef.DbType.XML);

    final long recordsKey;
    final long archiveKey;
    final long firstRecordKey;
    final long secondRecordKey;
    final long archivedRecordKey;
    final long leftKey;
    final long rightKey;
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      recordsKey = elementKey(rtx, "records", 0);
      firstRecordKey = elementKey(rtx, "record", 0);
      secondRecordKey = elementKey(rtx, "record", 1);
      archivedRecordKey = elementKey(rtx, "record", 2);
      archiveKey = elementKey(rtx, "archive", 0);
      leftKey = elementKey(rtx, "left", 0);
      rightKey = elementKey(rtx, "right", 0);
    }

    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
      wtx.commit();
    }
    final int baselineRevision;
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      baselineRevision = session.getMostRecentRevisionNumber();
    }
    assertPersistedOrder(database, definition, baselineRevision, 1L, 2L);

    // The record root stays in place, so an internal move is ordinary one-record maintenance.
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(rightKey));
      wtx.moveSubtreeToFirstChild(leftKey);
      wtx.commit();
    }
    assertElementParent(database, leftKey, rightKey);
    assertPersistedOrder(database, definition, 1L, 2L);

    // Existing root leaves the projection: remove its row from the source physical leaf.
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(archiveKey));
      wtx.moveSubtreeToFirstChild(firstRecordKey);
      wtx.commit();
    }
    assertElementParent(database, firstRecordKey, archiveKey);
    assertPersistedOrder(database, definition, 2L);

    // Outside→inside entry is inserted at its explicit first-child document position.
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(recordsKey));
      wtx.moveSubtreeToFirstChild(archivedRecordKey);
      wtx.commit();
    }
    assertElementParent(database, archivedRecordKey, recordsKey);
    assertPersistedOrder(database, definition, 9L, 2L);

    // A same-root structural move retains identity and splices the row to the new first position.
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(recordsKey));
      wtx.moveSubtreeToFirstChild(secondRecordKey);
      wtx.commit();
    }
    assertPersistedOrder(database, definition, 2L, 9L);

    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(secondRecordKey));
      wtx.insertElementAsLeftSibling(new QNm("record"))
          .insertElementAsFirstChild(new QNm("score"))
          .insertTextAsFirstChild("0");
      wtx.commit();
    }
    assertPersistedOrder(database, definition, 0L, 2L, 9L);

    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(secondRecordKey));
      wtx.insertElementAsRightSibling(new QNm("record"))
          .insertElementAsFirstChild(new QNm("score"))
          .insertTextAsFirstChild("5");
      wtx.commit();
    }
    assertPersistedOrder(database, definition, 0L, 2L, 5L, 9L);

    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(archivedRecordKey));
      wtx.insertElementAsRightSibling(new QNm("record"))
          .insertElementAsFirstChild(new QNm("score"))
          .insertTextAsFirstChild("10");
      wtx.commit();
    }
    assertElementChildCount(database, recordsKey, 5L);
    assertPersistedOrder(database, definition, 0L, 2L, 5L, 9L, 10L);
    assertPersistedOrder(database, definition, baselineRevision, 1L, 2L);

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    assertPersistedOrder(database, definition, 0L, 2L, 5L, 9L, 10L);
  }

  @Test
  void adjacentLeafDeleteUpdateInsertAndMoveRemainOrderedAfterColdReopen() {
    final int recordCount = ProjectionIndexRowGroupPage.MAX_ROWS * 2 + 1;
    final StringBuilder document = new StringBuilder(recordCount * 48);
    document.append("<records>");
    for (int value = 0; value < recordCount; value++) {
      document.append("<record><score>").append(value).append("</score></record>");
    }
    document.append("</records>");

    final Database<XmlResourceSession> database =
        XmlTestHelper.getDatabaseWithDeweyIDsEnabled(XmlTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      new XmlShredder.Builder(wtx, XmlShredder.createStringReader(document.toString()),
          InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
    }

    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        Path.parse("/records/record", PathParser.Type.XML),
        List.of(Path.parse("/records/record/score", PathParser.Type.XML)),
        List.of(Type.INT), 0, IndexDef.DbType.XML);
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
      wtx.commit();
    }

    final long recordsKey;
    final long firstSecondLeafRecordKey;
    final long secondSecondLeafRecordKey;
    final long secondSecondLeafScoreTextKey;
    final long finalFirstLeafRecordKey;
    final long tailRecordKey;
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      recordsKey = elementKey(rtx, "records", 0);
      finalFirstLeafRecordKey = elementKey(rtx, "record", ProjectionIndexRowGroupPage.MAX_ROWS - 1);
      firstSecondLeafRecordKey = elementKey(rtx, "record", ProjectionIndexRowGroupPage.MAX_ROWS);
      secondSecondLeafRecordKey = elementKey(rtx, "record", ProjectionIndexRowGroupPage.MAX_ROWS + 1);
      tailRecordKey = elementKey(rtx, "record", recordCount - 1);
      assertTrue(rtx.moveTo(secondSecondLeafRecordKey));
      assertTrue(rtx.moveToFirstChild());
      assertTrue(rtx.moveToFirstChild());
      secondSecondLeafScoreTextKey = rtx.getNodeKey();
    }

    ProjectionIndexChangeListener.resetMaintenanceTelemetry();
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      assertTrue(wtx.moveTo(firstSecondLeafRecordKey));
      wtx.remove();
      assertTrue(wtx.moveTo(secondSecondLeafScoreTextKey));
      wtx.setValue("9000");
      assertTrue(wtx.moveTo(finalFirstLeafRecordKey));
      wtx.insertElementAsRightSibling(new QNm("record"))
          .insertElementAsFirstChild(new QNm("score"))
          .insertTextAsFirstChild("7777");
      assertTrue(wtx.moveTo(recordsKey));
      wtx.moveSubtreeToFirstChild(tailRecordKey);
      wtx.commit();
    }

    final long[] expected = new long[recordCount];
    int offset = 0;
    expected[offset++] = recordCount - 1L;
    for (int value = 0; value < ProjectionIndexRowGroupPage.MAX_ROWS; value++) {
      expected[offset++] = value;
    }
    expected[offset++] = 7777L;
    expected[offset++] = 9000L;
    for (int value = ProjectionIndexRowGroupPage.MAX_ROWS + 2; value < recordCount - 1; value++) {
      expected[offset++] = value;
    }
    assertEquals(expected.length, offset);
    assertPersistedOrder(database, definition, expected);
    assertEquals(0L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds());

    database.close();
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (final Database<XmlResourceSession> reopened =
             Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile())) {
      assertPersistedOrder(reopened, definition, expected);
    }
  }

  @ParameterizedTest(name = "{0} preserves sibling projection maintenance across history")
  @EnumSource(VersioningType.class)
  void siblingMaintenanceStaysIncrementalForEveryVersioningType(
      final VersioningType versioningType) {
    createVersionedResource(versioningType);
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        Path.parse("/root/records/record", PathParser.Type.XML),
        List.of(Path.parse("/root/records/record/score", PathParser.Type.XML)),
        List.of(Type.INT), 0, IndexDef.DbType.XML);

    final int baselineRevision;
    final int headRevision;
    final int middleRevision;
    final int tailRevision;
    final int updateRevision;
    final int deleteRevision;
    final int moveRevision;
    final long recordsKey;
    final long firstRecordKey;
    final long secondRecordKey;
    final long firstScoreTextKey;
    final long headRecordKey;
    final long middleRecordKey;
    final long tailRecordKey;

    try (final Database<XmlResourceSession> database =
             Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile())) {
      try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        assertEquals(versioningType, session.getResourceConfig().versioningType,
            "the parameterized resource must persist the requested versioning strategy");
        new XmlShredder.Builder(wtx,
            XmlShredder.createStringReader("<root><records><record><score>1</score></record>"
                + "<record><score>2</score></record></records></root>"),
            InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      }
      try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
        wtx.commit();
      }
      baselineRevision = mostRecentRevision(database, VERSIONED_RESOURCE);

      try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
           final var rtx = session.beginNodeReadOnlyTrx(baselineRevision)) {
        recordsKey = elementKey(rtx, "records", 0);
        firstRecordKey = elementKey(rtx, "record", 0);
        secondRecordKey = elementKey(rtx, "record", 1);
        assertTrue(rtx.moveTo(firstRecordKey));
        assertTrue(rtx.moveToFirstChild());
        assertEquals("score", rtx.getName().getLocalName());
        assertTrue(rtx.moveToFirstChild());
        assertEquals(NodeKind.TEXT, rtx.getKind());
        firstScoreTextKey = rtx.getNodeKey();
      }
      assertVersionedProjectionState(database, versioningType, definition, baselineRevision, 1L, 2L);
      ProjectionIndexChangeListener.resetMaintenanceTelemetry();

      // A high stable key inserted at the head is represented as a sparse order exception.
      try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(firstRecordKey));
        wtx.insertElementAsLeftSibling(new QNm("record"));
        headRecordKey = wtx.getNodeKey();
        wtx.insertElementAsFirstChild(new QNm("score")).insertTextAsFirstChild("0");
        wtx.commit();
      }
      headRevision = mostRecentRevision(database, VERSIONED_RESOURCE);
      assertVersionedProjectionState(database, versioningType, definition, headRevision, 0L, 1L, 2L);
      assertExceptionLocator(database, definition, headRevision, headRecordKey, true);

      // The middle insertion is a separate commit and must not renumber or rewrite the suffix.
      try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(firstRecordKey));
        wtx.insertElementAsRightSibling(new QNm("record"));
        middleRecordKey = wtx.getNodeKey();
        wtx.insertElementAsFirstChild(new QNm("score")).insertTextAsFirstChild("5");
        wtx.commit();
      }
      middleRevision = mostRecentRevision(database, VERSIONED_RESOURCE);
      assertVersionedProjectionState(database, versioningType, definition, middleRevision, 0L, 1L, 5L, 2L);
      assertExceptionLocator(database, definition, middleRevision, middleRecordKey, true);

      // A genuine tail append extends the normal numeric backbone and needs no exact locator.
      try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(secondRecordKey));
        wtx.insertElementAsRightSibling(new QNm("record"));
        tailRecordKey = wtx.getNodeKey();
        wtx.insertElementAsFirstChild(new QNm("score")).insertTextAsFirstChild("10");
        wtx.commit();
      }
      tailRevision = mostRecentRevision(database, VERSIONED_RESOURCE);
      assertVersionedProjectionState(database, versioningType, definition, tailRevision, 0L, 1L, 5L, 2L, 10L);
      assertExceptionLocator(database, definition, tailRevision, tailRecordKey, false);

      // A projected TEXT value update must patch the owning column without changing row identity/order.
      try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(firstScoreTextKey));
        wtx.setValue("11");
        wtx.commit();
      }
      updateRevision = mostRecentRevision(database, VERSIONED_RESOURCE);
      assertVersionedProjectionState(database, versioningType, definition, updateRevision, 0L, 11L, 5L, 2L, 10L);
      assertExceptionLocator(database, definition, updateRevision, firstRecordKey, false);

      // Removing the exceptional head row must tombstone its exact locator.
      try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(headRecordKey));
        wtx.remove();
        wtx.commit();
      }
      deleteRevision = mostRecentRevision(database, VERSIONED_RESOURCE);
      assertVersionedProjectionState(database, versioningType, definition, deleteRevision, 11L, 5L, 2L, 10L);
      assertExceptionLocator(database, definition, deleteRevision, headRecordKey, false);

      // Reorder an existing normal row to the head. Identity stays stable; its route becomes exact.
      try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
           final var wtx = session.beginNodeTrx()) {
        assertTrue(wtx.moveTo(recordsKey));
        wtx.moveSubtreeToFirstChild(secondRecordKey);
        wtx.commit();
      }
      moveRevision = mostRecentRevision(database, VERSIONED_RESOURCE);
      assertVersionedProjectionState(database, versioningType, definition, moveRevision, 2L, 11L, 5L, 10L);
      assertExceptionLocator(database, definition, moveRevision, secondRecordKey, true);
      assertEquals(0L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds(),
          versioningType + " ordinary sibling maintenance must never rebuild the projection");
    }

    assertEquals(baselineRevision + 1, headRevision, "head insertion must own one committed revision");
    assertEquals(headRevision + 1, middleRevision, "middle insertion must own one committed revision");
    assertEquals(middleRevision + 1, tailRevision, "tail insertion must own one committed revision");
    assertEquals(tailRevision + 1, updateRevision, "value update must own one committed revision");
    assertEquals(updateRevision + 1, deleteRevision, "deletion must own one committed revision");
    assertEquals(deleteRevision + 1, moveRevision, "reorder must own one committed revision");

    assertColdVersionedProjectionState(versioningType, definition, baselineRevision, 1L, 2L);
    assertColdVersionedProjectionState(versioningType, definition, headRevision, 0L, 1L, 2L);
    assertColdVersionedProjectionState(versioningType, definition, middleRevision, 0L, 1L, 5L, 2L);
    assertColdVersionedProjectionState(versioningType, definition, tailRevision, 0L, 1L, 5L, 2L, 10L);
    assertColdVersionedProjectionState(versioningType, definition, updateRevision, 0L, 11L, 5L, 2L, 10L);
    assertColdVersionedProjectionState(versioningType, definition, deleteRevision, 11L, 5L, 2L, 10L);
    assertColdVersionedProjectionState(versioningType, definition, moveRevision, 2L, 11L, 5L, 10L);

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (final Database<XmlResourceSession> reopened =
             Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile())) {
      assertCurrentLookupRoutes(reopened, definition, headRecordKey,
          new long[] {secondRecordKey, firstRecordKey, middleRecordKey, tailRecordKey},
          new boolean[] {true, false, true, false});
    }
    assertEquals(0L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds(),
        versioningType + " cold historical reads must not trigger a rebuild");
  }

  private static void createVersionedResource(final VersioningType versioningType) {
    assertTrue(Databases.createXmlDatabase(
        new DatabaseConfiguration(XmlTestHelper.PATHS.PATH1.getFile())));
    try (final Database<XmlResourceSession> database =
             Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile())) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(VERSIONED_RESOURCE)
          .storageType(StorageType.FILE_CHANNEL)
          .storeDiffs(false)
          .hashKind(HashType.NONE)
          .buildPathSummary(true)
          .buildPathStatistics(false)
          .useDeweyIDs(true)
          .versioningApproach(versioningType)
          .maxNumberOfRevisionsToRestore(3)
          .build()));
    }
  }

  private static int mostRecentRevision(final Database<XmlResourceSession> database,
      final String resource) {
    try (final var session = database.beginResourceSession(resource)) {
      return session.getMostRecentRevisionNumber();
    }
  }

  private static void assertColdVersionedProjectionState(final VersioningType versioningType,
      final IndexDef definition, final int revision, final long... expectedValues) {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (final Database<XmlResourceSession> reopened =
             Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile())) {
      assertVersionedProjectionState(reopened, versioningType, definition, revision, expectedValues);
    }
  }

  private static void assertVersionedProjectionState(final Database<XmlResourceSession> database,
      final VersioningType versioningType, final IndexDef definition, final int revision,
      final long... expectedValues) {
    ProjectionIndexCatalog.clearCache();
    try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx(revision)) {
      assertEquals(versioningType, session.getResourceConfig().versioningType,
          "the reopened resource must retain its requested versioning strategy");
      final long[] documentRecordKeys = assertVersionedTreeOrder(rtx, expectedValues);
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.load(session, revision, definition);
      assertNotNull(handle, "the versioned XML projection must remain catalog-servable");
      final int[] physicalOrder = ProjectionIndexFences.readPhysicalOrder(rtx.getStorageEngineReader(),
          definition.getID(), handle.rowGroupCount());
      final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(
          session, revision, definition.getID(), handle.rowGroupCount()));
      assertEquals(physicalOrder.length, leaves.size(),
          "the document-order fence chain must cover every live row group");

      final long[] actualValues = new long[expectedValues.length];
      final long[] actualRecordKeys = new long[expectedValues.length];
      int offset = 0;
      long priorNormalKey = Long.MIN_VALUE;
      for (int leafIndex = 0; leafIndex < leaves.size(); leafIndex++) {
        final int physicalSlot = physicalOrder[leafIndex];
        final ProjectionIndexRowGroupPage leaf =
            ProjectionIndexRowGroupPage.deserialize(leaves.get(leafIndex));
        long firstNormalKey = Long.MAX_VALUE;
        long lastNormalKey = Long.MIN_VALUE;
        for (int row = 0; row < leaf.getRowCount(); row++) {
          assertTrue(offset < actualValues.length, "the projection contains more rows than expected");
          final long recordKey = leaf.recordKeys()[row];
          actualValues[offset] = leaf.numericColumn(0)[row];
          actualRecordKeys[offset++] = recordKey;
          final int locatedSlot = ProjectionRecordLocator.read(rtx.getStorageEngineReader(),
              definition.getID(), recordKey);
          if (leaf.orderExceptionAt(row)) {
            assertEquals(physicalSlot, locatedSlot,
                "an order-exception row must have an exact locator to its physical leaf");
          } else {
            assertEquals(0, locatedSlot, "a normal-backbone row must not consume a sparse locator");
            assertTrue(recordKey > priorNormalKey,
                "normal projection keys must remain globally increasing across document-order leaves");
            priorNormalKey = recordKey;
            if (firstNormalKey == Long.MAX_VALUE) {
              firstNormalKey = recordKey;
            }
            lastNormalKey = recordKey;
          }
        }
        assertEquals(firstNormalKey, leaf.firstRecordKey(),
            "the KEYS first fence must describe only this leaf's normal backbone");
        assertEquals(lastNormalKey, leaf.lastRecordKey(),
            "the KEYS last fence must describe only this leaf's normal backbone");
      }
      assertEquals(expectedValues.length, offset, "the projection contains fewer rows than expected");
      assertArrayEquals(expectedValues, actualValues,
          "the versioned projection must match exact XML document order");
      assertArrayEquals(documentRecordKeys, actualRecordKeys,
          "the persisted KEYS lane must match the XML record identities exactly once and in order");
    }
  }

  private static long[] assertVersionedTreeOrder(final XmlNodeReadOnlyTrx rtx,
      final long... expectedValues) {
    final long[] recordKeys = new long[expectedValues.length];
    final long recordsKey = elementKey(rtx, "records", 0);
    assertTrue(rtx.moveTo(recordsKey));
    assertEquals(expectedValues.length, rtx.getChildCount(),
        "the versioned XML tree must have the expected record cardinality");
    assertTrue(expectedValues.length > 0, "this scenario always retains at least one record");
    assertTrue(rtx.moveToFirstChild());
    for (int index = 0; index < expectedValues.length; index++) {
      final long recordKey = rtx.getNodeKey();
      recordKeys[index] = recordKey;
      assertEquals("record", rtx.getName().getLocalName());
      assertTrue(rtx.moveToFirstChild());
      assertEquals("score", rtx.getName().getLocalName());
      assertTrue(rtx.moveToFirstChild());
      assertEquals(expectedValues[index], Long.parseLong(rtx.getValue()));
      assertTrue(rtx.moveTo(recordKey));
      if (index + 1 < expectedValues.length) {
        assertTrue(rtx.moveToRightSibling(), "missing XML record at document position " + (index + 1));
      } else {
        assertFalse(rtx.moveToRightSibling(), "the XML record set contains an unexpected suffix");
      }
    }
    return recordKeys;
  }

  private static void assertExceptionLocator(final Database<XmlResourceSession> database,
      final IndexDef definition, final int revision, final long recordKey,
      final boolean expectedException) {
    try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx(revision)) {
      final int slot = ProjectionRecordLocator.read(rtx.getStorageEngineReader(),
          definition.getID(), recordKey);
      assertEquals(expectedException, slot != 0,
          "unexpected sparse-locator classification for record " + recordKey);
    }
  }

  private static void assertCurrentLookupRoutes(final Database<XmlResourceSession> database,
      final IndexDef definition, final long deletedRecordKey, final long[] recordKeys,
      final boolean[] expectedExceptions) {
    assertEquals(recordKeys.length, expectedExceptions.length);
    try (final var session = database.beginResourceSession(VERSIONED_RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), definition.getID());
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(storage.getBlob(0L));
      assertNotNull(metadata, "the current projection metadata must remain readable after a cold reopen");
      final ProjectionIndexFences.Accessor fences =
          ProjectionIndexFences.open(storage, metadata.rowGroupCount());
      final ProjectionRecordLocator.Accessor locator = ProjectionRecordLocator.open(storage);
      final ProjectionPersistedRecordLookup lookup =
          new ProjectionPersistedRecordLookup(storage, fences, locator);
      assertEquals(0, locator.find(deletedRecordKey), "the deleted exception locator must be tombstoned");
      assertEquals(ProjectionPersistedRecordLookup.ABSENT, lookup.find(deletedRecordKey),
          "a deleted record must be absent from both exact and normal lookup routes");
      for (int index = 0; index < recordKeys.length; index++) {
        final long packed = lookup.find(recordKeys[index]);
        assertTrue(packed != ProjectionPersistedRecordLookup.ABSENT,
            "the current lookup must find record " + recordKeys[index]);
        assertEquals(expectedExceptions[index], ProjectionPersistedRecordLookup.orderException(packed),
            "unexpected lookup route for record " + recordKeys[index]);
        fences.validateDocumentLinks(ProjectionPersistedRecordLookup.slot(packed));
      }
      wtx.rollback();
    }
  }

  private static void assertElementParent(final Database<XmlResourceSession> database,
      final long elementKey, final long expectedParentKey) {
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(elementKey));
      assertEquals(expectedParentKey, rtx.getParentKey());
    }
  }

  private static void assertElementChildCount(final Database<XmlResourceSession> database,
      final long elementKey, final long expectedChildCount) {
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx()) {
      assertTrue(rtx.moveTo(elementKey));
      assertEquals(expectedChildCount, rtx.getChildCount());
    }
  }

  private static void assertPersistedOrder(final Database<XmlResourceSession> database,
      final IndexDef definition, final long... expectedValues) {
    final int revision;
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      revision = session.getMostRecentRevisionNumber();
    }
    assertPersistedOrder(database, definition, revision, expectedValues);
  }

  private static void assertPersistedOrder(final Database<XmlResourceSession> database,
      final IndexDef definition, final int revision, final long... expectedValues) {
    ProjectionIndexCatalog.clearCache();
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.load(session, revision, definition);
      assertNotNull(handle, "the committed XML projection must remain catalog-servable");
      final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(
          session, revision, definition.getID(), handle.rowGroupCount()));
      final long[] actualValues = new long[expectedValues.length];
      int offset = 0;
      for (final byte[] leafBytes : leaves) {
        final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(leafBytes);
        final int rowCount = leaf.getRowCount();
        assertTrue(offset + rowCount <= actualValues.length,
            "the projection contains more rows than expected");
        System.arraycopy(leaf.numericColumn(0), 0, actualValues, offset, rowCount);
        offset += rowCount;
      }
      assertEquals(expectedValues.length, offset, "the projection contains fewer rows than expected");
      assertArrayEquals(expectedValues, actualValues, "projection rows must remain in document order");
    }
  }

  private static long elementKey(final XmlNodeReadOnlyTrx rtx, final String localName,
      final int occurrence) {
    final long restoreKey = rtx.getNodeKey();
    try {
      rtx.moveToDocumentRoot();
      final Axis descendants = new DescendantAxis(rtx);
      int seen = 0;
      while (descendants.hasNext()) {
        descendants.nextLong();
        if (rtx.getKind() == NodeKind.ELEMENT && localName.equals(rtx.getName().getLocalName())) {
          if (seen++ == occurrence) {
            return rtx.getNodeKey();
          }
        }
      }
      throw new AssertionError("missing XML element " + localName + " occurrence " + occurrence);
    } finally {
      rtx.moveTo(restoreKey);
    }
  }

  private ProjectionIndexRowGroupPage buildSingleLeaf(final String document, final IndexDef definition) {
    final var database = XmlTestHelper.getDatabaseWithDeweyIDsEnabled(XmlTestHelper.PATHS.PATH1.getFile());
    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = session.beginNodeTrx()) {
      new XmlShredder.Builder(wtx, XmlShredder.createStringReader(document), InsertPosition.AS_FIRST_CHILD)
          .commitAfterwards()
          .build()
          .call();
    }

    try (final var session = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var rtx = session.beginNodeReadOnlyTrx();
         final var pathSummary = session.openPathSummary()) {
      final List<byte[]> leaves = new ArrayList<>();
      final ProjectionIndexBuilder builder = new ProjectionIndexBuilder(definition, pathSummary, leaves::add);
      builder.build(rtx);
      assertEquals(1, leaves.size());
      return ProjectionIndexRowGroupPage.deserialize(leaves.get(0));
    }
  }
}
