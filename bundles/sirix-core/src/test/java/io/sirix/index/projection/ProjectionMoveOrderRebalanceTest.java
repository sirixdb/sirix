/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.Axis;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.node.NodeKind;
import io.sirix.service.InsertPosition;
import io.sirix.service.json.shredder.JacksonJsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The move-to-first-child twin of {@link ProjectionPrependOrderRebalanceTest}. Moving a record to
 * the front of its array repeatedly is the same ORDPATH worst case as inserting one there
 * repeatedly: each move re-derives the moved record's label from the current head with
 * {@code newBetween(null, head)}, adding a division every few moves. Unless the bounded rebalance
 * is armed on the MOVE path too — not just the insert path — the label grows without limit and
 * eventually aborts an otherwise valid transaction.
 *
 * <p>
 * The records are shredded once and then only ever moved, so this exercises sibling reordering with
 * no insertions at all, on a DEFAULT resource with {@code storeDeweyIDs} off.
 */
final class ProjectionMoveOrderRebalanceTest {

  private static final String RESOURCE = "move-order-rebalance";
  private static final int RECORDS = 24;
  private static final int MOVES = 240;

  /** Same budget as the insert twin: a bounded label lands in single-digit bytes. */
  private static final int MAX_PERSISTED_ORDER_LABEL_BYTES = 24;

  @TempDir
  Path temporaryDirectory;

  @AfterEach
  void clearCaches() {
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  @Test
  void repeatedMoveToFirstChildCommitsAndKeepsOrderLabelsBounded() throws Exception {
    final Path databasePath = temporaryDirectory.resolve("move");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final IndexDef definition = IndexDefs.createProjectionIdxDef(parse("/records/[]", PathParser.Type.JSON),
        List.of(parse("/records/[]/score", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON);

    final StringBuilder json = new StringBuilder("{\"records\":[");
    for (int record = 0; record < RECORDS; record++) {
      json.append(record == 0
          ? ""
          : ",").append("{\"score\":").append(record).append('}');
    }
    json.append("]}");

    final int finalRevision;
    final Deque<Long> expectedOrder = new ArrayDeque<>();
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .storeDiffs(false)
                                                              .buildPathSummary(true)
                                                              .build()));

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          var wtx = session.beginNodeTrx();
          var parser = JacksonJsonShredder.createStringParser(json.toString())) {
        new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards().build().call();
      }

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE); var wtx = session.beginNodeTrx()) {
        session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
        wtx.commit();
      }

      final long recordsKey;
      final List<Long> recordKeys = new ArrayList<>(RECORDS);
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
          JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        recordsKey = namedArrayKey(rtx);
        assertTrue(rtx.moveTo(recordsKey));
        assertTrue(rtx.moveToFirstChild());
        do {
          recordKeys.add(rtx.getNodeKey());
        } while (rtx.moveToRightSibling());
        assertEquals(RECORDS, recordKeys.size());
      }
      for (long score = 0L; score < RECORDS; score++) {
        expectedOrder.addLast(score);
      }

      ProjectionIndexChangeListener.resetMaintenanceTelemetry();
      for (int move = 0; move < MOVES; move++) {
        final int position = move % RECORDS;
        final long moved = recordKeys.get(position);
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE); var wtx = session.beginNodeTrx()) {
          assertTrue(wtx.moveTo(recordsKey));
          wtx.moveSubtreeToFirstChild(moved);
          wtx.commit();
        }
        expectedOrder.remove((long) position);
        expectedOrder.addFirst((long) position);
      }

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        finalRevision = session.getMostRecentRevisionNumber();
      }
      assertProjection(database, definition, finalRevision, new ArrayList<>(expectedOrder));
      assertPersistedLabelsMatchTheDirectory(database, definition, recordKeys);
      assertEquals(0L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds(),
          "the move workload must stay incremental — no complete projection rebuild");
    }

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath)) {
      assertProjection(reopened, definition, finalRevision, new ArrayList<>(expectedOrder));
    }
  }

  /**
   * A bounded rebalance re-spreads sibling records in the ORDER DIRECTORY; the persisted row-group
   * lane only follows if those records are carried into the apply pass. Asserting that every
   * persisted order label equals the label the directory now holds for that record is what catches a
   * re-spread whose rows were stranded — a commit that succeeds while the two disagree leaves later
   * inserts resolving their position against stale labels, i.e. wrong document order with no error.
   */
  private static void assertPersistedLabelsMatchTheDirectory(final Database<JsonResourceSession> database,
      final IndexDef definition, final List<Long> recordKeys) {
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE); var wtx = session.beginNodeTrx()) {
      final ProjectionIndexHOTStorage storage =
          new ProjectionIndexHOTStorage(wtx.getStorageEngineWriter(), definition.getID());
      final ProjectionStructuralOrderDirectory.Accessor directory = ProjectionStructuralOrderDirectory.open(storage);
      final ProjectionIndexMetadata metadata = ProjectionIndexMetadata.parse(storage.getBlob(0));
      assertNotNull(metadata, "the projection must still carry live metadata");
      final ProjectionPersistedRecordLookup lookup = new ProjectionPersistedRecordLookup(storage,
          ProjectionIndexFences.open(storage, metadata.rowGroupCount()), ProjectionRecordLocator.open(storage));

      for (final long recordKey : recordKeys) {
        final long location = lookup.find(recordKey);
        assertTrue(location != ProjectionPersistedRecordLookup.ABSENT,
            "record " + recordKey + " must still have a persisted row");
        final byte[] persisted = lookup.keys(ProjectionPersistedRecordLookup.slot(location))
                                       .view()
                                       .copyOrderLabelAt(ProjectionPersistedRecordLookup.row(location));
        final byte[] current = directory.fullLabel(recordKey,
            nodeKey -> (io.sirix.node.interfaces.immutable.ImmutableNode) wtx.getStorageEngineWriter()
                                                                             .getRecord(nodeKey,
                                                                                 io.sirix.index.IndexType.DOCUMENT, -1),
            ProjectionStructuralOrderDirectory.RelabelSink.SEALED).toBytes();
        assertEquals(0,
            ProjectionIndexRowGroupPage.compareOrderLabels(persisted, 0, persisted.length, current, 0, current.length),
            "record " + recordKey + " kept a stale persisted order label after a rebalance");
      }
    }
  }

  /** Reads the projection's own persisted row-group pages — the serialized state under test here. */
  private static void assertProjection(final Database<JsonResourceSession> database, final IndexDef definition,
      final int revision, final List<Long> expected) {
    ProjectionIndexCatalog.clearCache();
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.load(session, revision, definition);
      assertNotNull(handle, "the projection index must still be usable after a move-heavy history");
      final List<byte[]> leaves = handle.rowGroupPayloads(
          ProjectionIndexCatalog.rowGroupMaterializer(session, revision, definition.getID(), handle.rowGroupCount()));

      final List<Long> values = new ArrayList<>(expected.size());
      byte[] previousLabel = null;
      int widestLabel = 0;
      for (final byte[] payload : leaves) {
        final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(payload);
        for (int row = 0; row < page.getRowCount(); row++) {
          values.add(page.numericColumn(0)[row]);
          final byte[] label = page.copyOrderLabelAt(row);
          widestLabel = Math.max(widestLabel, label.length);
          if (previousLabel != null) {
            assertTrue(ProjectionIndexRowGroupPage.compareOrderLabels(previousLabel, 0, previousLabel.length, label, 0,
                label.length) < 0, "persisted order labels must be strictly increasing");
          }
          previousLabel = label;
        }
      }
      assertEquals(expected, values, "the projection must report its records in document order");
      assertTrue(widestLabel <= MAX_PERSISTED_ORDER_LABEL_BYTES, "repeated move-to-first-child grew an order label to "
          + widestLabel + " bytes; the bounded rebalance must keep it within " + MAX_PERSISTED_ORDER_LABEL_BYTES);
    }
  }

  private static long namedArrayKey(final JsonNodeReadOnlyTrx rtx) {
    rtx.moveToDocumentRoot();
    final Axis descendants = new DescendantAxis(rtx);
    while (descendants.hasNext()) {
      descendants.nextLong();
      if (rtx.getKind() == NodeKind.OBJECT_NAMED_ARRAY && "records".equals(rtx.getName().getLocalName())) {
        return rtx.getNodeKey();
      }
      if (rtx.getKind() == NodeKind.ARRAY) {
        final long arrayKey = rtx.getNodeKey();
        final boolean named =
            rtx.moveToParent() && rtx.getKind().playsObjectKeyRole() && "records".equals(rtx.getName().getLocalName());
        rtx.moveTo(arrayKey);
        if (named) {
          return arrayKey;
        }
      }
    }
    throw new AssertionError("missing JSON array records");
  }
}
