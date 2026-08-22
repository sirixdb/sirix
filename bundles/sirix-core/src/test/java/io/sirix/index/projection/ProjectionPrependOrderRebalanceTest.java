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
import io.sirix.access.trx.node.json.objectvalue.NumberValue;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Repeated insert-as-first-child at the SAME position is the ORDPATH worst case: every prepend
 * derives its order label from the current head with {@code newBetween(null, head)}, which adds a
 * division every few insertions and never gives any of it back. Left alone, the projection's order
 * labels grow without bound — a raw prepend chain reaches 101 bytes after 400 links and 151 after
 * 600 — until they blow the bounded local encoding and abort an otherwise valid transaction, and
 * long before that they shrink how many rows fit a row group's order-label lane. With the bounded
 * rebalance switched off, the history below persists a 55-byte order label.
 *
 * <p>
 * This exercises exactly that shape against a DEFAULT resource with {@code storeDeweyIDs} off and
 * asserts the three things the bounded rebalance owes: every transaction commits, the projection
 * still reports its records in document order (hot and after a cold reopen), and the persisted order
 * labels stay small. No complete rebuild is allowed to be what makes it work.
 */
final class ProjectionPrependOrderRebalanceTest {

  private static final String RESOURCE = "prepend-order-rebalance";
  private static final int COMMITS = 200;
  private static final int PREPENDS_PER_COMMIT = 2;
  private static final int PREPENDS = COMMITS * PREPENDS_PER_COMMIT;

  /**
   * Bound on one persisted order label. The rebalance caps a record's LOCAL label at a handful of
   * divisions and the record set sits two containers below the document root, so a label that stays
   * bounded lands in single-digit bytes — while unbounded ORDPATH growth passes this in well under
   * a hundred prepends.
   */
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
  void repeatedInsertAsFirstChildCommitsAndKeepsOrderLabelsBounded() throws Exception {
    final Path databasePath = temporaryDirectory.resolve("prepend");
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(databasePath)));
    final IndexDef definition = IndexDefs.createProjectionIdxDef(
        parse("/records/[]", PathParser.Type.JSON),
        List.of(parse("/records/[]/score", PathParser.Type.JSON)),
        List.of(Type.LON), 0, IndexDef.DbType.JSON);

    final int finalRevision;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      assertTrue(database.createResource(ResourceConfiguration.newBuilder(RESOURCE)
                                                              .storageType(StorageType.FILE_CHANNEL)
                                                              .hashKind(HashType.NONE)
                                                              .storeDiffs(false)
                                                              .buildPathSummary(true)
                                                              .build()));

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           var wtx = session.beginNodeTrx();
           var parser = JacksonJsonShredder.createStringParser("{\"records\":[{\"score\":0}]}")) {
        new JacksonJsonShredder.Builder(wtx, parser, InsertPosition.AS_FIRST_CHILD).commitAfterwards()
                                                                                   .build()
                                                                                   .call();
      }

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           var wtx = session.beginNodeTrx()) {
        session.getWtxIndexController(wtx.getRevisionNumber()).createIndexes(Set.of(definition), wtx);
        wtx.commit();
      }

      final long recordsKey;
      try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
           JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        recordsKey = namedArrayKey(rtx);
      }

      ProjectionIndexChangeListener.resetMaintenanceTelemetry();
      int score = 0;
      for (int commit = 0; commit < COMMITS; commit++) {
        try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
             var wtx = session.beginNodeTrx()) {
          for (int prepend = 0; prepend < PREPENDS_PER_COMMIT; prepend++) {
            assertTrue(wtx.moveTo(recordsKey));
            wtx.insertObjectAsFirstChild();
            wtx.insertObjectRecordAsFirstChild("score", new NumberValue(++score));
          }
          wtx.commit();
        }
      }
      assertEquals(PREPENDS, score);

      try (JsonResourceSession session = database.beginResourceSession(RESOURCE)) {
        finalRevision = session.getMostRecentRevisionNumber();
      }
      assertProjection(database, definition, finalRevision);
      assertEquals(0L, ProjectionIndexChangeListener.maintenanceTelemetry().fullRebuilds(),
          "the prepend workload must stay incremental — no complete projection rebuild");
    }

    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (Database<JsonResourceSession> reopened = Databases.openJsonDatabase(databasePath)) {
      assertProjection(reopened, definition, finalRevision);
    }
  }

  /**
   * The persisted projection is read through its own row-group pages — the index's serialized state,
   * which is the contract under test here — and checked for document order, order-label
   * monotonicity, and a bounded label encoding.
   */
  private static void assertProjection(final Database<JsonResourceSession> database, final IndexDef definition,
      final int revision) {
    ProjectionIndexCatalog.clearCache();
    try (JsonResourceSession session = database.beginResourceSession(RESOURCE);
         JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx(revision)) {
      final ProjectionIndexRegistry.Handle handle = ProjectionIndexCatalog.load(session, revision, definition);
      assertNotNull(handle, "the projection index must still be usable after a prepend-heavy history");
      final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(
          session, revision, definition.getID(), handle.rowGroupCount()));

      final List<Long> values = new ArrayList<>(PREPENDS + 1);
      byte[] previousLabel = null;
      int widestLabel = 0;
      for (final byte[] payload : leaves) {
        final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(payload);
        for (int row = 0; row < page.getRowCount(); row++) {
          values.add(page.numericColumn(0)[row]);
          final byte[] label = page.copyOrderLabelAt(row);
          widestLabel = Math.max(widestLabel, label.length);
          if (previousLabel != null) {
            assertTrue(ProjectionIndexRowGroupPage.compareOrderLabels(previousLabel, 0, previousLabel.length,
                label, 0, label.length) < 0, "persisted order labels must be strictly increasing");
          }
          previousLabel = label;
        }
      }

      // Prepending pushes each new score in front of every earlier one, so the projection's document
      // order is the reverse of the insertion order, with the shredded record last.
      final List<Long> expected = new ArrayList<>(PREPENDS + 1);
      for (long score = PREPENDS; score >= 0L; score--) {
        expected.add(score);
      }
      assertEquals(expected, values, "the projection must report its records in document order");
      assertTrue(widestLabel <= MAX_PERSISTED_ORDER_LABEL_BYTES,
          "repeated insert-as-first-child grew an order label to " + widestLabel
              + " bytes; the bounded rebalance must keep it within " + MAX_PERSISTED_ORDER_LABEL_BYTES);
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
        final boolean named = rtx.moveToParent() && rtx.getKind().playsObjectKeyRole()
            && "records".equals(rtx.getName().getLocalName());
        rtx.moveTo(arrayKey);
        if (named) {
          return arrayKey;
        }
      }
    }
    throw new AssertionError("missing JSON array records");
  }
}
