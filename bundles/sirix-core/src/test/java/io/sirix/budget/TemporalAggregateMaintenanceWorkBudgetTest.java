package io.sirix.budget;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.PageContainer;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.index.aggregate.TemporalAggregateIndexes;
import io.sirix.index.aggregate.TemporalAggregateStorage;
import io.sirix.index.aggregate.TemporalAggregateParts;
import io.sirix.index.aggregate.AggregateSummary;
import io.sirix.index.aggregate.TemporalAggregateDomain;
import io.sirix.page.HOTLeafPage;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.math.BigInteger;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A 20-row publication rewrites only its contributor paths, independently of untouched population. */
final class TemporalAggregateMaintenanceWorkBudgetTest {
  @TempDir
  File directory;

  @ParameterizedTest
  @ValueSource(ints = {256, 4096})
  void changedRowsBoundNodeAndPageWrites(final int rows) throws Exception {
    final var databasePath = directory.toPath().resolve("budget");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("records").validTimePaths("lo", "hi")
          .storageType(StorageType.FILE_CHANNEL).buildPathSummary(true).storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("records"); JsonNodeTrx writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(rows * 110).append('[');
        for (int row = 0; row < rows; row++) {
          if (row != 0) {
            json.append(',');
          }
          json.append("{\"g\":1,\"ignored\":0,\"amount\":").append(row)
              .append(",\"lo\":\"1970-01-01T00:00:00.001Z\",\"hi\":\"1970-01-01T00:00:00.002Z\"}");
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()), JsonNodeTrx.Commit.NO);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        final List<Path<QNm>> fields = List.of(path("g"), path("amount"), path("lo"), path("hi"), path("ignored"));
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
            List.of(Type.LON, Type.LON, Type.STR, Type.STR, Type.LON), 0, IndexDef.DbType.JSON);
        controller.createIndexes(Set.of(projection), writer);
        final IndexDef aggregate = TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"),
            List.of("sum(amount)", "min(amount)", "max(amount)"));
        writer.commit();
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final int changed = 20;
        final AtomicInteger readerIds = readerIds(session);
        final int beforeReaders = readerIds.get();
        final TemporalAggregateIndexes.MaintenanceWork beforePublication = TemporalAggregateIndexes.lastMaintenance();
        for (int row = 0; row < changed; row++) {
          final long object = writer.getNodeKey();
          writer.moveToFirstChild();
          while (!"amount".equals(writer.getName().getLocalName())) {
            writer.moveToRightSibling();
          }
          writer.moveToFirstChild();
          writer.setNumberValue(row + 7);
          writer.moveTo(object);
          writer.moveToRightSibling();
          session.getWtxIndexController(writer.getRevisionNumber()).notifyBeforePageFlush();
          assertSame(beforePublication, TemporalAggregateIndexes.lastMaintenance(),
              "Projection flushes must queue aggregate changes rather than apply each segment separately");
        }
        session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
        assertEquals(beforeReaders, readerIds.get(), "Maintenance must open zero represented-revision readers");
        final TemporalAggregateIndexes.MaintenanceWork work = TemporalAggregateIndexes.lastMaintenance();
        assertEquals(changed, work.changedRecords());
        assertTrue(work.nodesWritten() > 0 && work.nodesWritten() <= 48 + 64L * changed,
            "Appended part chunks must be bounded by changed rows: " + work);
        assertTrue(work.nodesRead() <= 4 * (48 + 64L * changed),
            "Appending a delta must not read untouched part chunks: " + work);
        final Set<HOTLeafPage> pages = Collections.newSetFromMap(new IdentityHashMap<>());
        for (final PageContainer container : writer.getStorageEngineWriter().getLog().getList()) {
          if (container.getModified() instanceof HOTLeafPage page && page.getIndexType() == IndexType.TEMPORAL_AGGREGATE) {
            pages.add(page);
          }
        }
        assertTrue(!pages.isEmpty() && pages.size() <= 8 + 4 * changed,
            "One publication must not copy untouched aggregate leaf pages: " + pages.size());
        if (Boolean.getBoolean("sirix.workBudget.print")) {
          System.out.printf("TEMPORAL_AGGREGATE_BUDGET rows=%d changed=%d nodesRead=%d nodesWritten=%d pages=%d%n",
              rows, changed, work.nodesRead(), work.nodesWritten(), pages.size());
        }
        writer.commit();
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        while (!"ignored".equals(writer.getName().getLocalName())) {
          writer.moveToRightSibling();
        }
        writer.moveToFirstChild();
        writer.setNumberValue(1);
        session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
        assertEquals(new TemporalAggregateIndexes.MaintenanceWork(0, 0, 0),
            TemporalAggregateIndexes.lastMaintenance(), "Unrelated projection fields must not touch aggregate nodes");
        writer.rollback();
        for (int revision = 1; revision <= 2; revision++) {
          try (var transaction = session.beginNodeReadOnlyTrx(revision);
              var storage = new TemporalAggregateStorage.Reader(transaction.getStorageEngineReader(), aggregate.getID())) {
            final AggregateSummary[] summaries = new AggregateSummary[1];
            storage.groups((key, group) -> summaries[0] = new TemporalAggregateParts(
                aggregate.getTemporalAggregateSpec(), new TemporalAggregateDomain().height())
                .point(storage.parts(), group.parts(), 1));
            final AggregateSummary summary = summaries[0];
            assertTrue(storage.nodesRead() <= 3,
                "Point queries must read compressed summaries, never the contributor population");
            assertEquals(rows, summary.count());
            assertEquals(BigInteger.valueOf((long) rows * (rows - 1) / 2 + (revision == 2 ? changed * 7 : 0)), summary.sum(0));
          }
        }
      }
    }
  }

  private static AtomicInteger readerIds(final JsonResourceSession session) throws Exception {
    for (Class<?> type = session.getClass(); type != null; type = type.getSuperclass()) {
      try {
        final Field field = type.getDeclaredField("trxIDCounter");
        field.setAccessible(true);
        return (AtomicInteger) field.get(session);
      } catch (final NoSuchFieldException next) {
        // Continue to the resource-session implementation that owns transaction IDs.
      }
    }
    throw new AssertionError("No transaction ID allocator");
  }

  private static Path<QNm> path(final String field) {
    return Path.parse("/[]/" + field, PathParser.Type.JSON);
  }
}
