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
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.File;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicInteger;
import java.math.BigInteger;
import java.time.Instant;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A 20-row publication rewrites only its contributor paths, independently of untouched population.
 */
@Isolated
final class TemporalAggregateMaintenanceWorkBudgetTest {
  @TempDir
  File directory;

  @ParameterizedTest
  @CsvSource({"256,false", "4096,false", "256,true", "4096,true"})
  void changedRowsBoundNodeAndPageWrites(final int rows, final boolean typedTimestamps) throws Exception {
    final var databasePath = directory.toPath().resolve("budget");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .buildPathSummary(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(rows * 110).append('[');
        for (int row = 0; row < rows; row++) {
          if (row != 0) {
            json.append(',');
          }
          json.append("{\"g\":1,\"ignored\":0,\"amount\":")
              .append(row)
              .append(typedTimestamps
                  ? ",\"lo\":\"1970-01-01T00:00:01Z\",\"hi\":\"1970-01-01T00:00:02Z\"}"
                  : ",\"lo\":\"1970-01-01T00:00:00.001Z\",\"hi\":\"1970-01-01T00:00:00.002Z\"}");
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
            JsonNodeTrx.Commit.NO);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        final List<Path<QNm>> fields = List.of(path("g"), path("amount"), path("lo"), path("hi"), path("ignored"));
        final Type endpointType = typedTimestamps ? Type.DATI : Type.STR;
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
            List.of(Type.LON, Type.LON, endpointType, endpointType, Type.LON), 0, IndexDef.DbType.JSON);
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
          if (container.getModified() instanceof HOTLeafPage page
              && page.getIndexType() == IndexType.TEMPORAL_AGGREGATE) {
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
        assertEquals(new TemporalAggregateIndexes.MaintenanceWork(0, 0, 0, 0, 0, 0, 0),
            TemporalAggregateIndexes.lastMaintenance(), "Unrelated projection fields must not touch aggregate nodes");
        writer.rollback();
        for (int revision = 1; revision <= 2; revision++) {
          try (var transaction = session.beginNodeReadOnlyTrx(revision);
              var storage =
                  new TemporalAggregateStorage.Reader(transaction.getStorageEngineReader(), aggregate.getID())) {
            final AggregateSummary[] summaries = new AggregateSummary[1];
            storage.groups(Long.MAX_VALUE,
                (key, group) -> summaries[0] = new TemporalAggregateParts(aggregate.getTemporalAggregateSpec(),
                    new TemporalAggregateDomain().height()).point(storage.parts(), group.parts(), typedTimestamps
                        ? 1000
                        : 1));
            final AggregateSummary summary = summaries[0];
            assertTrue(storage.nodesRead() <= 3,
                "Point queries must read compressed summaries, never the contributor population");
            assertEquals(rows, summary.count());
            assertEquals(BigInteger.valueOf((long) rows * (rows - 1) / 2 + (revision == 2
                ? changed * 7
                : 0)), summary.sum(0));
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"256,528", "4096,528", "4096,4096"})
  void tierCascadesBoundTotalWorkAcrossPublicationsAndBarriers(final int rows, final int publications)
      throws Exception {
    final var databasePath = directory.toPath().resolve("cascades");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final int barriersPerCommit = 16;
    final int[] checkpointTier = new int[1];
    final long[] firstCheckpoint = new long[1];
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .buildPathSummary(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(rows * 110).append('[');
        for (int row = 0; row < rows; row++) {
          if (row != 0) {
            json.append(',');
          }
          json.append("{\"g\":1,\"amount\":")
              .append(row)
              .append(",\"lo\":\"")
              .append(Instant.ofEpochMilli(2L * row + 1))
              .append("\",\"hi\":\"")
              .append(Instant.ofEpochMilli(2L * row + 2))
              .append("\"}");
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
            JsonNodeTrx.Commit.NO);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
            List.of(path("g"), path("amount"), path("lo"), path("hi")), List.of(Type.LON, Type.LON, Type.STR, Type.STR),
            0, IndexDef.DbType.JSON);
        controller.createIndexes(Set.of(projection), writer);
        final IndexDef aggregate = TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"),
            List.of("sum(amount)", "min(amount)", "max(amount)"));
        writer.commit();
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        while (!"amount".equals(writer.getName().getLocalName())) {
          writer.moveToRightSibling();
        }
        writer.moveToFirstChild();
        final long amount = writer.getNodeKey();
        try (var storage = new TemporalAggregateStorage.Reader(writer.getStorageEngineWriter(), aggregate.getID())) {
          storage.groups(Long.MAX_VALUE, (key, head) -> {
            checkpointTier[0] = head.checkpointTier();
            firstCheckpoint[0] = head.parts()[0];
          });
        }
        final AtomicInteger readers = readerIds(session);
        long chunksRead = 0;
        long chunksWritten = 0;
        long endpointsRead = 0;
        long endpointsWritten = 0;
        long nodesRead = 0;
        long nodesWritten = 0;
        int promotedParts = 0;
        int checkpointCascades = 0;
        int lastCheckpointPublication = 0;
        long checkpoint = firstCheckpoint[0];
        final long checkpointBoundary = 1L << (4 * checkpointTier[0]);
        assertEquals(rows == 256
            ? 256
            : 4096, checkpointBoundary, "The distinct-endpoint fixture must reach the intended checkpoint tier");
        final long[] currentCheckpoint = new long[1];
        final int[] currentTier = new int[1];
        for (int publication = 1; publication <= publications; publication++) {
          writer.moveTo(amount);
          writer.setNumberValue(publication);
          final int beforeReaders = readers.get();
          session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
          assertEquals(beforeReaders, readers.get(), "Folds must not open represented-revision readers");
          final TemporalAggregateIndexes.MaintenanceWork work = TemporalAggregateIndexes.lastMaintenance();
          assertEquals(1, work.changedRecords());
          chunksRead += work.nodesRead();
          chunksWritten += work.nodesWritten();
          endpointsRead += work.endpointsRead();
          endpointsWritten += work.endpointsWritten();
          nodesRead += work.summaryNodesRead();
          nodesWritten += work.summaryNodesWritten();
          final long expectedValue = publication;
          currentCheckpoint[0] = 0;
          currentTier[0] = 0;
          try (var storage = new TemporalAggregateStorage.Reader(writer.getStorageEngineWriter(), aggregate.getID())) {
            storage.groups(Long.MAX_VALUE, (key, head) -> {
              currentCheckpoint[0] = head.parts()[0];
              assertEquals(checkpointTier[0], head.checkpointTier());
              final byte[] tiers = head.tiers();
              currentTier[0] = tiers.length == 1
                  ? head.checkpointTier()
                  : tiers[tiers.length - 1];
              for (int tier = 0; tier < checkpointTier[0]; tier++) {
                int count = 0;
                for (int part = 1; part < tiers.length; part++) {
                  if (tiers[part] == tier) {
                    count++;
                  }
                }
                final long tierBoundary = 1L << (4 * tier);
                assertEquals(expectedValue / tierBoundary % TemporalAggregateParts.FAN_IN, count,
                    "Each tier must carry exactly once at its fan-in boundary");
              }
              final AggregateSummary summary = new TemporalAggregateParts(aggregate.getTemporalAggregateSpec(),
                  new TemporalAggregateDomain().height()).point(storage.parts(), head.parts(), 1);
              assertEquals(1, summary.count());
              assertEquals(BigInteger.valueOf(expectedValue), summary.sum(0));
              assertEquals(expectedValue, summary.min(0));
              assertEquals(expectedValue, summary.max(0));
            });
          }
          assertTrue(currentCheckpoint[0] > 0, "The group dictionary must contain the published group");
          if (currentCheckpoint[0] != checkpoint) {
            assertEquals(0, publication % checkpointBoundary,
                "Small equal-sized deltas must reach the checkpoint only at its tier boundary");
            assertTrue(publication - lastCheckpointPublication >= checkpointBoundary,
                "A checkpoint cascade must happen at most once per tier boundary");
            lastCheckpointPublication = publication;
            checkpointCascades++;
            checkpoint = currentCheckpoint[0];
            assertTrue(work.endpointsRead() >= 2L * rows,
                "The cascade fixture must include the untouched group's endpoint reads");
            if (Boolean.getBoolean("sirix.workBudget.print")) {
              System.out.printf(
                  "TEMPORAL_AGGREGATE_CHECKPOINT_CASCADE rows=%d publication=%d boundary=%d "
                      + "chunkReads=%d chunkWrites=%d endpointReads=%d endpointWrites=%d nodeReads=%d nodeWrites=%d%n",
                  rows, publication, checkpointBoundary, work.nodesRead(), work.nodesWritten(), work.endpointsRead(),
                  work.endpointsWritten(), work.summaryNodesRead(), work.summaryNodesWritten());
            }
          } else if (currentTier[0] > 0) {
            promotedParts++;
          }
          assertEquals(publication / checkpointBoundary, checkpointCascades,
              "Every completed checkpoint-tier boundary must produce exactly one cascade");
          if (publication % barriersPerCommit == 0) {
            writer.commit();
          }
        }
        assertTrue(publications > TemporalAggregateParts.MAX_PARTS * 4);
        assertTrue(promotedParts >= publications / TemporalAggregateParts.FAN_IN - checkpointCascades,
            "The sequence must exercise repeated tier promotions");
        assertEquals(publications >= checkpointBoundary, checkpointCascades > 0,
            "Every fixture that reaches its top-tier boundary must rewrite its checkpoint");
        assertTrue(chunksRead > 0 && chunksRead <= 8L * publications,
            "Amortized part-chunk reads must be bounded by changes: " + chunksRead);
        assertTrue(chunksWritten > 0 && chunksWritten <= 8L * publications,
            "Amortized part-chunk writes must be bounded by changes: " + chunksWritten);
        assertTrue(endpointsRead > 0 && endpointsRead <= 16L * publications,
            "Amortized endpoint reads must be bounded by changes: " + endpointsRead);
        assertTrue(endpointsWritten > 0 && endpointsWritten <= 16L * publications,
            "Amortized endpoint writes must be bounded by changes: " + endpointsWritten);
        assertTrue(nodesRead > 0 && nodesRead <= 128L * publications,
            "Amortized summary-node reads must be bounded by changes: " + nodesRead);
        assertTrue(nodesWritten > 0 && nodesWritten <= 128L * publications,
            "Amortized summary-node writes must be bounded by changes: " + nodesWritten);
        if (Boolean.getBoolean("sirix.workBudget.print")) {
          System.out.printf(
              "TEMPORAL_AGGREGATE_CASCADE_BUDGET rows=%d changed=%d chunkReads=%d chunkWrites=%d "
                  + "endpointReads=%d endpointWrites=%d nodeReads=%d nodeWrites=%d promotions=%d checkpoints=%d "
                  + "checkpointBoundary=%d%n",
              rows, publications, chunksRead, chunksWritten, endpointsRead, endpointsWritten, nodesRead, nodesWritten,
              promotedParts, checkpointCascades, checkpointBoundary);
        }
        writer.rollback();
        for (int revision = 1; revision <= 1 + publications / barriersPerCommit; revision++) {
          final long expected = (long) (revision - 1) * barriersPerCommit;
          try (var transaction = session.beginNodeReadOnlyTrx(revision);
              var storage =
                  new TemporalAggregateStorage.Reader(transaction.getStorageEngineReader(), aggregate.getID())) {
            final int[] groups = new int[1];
            storage.groups(Long.MAX_VALUE, (key, head) -> {
              groups[0]++;
              final TemporalAggregateParts parts = new TemporalAggregateParts(aggregate.getTemporalAggregateSpec(),
                  new TemporalAggregateDomain().height());
              final AggregateSummary summary = parts.point(storage.parts(), head.parts(), 1);
              assertEquals(rows, head.rows());
              assertEquals(1, summary.count());
              assertEquals(BigInteger.valueOf(expected), summary.sum(0));
              assertEquals(expected, summary.min(0));
              assertEquals(expected, summary.max(0));
              assertEquals(0, parts.point(storage.parts(), head.parts(), 2).count());
              assertEquals(BigInteger.valueOf(rows - 1),
                  parts.point(storage.parts(), head.parts(), 2L * rows - 1).sum(0));
            });
            assertEquals(1, groups[0]);
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
