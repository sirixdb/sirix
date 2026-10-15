package io.sirix.budget;

import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.DerivedIndexTestSupport;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.index.aggregate.TemporalAggregateStorage;
import io.sirix.index.changelog.ChangeLogStorage;
import io.sirix.index.projection.ProjectionAggregateRows;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Source publication writes only its log; a derived delta never reopens projection row groups. */
@Isolated
final class DerivedMaintainerWorkBudgetTest {
  @TempDir
  File directory;

  @ParameterizedTest
  @ValueSource(ints = {256, 4096})
  void publicationAndMaintenanceAreBoundedByChangedGroups(final int population) throws Exception {
    final var path = directory.toPath().resolve("budget");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(population * 80).append('[');
        for (int row = 0; row < population; row++) {
          if (row != 0) {
            json.append(',');
          }
          json.append("{\"g\":1,\"n\":")
              .append(row)
              .append(",\"lo\":\"2024-01-01T00:00:00Z\",\"hi\":\"2025-01-01T00:00:00Z\"}");
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
            JsonNodeTrx.Commit.NO);
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
            List.of(field("g"), field("n"), field("lo"), field("hi")), List.of(Type.LON, Type.LON, Type.STR, Type.STR),
            0, IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
        final long beforeBuildSlots = TemporalAggregateStorage.slotsWritten();
        TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
            List.of("g"), List.of("sum(n)"));
        final DerivedIndexMaintainer.Resource resource =
            Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
        resource.pause();
        assertEquals(beforeBuildSlots, TemporalAggregateStorage.slotsWritten(),
            "Declaration must not build aggregate pages");
        writer.commit();
        resource.drain();
        final long[] records = new long[20];
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        for (int row = 0; row < records.length; row++) {
          records[row] = writer.getNodeKey();
          writer.moveToRightSibling();
        }
        for (final long record : records) {
          writer.moveTo(record);
          writer.moveToFirstChild();
          writer.moveToRightSibling();
          writer.moveToFirstChild();
          writer.setNumberValue(100);
        }
        final long sourceSlots = TemporalAggregateStorage.slotsWritten();
        writer.commit();
        assertEquals(sourceSlots, TemporalAggregateStorage.slotsWritten(), "Source commits write zero aggregate slots");
        final ChangeLogStorage.WriteWork log = ChangeLogStorage.lastWrite();
        assertEquals(20, log.records());
        assertEquals((log.bytes() + 2047) / 2048 + 1, log.slots());
        final long chunks = ChangeLogStorage.chunksRead();
        final long projections = ProjectionAggregateRows.rowGroupsRead();
        final long heads = TemporalAggregateStorage.headsRead();
        final long parts = TemporalAggregateStorage.partChunksRead();
        final long slots = TemporalAggregateStorage.slotsWritten();
        final long documents = resource.documentPagesWritten();
        final var catalogue =
            WorkCapture.of(EngineWorkCounters.INDEX_CATALOGUE_FILES_WRITTEN).call(resource::maintainNextBatch);
        assertTrue(catalogue.result());
        catalogue.work()
                 .assertExactly(EngineWorkCounters.INDEX_CATALOGUE_FILES_WRITTEN, 0,
                     "Ordinary derived commits reuse the mirrored catalogue");
        assertEquals((log.bytes() + 2047) / 2048, ChangeLogStorage.chunksRead() - chunks,
            "Each durable slab chunk is read once for the source projection");
        assertEquals(0, ProjectionAggregateRows.rowGroupsRead() - projections,
            "Delta maintenance uses the log, never the projection population");
        assertEquals(1, TemporalAggregateStorage.headsRead() - heads, "One touched group head");
        assertEquals(0, TemporalAggregateStorage.partChunksRead() - parts, "Catch-up never folds old parts");
        assertEquals(5, TemporalAggregateStorage.slotsWritten() - slots,
            "One part, one head, header, revision mapping, frontier mapping");
        assertEquals(0, resource.documentPagesWritten() - documents, "Derived commits write no document pages");
        try (StorageEngineReader source = session.createStorageEngineReader(2);
            StorageEngineReader derived = session.openDerivedIndexReader(source)) {
          assertFalse(Files.exists(Objects.requireNonNull(derived)
                                          .getResourceSession()
                                          .getResourcePath()
                                          .resolve("indexes")
                                          .resolve(derived.getRevisionNumber() + ".xml")));
        }
        resource.resume();
        DerivedIndexTestSupport.await(session);
      }
    }
  }

  private static Path<QNm> field(final String name) {
    return Path.parse("/[]/" + name, PathParser.Type.JSON);
  }
}
