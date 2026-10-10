package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.path.summary.PathSummaryReader;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexRegistry.Handle;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class ProjectionUtcProvenanceTest {
  @TempDir
  Path directory;

  @AfterEach
  void clearCaches() {
    ProjectionBulkLoad.clearActive();
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
  }

  @ParameterizedTest
  @CsvSource({"'{}', true", "'{}', false", "'[]', true", "'[]', false"})
  void parallelImportPersistsUnrepresentableStructuredTimestampCells(final String structuredValue,
      final boolean timestampFirst) throws Exception {
    final Path dbPath = directory.resolve("parallel-utc");
    final IndexDef def = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/parts/[]/vf", PathParser.Type.JSON)), List.of(Type.DATI), 0, IndexDef.DbType.JSON);
    final String timestamp = "{\"vf\":\"2024-04-01T00:00:00Z\"}";
    final String structured = "{\"vf\":" + structuredValue + "}";
    final String parts = timestampFirst
        ? timestamp + "," + structured
        : structured + "," + timestamp;
    final byte[] json =
        ("[{\"parts\":[" + parts + "]},{\"parts\":[" + timestamp + "]}]").getBytes(StandardCharsets.UTF_8);
    assertTrue(Databases.createJsonDatabase(new DatabaseConfiguration(dbPath)));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      assertTrue(db.createResource(ResourceConfiguration.newBuilder("rows")
                                                        .hashKind(HashType.NONE)
                                                        .useDeweyIDs(false)
                                                        .storageType(StorageType.FILE_CHANNEL)
                                                        .build()));
      try (JsonResourceSession session = db.beginResourceSession("rows");
          JsonNodeTrx wtx = session.beginNodeTrx();
          ByteArrayInputStream input = new ByteArrayInputStream(json)) {
        final JsonIndexController controller = session.getWtxIndexController(wtx.getRevisionNumber());
        controller.createProjectionIndexAtLoadStart(def, wtx, 2);
        ParallelBulkJsonImporter.assembleBytes(wtx, input, 64, 2);
        wtx.commit();
      }
    }
    clearCaches();
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath);
        JsonResourceSession session = db.beginResourceSession("rows");
        JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final byte[] payload =
          ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(rtx.getStorageEngineReader(), def.getID(), 1);
      assertNotNull(payload);
      final ProjectionIndexRowGroupPage page = ProjectionIndexRowGroupPage.deserialize(payload);
      assertEquals(2, page.getRowCount());
      assertTrue(page.columnUnrepresentable(0), "structured timestamps must remain UNREPRESENTABLE");
      assertFalse(page.columnPureUtcTimestamp(0), "a structured cell cannot supply scalar UTC provenance");
      final Handle handle = ProjectionIndexCatalog.load(session, rtx.getRevisionNumber(), def);
      assertNotNull(handle);
      final ColumnSegmentFetcher fetcher =
          ProjectionIndexCatalog.columnSegmentFetcher(session, rtx.getRevisionNumber());
      assertFalse(handle.timestampColumnPureUtc(0, fetcher), "persisted proof must decline scalar UTC serving");
    }
  }

  @Test
  void repeatedNestedStructuredCellsPoisonUtcProvenanceInEitherOrder() {
    final Path dbPath = directory.resolve("utc");
    Databases.createJsonDatabase(new DatabaseConfiguration(dbPath));
    try (Database<JsonResourceSession> db = Databases.openJsonDatabase(dbPath)) {
      db.createResource(ResourceConfiguration.newBuilder("rows").build());
      try (JsonResourceSession session = db.beginResourceSession("rows")) {
        try (JsonNodeTrx wtx = session.beginNodeTrx()) {
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              [{"parts":[{"vf":"2024-04-01T00:00:00Z"},{"vf":{}}]},
               {"parts":[{"vf":{}},{"vf":"2024-04-01T00:00:00Z"}]},
               {"parts":[{"vf":"2024-04-01T00:00:00Z"},{"vf":[]}]},
               {"parts":[{"vf":[]},{"vf":"2024-04-01T00:00:00Z"}]},
               {"parts":[{"vf":"2024-04-01T00:00:00Z"}]}]
              """), JsonNodeTrx.Commit.NO);
          wtx.commit();
        }
        try (JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx();
            PathSummaryReader summary = session.openPathSummary()) {
          final IndexDef def = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
              List.of(parse("/[]/parts/[]/vf", PathParser.Type.JSON)), List.of(Type.DATI), 0, IndexDef.DbType.JSON);
          final ProjectionIndexRowExtractor extractor = new ProjectionIndexRowExtractor(def, summary);
          rtx.moveToDocumentRoot();
          rtx.moveToFirstChild();
          rtx.moveToFirstChild();
          for (int row = 0; row < 5; row++) {
            final long key = rtx.getNodeKey();
            assertTrue(extractor.extractInto(rtx, key));
            assertTrue(extractor.rowUnrepresentable(0), "text still requires the original Z suffix");
            if (row < 4) {
              assertTrue(extractor.rowNonDoubleSource(0), "multi-valued timestamps cannot supply a scalar UTC lane");
            } else {
              assertFalse(extractor.rowNonDoubleSource(0), "one canonical UTC cell is the positive control");
            }
            if (row < 4) {
              rtx.moveToRightSibling();
            }
          }
        }
      }
    }
  }
}
