package io.sirix.index.projection;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.List;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class UnstoredChildCountProjectionTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void loadTimeProjectionCountsEveryRecordAcrossDrains(final VersioningType versioning) {
    final Path databasePath = directory.resolve("counts");
    final IndexDef definition = IndexDefs.createProjectionIdxDef(parse("/[]", PathParser.Type.JSON),
        List.of(parse("/[]/value", PathParser.Type.JSON)), List.of(Type.LON), 0, IndexDef.DbType.JSON);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("omitted")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .hashKind(HashType.NONE)
                                                   .storeChildCount(false)
                                                   .build());
      try (var session = database.beginResourceSession("omitted"); var writer = session.beginNodeTrx()) {
        final JsonIndexController controller =
            (JsonIndexController) session.getWtxIndexController(writer.getRevisionNumber());
        final ProjectionBulkLoad load = controller.createProjectionIndexAtLoadStart(definition, writer, -1L);
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"value\":1},{\"value\":2}]"),
            JsonNodeTrx.Commit.NO);
        controller.applyPendingIndexMaintenance(false);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"value\":3}"), JsonNodeTrx.Commit.NO);
        controller.applyPendingIndexMaintenance(false);
        writer.commit();
        assertEquals(3, load.rowsEmitted());
      }
    }
    try (var database = Databases.openJsonDatabase(databasePath);
        var session = database.beginResourceSession("omitted");
        var reader = session.beginNodeReadOnlyTrx()) {
      final var handle = ProjectionIndexCatalog.load(session, reader.getRevisionNumber(), definition);
      final List<byte[]> leaves = handle.rowGroupPayloads(ProjectionIndexCatalog.rowGroupMaterializer(session,
          reader.getRevisionNumber(), definition.getID(), handle.rowGroupCount()));
      long rows = 0;
      long sum = 0;
      for (final byte[] payload : leaves) {
        final var page = ProjectionIndexRowGroupPage.deserialize(payload);
        rows += page.getRowCount();
        final long[] values = page.numericColumn(0);
        for (int row = 0; row < page.getRowCount(); row++) {
          sum += values[row];
        }
      }
      assertEquals(3, rows);
      assertEquals(6, sum);
    }
  }
}
