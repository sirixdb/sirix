package io.sirix.index;

import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.access.trx.node.HashType;
import io.sirix.index.path.json.JsonPCRCollector;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASNumericBulkFeedTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void preexistingIndexReceivesNumericTuplesWithoutTruncatingFractions(final boolean unsupported) throws Exception {
    final Path databasePath = directory.resolve("bulk");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.NONE)
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.INR,
                   Set.of(parse("/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), writer);
        final StringBuilder json = new StringBuilder(24_000).append('[');
        for (int i = 0; i < 2_048; i++) {
          if (i > 0) {
            json.append(',');
          }
          json.append(i % 2 == 0
              ? "{\"id\":1.0}"
              : "{\"id\":1.5}");
        }
        if (unsupported) {
          json.append(",{\"id\":null},{\"id\":{\"value\":1}},{\"id\":[1]}");
        }
        json.append(']');
        ParallelBulkJsonImporter.assembleBytes(writer,
            new ByteArrayInputStream(json.toString().getBytes(StandardCharsets.UTF_8)), 6 * 1024, 2);
        writer.commit();
      }
    }
    try (var database = Databases.openJsonDatabase(databasePath);
        var session = database.beginResourceSession("rows");
        var reader = session.beginNodeReadOnlyTrx()) {
      final var controller = session.getRtxIndexController(reader.getRevisionNumber());
      final IndexDef definition = controller.getIndexes().getIndexDef(0, IndexType.CAS);
      final var filter =
          controller.createCASFilter(Set.of("/[]/id"), new Int32(1), SearchMode.EQUAL, new JsonPCRCollector(reader));
      final var postings = controller.openCASIndex(reader.getStorageEngineReader(), definition, filter);
      long count = 0;
      while (postings.hasNext()) {
        count += postings.next().cardinality();
      }
      assertEquals(1_024, count);
      assertEquals(!unsupported, definition.hasNumericValuesOnly());
      assertFalse(definition.hasCompleteNumericCoverage());
      assertTrue(reader.moveToDocumentRoot());
    }
  }
}
