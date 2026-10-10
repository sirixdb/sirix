package io.sirix.query.bench.bitemporal;

import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class BitemporalBusinessKeyIndexTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @ValueSource(strings = {BitemporalSchema.CONTRACTS, BitemporalSchema.PRODUCTS, BitemporalSchema.SUPPLIERS})
  void publishedCatalogueContainsOnlyRequiredBusinessKeyPaths(final String resource) {
    final Path databasePath = directory.resolve(BitemporalSchema.DATABASE);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(resource)
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession(resource); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(
            JsonShredder.createStringReader(
                "[{\"id\":1,\"pid\":2,\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"2025-01-01T00:00:00Z\"}]"),
            JsonNodeTrx.Commit.NO);
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, BitemporalSchema.DATABASE);
        BitemporalSirixLoadMain.createBusinessKeyIndex(session, writer, resource);
        writer.commit();
      }
    }
    try (var database = Databases.openJsonDatabase(databasePath);
        var session = database.beginResourceSession(resource)) {
      final var indexes = session.getRtxIndexController(session.getMostRecentRevisionNumber()).getIndexes();
      assertEquals(1, indexes.getNrOfIndexDefsWithType(IndexType.VALIDTIME));
      final var idPath = parse("/[]/id", PathParser.Type.JSON);
      final var businessKey = indexes.findCASIndex(idPath, Type.INR);
      if (resource.equals(BitemporalSchema.SUPPLIERS)) {
        assertTrue(businessKey.isEmpty());
        assertEquals(2, indexes.getNrOfIndexDefsWithType(IndexType.CAS));
      } else {
        final IndexDef definition = businessKey.orElseThrow();
        assertEquals(Set.of(idPath), definition.getPaths());
        assertEquals(3, indexes.getNrOfIndexDefsWithType(IndexType.CAS));
      }
      assertTrue(indexes.findCASIndex(parse("/[]/pid", PathParser.Type.JSON), Type.INR).isEmpty());
    }
  }
}
