package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.ParallelBulkJsonImporter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASScalarBulkImportTest {
  @TempDir
  Path directory;

  @Test
  void casOnlyScalarImportKeepsCoverageAndQueryResultsAfterReopen() throws Exception {
    final Path databasePath = directory.resolve("bulk");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .hashKind(HashType.NONE)
                                                   .storeNodeHistory(false)
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set.of(
                   IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/id", PathParser.Type.JSON)), 0,
                       IndexDef.DbType.JSON),
                   IndexDefs.createCASIdxDef(false, Type.STR, Set.of(parse("/[]/label", PathParser.Type.JSON)), 1,
                       IndexDef.DbType.JSON)),
                   writer);
        final StringBuilder json = new StringBuilder("[");
        for (int row = 0; row < 2_048; row++) {
          if (row != 0) {
            json.append(',');
          }
          json.append("{\"id\":").append(row).append(".0,\"label\":\"row").append(row).append('"');
          for (int column = 0; column < 30; column++) {
            json.append(",\"n")
                .append(column)
                .append("\":")
                .append(row)
                .append(",\"s")
                .append(column)
                .append("\":\"scalar\",\"b")
                .append(column)
                .append("\":true");
          }
          json.append('}');
        }
        json.append(']');
        ParallelBulkJsonImporter.assembleBytes(writer,
            new ByteArrayInputStream(json.toString().getBytes(StandardCharsets.UTF_8)), 6 * 1024, 2);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var optimized = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final var controller = store.lookup("bulk").getDatabase().beginResourceSession("rows").getRtxIndexController(1);
      final IndexDef definition = controller.getIndexes().getIndexDef(0, IndexType.CAS);
      assertEquals(2, controller.getIndexes().getNrOfIndexDefsWithType(IndexType.CAS));
      assertEquals(0, controller.getIndexes().getNrOfIndexDefsWithType(IndexType.PATH));
      assertTrue(definition.hasNumericValuesOnly());
      assertTrue(definition.hasCompleteNumericCoverage());
      final CompileChain generic = new CompileChain();
      for (final String query : new String[] {"sum(for $c in jn:doc('bulk','rows')[] where $c.id eq 999 return $c.id)",
          "sum(jn:doc('bulk','rows')[][?$$.id eq 999].id)"}) {
        assertEquals(999, ((Numeric) new Query(generic, query).execute(context)).intValue());
        assertEquals(999, ((Numeric) new Query(optimized, query).execute(context)).intValue());
      }
      assertEquals(1, ((Numeric) new Query(optimized,
          "count(jn:scan-cas-index(jn:doc('bulk','rows'),0,999,'==','/[]/id'))").execute(context)).intValue());
      assertEquals(1,
          ((Numeric) new Query(optimized,
              "count(jn:scan-cas-index(jn:doc('bulk','rows'),1,'row999','==','/[]/label'))").execute(
                  context)).intValue());
    }
  }
}
