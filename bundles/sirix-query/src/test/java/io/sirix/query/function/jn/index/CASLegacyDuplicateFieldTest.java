package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.sdb.explain.QueryPlan;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASLegacyDuplicateFieldTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void legacyPublicationPreservesFirstFieldsAndUniqueItemsAcrossRevisions(final VersioningType versioning) {
    final Path databasePath = directory.resolve("duplicates");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        final StringBuilder json =
            new StringBuilder("[{\"item\":{\"id\":1,\"id\":1.0}}," + "{\"item\":{\"id\":3},\"item\":{\"id\":1.0}}");
        for (int i = 0; i < 2_048; i++) {
          json.append(",{\"item\":{\"id\":0}}");
        }
        json.append(']');
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.INR,
                   Set.of(parse("/[]/item/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), writer);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long firstId = writer.getNodeKey();
        writer.commit();
        writer.moveTo(firstId);
        writer.setNumberValue(0);
        writer.commit();
        writer.moveTo(firstId);
        writer.setNumberValue(2);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var optimized = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final CompileChain generic = new CompileChain();
      final List<String> predicates = List.of("$$.id eq 1", "$$.id gt 1", "$$.id ge 1", "$$.id ge 1 and $$.id lt 2",
          "$$.id gt 1 and $$.id le 2", "$$.id ge 1 and $$.id le 2");
      final List<List<Integer>> expected =
          List.of(List.of(1, 1, 2, 1, 0, 1), List.of(0, 1, 1, 0, 0, 0), List.of(0, 2, 2, 0, 1, 1));
      for (int revision = 1; revision <= 3; revision++) {
        final String document = "jn:doc('duplicates','rows'," + revision + ")";
        for (int i = 0; i < predicates.size(); i++) {
          final String rows = document + "[].item[?" + predicates.get(i) + "]";
          final String count = "count(" + rows + ")";
          assertTrue(QueryPlan.explain(rows, store, context.getNodeStore()).usesIndex(), predicates.get(i));
          assertEquals(expected.get(revision - 1).get(i).intValue(),
              ((Numeric) new Query(generic, count).execute(context)).intValue(), count);
          assertEquals(expected.get(revision - 1).get(i).intValue(),
              ((Numeric) new Query(optimized, count).execute(context)).intValue(), count);
        }
        assertEquals(revision == 1
            ? 3
            : 2,
            ((Numeric) new Query(optimized,
                "count(jn:scan-cas-index(" + document + ",0,1,'==','/[]/item/id'))").execute(context)).intValue());
        assertEquals(revision == 2
            ? 2
            : 3,
            ((Numeric) new Query(optimized,
                "count(jn:scan-cas-index-range(" + document + ",0,1,2,true(),true(),'/[]/item/id'))").execute(
                    context)).intValue());
      }
    }
  }
}
