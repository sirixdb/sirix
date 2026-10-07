package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.CompileChain;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class CASLegacySignedZeroTest {
  @TempDir
  Path directory;

  @Test
  void residualFreeZeroEqualityAndEitherRangeBoundMatchTheInterpreter() {
    final Path databasePath = directory.resolve("zero");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows").storageType(StorageType.FILE_CHANNEL)
          .storeDiffs(false).build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder("[{\"item\":{\"id\":1,\"value\":10}},"
            + "{\"item\":{\"id\":0,\"value\":20}}");
        for (int i = 0; i < 2_048; i++) {
          json.append(",{\"item\":{\"id\":2,\"value\":0}}");
        }
        json.append(']');
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.toString()), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.setNumberValue(-0.0d);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(
            IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse("/[]/item/id", PathParser.Type.JSON)), 0,
                IndexDef.DbType.JSON)), writer);
        writer.commit();
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final CompileChain generic = new CompileChain();
      final String source = "jn:doc('zero','rows')[].item";
      for (final String predicate : List.of("$$.id eq 0", "$$.id lt 0", "$$.id le 0", "$$.id gt 0", "$$.id ge 0",
          "$$.id ge -1 and $$.id le 0", "$$.id ge 0 and $$.id le 1")) {
        final String text = source + "[?" + predicate + "].value";
        assertEquals(values(new Query(generic, text).execute(context)), values(new Query(chain, text).execute(context)), predicate);
      }
      final String equality = source + "[?$$.id eq 0].value";
      assertTrue(QueryPlan.explain(equality, store, null).usesIndex());
      assertEquals(List.of(20d), values(new Query(generic, equality).execute(context)));
      assertEquals(List.of(20d), values(new Query(chain, equality).execute(context)));
      assertEquals(List.of(10d), values(new Query(chain, source + "[?$$.id lt 0].value").execute(context)));
    }
  }

  private static List<Double> values(final Sequence sequence) {
    final List<Double> values = new ArrayList<>();
    if (sequence != null) {
      try (var iterator = sequence.iterate()) {
        Item item;
        while ((item = iterator.next()) != null) {
          values.add(((Numeric) item).doubleValue());
        }
      }
    }
    return values;
  }
}
