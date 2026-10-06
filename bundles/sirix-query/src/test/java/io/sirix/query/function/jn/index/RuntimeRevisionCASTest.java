package io.sirix.query.function.jn.index;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexDef;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static io.brackit.query.util.path.Path.parse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class RuntimeRevisionCASTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void historicalFilterKeepsTimestamp(final VersioningType versioning) {
    create(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String suffix : List.of("root')[]", "nested').rows[]")) {
        final String source = "jn:open('cas','" + suffix.replace("')", "',xs:dateTime('2020-01-01T00:00:00Z'))");
        final Query query = new Query(chain, source + "[?$$.id eq 1].value");
        assertEquals(List.of(10L), values(query.execute(context)));
        assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr), "CAS filter must route through the index");
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void flworKeepsResidualAndEvaluatesRevisionPerTuple(final VersioningType versioning) {
    create(versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String text = "for $e in [{\"ts\":\"2020-01-01T00:00:00Z\"},{\"ts\":\"2021-01-01T00:00:00Z\"}][] "
          + "for $c in jn:open('cas','root',xs:dateTime($e.ts))[] "
          + "where $c.id eq 1 and $c.value lt 15 return $c.value";
      final Query query = new Query(chain, text);
      assertEquals(List.of(10L), values(query.execute(context)));
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr), "FLWOR must route through CAS");
      assertTrue(contains(chain.getOptimizedAST(), XQ.Selection), "residual must survive");
      final Query both = new Query(chain, text.replace(" and $c.value lt 15", ""));
      assertEquals(List.of(10L, 20L), values(both.execute(context)));
    }
  }

  @Test
  void prologAndIntegerRevisionOperandsStayRuntimeBound() {
    create(VersioningType.FULL);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final Query prolog = new Query(chain, "declare variable $T := xs:dateTime('2020-06-01T00:00:00Z'); "
          + "for $c in jn:open('cas','root',$T)[] where $c.id eq 1 return $c.value");
      assertEquals(List.of(10L), values(prolog.execute(context)));
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
      final Query integer =
          new Query(chain, "for $r in (1,2,1) for $c in jn:doc('cas','root',$r)[] where $c.id eq 1 return $c.value");
      assertEquals(List.of(10L, 20L, 10L), values(integer.execute(context)));
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
      final Query before = new Query(chain,
          "for $c in jn:open('cas','root',xs:dateTime('2019-01-01T00:00:00Z'))[] where $c.id eq 1 return $c.value");
      assertEquals(List.of(), values(before.execute(context)));
    }
  }

  @Test
  void revisionBeforeIndexCreationFallsBackToItsOwnData() {
    final Path databasePath = directory.resolve("cas");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("root").customCommitTimestamps(true).build());
      try (var session = database.beginResourceSession("root"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json(false, 10)), JsonNodeTrx.Commit.NO);
        writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
        session.getWtxIndexController(writer.getRevisionNumber())
               .createIndexes(Set.of(IndexDefs.createCASIdxDef(false, Type.INR,
                   Set.of(parse("/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON)), writer);
        writer.commit(null, Instant.parse("2021-01-01T00:00:00Z"));
      }
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final Query query = new Query(chain, "for $c in jn:open('cas','root',xs:dateTime('2020-01-01T00:00:00Z'))[] "
          + "where $c.id eq 1 and $c.value gt 5 return $c.value");
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
      assertEquals(List.of(10L), values(query.execute(context)));
    }
  }

  @Test
  void revisionOperandsRetainFunctionArgumentConversion() {
    create(VersioningType.FULL);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String source : List.of("jn:open('cas','root',xs:untypedAtomic('2020-01-01T00:00:00Z'))",
          "jn:doc('cas','root',xs:untypedAtomic('1'))", "jn:doc('cas','root',xs:decimal('1.9'))")) {
        final Query query = new Query(chain, "for $c in " + source + "[] where $c.id eq 1 return $c.value");
        assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
        assertEquals(List.of(10L), values(query.execute(context)));
      }
      for (final String source : List.of("jn:open('cas','root','2020-01-01T00:00:00Z')",
          "jn:doc('cas','root',xs:integer('4294967297'))")) {
        assertThrows(QueryException.class, () -> values(
            new Query(chain, "for $c in " + source + "[] where $c.id eq 1 return $c.value").execute(context)));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void insertedCandidatesRetainArrayOrder(final boolean deweyIDs) {
    create(VersioningType.FULL, deweyIDs);
    try (var database = Databases.openJsonDatabase(directory.resolve("cas"));
        var session = database.beginResourceSession("root");
        var writer = session.beginNodeTrx()) {
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{\"id\":1,\"value\":30}"),
          JsonNodeTrx.Commit.NO);
      writer.commit(null, Instant.parse("2022-01-01T00:00:00Z"));
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final Query query = new Query(chain, "for $c in jn:doc('cas','root')[] where $c.id eq 1 return $c.value");
      assertTrue(contains(chain.getOptimizedAST(), XQExt.IndexExpr));
      assertEquals(List.of(30L, 20L), values(query.execute(context)));
    }
  }

  private void create(final VersioningType versioning) {
    create(versioning, false);
  }

  private void create(final VersioningType versioning, final boolean deweyIDs) {
    final Path databasePath = directory.resolve("cas");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      for (final String resource : List.of("root", "nested")) {
        database.createResource(ResourceConfiguration.newBuilder(resource)
                                                     .versioningApproach(versioning)
                                                     .customCommitTimestamps(true)
                                                     .storeDiffs(false)
                                                     .useDeweyIDs(deweyIDs)
                                                     .build());
        try (var session = database.beginResourceSession(resource); var writer = session.beginNodeTrx()) {
          final boolean nested = resource.equals("nested");
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json(nested, 10)), JsonNodeTrx.Commit.NO);
          final var index = IndexDefs.createCASIdxDef(false, Type.INR, Set.of(parse(nested
              ? "/rows/[]/id"
              : "/[]/id", PathParser.Type.JSON)), 0, IndexDef.DbType.JSON);
          session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(index), writer);
          writer.commit(null, Instant.parse("2020-01-01T00:00:00Z"));
          writer.moveToDocumentRoot();
          writer.moveToFirstChild();
          writer.remove();
          writer.moveToDocumentRoot();
          writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json(nested, 20)), JsonNodeTrx.Commit.NO);
          writer.commit(null, Instant.parse("2021-01-01T00:00:00Z"));
        }
      }
    }
  }

  private static String json(final boolean nested, final int value) {
    final String rows = "[{\"id\":1,\"value\":" + value + "},{\"id\":2,\"value\":5}]";
    return nested
        ? "{\"rows\":" + rows + "}"
        : rows;
  }

  private static List<Long> values(final Sequence sequence) {
    final List<Long> result = new ArrayList<>();
    if (sequence != null) {
      try (var iter = sequence.iterate()) {
        Item item;
        while ((item = iter.next()) != null) {
          result.add(((Numeric) item).longValue());
        }
      }
    }
    return result;
  }

  private static boolean contains(final AST node, final int type) {
    if (node.getType() == type) {
      return true;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      if (contains(node.getChild(i), type)) {
        return true;
      }
    }
    return false;
  }
}
