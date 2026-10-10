package io.sirix.query;

import io.brackit.query.Query;
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
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class TemporalAggregateCommitRegressionTest {
  @TempDir
  File directory;

  static Stream<Arguments> ddlCases() {
    return Stream.of("create", "aggregate-one", "aggregate-all", "projection-one", "projection-all")
        .flatMap(action -> Stream.of(false, true).flatMap(owned -> IntStream.rangeClosed(1, 3)
            .mapToObj(signature -> Arguments.of(action, owned, signature))));
  }

  @ParameterizedTest
  @MethodSource("ddlCases")
  void historicalDdlSurvivesCommitAndColdReopen(final String action, final boolean callerOwned,
      final int signature) throws Exception {
    final long first = build(!action.equals("create"));
    final int value = action.equals("create") ? 7 : 1;
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records")) {
      final JsonNodeTrx existing = callerOwned ? session.beginNodeTrx() : null;
      try {
        if (existing != null) {
          number(existing, first, 99);
          assertEquals(2, existing.getStorageEngineWriter().getRevisionToRepresent());
        }
        final String document = "jn:doc('lineage','records',1)";
        final String argument;
        if (action.equals("create")) {
          run(chain, context, "jn:create-temporal-aggregate-index(" + document + ",'/[]','g',('count(*)','sum(n)'))");
          final JsonNodeTrx writer = session.getNodeTrx().orElseThrow();
          assertEquals(1, writer.getStorageEngineWriter().getRevisionToRepresent());
          number(writer, first, value);
          argument = document;
        } else {
          final String function = action.startsWith("aggregate") ? "temporal-aggregate" : "projection";
          argument = "jn:drop-" + function + "-index(" + document + (action.endsWith("one") ? ",0" : "") + ")";
        }
        assertEquals("3", run(chain, context, commit(argument, signature)));
        final JsonNodeTrx writer = session.getNodeTrx().orElseThrow();
        if (existing != null) {
          assertSame(existing, writer);
        }
        assertEquals(3, session.getMostRecentRevisionNumber());
        assertEquals(3, writer.getStorageEngineWriter().getRevisionToRepresent());
        assertEquals(4, writer.getRevisionNumber());
        writer.getStorageEngineWriter().assertTransactionWritable();
        writer.moveTo(first);
        writer.moveToFirstChild();
        writer.moveToRightSibling();
        writer.moveToFirstChild();
        assertEquals(value, writer.getNumberValue().intValue());
        writer.close();
      } finally {
        if (existing != null && !existing.isClosed()) {
          existing.close();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records")) {
      assertEquals(3, session.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= 3; revision++) {
        final boolean latest = revision == 3;
        final var indexes = session.getRtxIndexController(revision).getIndexes();
        assertEquals(latest && action.startsWith("projection") ? 0 : 1,
            indexes.getNrOfIndexDefsWithType(IndexType.PROJECTION));
        assertEquals(latest ? (action.equals("create") ? 1 : 0) : (action.equals("create") ? 0 : 1),
            indexes.getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE));
        final String query = grouped(revision);
        final int expected = latest ? value : revision;
        final String result = "{\"g\":1,\"count\":1,\"sum\":" + expected + "}";
        assertEquals(result, run(oracle, context, query));
        assertEquals(result, run(chain, context, query));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void ordinaryCommitOnlyRevertsADifferentRepresentedRevision(final boolean alreadyBased) throws Exception {
    final long first = build(true);
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      if (alreadyBased) {
        writer.revertTo(1);
      }
      number(writer, first, 7);
      assertEquals("3", run(chain, context, commit("jn:doc('lineage','records',1)", 1)));
      assertSame(writer, session.getNodeTrx().orElseThrow());
      assertEquals("{\"g\":1,\"count\":1,\"sum\":" + (alreadyBased ? 7 : 1) + "}",
          run(chain, context, grouped(3)));
    }
  }

  @Test
  void commitThroughAWriterDocumentRetainsOrdinaryEdits() throws Exception {
    final long first = build(true);
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      number(writer, first, 7);
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      context.bind(new QNm("document"), new JsonDBArray(writer, store.lookup("lineage")));
      assertEquals("3", run(chain, context, "declare variable $document external; sdb:commit($document)"));
      assertSame(writer, session.getNodeTrx().orElseThrow());
      assertEquals("{\"g\":1,\"count\":1,\"sum\":7}", run(chain, context, grouped(3)));
    }
  }

  private long build(final boolean aggregate) {
    final var path = directory.toPath().resolve("lineage");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records").validTimePaths("lo", "hi")
          .storageType(StorageType.FILE_CHANNEL).customCommitTimestamps(true).storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"g":1,"n":1,"lo":"2024-01-01T00:00:00Z","hi":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "lineage");
        final List<Path<QNm>> fields = List.of("g", "n", "lo", "hi").stream()
            .map(field -> Path.<QNm>parse("/[]/" + field, PathParser.Type.JSON)).toList();
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
            List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        controller.createIndexes(Set.of(projection), writer);
        if (aggregate) {
          TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"), List.of("count(*)", "sum(n)"));
        }
        writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"));
        number(writer, first, 2);
        writer.commit("latest", Instant.parse("2024-02-15T00:00:00Z"));
        return first;
      }
    }
  }

  private static void number(final JsonNodeTrx writer, final long first, final int value) {
    assertTrue(writer.moveTo(first));
    writer.moveToFirstChild();
    writer.moveToRightSibling();
    writer.moveToFirstChild();
    writer.setNumberValue(value);
  }

  private static String commit(final String document, final int signature) {
    return "sdb:commit(" + document + (signature > 1 ? ",'historical DDL'" : "")
        + (signature == 3 ? ",xs:dateTime('2024-03-15T00:00:00Z')" : "") + ")";
  }

  private static String grouped(final int revision) {
    return "for $r in jn:doc('lineage','records'," + revision + ")[] "
        + "where xs:dateTime($r.lo) le xs:dateTime('2024-01-02T00:00:00Z') "
        + "and xs:dateTime('2024-01-02T00:00:00Z') lt xs:dateTime($r.hi) "
        + "let $g := $r.g group by $g let $count := count($r), $sum := sum($r.n) "
        + "order by $g return {\"g\":$g,\"count\":$count,\"sum\":$sum}";
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String query)
      throws Exception {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream(); PrintWriter writer = new PrintWriter(output)) {
      new Query(chain, query).serialize(context, writer);
      writer.flush();
      return output.toString().trim().replaceAll("\\s+", "");
    }
  }
}
