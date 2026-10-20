package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.json.Object;
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
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.io.StorageType;
import io.sirix.query.compiler.translator.TemporalAggregateRoute;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

final class TemporalAggregateColumnarOutputTest {
  @TempDir
  File directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void integralGroupsKeepOrderPresenceAndMutableJdmSemantics(final VersioningType versioning) throws Exception {
    assertIntegralGroups(versioning, true);
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void integralGroupsWorkWithoutStoredChildCounts(final VersioningType versioning) throws Exception {
    assertIntegralGroups(versioning, false);
  }

  private void assertIntegralGroups(final VersioningType versioning, final boolean storeChildCount) throws Exception {
    build(versioning, storeChildCount);
    final Item retained;
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      TemporalAggregateTestSupport.await(context);
      for (final String order : List.of("$h descending, $g ascending empty greatest", "$g descending, $h",
          "$n, $g, $h")) {
        final String query = query("$g,$h", order, false);
        final String expected = serialize(new Query(generic, query).execute(context));
        final long served = TemporalAggregateRoute.servedCount();
        final Sequence result = new Query(chain, query).execute(context);
        assertEquals(expected, serialize(result));
        assertEquals(served + 1, TemporalAggregateRoute.servedCount());
        try (final Iter items = result.iterate()) {
          assertInstanceOf(ColumnarRecordSequence.RowRecord.class, items.next());
        }
      }
      // The row with no operand has a zero count and empty extrema, and still permits mutation.
      final Sequence result = new Query(chain, query("$g,$h", "$g,$h", false)).execute(context);
      retained = result.get(Int32.ONE);
      final Object first = assertInstanceOf(Object.class, retained);
      assertNull(first.get(new QNm("min_cost")));
      assertEquals(new Int64(0), first.get(new QNm("n")));
      first.replace(new QNm("min_cost"), new Str("owned"));
      assertEquals(new Str("owned"), first.get(new QNm("min_cost")));
      // Projecting fewer maintained keys must coalesce groups; overflowing sums keep big integers.
      for (final String query : List.of(query("$g", "$g", false), query("$g,$h", "$g,$h", true))) {
        assertEquals(serialize(new Query(generic, query).execute(context)),
            serialize(new Query(chain, query).execute(context)));
      }
    }
    assertEquals(new Str("owned"), assertInstanceOf(Object.class, retained).get(new QNm("min_cost")));
  }

  private static String query(final String groups, final String order, final boolean sum) {
    return """
        for $r in jn:open-bitemporal('output','records',xs:dateTime('2024-02-01T00:00:00Z'),xs:dateTime('2024-03-01T00:00:00Z'))
        let $g := $r.g, $h := $r.h, $cost := $r.n
        group by GROUPS
        let $n := count($cost), $min := min($cost), $max := max($cost)
        SUM_BINDING
        order by ORDER
        return {"g":$g,H_FIELD"n":$n,"min_cost":$min,"max_cost":$max SUM_FIELD}
        """.replace(
        "GROUPS", groups)
           .replace("ORDER", order)
           .replace("H_FIELD", groups.contains("$h")
               ? "\"h\":$h,"
               : "")
           .replace("SUM_BINDING", sum
               ? "let $sum := sum($cost)"
               : "")
           .replace("SUM_FIELD", sum
               ? ",\"sum\":$sum"
               : "");
  }

  private void build(final VersioningType versioning, final boolean storeChildCount) {
    final var databasePath = directory.toPath().resolve("output");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .customCommitTimestamps(true)
                                                   .versioningApproach(versioning)
                                                   .storeChildCount(storeChildCount)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"h":0,"lo":"2024-01-01T00:00:00Z","hi":"2024-06-01T00:00:00Z"},
             {"g":1,"h":1,"n":-9223372036854775808,"lo":"2024-01-01T00:00:00Z","hi":"2024-06-01T00:00:00Z"},
             {"g":2,"h":1,"n":9223372036854775807,"lo":"2024-01-01T00:00:00Z","hi":"2024-06-01T00:00:00Z"},
             {"g":2,"h":1,"n":9223372036854775807,"lo":"2024-01-01T00:00:00Z","hi":"2024-06-01T00:00:00Z"},
             {"g":1,"h":2,"n":7,"lo":"2024-01-01T00:00:00Z","hi":"2024-06-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "output");
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON),
            List.of(Path.parse("/[]/g", PathParser.Type.JSON), Path.parse("/[]/h", PathParser.Type.JSON),
                Path.parse("/[]/n", PathParser.Type.JSON), Path.parse("/[]/lo", PathParser.Type.JSON),
                Path.parse("/[]/hi", PathParser.Type.JSON)),
            List.of(Type.LON, Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        controller.createIndexes(Set.of(projection), writer);
        TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g", "h"),
            List.of("count(n)", "min(n)", "max(n)", "sum(n)"));
        writer.commit("output", Instant.parse("2024-01-15T00:00:00Z"));
      }
    }
    Databases.clearGlobalCaches();
  }

  private static String serialize(final Sequence sequence) {
    final StringWriter output = new StringWriter();
    try (final SirixStringSerializer serializer = new SirixStringSerializer(new PrintWriter(output))) {
      serializer.serialize(sequence);
    }
    return output.toString();
  }
}
