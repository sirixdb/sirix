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
import io.sirix.io.StorageType;
import io.sirix.query.compiler.translator.TemporalAggregateRoute;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class TemporalAggregateIndexTest {
  @TempDir
  File directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void columnsAndMaintenanceAreExactAfterColdReopen(final VersioningType versioning) throws Exception {
    final var databasePath = directory.toPath().resolve("aggregate-test");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("instants").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("instants");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"day":1,"ts":"2024-01-20T00:00:00Z"},{"day":2,"ts":"2024-02-20T00:00:00Z"}]
            """));
      }
      database.createResource(ResourceConfiguration.newBuilder("inventory")
                                                   .validTimePaths("begin", "end")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .customCommitTimestamps(true)
                                                   .buildPathSummary(true)
                                                   .versioningApproach(versioning)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("inventory");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"zone":"north","tier":1,"price":5,"units":3,"begin":"2024-01-01T00:00:00Z","end":"2024-06-01T00:00:00Z"},
             {"zone":"north","tier":2,"price":20,"units":2,"begin":"2024-02-01T00:00:00Z","end":"2024-07-01T00:00:00Z"},
             {"zone":"south","tier":1,"price":-7,"units":4,"begin":"2024-01-01T00:00:00Z","end":"2024-07-01T00:00:00Z"},
             {"tier":3,"begin":"2024-01-01T00:00:00Z","end":"2024-07-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        final long array = writer.getNodeKey();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        writer.moveToRightSibling();
        final long second = writer.getNodeKey();
        writer.moveToRightSibling();
        final long third = writer.getNodeKey();
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "aggregate-test");
        final List<Path<QNm>> fields = new ArrayList<>();
        final List<Type> types = new ArrayList<>();
        for (final String field : List.of("zone", "tier", "price", "units", "begin", "end")) {
          fields.add(Path.parse("/[]/" + field, PathParser.Type.JSON));
          types.add(Set.of("zone", "begin", "end").contains(field)
              ? Type.STR
              : Type.LON);
        }
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
            types, 0, IndexDef.DbType.JSON);
        controller.createIndexes(Set.of(projection), writer);
        TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("zone", "tier"),
            List.of("count(*)", "sum(units)", "sum(price*units)", "min(price)", "max(price)"));
        writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"));
        writer.moveTo(first);
        writer.remove();
        field(writer, second, "price");
        writer.setNumberValue(11);
        field(writer, third, "zone");
        writer.setStringValue("north");
        field(writer, third, "end");
        writer.setStringValue("2024-03-01T00:00:00Z");
        writer.moveTo(array);
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("""
            {"zone":"south","tier":2,"price":99,"units":1,"begin":"2024-04-01T00:00:00Z","end":"2024-08-01T00:00:00Z"}
            """), JsonNodeTrx.Commit.NO);
        writer.commit("changed", Instant.parse("2024-02-15T00:00:00Z"));
        for (int day = 16; day <= 27; day++) {
          field(writer, second, "price");
          writer.setNumberValue(day == 27
              ? 11
              : day);
          if (day == 20) {
            session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
            writer.setNumberValue(35);
            session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
            writer.setNumberValue(day);
          }
          writer.commit("cross fragment window", Instant.parse("2024-02-" + day + "T00:00:00Z"));
        }
        field(writer, second, "price");
        writer.setNumberValue(-999);
        session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
        writer.rollback();
        field(writer, second, "price");
        writer.setNumberValue(1.5);
        writer.commit("row-local poison", Instant.parse("2024-02-28T00:00:00Z"));
        field(writer, second, "price");
        writer.setNumberValue(2);
        writer.commit("poison recovered", Instant.parse("2024-03-01T00:00:00Z"));
        writer.revertTo(1);
        field(writer, first, "price");
        writer.setNumberValue(42);
        session.getWtxIndexController(writer.getRevisionNumber()).applyPendingIndexMaintenance();
        writer.setNumberValue(43);
        writer.commit("represented revision", Instant.parse("2024-03-15T00:00:00Z"));
      }
    }
    Databases.clearGlobalCaches();
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (final String tx : List.of("2024-01-20T00:00:00Z", "2024-02-20T00:00:00Z", "2024-02-27T00:00:00Z",
          "2024-02-28T00:00:00Z", "2024-03-02T00:00:00Z", "2024-03-20T00:00:00Z")) {
        for (final String point : List.of("2024-01-01T00:00:00Z", "2024-02-01T00:00:00Z", "2024-03-01T00:00:00Z",
            "2024-04-01T00:00:00Z", "2024-06-01T00:00:00Z", "2024-07-01T00:00:00Z", "2024-08-01T00:00:00Z")) {
          final String source =
              "jn:open-bitemporal('aggregate-test','inventory',xs:dateTime('" + tx + "'),xs:dateTime('" + point + "'))";
          final String body =
              """
                  for $r in SOURCE
                  let $zone := $r.zone, $amount := $r.price * $r.units
                  group by $zone
                  let $n := count($r), $units := sum($r.units), $amount := sum($amount), $low := min($r.price), $high := max($r.price)
                  order by $zone
                  return {"zone":$zone,"n":$n,"units":$units,"amount":$amount,"low":$low,"high":$high}
                  """;
          final String expected = run(chain, context, body.replace("SOURCE", "(" + source + ")"));
          final long before = TemporalAggregateRoute.servedCount();
          assertEquals(expected, run(chain, context, body.replace("SOURCE", source)), tx + " / " + point);
          assertEquals(before + (tx.startsWith("2024-01") || tx.startsWith("2024-03-20")
              ? 1
              : 0), TemporalAggregateRoute.servedCount(),
              "Current-sized historical dictionaries serve; retained group excess and unsupported rows decline");
        }
      }
      final String unorderedSource = "jn:open-bitemporal('aggregate-test','inventory',"
          + "xs:dateTime('2024-01-20T00:00:00Z'),xs:dateTime('2024-04-01T00:00:00Z'))";
      final String unordered =
          "for $r in SOURCE let $zone := $r.zone group by $zone " + "return {\"zone\":$zone,\"n\":count($r)}";
      final String unorderedExpected = run(chain, context, unordered.replace("SOURCE", "(" + unorderedSource + ")"));
      final long unorderedBefore = TemporalAggregateRoute.servedCount();
      assertEquals(unorderedExpected, run(chain, context, unordered.replace("SOURCE", unorderedSource)));
      assertEquals(unorderedBefore, TemporalAggregateRoute.servedCount(),
          "First-seen ordering uses the existing route");
      final String nonExact =
          """
              for $r in jn:open-bitemporal('aggregate-test','inventory',xs:dateTime('2024-02-20T00:00:00Z'),xs:dateTime('2024-04-01T00:00:00.000001Z'))
              let $zone := $r.zone
              group by $zone
              order by $zone
              return {"zone":$zone,"n":count($r)}
              """;
      final long before = TemporalAggregateRoute.servedCount();
      assertTrue(!run(chain, context, nonExact).isEmpty());
      assertEquals(before, TemporalAggregateRoute.servedCount(), "Sub-millisecond queries must fall back");
      for (final boolean transactionAxis : new boolean[] {true, false}) {
        final String source = "jn:open-bitemporal('aggregate-test','inventory'," + (transactionAxis
            ? "xs:dateTime($d.ts)"
            : "xs:dateTime('2024-02-20T00:00:00Z')") + ","
            + (transactionAxis
                ? "xs:dateTime('2024-04-01T00:00:00Z')"
                : "xs:dateTime($d.ts)")
            + ")";
        final String correlated = """
            for $d in jn:doc('aggregate-test','instants')[]
            for $r in SOURCE
            let $day := $d.day, $zone := $r.zone, $value := $r.price * $r.units
            group by $day,$zone
            let $n := count($r), $units := sum($r.units), $amount := sum($value)
            order by $day,$zone
            return {"day":$day,"zone":$zone,"n":$n,"units":$units,"amount":$amount}
            """;
        final String expected = run(chain, context, correlated.replace("SOURCE", "(" + source + ")"));
        final long served = TemporalAggregateRoute.servedCount();
        assertEquals(expected, run(chain, context, correlated.replace("SOURCE", source)));
        assertEquals(served + (transactionAxis
            ? 1
            : 0), TemporalAggregateRoute.servedCount(),
            "Each correlated revision applies its own dictionary population bound");
      }
    }
  }

  @Test
  void widensSumsDeclinesProductsAndPreservesDroppedHistory() throws Exception {
    final var databasePath = directory.toPath().resolve("overflow");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("values")
                                                   .validTimePaths("begin", "end")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .customCommitTimestamps(true)
                                                   .buildPathSummary(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("values");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"g":1,"v":9223372036854775807,"m":1,"begin":"2024-01-01T00:00:00Z"},
             {"g":1,"v":9223372036854775807,"m":1,"begin":"2024-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "overflow");
        final List<Path<QNm>> fields = new ArrayList<>();
        for (final String name : List.of("g", "v", "m", "begin", "end")) {
          fields.add(Path.parse("/[]/" + name, PathParser.Type.JSON));
        }
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
            List.of(Type.LON, Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
        controller.createIndexes(Set.of(projection), writer);
        // The declaration must build from pending projection changes as well as committed columns.
        field(writer, first, "m");
        writer.setNumberValue(0);
        final IndexDef aggregate = TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"),
            List.of("sum(v)", "sum(v*m)"));
        writer.commit("exact", Instant.parse("2024-01-15T00:00:00Z"));
        field(writer, first, "m");
        writer.setNumberValue(2);
        writer.commit("product overflow", Instant.parse("2024-02-15T00:00:00Z"));
        writer.moveTo(first);
        writer.remove();
        writer.commit("recovered", Instant.parse("2024-03-15T00:00:00Z"));
        session.getWtxIndexController(writer.getRevisionNumber()).dropIndexes(Set.of(aggregate), writer);
        writer.commit("dropped", Instant.parse("2024-04-15T00:00:00Z"));
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      for (int month = 1; month <= 4; month++) {
        final String source = "jn:open-bitemporal('overflow','values',xs:dateTime('2024-0" + month
            + "-20T00:00:00Z'),xs:dateTime('2024-05-01T00:00:00Z'))";
        final String query = """
            for $r in SOURCE
            let $g := $r.g, $product := $r.v * $r.m
            group by $g
            order by $g
            return {"g":$g,"n":count($r),"sum":sum($r.v),"product":sum($product)}
            """;
        final String expected = run(chain, context, query.replace("SOURCE", "(" + source + ")"));
        final long before = TemporalAggregateRoute.servedCount();
        assertEquals(expected, run(chain, context, query.replace("SOURCE", source)), "month " + month);
        assertEquals(before + (month == 1 || month == 3
            ? 1
            : 0), TemporalAggregateRoute.servedCount());
        if (month == 1) {
          assertTrue(expected.contains("18446744073709551614"), "Sum must widen without loss");
          final String minimum =
              "for $r in SOURCE let $g := $r.g group by $g order by $g " + "return {\"g\":$g,\"min\":min($r.v)}";
          final String minimumExpected = run(chain, context, minimum.replace("SOURCE", "(" + source + ")"));
          final long prior = TemporalAggregateRoute.servedCount();
          assertEquals(minimumExpected, run(chain, context, minimum.replace("SOURCE", source)));
          assertEquals(prior, TemporalAggregateRoute.servedCount(), "An undeclared operation must fall back");
        }
      }
      final JsonDBCollection collection = store.lookup("overflow");
      final JsonResourceSession session = collection.getDatabase().beginResourceSession("values");
      assertThrows(Exception.class, () -> run(chain, context,
          "jn:create-temporal-aggregate-index(jn:doc('overflow','values'),'/[]','absent','sum(v)')"));
      assertTrue(session.getNodeTrx().isEmpty(), "A failed declaration must release a writer it opened");
      run(chain, context, "jn:create-temporal-aggregate-index(jn:doc('overflow','values'),'/[]','g','sum(v)')");
      run(chain, context, "sdb:commit(jn:doc('overflow','values'))");
      final var indexes = session.getRtxIndexController(session.getMostRecentRevisionNumber()).getIndexes();
      assertEquals(1, indexes.getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE));
      assertTrue(indexes.getIndexDef(1, IndexType.TEMPORAL_AGGREGATE) != null,
          "A recreated declaration must not reuse the dropped physical root");
      run(chain, context, "jn:drop-temporal-aggregate-index(jn:doc('overflow','values'),1)");
      run(chain, context, "sdb:commit(jn:doc('overflow','values'))");
      assertEquals(0,
          session.getRtxIndexController(session.getMostRecentRevisionNumber())
                 .getIndexes()
                 .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE));
    }
  }

  private static void field(final JsonNodeTrx writer, final long object, final String field) {
    writer.moveTo(object);
    writer.moveToFirstChild();
    while (!field.equals(writer.getName().getLocalName())) {
      if (!writer.moveToRightSibling()) {
        throw new IllegalStateException("Missing field " + field);
      }
    }
    writer.moveToFirstChild();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String text)
      throws Exception {
    TemporalAggregateTestSupport.await(context);
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(output, false, StandardCharsets.UTF_8)) {
      new Query(chain, text).serialize(context, writer);
      writer.flush();
      return output.toString(StandardCharsets.UTF_8).trim();
    }
  }
}
