package io.sirix.query;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.io.StorageType;
import io.sirix.query.compiler.translator.TemporalAggregateRoute;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A separate BigInteger model checks exact queries before, during and after asynchronous replay.
 */
@Isolated
final class TemporalAggregateDerivedLagTest {
  @TempDir
  File directory;

  private record Row(long key, int group, long value, int endDay) {
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void declarationChangesUnsupportedTailsAndReissuedRevisionsRemainExact(final VersioningType versioning)
      throws Exception {
    final var path = directory.toPath().resolve("edges");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).build();
        JsonResourceSession session = store.lookup("edges").getDatabase().beginResourceSession("records")) {
      final JsonDBStore borrowed = mock(JsonDBStore.class, delegatesTo(store));
      doNothing().when(borrowed).close();
      final long retained;
      final DerivedIndexMaintainer.Resource resource;
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"), JsonNodeTrx.Commit.NO);
        final Row removed = append(writer, 1, 10, 4);
        retained = append(writer, 2, 20, 4).key();
        // Retaining an empty group must not independently trip the dictionary/live-row guard.
        append(writer, 2, 40, 4);
        append(writer, 2, 50, 4);
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "edges");
        final List<Path<QNm>> fields = List.of("g", "n", "lo", "hi")
                                           .stream()
                                           .map(field -> Path.parse("/[]/" + field, PathParser.Type.JSON))
                                           .toList();
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
            List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
        session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
        writer.commit();
        assertEdgeQuery(borrowed, 1, false);
        final IndexDef aggregate =
            TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer,
                "/[]", List.of("g"), List.of("count(*)", "sum(n)", "min(n)", "max(n)"));
        resource = Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
        resource.pause();
        writer.commit();
        assertEdgeQuery(borrowed, 2, false);
        resource.drain();
        assertEdgeQuery(borrowed, 2, true);
        assertTrue(writer.moveTo(removed.key()));
        writer.remove();
        append(writer, 3, 30, 4);
        writer.commit();
        assertEdgeQuery(borrowed, 3, true);
        setValue(writer, retained, 1.5);
        writer.commit();
        assertEdgeQuery(borrowed, 4, false);
        setValue(writer, retained, 21);
        writer.commit();
        assertEdgeQuery(borrowed, 5, false);
        resource.drain();
        assertEdgeQuery(borrowed, 5, true);
        session.getWtxIndexController(writer.getRevisionNumber()).dropIndexes(Set.of(aggregate), writer);
        writer.commit();
        assertEdgeQuery(borrowed, 6, false);
        TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
            List.of("g"), List.of("count(*)", "sum(n)", "min(n)", "max(n)"));
        writer.commit();
        assertEdgeQuery(borrowed, 7, false);
        resource.drain();
        assertEdgeQuery(borrowed, 7, true);
        writer.truncateTo(2);
      }
      resource.pause();
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        setValue(writer, retained, 23);
        writer.commit();
      }
      assertEdgeQuery(borrowed, 3, null);
      resource.drain();
      assertEdgeQuery(borrowed, 3, true);
    }
  }

  private static void setValue(final JsonNodeTrx writer, final long record, final Number value) {
    assertTrue(writer.moveTo(record));
    writer.moveToFirstChild();
    writer.moveToRightSibling();
    writer.moveToFirstChild();
    writer.setNumberValue(value);
  }

  private static void assertEdgeQuery(final JsonDBStore store, final int revision, final Boolean admitted)
      throws Exception {
    final String query = "for $r in jn:doc('edges','records'," + revision + ")[] "
        + "where xs:dateTime($r.lo) le xs:dateTime('2024-01-02T00:00:00Z') "
        + "and xs:dateTime('2024-01-02T00:00:00Z') lt xs:dateTime($r.hi) "
        + "let $g := $r.g group by $g let $c := count($r), $s := sum($r.n), $l := min($r.n), $h := max($r.n) "
        + "order by $g return {\"g\":$g,\"c\":$c,\"s\":$s,\"l\":$l,\"h\":$h}";
    try (var oracleContext = SirixQueryContext.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String expected = evaluate(oracle, oracleContext, query);
      final long before = TemporalAggregateRoute.servedCount();
      assertEquals(expected, evaluate(chain, context, query), "revision=" + revision);
      if (admitted != null) {
        assertEquals(before + (admitted
            ? 1
            : 0), TemporalAggregateRoute.servedCount(), "revision=" + revision);
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void randomPublicationsStayExactAtEveryMaintainedFrontier(final VersioningType versioning) throws Exception {
    final var databasePath = directory.toPath().resolve("lag");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final List<List<Row>> history = new ArrayList<>();
    history.add(List.of());
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
    }
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lag").getDatabase().beginResourceSession("records");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[]"), JsonNodeTrx.Commit.NO);
      final Random random = new Random(918723L);
      List<Row> rows = new ArrayList<>();
      for (int row = 0; row < 18; row++) {
        rows.add(append(writer, row % 3, row % 2 == 0
            ? Long.MAX_VALUE - row
            : -row, 2 + row % 3));
      }
      ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "lag");
      final List<Path<QNm>> fields = List.of("g", "n", "lo", "hi")
                                         .stream()
                                         .map(field -> Path.parse("/[]/" + field, PathParser.Type.JSON))
                                         .toList();
      final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
          List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
      session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
      TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
          List.of("g"), List.of("count(*)", "sum(n)", "min(n)", "max(n)"));
      final DerivedIndexMaintainer.Resource resource =
          Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
      resource.pause();
      writer.commit();
      history.add(List.copyOf(rows));
      for (int revision = 2; revision <= 20; revision++) {
        if (revision == 20) {
          writer.revertTo(5);
          rows = new ArrayList<>(history.get(5));
        }
        final int index = random.nextInt(rows.size());
        final Row prior = rows.get(index);
        if (revision == 7) {
          assertTrue(writer.moveTo(prior.key()));
          writer.moveToFirstChild();
          writer.moveToFirstChild();
          writer.setNumberValue(3);
          rows.set(index, new Row(prior.key(), 3, prior.value(), prior.endDay()));
        } else if (revision == 8) {
          assertTrue(writer.moveTo(prior.key()));
          writer.moveToLastChild();
          writer.moveToFirstChild();
          writer.setStringValue("2024-01-02T00:00:00Z");
          rows.set(index, new Row(prior.key(), prior.group(), prior.value(), 2));
        } else if (revision % 3 == 0) {
          assertTrue(writer.moveTo(prior.key()));
          writer.remove();
          rows.remove(index);
        } else if (revision % 3 == 1) {
          rows.add(append(writer, random.nextInt(3), random.nextInt(1000) - 500, 2 + random.nextInt(3)));
        } else {
          assertTrue(writer.moveTo(prior.key()));
          writer.moveToFirstChild();
          writer.moveToRightSibling();
          writer.moveToFirstChild();
          final long value = random.nextBoolean()
              ? Long.MIN_VALUE + revision
              : random.nextInt(100);
          writer.setNumberValue(value);
          rows.set(index, new Row(prior.key(), prior.group(), value, prior.endDay()));
        }
        writer.commit();
        history.add(List.copyOf(rows));
      }
      final JsonDBStore borrowedStore = mock(JsonDBStore.class, delegatesTo(store));
      doNothing().when(borrowedStore).close();
      do {
        try (var fresh = SirixQueryContext.createWithJsonStore(borrowedStore)) {
          assertHistory(chain, fresh, session, history);
          final String await = "jn:await-derived(jn:doc('lag','records',20),20,0)";
          assertEquals(resource.maintainedRevision() == 20
              ? "true"
              : "false", evaluate(chain, fresh, await));
        }
      } while (resource.maintainNextBatch(1));
      if (resource.failure() != null) {
        throw new AssertionError("Derived maintenance failed", resource.failure());
      }
      assertEquals(20, resource.maintainedRevision());
      assertEquals("true", evaluate(chain, context, "jn:await-derived(jn:doc('lag','records',20),1)"));
      for (final String arguments : List.of("0,0", "21,0", "1,-1", "18446744073709551617,0",
          "1,18446744073709551616")) {
        assertThrows(QueryException.class,
            () -> evaluate(chain, context, "jn:await-derived(jn:doc('lag','records',20)," + arguments + ")"));
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lag").getDatabase().beginResourceSession("records")) {
      assertHistory(chain, context, session, history);
    }
  }

  private static String evaluate(final SirixCompileChain chain, final SirixQueryContext context, final String query)
      throws Exception {
    final StringWriter output = new StringWriter();
    try (PrintWriter printer = new PrintWriter(output)) {
      new Query(chain, query).serialize(context, printer);
    }
    return output.toString().trim();
  }

  private static Row append(final JsonNodeTrx writer, final int group, final long value, final int endDay) {
    writer.moveToDocumentRoot();
    writer.moveToFirstChild();
    writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"g\":" + group + ",\"n\":" + value
        + ",\"lo\":\"2024-01-01T00:00:00Z\",\"hi\":\"2024-01-0" + endDay + "T00:00:00Z\"}"), JsonNodeTrx.Commit.NO);
    writer.moveToDocumentRoot();
    writer.moveToFirstChild();
    writer.moveToLastChild();
    return new Row(writer.getNodeKey(), group, value, endDay);
  }

  private static void assertHistory(final SirixCompileChain chain, final SirixQueryContext context,
      final JsonResourceSession session, final List<List<Row>> history) throws Exception {
    for (int revision = 1; revision < history.size(); revision++) {
      final String query = "for $r in jn:doc('lag','records'," + revision + ")[] "
          + "where xs:dateTime($r.lo) le xs:dateTime('2024-01-02T00:00:00Z') "
          + "and xs:dateTime('2024-01-02T00:00:00Z') lt xs:dateTime($r.hi) "
          + "let $g := $r.g group by $g let $c := count($r), $s := sum($r.n), $l := min($r.n), $h := max($r.n) "
          + "order by $g return {\"g\":$g,\"c\":$c,\"s\":$s,\"l\":$l,\"h\":$h}";
      final long served = TemporalAggregateRoute.servedCount();
      final StringWriter output = new StringWriter();
      try (PrintWriter printer = new PrintWriter(output)) {
        new Query(chain, query).serialize(context, printer);
      }
      assertEquals(oracle(history.get(revision)), output.toString().replaceAll("\\s+", ""), "revision=" + revision);
      try (StorageEngineReader source = session.createStorageEngineReader(revision);
          StorageEngineReader derived = session.openDerivedIndexReader(source)) {
        final int frontier =
            Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource()).maintainedRevision();
        if (derived == null
            && (frontier == 0 || revision - frontier > DerivedIndexMaintainer.MAX_QUERY_LAG || revision == 20)) {
          assertEquals(served, TemporalAggregateRoute.servedCount(), "Uncovered or discontinuous tails must decline");
        } else {
          assertEquals(served + 1, TemporalAggregateRoute.servedCount(),
              "An exact mapping or bounded tail must serve at revision=" + revision + ", frontier=" + frontier);
        }
      }
    }
  }

  private static String oracle(final List<Row> rows) {
    final StringBuilder result = new StringBuilder();
    for (int group = 0; group < 4; group++) {
      int count = 0;
      BigInteger sum = BigInteger.ZERO;
      long minimum = Long.MAX_VALUE;
      long maximum = Long.MIN_VALUE;
      for (final Row row : rows) {
        if (row.group() == group && row.endDay() > 2) {
          count++;
          sum = sum.add(BigInteger.valueOf(row.value()));
          minimum = Math.min(minimum, row.value());
          maximum = Math.max(maximum, row.value());
        }
      }
      if (count != 0) {
        result.append("{\"g\":")
              .append(group)
              .append(",\"c\":")
              .append(count)
              .append(",\"s\":")
              .append(sum)
              .append(",\"l\":")
              .append(minimum)
              .append(",\"h\":")
              .append(maximum)
              .append('}');
      }
    }
    return result.toString();
  }
}
