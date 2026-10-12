package io.sirix.query;

import io.brackit.query.Query;
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
import static org.junit.jupiter.api.Assertions.assertNull;
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
      for (int revision = 2; revision <= 14; revision++) {
        if (revision == 10) {
          writer.revertTo(5);
          rows = new ArrayList<>(history.get(5));
        }
        final int index = random.nextInt(rows.size());
        final Row prior = rows.get(index);
        if (revision % 3 == 0) {
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
      do {
        assertHistory(chain, context, session, history);
      } while (resource.maintainNextBatch());
      assertEquals(14, resource.maintainedRevision());
      assertNull(resource.failure());
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lag").getDatabase().beginResourceSession("records")) {
      assertHistory(chain, context, session, history);
    }
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
        if (derived == null) {
          assertEquals(served, TemporalAggregateRoute.servedCount(), "A lagging mapping must decline");
        } else {
          assertEquals(served + 1, TemporalAggregateRoute.servedCount(), "An exact mapping must serve");
        }
      }
    }
  }

  private static String oracle(final List<Row> rows) {
    final StringBuilder result = new StringBuilder();
    for (int group = 0; group < 3; group++) {
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
