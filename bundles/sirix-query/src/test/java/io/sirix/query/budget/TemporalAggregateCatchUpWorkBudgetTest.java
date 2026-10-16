package io.sirix.query.budget;

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
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.translator.TemporalAggregateRoute;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.File;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;

/**
 * Tail work must depend on lag and changed rows, never on the unchanged population or point count.
 */
@Isolated
final class TemporalAggregateCatchUpWorkBudgetTest {
  private static final WorkCounter SERVED = WorkCounter.alwaysOn("served.temporalAggregates",
      "one grouped point answered from temporal aggregates", TemporalAggregateRoute::servedCount);

  @TempDir
  File directory;

  @ParameterizedTest
  @ValueSource(ints = {256, 4096})
  void pausedFrontierUsesAtMostTwoTailChunksPerRevisionAndReusesTheFold(final int rows) throws Exception {
    final var path = directory.toPath().resolve("tail");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
    }
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        JsonResourceSession session = store.lookup("tail").getDatabase().beginResourceSession("records")) {
      build(session, rows);
      Databases.clearGlobalCaches();
      ProjectionIndexCatalog.clearCache();
      ProjectionIndexRegistry.clear();
      final JsonDBStore borrowed = mock(JsonDBStore.class, delegatesTo(store));
      doNothing().when(borrowed).close();
      for (final int lag : new int[] {0, 1, 8, DerivedIndexMaintainer.MAX_QUERY_LAG,
          DerivedIndexMaintainer.MAX_QUERY_LAG + 1}) {
        try (var context = SirixQueryContext.createWithJsonStore(borrowed);
            var chain = SirixCompileChain.createWithJsonStore(borrowed)) {
          final Query query = new Query(chain, query(lag + 1));
          final boolean admitted = lag <= DerivedIndexMaintainer.MAX_QUERY_LAG;
          for (int repeat = 0; repeat < 2; repeat++) {
            final var capture = WorkCapture.of(SERVED, EngineWorkCounters.CHANGE_LOG_CHUNKS_READ,
                EngineWorkCounters.TEMPORAL_PART_CHUNKS_READ, EngineWorkCounters.AGGREGATE_PROJECTION_ROW_GROUPS_READ,
                EngineWorkCounters.PROJECTION_BODY_SEGMENTS, QueryWorkCounters.GROUP_AGGREGATES)
                                           .call(() -> run(query, context));
            assertEquals(expected(rows, lag), capture.result());
            capture.work()
                   .assertExactly(SERVED, admitted
                       ? 1
                       : 0, "Bounded lag must use the aggregate route")
                   .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, admitted
                       ? 0
                       : 1, "Excessive lag must retain the exact masked route")
                   .assertExactly(EngineWorkCounters.AGGREGATE_PROJECTION_ROW_GROUPS_READ, 0,
                       "Catch-up never rebuilds aggregates from projection rows");
            if (admitted) {
              capture.work()
                     .assertBetween(EngineWorkCounters.TEMPORAL_PART_CHUNKS_READ, 1, 3,
                         "The base keeps its existing point-read budget")
                     .assertBetween(EngineWorkCounters.CHANGE_LOG_CHUNKS_READ, repeat == 0
                         ? lag
                         : 0,
                         repeat == 0
                             ? 2L * lag
                             : 0,
                         "Each changed group reads at most two chunks per lagging revision")
                     .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0,
                         "Exact tail folds must not load source columns");
            } else {
              capture.work()
                     .assertExactly(EngineWorkCounters.CHANGE_LOG_CHUNKS_READ, 0,
                         "The lag bound declines before reading the tail");
            }
          }
        }
      }
    }
  }

  private static void build(final JsonResourceSession session, final int rows) {
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      final StringBuilder json = new StringBuilder(rows * 110).append('[');
      for (int row = 0; row < rows; row++) {
        if (row != 0) {
          json.append(',');
        }
        json.append("{\"g\":1,\"n\":")
            .append(row)
            .append(",\"lo\":\"1970-01-01T00:00:00.001Z\",\"hi\":\"1970-01-01T00:00:00.002Z\"}");
      }
      writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
          JsonNodeTrx.Commit.NO);
      ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "tail");
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
      resource.drain();
      for (int lag = 1; lag <= DerivedIndexMaintainer.MAX_QUERY_LAG + 1; lag++) {
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        for (int row = 0; row < 20; row++) {
          final long record = writer.getNodeKey();
          writer.moveToFirstChild();
          writer.moveToRightSibling();
          writer.moveToFirstChild();
          writer.setNumberValue(row + 100L * lag);
          writer.moveTo(record);
          writer.moveToRightSibling();
        }
        writer.commit();
      }
      assertEquals(1, resource.maintainedRevision());
    }
  }

  private static String query(final int revision) {
    return "for $r in jn:doc('tail','records'," + revision + ")[] "
        + "where xs:dateTime($r.lo) le xs:dateTime('1970-01-01T00:00:00.001Z') "
        + "and xs:dateTime('1970-01-01T00:00:00.001Z') lt xs:dateTime($r.hi) "
        + "let $g := $r.g group by $g let $c := count($r), $s := sum($r.n), $l := min($r.n), $h := max($r.n) "
        + "order by $g return {\"g\":$g,\"c\":$c,\"s\":$s,\"l\":$l,\"h\":$h}";
  }

  private static String expected(final int rows, final int lag) {
    return "{\"g\":1,\"c\":" + rows + ",\"s\":" + ((long) rows * (rows - 1) / 2 + 2000L * lag) + ",\"l\":" + (lag == 0
        ? 0
        : 20) + ",\"h\":" + Math.max(rows - 1, 19 + 100 * lag) + "}";
  }

  private static String run(final Query query, final SirixQueryContext context) throws Exception {
    final StringWriter output = new StringWriter();
    try (PrintWriter printer = new PrintWriter(output)) {
      query.serialize(context, printer);
    }
    return output.toString().replaceAll("\\s+", "");
  }
}
