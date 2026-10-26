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
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.index.changelog.TemporalAggregateSummaryFolds;
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
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.EnumSource;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
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
      for (final int lag : new int[] {0, 1, 8, 16, 17}) {
        try (var context = SirixQueryContext.createWithJsonStore(borrowed);
            var chain = SirixCompileChain.createWithJsonStore(borrowed)) {
          final Query query = new Query(chain, query(lag + 1));
          for (int repeat = 0; repeat < 2; repeat++) {
            final var capture = WorkCapture.of(SERVED, EngineWorkCounters.CHANGE_LOG_CHUNKS_READ,
                EngineWorkCounters.TEMPORAL_PART_CHUNKS_READ, EngineWorkCounters.AGGREGATE_PROJECTION_ROW_GROUPS_READ,
                EngineWorkCounters.PROJECTION_BODY_SEGMENTS, QueryWorkCounters.GROUP_AGGREGATES)
                                           .call(() -> run(query, context));
            assertEquals(expected(rows, lag), capture.result());
            capture.work()
                   .assertExactly(SERVED, 1, "Complete tails must use the aggregate route")
                   .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 0, "Lag must not cause a source scan")
                   .assertExactly(EngineWorkCounters.AGGREGATE_PROJECTION_ROW_GROUPS_READ, 0,
                       "Catch-up never rebuilds aggregates from projection rows");
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
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void foldedOutstandingSummariesBoundLagEightSixteenAndSixtyFour(final VersioningType versioning) throws Exception {
    final var path = directory.toPath().resolve("tail");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
    }
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        JsonResourceSession session = store.lookup("tail").getDatabase().beginResourceSession("records")) {
      build(session, 256, 20, false, 64);
      final DerivedIndexMaintainer.Resource resource =
          Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
      final JsonDBStore borrowed = mock(JsonDBStore.class, delegatesTo(store));
      doNothing().when(borrowed).close();
      try (var context = SirixQueryContext.createWithJsonStore(borrowed);
          var chain = SirixCompileChain.createWithJsonStore(borrowed)) {
        for (int pass = 0; pass < 2; pass++) {
          final boolean reverse = pass != 0;
          final var raw = WorkCapture.of(SERVED, EngineWorkCounters.CHANGE_LOG_CHUNKS_READ,
              EngineWorkCounters.PROJECTION_BODY_SEGMENTS, QueryWorkCounters.GROUP_AGGREGATES)
                                     .call(() -> runHistoricalPoints(chain, context, reverse));
          final StringBuilder expected = new StringBuilder();
          for (int offset = 1; offset <= 64; offset++) {
            expected.append(expected(256, pass == 0
                ? offset
                : 65 - offset));
          }
          assertEquals(expected.toString(), raw.result());
          raw.work()
             .assertExactly(SERVED, 64, "Every historical point must use the aggregate route")
             .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 0, "No raw-tail fallback")
             .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "No source columns")
             .assertBetween(EngineWorkCounters.CHANGE_LOG_CHUNKS_READ, pass == 0
                 ? 64
                 : 0,
                 pass == 0
                     ? 128
                     : 0,
                 "One prefix read per revision; reverse reads reuse immutable snapshots, not the last prefix state");
        }
      }
      while (resource.foldNextSummaryBatch()) {
        assertEquals(1, resource.maintainedRevision(), "Summary folds must not advance the aggregate base");
      }
      for (final int lag : new int[] {8, 16, 64}) {
        Databases.clearGlobalCaches();
        ProjectionIndexCatalog.clearCache();
        ProjectionIndexRegistry.clear();
        try (var context = SirixQueryContext.createWithJsonStore(borrowed);
            var chain = SirixCompileChain.createWithJsonStore(borrowed)) {
          final Query query = new Query(chain, query(lag + 1));
          for (int repeat = 0; repeat < 2; repeat++) {
            final var capture = WorkCapture.of(SERVED, EngineWorkCounters.CHANGE_LOG_CHUNKS_READ,
                EngineWorkCounters.PROJECTION_BODY_SEGMENTS, QueryWorkCounters.GROUP_AGGREGATES)
                                           .call(() -> run(query, context));
            assertEquals(expected(256, lag), capture.result());
            capture.work()
                   .assertExactly(SERVED, 1, "Every complete tail must serve at lag " + lag)
                   .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 0, "No fallback after folding")
                   .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "No source columns")
                   .assertBetween(EngineWorkCounters.CHANGE_LOG_CHUNKS_READ, repeat == 0
                       ? 1
                       : 0,
                       repeat == 0
                           ? 7
                           : 0,
                       "At most seven compact ranges cover this fixture, including lag 64");
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void automaticMaintenanceBoundsOutstandingSummariesDuringContinuingPublications(final VersioningType versioning)
      throws Exception {
    final var path = directory.toPath().resolve("tail");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .versioningApproach(versioning)
                                                   .validTimePaths("lo", "hi")
                                                   .storeDiffs(false)
                                                   .build());
    }
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        JsonResourceSession session = store.lookup("tail").getDatabase().beginResourceSession("records")) {
      build(session, 256, 20, false, 0);
      final DerivedIndexMaintainer.Resource resource =
          Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
      final JsonDBStore borrowed = mock(JsonDBStore.class, delegatesTo(store));
      doNothing().when(borrowed).close();
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        for (int publication = 1; publication <= 64; publication++) {
          publishChanges(writer, 20, false, publication);
          assertTrue(resource.maintainNextBatch());
          assertEquals(publication + 1, resource.maintainedRevision());
        }
        try (StorageEngineReader source = session.createStorageEngineReader();
            StorageEngineReader derived = resource.openSummaryReader()) {
          assertEquals(0, TemporalAggregateSummaryFolds.frontier(derived, source));
        }
        for (int publication = 65; publication <= 128; publication++) {
          publishChanges(writer, 20, false, publication);
        }
        for (int batch = 1; batch <= 8; batch++) {
          assertTrue(resource.maintainNextBatch());
          assertEquals(65, resource.maintainedRevision(), "Fold the outstanding tail before advancing its base");
          try (StorageEngineReader source = session.createStorageEngineReader();
              StorageEngineReader derived = resource.openSummaryReader()) {
            assertEquals(65 + 8 * batch, TemporalAggregateSummaryFolds.frontier(derived, source),
                "Automatic folds must skip already maintained history");
          }
        }
        for (int cycle = 0; cycle <= 8; cycle++) {
          if (cycle > 0) {
            final int previousBase = resource.maintainedRevision();
            assertTrue(resource.maintainNextBatch());
            assertEquals(previousBase + 8, resource.maintainedRevision());
            for (int publication = 121 + 8 * cycle; publication <= 128 + 8 * cycle; publication++) {
              publishChanges(writer, 20, false, publication);
            }
            assertTrue(resource.maintainNextBatch());
            assertEquals(previousBase + 8, resource.maintainedRevision(), "New publications receive a summary fold");
          }
          assertEquals(64, resource.lag());
          Databases.clearGlobalCaches();
          ProjectionIndexCatalog.clearCache();
          ProjectionIndexRegistry.clear();
          try (var context = SirixQueryContext.createWithJsonStore(borrowed);
              var chain = SirixCompileChain.createWithJsonStore(borrowed)) {
            final Query query = new Query(chain, query(session.getMostRecentRevisionNumber()));
            final var capture = WorkCapture.of(SERVED, EngineWorkCounters.CHANGE_LOG_CHUNKS_READ,
                EngineWorkCounters.PROJECTION_BODY_SEGMENTS, QueryWorkCounters.GROUP_AGGREGATES)
                                           .call(() -> run(query, context));
            assertEquals(expected(256, 128 + 8 * cycle), capture.result());
            capture.work()
                   .assertExactly(SERVED, 1, "Automatic maintenance must retain the aggregate route")
                   .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 0, "No fallback during continued publication")
                   .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "No source columns")
                   .assertBetween(EngineWorkCounters.CHANGE_LOG_CHUNKS_READ, 1, 7,
                       "Automatic folding must retain the seven-chunk bound as both frontiers advance");
          }
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void largeRepeatedPublicationFitsTheOriginalTwoChunkBudget(final VersioningType versioning) throws Exception {
    final var path = directory.toPath().resolve("tail");
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
        JsonResourceSession session = store.lookup("tail").getDatabase().beginResourceSession("records")) {
      build(session, 1250, 1250, true, 1);
      final JsonDBStore borrowed = mock(JsonDBStore.class, delegatesTo(store));
      doNothing().when(borrowed).close();
      try (var context = SirixQueryContext.createWithJsonStore(borrowed);
          var chain = SirixCompileChain.createWithJsonStore(borrowed)) {
        final Query query = new Query(chain, query(2));
        for (int repeat = 0; repeat < 2; repeat++) {
          final var capture = WorkCapture.of(SERVED, EngineWorkCounters.CHANGE_LOG_CHUNKS_READ,
              EngineWorkCounters.PROJECTION_BODY_SEGMENTS, EngineWorkCounters.AGGREGATE_PROJECTION_ROW_GROUPS_READ)
                                         .call(() -> run(query, context));
          assertEquals("{\"g\":1,\"c\":1250,\"s\":133750,\"l\":107,\"h\":107}", capture.result());
          capture.work()
                 .assertExactly(SERVED, 1, "Compact summaries must serve a large raw-row publication")
                 .assertBetween(EngineWorkCounters.CHANGE_LOG_CHUNKS_READ, repeat == 0
                     ? 1
                     : 0,
                     repeat == 0
                         ? 2
                         : 0,
                     "The existing per-group chunk bound is unchanged")
                 .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "No source columns in a tail fold")
                 .assertExactly(EngineWorkCounters.AGGREGATE_PROJECTION_ROW_GROUPS_READ, 0, "No projection rebuild");
        }
      }
    }
  }

  @Test
  void reusedContextReplenishesCatchUpAcross600LagOneExecutions() throws Exception {
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
      build(session, 256, 1, true, 0);
      final DerivedIndexMaintainer.Resource resource =
          Objects.requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
      final JsonDBStore borrowed = mock(JsonDBStore.class, delegatesTo(store));
      doNothing().when(borrowed).close();
      Databases.clearGlobalCaches();
      ProjectionIndexCatalog.clearCache();
      ProjectionIndexRegistry.clear();
      try (var context = SirixQueryContext.createWithJsonStore(borrowed);
          var chain = SirixCompileChain.createWithJsonStore(borrowed);
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        writer.moveToRightSibling();
        writer.moveToFirstChild();
        final long number = writer.getNodeKey();
        for (int execution = 1; execution <= 600; execution++) {
          writer.moveTo(number);
          writer.setNumberValue(7 + execution);
          writer.commit();
          final int sourceRevision = session.getMostRecentRevisionNumber();
          assertEquals(sourceRevision - 1, resource.maintainedRevision());
          final Query query = new Query(chain, query(sourceRevision));
          final var capture = WorkCapture
                                         .of(SERVED, EngineWorkCounters.CHANGE_LOG_CHUNKS_READ,
                                             EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                                         .call(() -> run(query, context));
          assertEquals("{\"g\":1,\"c\":256,\"s\":" + (1792 + execution) + ",\"l\":7,\"h\":" + (7 + execution) + "}",
              capture.result(), "execution=" + execution);
          capture.work()
                 .assertExactly(SERVED, 1, "Every independent lag-one execution receives a fresh budget")
                 .assertBetween(EngineWorkCounters.CHANGE_LOG_CHUNKS_READ, 1, 2, "Each publication folds its own tail")
                 .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "No source-column fallback");
          resource.drain();
        }
      }
    }
  }

  private static void build(final JsonResourceSession session, final int rows) {
    build(session, rows, 20, false, 17);
  }

  private static void build(final JsonResourceSession session, final int rows, final int changed,
      final boolean repeated, final int maximumLag) {
    try (JsonNodeTrx writer = session.beginNodeTrx()) {
      final StringBuilder json = new StringBuilder(rows * 110).append('[');
      for (int row = 0; row < rows; row++) {
        if (row != 0) {
          json.append(',');
        }
        json.append("{\"g\":1,\"n\":")
            .append(repeated
                ? 7
                : row)
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
      for (int lag = 1; lag <= maximumLag; lag++) {
        publishChanges(writer, changed, repeated, lag);
      }
      assertEquals(1, resource.maintainedRevision());
    }
  }

  private static void publishChanges(final JsonNodeTrx writer, final int changed, final boolean repeated,
      final int publication) {
    writer.moveToDocumentRoot();
    writer.moveToFirstChild();
    writer.moveToFirstChild();
    for (int row = 0; row < changed; row++) {
      final long record = writer.getNodeKey();
      writer.moveToFirstChild();
      writer.moveToRightSibling();
      writer.moveToFirstChild();
      writer.setNumberValue((repeated
          ? 7
          : row) + 100L * publication);
      writer.moveTo(record);
      writer.moveToRightSibling();
    }
    writer.commit();
  }

  private static String query(final int revision) {
    return query(Integer.toString(revision));
  }

  private static String query(final String revision) {
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

  private static String runHistoricalPoints(final SirixCompileChain chain, final SirixQueryContext context,
      final boolean reverse) throws Exception {
    // Scalar revision loops are not admitted by the grouped route, even without catch-up.
    // Separate admitted queries share one context and exercise the same bounded prefix cache.
    final StringBuilder result = new StringBuilder(64 * 128);
    for (int offset = 1; offset <= 64; offset++) {
      final int revision = reverse
          ? 66 - offset
          : offset + 1;
      result.append(run(new Query(chain, query(revision)), context));
    }
    return result.toString();
  }
}
