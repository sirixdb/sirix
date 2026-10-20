package io.sirix.query.budget;

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
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.index.aggregate.DerivedIndexTestSupport;
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexRegistry;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.translator.TemporalAggregateRoute;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class TemporalAggregateDictionaryWorkBudgetTest {
  private static final int CHURN = 16;
  private static final WorkCounter SERVED = WorkCounter.alwaysOn("served.temporalAggregates",
      "one grouped point answered from temporal aggregates", TemporalAggregateRoute::servedCount);
  private static final WorkCapture CAPTURE = WorkCapture.of(EngineWorkCounters.TEMPORAL_READS)
                                                        .and(EngineWorkCounters.HOT_LEAVES)
                                                        .and(SERVED, QueryWorkCounters.GROUP_AGGREGATES);

  @TempDir
  File directory;

  @ParameterizedTest
  @CsvSource({"256,FULL", "4096,FULL", "256,DIFFERENTIAL", "4096,DIFFERENTIAL", "256,INCREMENTAL", "4096,INCREMENTAL",
      "256,SLIDING_SNAPSHOT", "4096,SLIDING_SNAPSHOT"})
  void historySizedDictionariesDeclineBeforeDecoding(final int rows, final VersioningType versioning) throws Exception {
    build(rows, versioning, true, false);
    for (final boolean correlated : new boolean[] {false, true}) {
      for (final int publication : new int[] {0, 1, 2, CHURN / 2 + 1, CHURN + 1, CHURN + 2}) {
        ProjectionIndexCatalog.clearCache();
        ProjectionIndexRegistry.clear();
        Databases.clearGlobalCaches();
        try (var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
            var context = SirixQueryContext.createWithJsonStore(store);
            var chain = SirixCompileChain.createWithJsonStore(store);
            var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
          DerivedIndexTestSupport.await(store.lookup("dictionary").getDatabase());
          Databases.clearGlobalCaches();
          final String query = query(publication, correlated);
          final String expected = expected(rows, publication, correlated);
          for (int temperature = 0; temperature < 2; temperature++) {
            final WorkCapture.Captured<String> answer = CAPTURE.call(() -> run(chain, context, query));
            assertEquals(expected, answer.result());
            final int bindings = correlated
                ? 2
                : 1;
            final boolean initial = publication == 0;
            final boolean empty = publication == CHURN + 2;
            answer.work()
                  .assertExactly(EngineWorkCounters.TEMPORAL_GROUP_HEADS_DECODED, initial
                      ? rows * bindings
                      : 0, "an oversized dictionary copies and decodes no heads")
                  .assertExactly(EngineWorkCounters.TEMPORAL_GROUP_HEADS_VISITED, (initial
                      ? rows
                      : empty
                          ? 1
                          : 2)
                      * bindings, "admission stops after current population plus one, independent of retained groups")
                  .assertExactly(SERVED, initial
                      ? bindings
                      : 0, "only the populated historical dictionary is admitted")
                  .assertBetween(EngineWorkCounters.TEMPORAL_PART_CHUNKS_READ, initial
                      ? rows * bindings
                      : 0,
                      initial
                          ? 3L * rows * bindings
                          : 0,
                      "declined dictionaries read no aggregate nodes")
                  .assertBetween(EngineWorkCounters.HOT_LEAF_LOADS,
                      temperature == 0 && versioning != VersioningType.FULL
                          ? 1
                          : 0,
                      initial
                          ? 64L * rows * bindings
                          : 64L * bindings,
                      "the guard and fallback must not load pages for historical dictionary cardinality")
                  .assertBetween(EngineWorkCounters.HOT_FRAGMENTS_WALKED, 0, initial
                      ? 256L * rows * bindings
                      : 256L * bindings, "cold and warm fragment work stays bounded by the current population");
            if (!initial && !empty) {
              answer.work()
                    .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, bindings,
                        "ordinary and correlated declines retain the masked projection route");
            }
          }
          assertEquals(expected, run(oracle, context, query));
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"16,FULL,true", "256,FULL,true", "16,DIFFERENTIAL,true", "256,DIFFERENTIAL,true",
      "16,INCREMENTAL,true", "256,INCREMENTAL,true", "16,SLIDING_SNAPSHOT,true", "256,SLIDING_SNAPSHOT,true",
      "16,FULL,false", "256,FULL,false", "16,DIFFERENTIAL,false", "256,DIFFERENTIAL,false",
      "16,INCREMENTAL,false", "256,INCREMENTAL,false", "16,SLIDING_SNAPSHOT,false", "256,SLIDING_SNAPSHOT,false"})
  void integralOutputDeclinesOversizedDictionaryOnce(final int rows, final VersioningType versioning,
      final boolean storeChildCount) throws Exception {
    build(rows, versioning, storeChildCount, true);
    for (final int publication : new int[] {0, 1}) {
      for (final boolean ascending : new boolean[] {true, false}) {
        ProjectionIndexCatalog.clearCache();
        ProjectionIndexRegistry.clear();
        Databases.clearGlobalCaches();
        try (var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
            var context = SirixQueryContext.createWithJsonStore(store);
            var chain = SirixCompileChain.createWithJsonStore(store);
            var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
          DerivedIndexTestSupport.await(store.lookup("dictionary").getDatabase());
          Databases.clearGlobalCaches();
          final String query = """
              for $r in jn:open-bitemporal('dictionary','records',xs:dateTime('TX'),xs:dateTime('2024-01-02T00:00:00Z'))
              let $g := $r.g group by $g
              let $count := count($r), $min := min($r.n), $max := max($r.n)
              order by $g DIRECTION
              return {"g":$g,"count":$count,"min":$min,"max":$max}
              """.replace("TX", publicationInstant(publication).toString())
                 .replace("DIRECTION", ascending ? "ascending" : "descending");
          final StringBuilder expected = new StringBuilder();
          for (int position = 0; position < rows; position++) {
            final int group = (ascending ? position : rows - position - 1) + publication;
            final int value = group == rows ? 1 : group + 1;
            expected.append("{\"g\":").append(group).append(",\"count\":1,\"min\":").append(value)
                    .append(",\"max\":").append(value).append('}');
          }
          final String answer = expected.toString();
          for (int temperature = 0; temperature < 2; temperature++) {
            final WorkCapture.Captured<String> captured = CAPTURE.call(() -> run(chain, context, query));
            assertEquals(answer, captured.result());
            captured.work()
                    .assertExactly(EngineWorkCounters.TEMPORAL_GROUP_HEADS_VISITED, rows + publication,
                        "dictionary admission walks at most the current population plus one once")
                    .assertExactly(EngineWorkCounters.TEMPORAL_GROUP_HEADS_DECODED, publication == 0 ? rows : 0,
                        "a declined primitive dictionary decodes no heads")
                    .assertExactly(SERVED, publication == 0 ? 1 : 0,
                        "the initial integral dictionary is served and the changed dictionary declines");
            if (publication == 1) {
              captured.work().assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1,
                  "dictionary decline retains the masked projection fallback");
            }
          }
          assertEquals(answer, run(oracle, context, query));
        }
      }
    }
  }

  private void build(final int rows, final VersioningType versioning, final boolean storeChildCount,
      final boolean singleGroupChange) {
    final var path = directory.toPath().resolve("dictionary");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(
          ResourceConfiguration.newBuilder("instants").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("instants");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"day":1,"ts":"2024-01-02T00:00:00Z"},{"day":2,"ts":"2024-01-03T00:00:00Z"}]
            """));
      }
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .customCommitTimestamps(true)
                                                   .versioningApproach(versioning)
                                                   .storeChildCount(storeChildCount)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        final StringBuilder json = new StringBuilder(rows * 100).append('[');
        for (int row = 0; row < rows; row++) {
          if (row != 0) {
            json.append(',');
          }
          json.append("{\"g\":")
              .append(row)
              .append(",\"n\":")
              .append(row + 1)
              .append(",\"lo\":\"2024-01-01T00:00:00Z\",\"hi\":\"2025-01-01T00:00:00Z\"}");
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
            JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "dictionary");
        final List<Path<QNm>> columns = List.of("g", "n", "lo", "hi")
                                            .stream()
                                            .map(field -> Path.parse("/[]/" + field, PathParser.Type.JSON))
                                            .toList();
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), columns,
            List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        controller.createIndexes(Set.of(projection), writer);
        TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"), singleGroupChange
            ? List.of("count(*)", "min(n)", "max(n)")
            : List.of("count(*)", "sum(n)"));
        writer.commit("initial", publicationInstant(0));
        DerivedIndexTestSupport.await(session);
        if (singleGroupChange) {
          writer.moveTo(first);
          writer.moveToFirstChild();
          writer.moveToFirstChild();
          writer.setNumberValue(rows);
          writer.commit("change one group", publicationInstant(1));
          DerivedIndexTestSupport.await(session);
          return;
        }
        for (int row = 1; row < rows; row++) {
          writer.moveTo(first);
          assertTrue(writer.moveToRightSibling());
          writer.remove();
        }
        writer.commit("delete almost all groups", publicationInstant(1));
        DerivedIndexTestSupport.await(session);
        for (int change = 0; change < CHURN; change++) {
          writer.moveTo(first);
          writer.moveToFirstChild();
          writer.moveToFirstChild();
          writer.setNumberValue(rows + change);
          writer.commit("change group", publicationInstant(change + 2));
          DerivedIndexTestSupport.await(session);
        }
        writer.moveTo(first);
        writer.remove();
        writer.commit("empty", publicationInstant(CHURN + 2));
        DerivedIndexTestSupport.await(session);
      }
    }
  }

  private static Instant publicationInstant(final int publication) {
    return Instant.parse("2024-02-01T00:00:00Z").plusSeconds(publication * 86400L);
  }

  private static String query(final int publication, final boolean correlated) {
    final String instant = correlated
        ? "xs:dateTime($d.ts)"
        : "xs:dateTime('2024-01-02T00:00:00Z')";
    return (correlated
        ? "for $d in jn:doc('dictionary','instants')[] "
        : "")
        + "for $r in jn:open-bitemporal('dictionary','records',xs:dateTime('" + publicationInstant(publication) + "'),"
        + instant + ") let " + (correlated
            ? "$day := $d.day, "
            : "")
        + "$g := $r.g group by " + (correlated
            ? "$day,$g"
            : "$g")
        + " let $count := count($r), $sum := sum($r.n) order by " + (correlated
            ? "$day,$g"
            : "$g")
        + " return {" + (correlated
            ? "\"day\":$day,"
            : "")
        + "\"g\":$g,\"count\":$count,\"sum\":$sum}";
  }

  private static String expected(final int rows, final int publication, final boolean correlated) {
    if (publication == CHURN + 2) {
      return "";
    }
    final StringBuilder answer = new StringBuilder();
    for (int day = 1; day <= (correlated
        ? 2
        : 1); day++) {
      for (int row = 0; row < (publication == 0
          ? rows
          : 1); row++) {
        final int group = publication < 2
            ? row
            : rows + publication - 2;
        answer.append('{');
        if (correlated) {
          answer.append("\"day\":").append(day).append(',');
        }
        answer.append("\"g\":").append(group).append(",\"count\":1,\"sum\":").append(row + 1).append('}');
      }
    }
    return answer.toString();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String query)
      throws Exception {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(output, false, StandardCharsets.UTF_8)) {
      new Query(chain, query).serialize(context, writer);
      writer.flush();
      return output.toString(StandardCharsets.UTF_8).trim().replaceAll("\\s+", "");
    }
  }
}
