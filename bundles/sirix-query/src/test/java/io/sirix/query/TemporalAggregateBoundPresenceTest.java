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
import io.sirix.access.trx.node.json.objectvalue.StringValue;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
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
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class TemporalAggregateBoundPresenceTest {
  private static final int[] COUNTS = {3, 3, 1, 1, 1, 1};
  private static final int[] SUMS = {23, 33, 11, 199, 11, 199};

  @TempDir
  File directory;

  static Stream<Arguments> representations() {
    return Arrays.stream(VersioningType.values())
                 .flatMap(versioning -> Stream.of(Type.STR, Type.DATI).map(type -> Arguments.of(versioning, type)));
  }

  @ParameterizedTest
  @MethodSource("representations")
  void boundPresenceIsExactAcrossBuildReplayReopenAndHistory(final VersioningType versioning, final Type type)
      throws Exception {
    final var databasePath = directory.toPath().resolve("bounds");
    final long[] records = new long[5];
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(
          ResourceConfiguration.newBuilder("instants").storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      try (var session = database.beginResourceSession("instants"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"day":1,"ts":"2024-06-15T00:00:00Z"},{"day":2,"ts":"2024-06-30T00:00:00Z"}]
            """));
      }
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .customCommitTimestamps(true)
                                                   .versioningApproach(versioning)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"g":1,"n":5,"hi":"2025-01-01T00:00:00Z"},
             {"g":1,"n":7,"lo":"2024-01-01T00:00:00Z"},
             {"g":1,"n":99},
             {"g":1,"n":11,"lo":"2024-01-01T00:00:00Z","hi":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        for (int row = 0; row < 4; row++) {
          records[row] = writer.getNodeKey();
          writer.moveToRightSibling();
        }
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "bounds");
        final List<Path<QNm>> fields = List.of("g", "n", "lo", "hi")
                                           .stream()
                                           .map(name -> Path.parse("/[]/" + name, PathParser.Type.JSON))
                                           .toList();
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
            List.of(Type.LON, Type.LON, type, type), 0, IndexDef.DbType.JSON);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        controller.createIndexes(Set.of(projection), writer);
        TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"), List.of("count(*)", "sum(n)"));
        writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"));
      }
    }
    assertHistory(1);
    for (int revision = 2; revision <= 6; revision++) {
      try (var database = Databases.openJsonDatabase(databasePath);
          var session = database.beginResourceSession("records");
          var writer = session.beginNodeTrx()) {
        switch (revision) {
          case 2 -> {
            number(writer, records[0], 9);
            number(writer, records[1], 13);
            number(writer, records[2], 199);
          }
          case 3 -> {
            writer.moveTo(records[0]);
            writer.remove();
            writer.moveTo(records[1]);
            writer.remove();
            writer.moveToDocumentRoot();
            writer.moveToFirstChild();
            writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("{\"g\":1,\"n\":101}"),
                JsonNodeTrx.Commit.NO);
            writer.moveToDocumentRoot();
            writer.moveToFirstChild();
            writer.moveToLastChild();
            records[4] = writer.getNodeKey();
          }
          case 4 -> {
            addBound(writer, records[2], "lo");
            removeBound(writer, records[3], "lo");
            removeBound(writer, records[3], "hi");
          }
          case 5 -> {
            removeBound(writer, records[2], "lo");
            addBound(writer, records[3], "hi");
          }
          case 6 -> {
            addBound(writer, records[2], "lo");
            addBound(writer, records[2], "hi");
            removeBound(writer, records[3], "hi");
            writer.moveTo(records[4]);
            writer.remove();
          }
          default -> throw new IllegalStateException("Unexpected revision " + revision);
        }
        writer.commit("bound presence", Instant.parse("2024-0" + revision + "-15T00:00:00Z"));
      }
      assertHistory(revision);
    }
  }

  private void assertHistory(final int published) throws Exception {
    Databases.clearGlobalCaches();
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      for (int revision = 1; revision <= published; revision++) {
        for (final boolean correlated : new boolean[] {false, true}) {
          final String prefix = correlated
              ? "for $d in jn:doc('bounds','instants')[] "
              : "";
          final String point = correlated
              ? "xs:dateTime($d.ts)"
              : "xs:dateTime('2024-06-15T00:00:00Z')";
          final String source =
              "jn:open-bitemporal('bounds','records',xs:dateTime('2024-0" + revision + "-20T00:00:00Z')," + point + ")";
          final String keys = correlated
              ? "$day,$g"
              : "$g";
          final String text = prefix + "for $r in " + source + " let " + (correlated
              ? "$day := $d.day, "
              : "") + "$g := $r.g group by " + keys + " let $n := count($r), $sum := sum($r.n) order by " + keys
              + " return {" + (correlated
                  ? "\"day\":$day,"
                  : "")
              + "\"g\":$g,\"n\":$n,\"sum\":$sum}";
          final String literal = (correlated
              ? "for $day in (1,2) return {\"day\":$day,"
              : "{") + "\"g\":1,\"n\":" + COUNTS[revision - 1] + ",\"sum\":" + SUMS[revision - 1] + "}";
          final String expected = run(oracle, context, literal);
          assertEquals(expected, run(oracle, context, text), "independent valid-time oracle, revision " + revision);
          final long before = TemporalAggregateRoute.servedCount();
          final var result =
              WorkCapture.of(EngineWorkCounters.TEMPORAL_READS)
                         .and(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, EngineWorkCounters.VALID_TIME_INTERVAL_REFS)
                         .call(() -> run(chain, context, text));
          final int bindings = correlated
              ? 2
              : 1;
          assertEquals(expected, result.result(), "aggregate revision " + revision);
          assertEquals(before + bindings, TemporalAggregateRoute.servedCount(),
              "open-ended bounds must remain servable");
          result.work()
                .assertExactly(EngineWorkCounters.TEMPORAL_GROUP_HEADS_DECODED, bindings, "one live group per binding")
                .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "no masked operand scan")
                .assertExactly(EngineWorkCounters.VALID_TIME_INTERVAL_REFS, 0, "no valid-time row enumeration");
        }
      }
    }
  }

  private static void number(final JsonNodeTrx writer, final long object, final int value) {
    field(writer, object, "n");
    writer.moveToFirstChild();
    writer.setNumberValue(value);
  }

  private static void addBound(final JsonNodeTrx writer, final long object, final String name) {
    assertTrue(writer.moveTo(object));
    writer.insertObjectRecordAsLastChild(name, new StringValue(name.equals("lo")
        ? "2024-01-01T00:00:00Z"
        : "2025-01-01T00:00:00Z"));
  }

  private static void removeBound(final JsonNodeTrx writer, final long object, final String name) {
    field(writer, object, name);
    writer.remove();
  }

  private static void field(final JsonNodeTrx writer, final long object, final String name) {
    assertTrue(writer.moveTo(object));
    assertTrue(writer.moveToFirstChild());
    while (!name.equals(writer.getName().getLocalName())) {
      assertTrue(writer.moveToRightSibling());
    }
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
