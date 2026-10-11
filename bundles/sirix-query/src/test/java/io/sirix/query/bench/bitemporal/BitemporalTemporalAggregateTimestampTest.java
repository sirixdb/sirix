package io.sirix.query.bench.bitemporal;

import io.brackit.query.Query;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.index.changelog.ChangeLogSlab;
import io.sirix.index.changelog.ChangeLogStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
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
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class BitemporalTemporalAggregateTimestampTest {
  @TempDir
  Path directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void sh1PointQueriesServeTypedBoundsAcrossMaintenanceAndHistory(final VersioningType versioning) throws Exception {
    final Path path = directory.resolve("bt");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (var database = Databases.openJsonDatabase(path)) {
      for (final String resource : List.of("epochs", "days", "contracts")) {
        final var configuration = ResourceConfiguration.newBuilder(resource)
                                                       .storageType(StorageType.FILE_CHANNEL)
                                                       .customCommitTimestamps(true)
                                                       .versioningApproach(versioning)
                                                       .storeDiffs(false);
        if (resource.equals("contracts")) {
          configuration.validTimePaths("vf", "vt");
        }
        database.createResource(configuration.build());
      }
      try (var session = database.beginResourceSession("epochs"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"epoch":1,"ts":"2024-01-20T00:00:00Z"},
             {"epoch":2,"ts":"2024-08-20T00:00:00Z"},
             {"epoch":3,"ts":"2024-11-20T00:00:00Z"}]
            """));
      }
      try (var session = database.beginResourceSession("days"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"day_no":149,"ts":"2024-05-29T00:00:00Z"},
             {"day_no":150,"ts":"2024-06-01T00:00:00Z"},
             {"day_no":165,"ts":"2024-06-15T00:00:00Z"},
             {"day_no":180,"ts":"2024-06-30T00:00:00Z"},
             {"day_no":210,"ts":"2024-07-29T00:00:00Z"}]
            """));
      }
      try (var session = database.beginResourceSession("contracts"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"id":1,"pid":1,"sid":1,"cost":5,"qty":3,"grade":1,"vf":"2024-01-01T00:00:00Z","vt":"2024-06-30T00:00:00Z"},
             {"id":2,"pid":1,"sid":1,"cost":20,"qty":2,"grade":1,"vf":"2024-06-15T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
             {"id":3,"pid":1,"sid":2,"cost":7,"qty":4,"grade":2,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "bt");
        BitemporalProjections.declare(session, writer, "contracts");
        final var metadata = ProjectionIndexMetadata.requireReady(writer.getStorageEngineWriter(), 0);
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, metadata.columnKinds()[6]);
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, metadata.columnKinds()[7]);
        BitemporalTemporalAggregates.declare(session, writer, "contracts");
        writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"));
      }
    }
    assertSh1Queries();
    final long second;
    try (var database = Databases.openJsonDatabase(path);
        var session = database.beginResourceSession("contracts"); var writer = session.beginNodeTrx()) {
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      writer.moveToFirstChild();
      final long first = writer.getNodeKey();
      writer.moveToRightSibling();
      second = writer.getNodeKey();
      field(writer, first, "cost");
      writer.setNumberValue(11);
      field(writer, first, "qty");
      writer.setNumberValue(6);
      field(writer, first, "vt");
      writer.setStringValue("2024-06-15T00:00:00Z");
      field(writer, second, "vf");
      writer.setStringValue("2024-06-01T00:00:00Z");
      field(writer, second, "vt");
      writer.setStringValue("2024-07-01T00:00:00Z");
      writer.commit("columns", Instant.parse("2024-08-15T00:00:00Z"));
    }
    assertSh1Queries();
    try (var database = Databases.openJsonDatabase(path);
        var session = database.beginResourceSession("contracts"); var writer = session.beginNodeTrx()) {
      writer.moveTo(second);
      writer.remove();
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("""
          {"id":4,"pid":1,"sid":1,"cost":9,"qty":8,"grade":1,"vf":"2024-06-15T00:00:00Z","vt":"2024-06-30T00:00:00Z"}
          """), JsonNodeTrx.Commit.NO);
      writer.commit("membership", Instant.parse("2024-11-15T00:00:00Z"));
    }
    assertSh1Queries();
    try (var database = Databases.openJsonDatabase(path); var session = database.beginResourceSession("contracts")) {
      for (int revision = 2; revision <= 3; revision++) {
        try (var reader = session.beginNodeReadOnlyTrx(revision)) {
          final ChangeLogSlab slab = ChangeLogStorage.read(reader.getStorageEngineReader(), 0, revision);
          assertEquals(2, slab.entries().size());
          for (final ChangeLogSlab.Entry entry : slab.entries()) {
            for (final ChangeLogSlab.Row row : new ChangeLogSlab.Row[] {entry.before(), entry.after()}) {
              if (row == null) {
                continue;
              }
              for (final int column : new int[] {6, 7}) {
                assertEquals(ChangeLogSlab.PRESENT | ChangeLogSlab.UNREPRESENTABLE, row.flags[column]);
                assertTrue(row.numbers[column] > 1_700_000_000L);
              }
            }
          }
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"vf,2024-01-01T00:00:00", "vt,2025-01-01T00:00:00",
      "vf,1969-01-01T00:00:00Z", "vt,1969-01-01T00:00:00Z", "both,2024-01-01T00:00:00"})
  void unsafeTypedBoundsDeclineBeforeAndAfterMaintenance(final String column, final String value) throws Exception {
    final Path path = directory.resolve("bt");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (var database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("contracts")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .storeDiffs(false)
                                                   .build());
      try (var session = database.beginResourceSession("contracts"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"id":1,"pid":1,"sid":1,"cost":5,"qty":3,"grade":1,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"},
             {"id":2,"pid":1,"sid":1,"cost":7,"qty":4,"grade":1,"vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        writer.moveToRightSibling();
        final long second = writer.getNodeKey();
        final List<String> endpoints = column.equals("both") ? List.of("vf", "vt") : List.of(column);
        final long[] records = column.equals("both") ? new long[] {first, second} : new long[] {first};
        for (final long record : records) {
          for (final String endpoint : endpoints) {
            field(writer, record, endpoint);
            writer.setStringValue(column.equals("both") && endpoint.equals("vt")
                ? "2025-01-01T00:00:00"
                : value);
          }
        }
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "bt");
        BitemporalProjections.declare(session, writer, "contracts");
        BitemporalTemporalAggregates.declare(session, writer, "contracts");
        writer.commit();
        field(writer, first, "qty");
        writer.setNumberValue(8);
        writer.commit();
        for (final long record : records) {
          for (final String endpoint : endpoints) {
            field(writer, record, endpoint);
            writer.setStringValue(endpoint.equals("vf")
                ? "2024-01-01T00:00:00Z"
                : "2025-01-01T00:00:00Z");
          }
        }
        writer.commit();
      }
      if (column.equals("both")) {
        try (var session = database.beginResourceSession("contracts"); var reader = session.beginNodeReadOnlyTrx(2)) {
          final ChangeLogSlab slab = ChangeLogStorage.read(reader.getStorageEngineReader(), 0, 2);
          final ChangeLogSlab.Row before = Objects.requireNonNull(slab.entries().getFirst().before());
          assertEquals(ChangeLogSlab.PRESENT | ChangeLogSlab.NON_DOUBLE, before.flags[6]);
          assertEquals(ChangeLogSlab.PRESENT | ChangeLogSlab.NON_DOUBLE, before.flags[7]);
          assertEquals(Instant.parse("2024-01-01T00:00:00Z").getEpochSecond(), before.numbers[6]);
          assertEquals(Instant.parse("2025-01-01T00:00:00Z").getEpochSecond(), before.numbers[7]);
        }
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      for (int revision = 1; revision <= 3; revision++) {
        final String text = "for $c in jn:doc('bt','contracts'," + revision + ")[] "
            + "where xs:dateTime($c.vf) le xs:dateTime('2024-06-15T00:00:00Z') "
            + "and xs:dateTime('2024-06-15T00:00:00Z') lt xs:dateTime($c.vt) "
            + "let $grade := $c.grade group by $grade let $total := sum($c.qty) "
            + "order by $grade return {\"grade\":$grade,\"total\":$total}";
        final String expected = run(oracle, context, text);
        final long before = TemporalAggregateRoute.servedCount();
        assertEquals(expected, run(chain, context, text));
        assertEquals(before + (revision == 3 ? 1 : 0), TemporalAggregateRoute.servedCount());
      }
    }
  }

  private void assertSh1Queries() throws Exception {
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      for (final int index : new int[] {6, 7, 8, 11}) {
        final String text = BitemporalQueries.all().get(index - 1).text();
        final String expected = run(oracle, context, text);
        assertFalse(expected.isEmpty());
        final int bindings = index == 6 || index == 11 ? 3 : 1;
        final long before = TemporalAggregateRoute.servedCount();
        final var result = WorkCapture.of(EngineWorkCounters.TEMPORAL_READS)
                                      .and(EngineWorkCounters.PROJECTION_BODY_SEGMENTS,
                                          EngineWorkCounters.VALID_TIME_INTERVAL_REFS)
                                      .call(() -> run(chain, context, text));
        assertEquals(expected, result.result(), "Q" + index);
        assertEquals(before + bindings, TemporalAggregateRoute.servedCount(), "Q" + index);
        result.work()
              .assertExactly(EngineWorkCounters.TEMPORAL_GROUP_HEADS_DECODED, 2L * bindings,
                  "each point reads only the two declared groups")
              .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0,
                  "aggregate serving reads no projection operand columns")
              .assertExactly(EngineWorkCounters.VALID_TIME_INTERVAL_REFS, 0,
                  "aggregate serving does not enumerate the valid-time row population");
      }
    }
  }

  private static void field(final JsonNodeTrx writer, final long object, final String name) {
    writer.moveTo(object);
    writer.moveToFirstChild();
    while (!name.equals(writer.getName().getLocalName())) {
      if (!writer.moveToRightSibling()) {
        throw new IllegalStateException("Missing field " + name);
      }
    }
    writer.moveToFirstChild();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String text)
      throws Exception {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(output, false, StandardCharsets.UTF_8)) {
      new Query(chain, text).serialize(context, writer);
      writer.flush();
      return output.toString(StandardCharsets.UTF_8).trim();
    }
  }
}
