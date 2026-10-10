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
import io.sirix.index.aggregate.TemporalAggregateIndexes;
import io.sirix.index.aggregate.TemporalAggregateStorage;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.io.StorageType;
import io.sirix.query.compiler.translator.TemporalAggregateRoute;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import io.sirix.settings.VersioningType;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class TemporalAggregateRegressionTest {
  @TempDir
  File directory;

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void rootObjectReplacementDeclinesOrdinaryAndCorrelatedAggregates(final VersioningType versioning) throws Exception {
    final var databasePath = directory.toPath().resolve("shape");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("instants").storageType(StorageType.FILE_CHANNEL).build());
      try (JsonResourceSession session = database.beginResourceSession("instants");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"day":1,"ts":"2024-01-02T00:00:00Z"},{"day":2,"ts":"2024-01-03T00:00:00Z"}]
            """));
      }
      database.createResource(ResourceConfiguration.newBuilder("records").validTimePaths("lo", "hi")
          .storageType(StorageType.FILE_CHANNEL).customCommitTimestamps(true).versioningApproach(versioning)
          .maxNumberOfRevisionsToRestore(3).storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"g":1,"n":7,"lo":"2024-01-01T00:00:00Z","hi":"2024-01-06T00:00:00Z"},
             {"g":1,"n":11,"lo":"2024-01-01T00:00:00Z","hi":"2024-01-06T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "shape");
        projection(session, writer, List.of("g", "n", "lo", "hi"));
        TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer,
            "/[]", List.of("g"), List.of("count(*)", "sum(n)"));
        writer.commit("array", Instant.parse("2024-01-15T00:00:00Z"));
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.remove();
        writer.moveToDocumentRoot();
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            {"g":9,"n":41,"lo":"2024-01-01T00:00:00Z","hi":"2024-01-06T00:00:00Z"}
            """), JsonNodeTrx.Commit.NO);
        writer.commit("object", Instant.parse("2024-02-15T00:00:00Z"));
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      final List<Executable> checks = new ArrayList<>();
      for (final boolean object : new boolean[] {false, true}) {
        for (final boolean correlated : new boolean[] {false, true}) {
          final String source = "jn:open-bitemporal('shape','records',xs:dateTime('2024-0" + (object ? 2 : 1)
              + "-20T00:00:00Z')," + (correlated ? "xs:dateTime($d.ts)" : "xs:dateTime('2024-01-02T00:00:00Z')") + ")";
          final String query = (correlated ? "for $d in jn:doc('shape','instants')[] " : "")
              + "for $r in SOURCE let " + (correlated ? "$day := $d.day, " : "") + "$g := $r.g group by "
              + (correlated ? "$day,$g" : "$g") + " let $count := count($r), $sum := sum($r.n) order by "
              + (correlated ? "$day,$g" : "$g") + " return {" + (correlated ? "\"day\":$day," : "")
              + "\"g\":$g,\"n\":$count,\"sum\":$sum}";
          final String values = object ? "\"g\":9,\"n\":1,\"sum\":41}" : "\"g\":1,\"n\":2,\"sum\":18}";
          final String expected = correlated ? "{\"day\":1," + values + "{\"day\":2," + values : "{" + values;
          checks.add(() -> {
            assertEquals(expected, run(oracle, context, query.replace("SOURCE", source)).replaceAll("\\s+", ""));
            assertRoute(chain, oracle, context, query, source, object ? 0 : correlated ? 2 : 1);
          });
        }
      }
      assertAll(checks);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void unreadyHistoricalProjectionPreservesCallerOwnedEdits(final boolean missing) throws Exception {
    final var databasePath = directory.toPath().resolve("readiness");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final long first;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("records").validTimePaths("lo", "hi")
          .storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"g\":1,\"n\":1}]"), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        first = writer.getNodeKey();
        projection(session, writer, List.of("g", "n", "lo", "hi"));
        final byte[] ready = ProjectionIndexHOTStorage.readMetadataBlob(writer.getStorageEngineWriter(), 0);
        unready(writer, missing);
        writer.commit();
        new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).putBlob(ProjectionIndexHOTStorage.METADATA_SLOT, ready);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.lookup("readiness");
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        field(writer, first, "n");
        writer.setNumberValue(99);
        final int represented = writer.getStorageEngineWriter().getRevisionToRepresent();
        assertThrows(Exception.class, () -> run(chain, context,
            "jn:create-temporal-aggregate-index(jn:doc('readiness','records',1),'/[]','g','sum(n)')"));
        assertWriterUnchanged(session, writer, first, represented, 99);
        assertNoAggregate(session, writer);
        writer.commit();
        try (var reader = session.beginNodeReadOnlyTrx()) {
          reader.moveTo(first);
          reader.moveToFirstChild();
          reader.moveToRightSibling();
          reader.moveToFirstChild();
          assertEquals(99, reader.getNumberValue().intValue());
        }
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"true,declare", "false,declare", "true,controller", "false,controller", "true,build", "false,build"})
  void javaCreationRejectsUnreadyMetadataBeforeSideEffects(final boolean missing, final String entry) throws Exception {
    final var databasePath = directory.toPath().resolve("java-readiness");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("records").validTimePaths("lo", "hi")
          .storageType(StorageType.FILE_CHANNEL).storeDiffs(false).build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"g\":1,\"n\":1}]"), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        final IndexDef projection = projection(session, writer, List.of("g", "n", "lo", "hi"));
        writer.commit();
        field(writer, first, "n");
        writer.setNumberValue(99);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        final var spec = TemporalAggregateDeclarations.resolve(controller.getIndexes(),
            session.getResourceConfig().getValidTimeConfig(), "/[]", List.of("g"), List.of("sum(n)"));
        final IndexDef definition = IndexDefs.createTemporalAggregateIdxDef(projection.getProjectionRootPath(), spec, 0);
        final byte[] ready = ProjectionIndexHOTStorage.readMetadataBlob(writer.getStorageEngineWriter(), 0);
        unready(writer, missing);
        final int represented = writer.getStorageEngineWriter().getRevisionToRepresent();
        assertThrows(Exception.class, () -> {
          switch (entry) {
            case "declare" -> TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"), List.of("sum(n)"));
            case "controller" -> controller.createIndexes(Set.of(definition), writer);
            case "build" -> TemporalAggregateIndexes.build(writer.getStorageEngineWriter(), definition, projection);
            default -> throw new AssertionError(entry);
          }
        });
        assertWriterUnchanged(session, writer, first, represented, 99);
        assertNoAggregate(session, writer);
        new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).putBlob(ProjectionIndexHOTStorage.METADATA_SLOT, ready);
        TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"), List.of("sum(n)"));
        writer.commit();
        assertEquals(1, session.getRtxIndexController(session.getMostRecentRevisionNumber()).getIndexes()
            .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE));
      }
    }
  }

  private static void unready(final JsonNodeTrx writer, final boolean missing) {
    new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).putBlob(ProjectionIndexHOTStorage.METADATA_SLOT,
        missing ? new byte[0] : ProjectionIndexMetadata.staleTombstone().serialize());
  }

  private static void assertNoAggregate(final JsonResourceSession session, final JsonNodeTrx writer) {
    assertEquals(0, session.getWtxIndexController(writer.getRevisionNumber()).getIndexes()
        .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE));
    writer.getStorageEngineWriter().assertTransactionWritable();
    try (var storage = new TemporalAggregateStorage.Reader(writer.getStorageEngineWriter(), 0)) {
      assertNull(storage.get(TemporalAggregateIndexes.headerKey()));
    }
  }

  @ParameterizedTest
  @CsvSource({"0,true", "0,false", "1,true", "1,false", "2,true", "2,false", "3,true", "3,false"})
  void fullRelativePathsControlOrdinaryAndCorrelatedAdmission(final int mismatch, final boolean nested) throws Exception {
    final var databasePath = directory.toPath().resolve("paths");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final String group = mismatch == 0 && nested ? "nested.g" : "g";
    final String left = (mismatch == 1 || mismatch == 2) && nested ? "nested.n" : "n";
    final String right = mismatch == 3 && nested ? "nested.m" : "m";
    final String otherGroup = mismatch == 0 ? (nested ? "g" : "nested.g") : group;
    final String otherLeft = mismatch == 1 || mismatch == 2 ? (nested ? "n" : "nested.n") : left;
    final String otherRight = mismatch == 3 ? (nested ? "m" : "nested.m") : right;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("instants").storageType(StorageType.FILE_CHANNEL).build());
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
                                                   .buildPathSummary(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"g":1,"n":7,"m":3,"nested":{"g":9,"n":101,"m":29},"lo":"2024-01-01T00:00:00Z","hi":"2024-01-06T00:00:00Z"},
             {"g":2,"n":11,"m":5,"nested":{"g":8,"n":103,"m":31},"lo":"2024-01-01T00:00:00Z","hi":"2024-01-06T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "paths");
        projection(session, writer, List.of(group, left, right, "lo", "hi"));
        TemporalAggregateDeclarations.declare(session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]",
            List.of(group), List.of("count(*)", "sum(" + left + ")", "sum(" + left + "*" + right + ")"));
        writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"));
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      for (final boolean correlated : new boolean[] {false, true}) {
        final String source = "jn:open-bitemporal('paths','records',xs:dateTime('2024-01-20T00:00:00Z'),"
            + (correlated ? "xs:dateTime($d.ts)" : "xs:dateTime('2024-01-02T00:00:00Z')") + ")";
        final String unrelated = pathQuery(otherGroup, otherLeft, otherRight, mismatch, correlated);
        assertRoute(chain, oracle, context, unrelated, source, 0);
        final String matching = pathQuery(group, left, right, mismatch, correlated);
        final boolean nestedProduct = mismatch >= 2 && nested;
        assertRoute(chain, oracle, context, matching, source, nestedProduct ? 0 : correlated ? 2 : 1);
        if (nestedProduct) {
          assertRoute(chain, oracle, context, pathQuery(group, left, right, 1, correlated), source,
              correlated ? 2 : 1);
        }
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void cancellingIntervalSplitsRetainMembershipAndHistoricalAnswers(final VersioningType versioning) throws Exception {
    final var databasePath = directory.toPath().resolve("split");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final long[][] parts = new long[2][];
    final byte[][] tiers = new byte[2][];
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .customCommitTimestamps(true)
                                                   .versioningApproach(versioning)
                                                   .maxNumberOfRevisionsToRestore(3)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"g":1,"n":7,"lo":"1970-01-01T00:00:00.001Z","hi":"1970-01-01T00:00:00.003Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        final long array = writer.getNodeKey();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "split");
        projection(session, writer, List.of("g", "n", "lo", "hi"));
        final IndexDef aggregate = TemporalAggregateDeclarations.declare(
            session.getWtxIndexController(writer.getRevisionNumber()), writer, "/[]", List.of("g"), List.of("sum(n)"));
        writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"));
        field(writer, first, "hi");
        writer.setStringValue("1970-01-01T00:00:00.002Z");
        writer.moveTo(array);
        writer.insertSubtreeAsLastChild(JsonShredder.createStringReader("""
            {"g":1,"n":7,"lo":"1970-01-01T00:00:00.002Z","hi":"1970-01-01T00:00:00.003Z"}
            """), JsonNodeTrx.Commit.NO);
        writer.commit("split", Instant.parse("2024-02-15T00:00:00Z"));
        writer.moveTo(first);
        writer.remove();
        writer.commit("delete first segment", Instant.parse("2024-03-15T00:00:00Z"));
        for (int revision = 1; revision <= 3; revision++) {
          final int current = revision;
          try (var transaction = session.beginNodeReadOnlyTrx(revision);
              var storage = new TemporalAggregateStorage.Reader(transaction.getStorageEngineReader(), aggregate.getID())) {
            final int[] groups = new int[1];
            storage.groups(Long.MAX_VALUE, (key, head) -> {
              groups[0]++;
              assertEquals(current == 2 ? 2 : 1, head.rows());
              if (current <= 2) {
                parts[current - 1] = head.parts();
                tiers[current - 1] = head.tiers();
              }
            });
            assertEquals(1, groups[0]);
          }
        }
        assertArrayEquals(parts[0], parts[1], "Cancelling summaries must preserve immutable parts");
        assertArrayEquals(tiers[0], tiers[1], "Cancelling summaries must preserve part tiers");
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
      for (int month = 1; month <= 3; month++) {
        for (int point = 1; point <= 3; point++) {
          final String source = "jn:open-bitemporal('split','records',xs:dateTime('2024-0" + month
              + "-20T00:00:00Z'),xs:dateTime('1970-01-01T00:00:00.00" + point + "Z'))";
          final String query = "for $r in SOURCE let $g := $r.g group by $g order by $g "
              + "return {\"g\":$g,\"n\":count($r),\"sum\":sum($r.n)}";
          assertRoute(chain, oracle, context, query, source, 1);
          final String expected = point == 3 || month == 3 && point == 1 ? "" : "{\"g\":1,\"n\":1,\"sum\":7}";
          assertEquals(expected, run(chain, context, query.replace("SOURCE", source)));
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void rejectedAndNoOpDdlPreserveCallerOwnedEdits(final boolean configured) throws Exception {
    final var databasePath = directory.toPath().resolve("ddl");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    final long first;
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      final var configuration = ResourceConfiguration.newBuilder("records").storageType(StorageType.FILE_CHANNEL)
          .storeDiffs(false);
      if (configured) {
        configuration.validTimePaths("lo", "hi");
      }
      database.createResource(configuration.build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("[{\"g\":1,\"n\":1,\"m\":2}]"),
            JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        first = writer.getNodeKey();
        final IndexDef projection = projection(session, writer, List.of("g", "n", "m", "lo", "hi"));
        writer.commit();
        field(writer, first, "n");
        writer.setNumberValue(2);
        session.getWtxIndexController(writer.getRevisionNumber()).dropIndexes(Set.of(projection), writer);
        writer.commit();
      }
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.lookup("ddl");
      try (JsonResourceSession session = collection.getDatabase().beginResourceSession("records")) {
        final String document = "jn:doc('ddl','records',1)";
        run(chain, context, "jn:drop-temporal-aggregate-index(" + document + ")");
        assertTrue(session.getNodeTrx().isEmpty(), "A no-op drop must not open a writer");
        try (JsonNodeTrx writer = session.beginNodeTrx()) {
          field(writer, first, "n");
          writer.setNumberValue(99);
          final int represented = writer.getStorageEngineWriter().getRevisionToRepresent();
          for (final String request : List.of(
              "'/[]','absent','sum(n)'", "'/[]',('g','g'),'sum(n)'", "'/missing','g','sum(n)'",
              "'broken','g','sum(n)'", "'/[]','g','median(n)'", "'/[]','g','count()'", "'/[]','g','sum(absent)'",
              "'/[]','/[]/g','sum(n)'", "'/[]','g','sum(/[]/n)'", "'/[]','g','sum(/[]/n*m)'",
              "'/[]','g','sum(n*/[]/m)'",
              "'/[]','g','sum(n*)'", "'/[]','g','sum(n*m*n)'", "'/[]',(),'sum(n)'")) {
            assertThrows(Exception.class, () -> run(chain, context,
                "jn:create-temporal-aggregate-index(" + document + "," + request + ")"));
            assertWriterUnchanged(session, writer, first, represented, 99);
          }
          assertThrows(Exception.class, () -> run(chain, context,
              "jn:drop-temporal-aggregate-index(" + document + ",999)"));
          assertWriterUnchanged(session, writer, first, represented, 99);
          run(chain, context, "jn:drop-temporal-aggregate-index(" + document + ")");
          assertWriterUnchanged(session, writer, first, represented, 99);
          if (!configured) {
            assertThrows(Exception.class, () -> run(chain, context,
                "jn:create-temporal-aggregate-index(" + document + ",'/[]','g','sum(n)')"));
            assertWriterUnchanged(session, writer, first, represented, 99);
          } else {
            run(chain, context, "jn:create-temporal-aggregate-index(" + document + ",'/[]','g','sum(n)')");
            assertEquals(1, writer.getStorageEngineWriter().getRevisionToRepresent());
            field(writer, first, "n");
            assertEquals(1, writer.getNumberValue().intValue());
            assertEquals(1, session.getWtxIndexController(writer.getRevisionNumber()).getIndexes()
                .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE));
            writer.commit();
            field(writer, first, "n");
            writer.setNumberValue(2);
            writer.commit();
            field(writer, first, "n");
            writer.setNumberValue(99);
            final int current = writer.getStorageEngineWriter().getRevisionToRepresent();
            run(chain, context, "jn:create-temporal-aggregate-index(jn:doc('ddl','records',3),'/[]','g','sum(n)')");
            assertWriterUnchanged(session, writer, first, current, 99);
            run(chain, context, "jn:drop-temporal-aggregate-index(jn:doc('ddl','records',3),0)");
            assertEquals(3, writer.getStorageEngineWriter().getRevisionToRepresent());
            assertEquals(0, session.getWtxIndexController(writer.getRevisionNumber()).getIndexes()
                .getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE));
          }
          writer.rollback();
        }
        assertThrows(Exception.class, () -> run(chain, context,
            "jn:create-temporal-aggregate-index(" + document + ",'/[]','absent','sum(n)')"));
        assertTrue(session.getNodeTrx().isEmpty(), "Preflight rejection must not acquire a writer permit");
      }
    }
  }

  private static void assertWriterUnchanged(final JsonResourceSession session, final JsonNodeTrx writer,
      final long record, final int represented, final int value) {
    assertSame(writer, session.getNodeTrx().orElseThrow());
    assertEquals(represented, writer.getStorageEngineWriter().getRevisionToRepresent());
    field(writer, record, "n");
    assertEquals(value, writer.getNumberValue().intValue());
  }

  private static String pathQuery(final String group, final String left, final String right, final int mismatch,
      final boolean correlated) {
    final String prefix = correlated ? "for $d in jn:doc('paths','instants')[] " : "";
    final String external = correlated ? "$day := $d.day, " : "";
    final String keys = correlated ? "$day,$g" : "$g";
    final String day = correlated ? "\"day\":$day," : "";
    final String value = mismatch >= 2 ? " let $value := $r." + left + " * $r." + right : "";
    final String aggregate = mismatch == 0 ? "count($r)"
        : mismatch == 1 ? "sum($r." + left + ")" : "sum($value)";
    return prefix + "for $r in SOURCE let " + external + "$g := $r." + group + value
        + " group by " + keys + " let $result := " + aggregate + " order by " + keys
        + " return {" + day + "\"g\":$g,\"value\":$result}";
  }

  private static void assertRoute(final SirixCompileChain chain, final SirixCompileChain oracle,
      final SirixQueryContext context, final String query, final String source, final int served) throws Exception {
    final String expected = run(oracle, context, query.replace("SOURCE", source));
    final long before = TemporalAggregateRoute.servedCount();
    assertEquals(expected, run(chain, context, query.replace("SOURCE", source)));
    assertEquals(before + served, TemporalAggregateRoute.servedCount());
  }

  private static IndexDef projection(final JsonResourceSession session, final JsonNodeTrx writer,
      final List<String> fields) {
    final List<Path<QNm>> paths = new ArrayList<>(fields.size());
    final List<Type> types = new ArrayList<>(fields.size());
    for (final String field : fields) {
      paths.add(Path.parse("/[]/" + field.replace('.', '/'), PathParser.Type.JSON));
      types.add(field.equals("lo") || field.equals("hi") ? Type.STR : Type.LON);
    }
    final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), paths, types,
        0, IndexDef.DbType.JSON);
    session.getWtxIndexController(writer.getRevisionNumber()).createIndexes(Set.of(projection), writer);
    return projection;
  }

  private static void field(final JsonNodeTrx writer, final long record, final String name) {
    writer.moveTo(record);
    writer.moveToFirstChild();
    while (!name.equals(writer.getName().getLocalName())) {
      assertTrue(writer.moveToRightSibling());
    }
    writer.moveToFirstChild();
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String query)
      throws Exception {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream(); PrintWriter writer = new PrintWriter(output)) {
      new Query(chain, query).serialize(context, writer);
      writer.flush();
      return output.toString().trim();
    }
  }
}
