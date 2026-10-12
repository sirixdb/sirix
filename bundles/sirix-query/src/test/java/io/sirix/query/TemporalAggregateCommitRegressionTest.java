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
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.Indexes;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.io.StorageType;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;

@Isolated
final class TemporalAggregateCommitRegressionTest {
  @TempDir
  File directory;

  @Test
  void querySnapshotLifetimeRejectsTruncatedAndReissuedPublication() throws Exception {
    final long first = build(true);
    try (var store = BasicJsonDBStore.newBuilder().location(directory.toPath()).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records")) {
      assertEquals("{\"g\":1,\"count\":1,\"sum\":2}", run(chain, context, grouped(2)));
      final StorageEngineReader original;
      try (StorageEngineReader source = session.createStorageEngineReader(2)) {
        original = context.borrowDerivedAggregateReader(source);
      }
      try (StorageEngineReader freshSource = session.createStorageEngineReader(2)) {
        assertSame(original, context.borrowDerivedAggregateReader(freshSource));
      }
      assertFalse(original.isClosed(), "The context owns the snapshot beyond an individual source cursor");
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.truncateTo(1);
      }
      try (JsonNodeTrx writer = session.beginNodeTrx()) {
        number(writer, first, 99);
        writer.commit("replacement", Instant.parse("2024-02-15T00:00:00Z"));
      }
      assertEquals("{\"g\":1,\"count\":1,\"sum\":99}", run(chain, context, grouped(2)));
      assertTrue(original.isClosed(), "The old publication was evicted");
      try (StorageEngineReader source = session.createStorageEngineReader(2)) {
        assertNotSame(original, context.borrowDerivedAggregateReader(source));
      }
    }
  }

  static Stream<Arguments> ddlCases() {
    return Stream.of("create", "aggregate-one", "aggregate-all", "projection-one", "projection-all")
                 .flatMap(
                     action -> Stream.of(false, true)
                                     .flatMap(
                                         owned -> IntStream.rangeClosed(1, 3)
                                                           .mapToObj(
                                                               signature -> Arguments.of(action, owned, signature))));
  }

  static Stream<Arguments> sequenceCases() {
    return Stream.of("create-create", "drop-drop", "drop-all", "create-drop-one", "create-drop-all",
        "create-cascade-one", "create-cascade-all", "drop-cascade-one", "drop-cascade-all", "failed-create",
        "failed-aggregate-drop", "failed-projection-drop-one", "failed-projection-drop-all")
                 .flatMap(
                     action -> Stream.of(false, true)
                                     .flatMap(
                                         owned -> IntStream.rangeClosed(1, 3)
                                                           .mapToObj(
                                                               signature -> Arguments.of(action, owned, signature))));
  }

  @ParameterizedTest
  @MethodSource("sequenceCases")
  void historicalDdlSequencesShareTheCurrentWriter(final String action, final boolean callerOwned, final int signature)
      throws Exception {
    final boolean seeded =
        action.startsWith("drop") || action.startsWith("failed-aggregate") || action.startsWith("failed-projection");
    final long first = build(seeded
        ? List.of(List.of("count(*)", "sum(n)"), List.of("min(n)"))
        : List.of());
    Set<Integer> aggregateIds;
    Set<Integer> projectionIds = Set.of(0);
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records")) {
      final JsonNodeTrx existing = callerOwned
          ? session.beginNodeTrx()
          : null;
      if (existing != null) {
        number(existing, first, 99);
      }
      final String document = "jn:doc('lineage','records',1)";
      if (action.startsWith("failed-projection")) {
        run(chain, context, projectionDrop(document, action.endsWith("one")));
      } else if (seeded) {
        run(chain, context, "jn:drop-temporal-aggregate-index(" + document + ",0)");
      } else {
        run(chain, context, create(document, "sum(n)"));
      }
      final JsonNodeTrx writer = session.getNodeTrx().orElseThrow();
      if (existing != null) {
        assertSame(existing, writer);
      }
      assertEquals(1, writer.getStorageEngineWriter().getRevisionToRepresent());
      number(writer, first, 7);
      switch (action) {
        case "create-create" -> {
          run(chain, context, create(document, "min(n)"));
          run(chain, context, create(document, "sum(n)"));
          aggregateIds = Set.of(0, 1);
        }
        case "drop-drop", "drop-all" -> {
          run(chain, context, "jn:drop-temporal-aggregate-index(" + document + (action.equals("drop-drop")
              ? ",1"
              : "") + ")");
          aggregateIds = Set.of();
        }
        case "create-drop-one", "create-drop-all" -> {
          run(chain, context, "jn:drop-temporal-aggregate-index(" + document + (action.endsWith("one")
              ? ",0"
              : "") + ")");
          assertEquals(Set.of(), ids(session.getWtxIndexController(writer.getRevisionNumber()).getIndexes(),
              IndexType.TEMPORAL_AGGREGATE));
          run(chain, context, create(document, "sum(n)"));
          aggregateIds = Set.of(1);
        }
        case "create-cascade-one", "create-cascade-all", "drop-cascade-one", "drop-cascade-all" -> {
          run(chain, context, projectionDrop(document, action.endsWith("one")));
          aggregateIds = Set.of();
          projectionIds = Set.of();
          final long sequence = writer.getMutationSequence();
          assertThrows(Exception.class, () -> run(chain, context, create(document, "min(n)")));
          assertEquals(sequence, writer.getMutationSequence());
        }
        case "failed-create" -> {
          final long sequence = writer.getMutationSequence();
          assertThrows(Exception.class, () -> run(chain, context, create(document, "sum(absent)")));
          assertEquals(sequence, writer.getMutationSequence());
          aggregateIds = Set.of(0);
        }
        case "failed-aggregate-drop" -> {
          final long sequence = writer.getMutationSequence();
          assertThrows(Exception.class,
              () -> run(chain, context, "jn:drop-temporal-aggregate-index(" + document + ",0)"));
          assertEquals(sequence, writer.getMutationSequence());
          aggregateIds = Set.of(1);
        }
        case "failed-projection-drop-one", "failed-projection-drop-all" -> {
          final long sequence = writer.getMutationSequence();
          assertThrows(Exception.class, () -> run(chain, context, projectionDrop(document, true)));
          run(chain, context, projectionDrop(document, false));
          assertEquals(sequence, writer.getMutationSequence());
          aggregateIds = Set.of();
          projectionIds = Set.of();
        }
        default -> throw new AssertionError(action);
      }
      assertSame(writer, session.getNodeTrx().orElseThrow());
      assertEquals(1, writer.getStorageEngineWriter().getRevisionToRepresent());
      writer.getStorageEngineWriter().assertTransactionWritable();
      assertNumber(writer, first, 7);
      final var indexes = session.getWtxIndexController(writer.getRevisionNumber()).getIndexes();
      assertEquals(aggregateIds, ids(indexes, IndexType.TEMPORAL_AGGREGATE));
      assertEquals(projectionIds, ids(indexes, IndexType.PROJECTION));
      assertEquals("3", run(chain, context, commit(document, signature)));
      assertSame(writer, session.getNodeTrx().orElseThrow());
      writer.close();
    }
    assertColdHistory(first, seeded
        ? Set.of(0, 1)
        : Set.of(), aggregateIds, projectionIds);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void currentProjectionReadinessRejectsASecondDdlWithoutDiscardingTheFirst(final boolean missing) throws Exception {
    final long first = build(true);
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      final String document = "jn:doc('lineage','records',1)";
      run(chain, context, create(document, "min(n)"));
      number(writer, first, 7);
      final byte[] ready = ProjectionIndexHOTStorage.readMetadataBlob(writer.getStorageEngineWriter(), 0);
      new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).putBlob(ProjectionIndexHOTStorage.METADATA_SLOT,
          missing
              ? new byte[0]
              : ProjectionIndexMetadata.staleTombstone().serialize());
      final byte[] unready = ProjectionIndexHOTStorage.readMetadataBlob(writer.getStorageEngineWriter(), 0);
      final long sequence = writer.getMutationSequence();
      for (final String operand : List.of("sum(n)", "max(n)")) {
        assertThrows(Exception.class, () -> run(chain, context, create(document, operand)));
        assertSame(writer, session.getNodeTrx().orElseThrow());
        assertEquals(sequence, writer.getMutationSequence());
        assertEquals(1, writer.getStorageEngineWriter().getRevisionToRepresent());
        assertNumber(writer, first, 7);
        assertArrayEquals(unready, ProjectionIndexHOTStorage.readMetadataBlob(writer.getStorageEngineWriter(), 0));
        assertEquals(Set.of(0, 1),
            ids(session.getWtxIndexController(writer.getRevisionNumber()).getIndexes(), IndexType.TEMPORAL_AGGREGATE));
        writer.getStorageEngineWriter().assertTransactionWritable();
      }
      new ProjectionIndexHOTStorage(writer.getStorageEngineWriter(), 0).putBlob(ProjectionIndexHOTStorage.METADATA_SLOT,
          Objects.requireNonNull(ready));
      assertEquals("3", run(chain, context, commit(document, 3)));
    }
    assertColdHistory(first, Set.of(0), Set.of(0, 1), Set.of(0));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void projectionDropsSeePendingDeclarationsAndCascadeOnlyTheirDependents(final boolean withId) throws Exception {
    final long first = build(false);
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      final String document = "jn:doc('lineage','records',1)";
      run(chain, context, create(document, "sum(n)"));
      number(writer, first, 7);
      final List<Path<QNm>> fields = List.of("g", "n", "lo", "hi", "extra")
                                         .stream()
                                         .map(field -> Path.parse("/[]/" + field, PathParser.Type.JSON))
                                         .toList();
      final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
          List.of(Type.LON, Type.LON, Type.STR, Type.STR, Type.LON), 1, IndexDef.DbType.JSON);
      final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
      controller.createIndexes(Set.of(projection), writer);
      run(chain, context, "jn:create-temporal-aggregate-index(" + document + ",'/[]','extra','sum(n)')");
      assertEquals(1,
          controller.getIndexes()
                    .getIndexDef(1, IndexType.TEMPORAL_AGGREGATE)
                    .getTemporalAggregateSpec()
                    .projectionId());
      run(chain, context, "jn:drop-projection-index(" + document + (withId
          ? ",1"
          : "") + ")");
      assertSame(writer, session.getNodeTrx().orElseThrow());
      assertEquals(1, writer.getStorageEngineWriter().getRevisionToRepresent());
      assertNumber(writer, first, 7);
      assertEquals(withId
          ? Set.of(0)
          : Set.of(), ids(controller.getIndexes(), IndexType.PROJECTION));
      assertEquals(withId
          ? Set.of(0)
          : Set.of(), ids(controller.getIndexes(), IndexType.TEMPORAL_AGGREGATE));
      assertEquals("3", run(chain, context, commit(document, 3)));
    }
    assertColdHistory(first, Set.of(), withId
        ? Set.of(0)
        : Set.of(),
        withId
            ? Set.of(0)
            : Set.of());
  }

  private void assertColdHistory(final long first, final Set<Integer> originalAggregates,
      final Set<Integer> aggregateIds, final Set<Integer> projectionIds) throws Exception {
    Databases.clearGlobalCaches();
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records")) {
      assertEquals(3, session.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= 3; revision++) {
        final var indexes = session.getRtxIndexController(revision).getIndexes();
        assertEquals(revision == 3
            ? aggregateIds
            : originalAggregates, ids(indexes, IndexType.TEMPORAL_AGGREGATE));
        assertEquals(revision == 3
            ? projectionIds
            : Set.of(0), ids(indexes, IndexType.PROJECTION));
        final int value = revision == 3
            ? 7
            : revision;
        final String expected = "{\"g\":1,\"count\":1,\"sum\":" + value + "}";
        assertEquals(expected, run(oracle, context, grouped(revision)));
        assertEquals(expected, run(chain, context, grouped(revision)));
        final String extremes = "for $r in jn:doc('lineage','records'," + revision + ")[] let $g := $r.g group by $g "
            + "let $low := min($r.n), $high := max($r.n) order by $g return {\"g\":$g,\"low\":$low,\"high\":$high}";
        assertEquals("{\"g\":1,\"low\":" + value + ",\"high\":" + value + "}", run(chain, context, extremes));
        try (var reader = session.beginNodeReadOnlyTrx(revision)) {
          reader.moveTo(first);
          reader.moveToFirstChild();
          reader.moveToRightSibling();
          reader.moveToFirstChild();
          assertEquals(value, reader.getNumberValue().intValue());
        }
      }
    }
  }

  private static Set<Integer> ids(final Indexes indexes, final IndexType type) {
    return indexes.getIndexDefs()
                  .stream()
                  .filter(definition -> definition.getType() == type)
                  .map(IndexDef::getID)
                  .collect(Collectors.toSet());
  }

  private static String create(final String document, final String operand) {
    return "jn:create-temporal-aggregate-index(" + document + ",'/[]','g',('count(*)','" + operand + "'))";
  }

  private static String projectionDrop(final String document, final boolean withId) {
    return "jn:drop-projection-index(" + document + (withId
        ? ",0"
        : "") + ")";
  }

  private static void assertNumber(final JsonNodeTrx writer, final long first, final int expected) {
    writer.moveTo(first);
    writer.moveToFirstChild();
    writer.moveToRightSibling();
    writer.moveToFirstChild();
    assertEquals(expected, writer.getNumberValue().intValue());
  }

  @ParameterizedTest
  @MethodSource("ddlCases")
  void historicalDdlSurvivesCommitAndColdReopen(final String action, final boolean callerOwned, final int signature)
      throws Exception {
    final long first = build(!action.equals("create"));
    final int value = action.equals("create")
        ? 7
        : 1;
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records")) {
      final JsonNodeTrx existing = callerOwned
          ? session.beginNodeTrx()
          : null;
      try {
        if (existing != null) {
          number(existing, first, 99);
          assertEquals(2, existing.getStorageEngineWriter().getRevisionToRepresent());
        }
        final String document = "jn:doc('lineage','records',1)";
        final String argument;
        if (action.equals("create")) {
          run(chain, context, "jn:create-temporal-aggregate-index(" + document + ",'/[]','g',('count(*)','sum(n)'))");
          final JsonNodeTrx writer = session.getNodeTrx().orElseThrow();
          assertEquals(1, writer.getStorageEngineWriter().getRevisionToRepresent());
          number(writer, first, value);
          argument = document;
        } else {
          final String function = action.startsWith("aggregate")
              ? "temporal-aggregate"
              : "projection";
          argument = "jn:drop-" + function + "-index(" + document + (action.endsWith("one")
              ? ",0"
              : "") + ")";
        }
        assertEquals("3", run(chain, context, commit(argument, signature)));
        final JsonNodeTrx writer = session.getNodeTrx().orElseThrow();
        if (existing != null) {
          assertSame(existing, writer);
        }
        assertEquals(3, session.getMostRecentRevisionNumber());
        assertEquals(3, writer.getStorageEngineWriter().getRevisionToRepresent());
        assertEquals(4, writer.getRevisionNumber());
        writer.getStorageEngineWriter().assertTransactionWritable();
        writer.moveTo(first);
        writer.moveToFirstChild();
        writer.moveToRightSibling();
        writer.moveToFirstChild();
        assertEquals(value, writer.getNumberValue().intValue());
        writer.close();
      } finally {
        if (existing != null && !existing.isClosed()) {
          existing.close();
        }
      }
    }
    Databases.clearGlobalCaches();
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records")) {
      assertEquals(3, session.getMostRecentRevisionNumber());
      for (int revision = 1; revision <= 3; revision++) {
        final boolean latest = revision == 3;
        final var indexes = session.getRtxIndexController(revision).getIndexes();
        assertEquals(latest && action.startsWith("projection")
            ? 0
            : 1, indexes.getNrOfIndexDefsWithType(IndexType.PROJECTION));
        assertEquals(latest
            ? (action.equals("create")
                ? 1
                : 0)
            : (action.equals("create")
                ? 0
                : 1),
            indexes.getNrOfIndexDefsWithType(IndexType.TEMPORAL_AGGREGATE));
        final String query = grouped(revision);
        final int expected = latest
            ? value
            : revision;
        final String result = "{\"g\":1,\"count\":1,\"sum\":" + expected + "}";
        assertEquals(result, run(oracle, context, query));
        assertEquals(result, run(chain, context, query));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void ordinaryCommitOnlyRevertsADifferentRepresentedRevision(final boolean alreadyBased) throws Exception {
    final long first = build(true);
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      if (alreadyBased) {
        writer.revertTo(1);
      }
      number(writer, first, 7);
      assertEquals("3", run(chain, context, commit("jn:doc('lineage','records',1)", 1)));
      assertSame(writer, session.getNodeTrx().orElseThrow());
      assertEquals("{\"g\":1,\"count\":1,\"sum\":" + (alreadyBased
          ? 7
          : 1) + "}", run(chain, context, grouped(3)));
    }
  }

  @Test
  void commitThroughAWriterDocumentRetainsOrdinaryEdits() throws Exception {
    final long first = build(true);
    try (
        var store =
            BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store);
        JsonResourceSession session = store.lookup("lineage").getDatabase().beginResourceSession("records");
        JsonNodeTrx writer = session.beginNodeTrx()) {
      number(writer, first, 7);
      writer.moveToDocumentRoot();
      writer.moveToFirstChild();
      context.bind(new QNm("document"), new JsonDBArray(writer, store.lookup("lineage")));
      assertEquals("3", run(chain, context, "declare variable $document external; sdb:commit($document)"));
      assertSame(writer, session.getNodeTrx().orElseThrow());
      assertEquals("{\"g\":1,\"count\":1,\"sum\":7}", run(chain, context, grouped(3)));
    }
  }

  private long build(final boolean aggregate) {
    return build(aggregate
        ? List.of(List.of("count(*)", "sum(n)"))
        : List.of());
  }

  private long build(final List<List<String>> declarations) {
    final var path = directory.toPath().resolve("lineage");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .customCommitTimestamps(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession("records");
          JsonNodeTrx writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
            [{"g":1,"n":1,"lo":"2024-01-01T00:00:00Z","hi":"2025-01-01T00:00:00Z"}]
            """), JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "lineage");
        final List<Path<QNm>> fields = List.of("g", "n", "lo", "hi")
                                           .stream()
                                           .map(field -> Path.parse("/[]/" + field, PathParser.Type.JSON))
                                           .toList();
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
            List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        controller.createIndexes(Set.of(projection), writer);
        for (final List<String> aggregates : declarations) {
          TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"), aggregates);
        }
        writer.commit("initial", Instant.parse("2024-01-15T00:00:00Z"));
        number(writer, first, 2);
        writer.commit("latest", Instant.parse("2024-02-15T00:00:00Z"));
        return first;
      }
    }
  }

  private static void number(final JsonNodeTrx writer, final long first, final int value) {
    assertTrue(writer.moveTo(first));
    writer.moveToFirstChild();
    writer.moveToRightSibling();
    writer.moveToFirstChild();
    writer.setNumberValue(value);
  }

  private static String commit(final String document, final int signature) {
    return "sdb:commit(" + document + (signature > 1
        ? ",'historical DDL'"
        : "")
        + (signature == 3
            ? ",xs:dateTime('2024-03-15T00:00:00Z')"
            : "")
        + ")";
  }

  private static String grouped(final int revision) {
    return "for $r in jn:doc('lineage','records'," + revision + ")[] "
        + "where xs:dateTime($r.lo) le xs:dateTime('2024-01-02T00:00:00Z') "
        + "and xs:dateTime('2024-01-02T00:00:00Z') lt xs:dateTime($r.hi) "
        + "let $g := $r.g group by $g let $count := count($r), $sum := sum($r.n) "
        + "order by $g return {\"g\":$g,\"count\":$count,\"sum\":$sum}";
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext context, final String query)
      throws Exception {
    TemporalAggregateTestSupport.await(context);
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(output, false, StandardCharsets.UTF_8)) {
      new Query(chain, query).serialize(context, writer);
      writer.flush();
      return output.toString(StandardCharsets.UTF_8).trim().replaceAll("\\s+", "");
    }
  }
}
