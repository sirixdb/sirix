package io.sirix.query.budget;

import io.brackit.query.Query;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.jdm.Type;
import io.brackit.query.util.path.Path;
import io.brackit.query.util.path.PathParser;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkCounter;
import io.sirix.index.IndexDef;
import io.sirix.index.IndexDefs;
import io.sirix.index.IndexType;
import io.sirix.index.aggregate.TemporalAggregateDeclarations;
import io.sirix.index.aggregate.DerivedIndexTestSupport;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.compiler.translator.TemporalAggregateRoute;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBArray;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@Isolated
final class TemporalAggregateSourceWorkBudgetTest {
  private static final WorkCounter SERVED = WorkCounter.alwaysOn("served.temporalAggregates",
      "one grouped point answered from temporal aggregates", TemporalAggregateRoute::servedCount);

  @TempDir
  File directory;

  static Stream<Arguments> cases() {
    return Stream.of("none", "uncovered", "dictionary", "covered", "empty")
                 .flatMap(kind -> Stream.of(false, true)
                                        .flatMap(correlated -> Stream.of(32, 512)
                                                                     .flatMap(rows -> IntStream
                                                                                               .range(
                                                                                                   kind.equals("empty")
                                                                                                       ? 0
                                                                                                       : 2,
                                                                                                   kind.equals("empty")
                                                                                                       ? 4
                                                                                                       : 3)
                                                                                               .mapToObj(
                                                                                                   mode -> Arguments.of(
                                                                                                       kind, correlated,
                                                                                                       rows, mode)))));
  }

  @ParameterizedTest
  @MethodSource("cases")
  void aggregateAdmissionAndMaskedFallbackShareOneReader(final String kind, final boolean correlated, final int rows,
      final int mode) throws Exception {
    build(kind, rows);
    Databases.clearGlobalCaches();
    try (var store =
        BasicJsonDBStore.newBuilder().location(directory.toPath()).storageType(StorageType.FILE_CHANNEL).build()) {
      final JsonDBCollection real = store.lookup("source");
      DerivedIndexTestSupport.await(real.getDatabase());
      final JsonDBCollection collection = mock(JsonDBCollection.class, delegatesTo(real));
      final JsonDBStore observedStore = mock(JsonDBStore.class, delegatesTo(store));
      doReturn(collection).when(observedStore).lookup("source");
      final AtomicLong evaluations = new AtomicLong();
      final AtomicLong moves = new AtomicLong();
      final AtomicLong records = new AtomicLong();
      final LongOpenHashSet readers = new LongOpenHashSet();
      doAnswer(call -> {
        evaluations.incrementAndGet();
        final int revision = call.getArgument(1);
        final JsonDBArray document = (JsonDBArray) real.getDocument("records", revision);
        readers.add(document.getTrx().getId());
        final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class, delegatesTo(document.getTrx()));
        doAnswer(move -> {
          moves.incrementAndGet();
          return document.getTrx().moveTo(move.getArgument(0));
        }).when(cursor).moveTo(anyLong());
        final StorageEngineReader reader =
            mock(StorageEngineReader.class, delegatesTo(document.getTrx().getStorageEngineReader()));
        doAnswer(read -> {
          records.incrementAndGet();
          return document.getTrx()
                         .getStorageEngineReader()
                         .getRecord(read.getArgument(0), IndexType.DOCUMENT, read.getArgument(2));
        }).when(reader).getRecord(anyLong(), eq(IndexType.DOCUMENT), anyInt());
        doReturn(reader).when(cursor).getStorageEngineReader();
        final JsonDBArray observed = spy(document);
        doReturn(cursor).when(observed).getTrx();
        return observed;
      }).when(collection).getDocument(eq("records"), anyInt());
      final WorkCounter documents =
          WorkCounter.alwaysOn("source.documentEvaluations", "one source document evaluated", evaluations::get);
      final WorkCounter opened =
          WorkCounter.alwaysOn("source.readersOpened", "one distinct source reader opened", readers::size);
      final WorkCounter cursorMoves =
          WorkCounter.alwaysOn("source.cursorMoves", "one observed source cursor move", moves::get);
      final WorkCounter documentRecords =
          WorkCounter.alwaysOn("source.documentRecords", "one observed document record read", records::get);
      try (var context = SirixQueryContext.createWithJsonStore(observedStore);
          var chain = SirixCompileChain.createWithJsonStore(observedStore);
          var oracleContext = SirixQueryContext.createWithJsonStore(store);
          var oracle = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store)) {
        context.bind(new QNm("point"), new DateTime("2024-01-02T00:00:00Z"));
        oracleContext.bind(new QNm("point"), new DateTime("2024-01-02T00:00:00Z"));
        final String text = query(correlated, mode, kind.equals("empty"));
        final String expected = run(new Query(oracle, text), oracleContext);
        assertEquals(expected(rows, kind, correlated), expected);
        final Query query = new Query(chain, text);
        final int bindings = correlated
            ? 2
            : 1;
        for (int repetition = 0; repetition < 2; repetition++) {
          final var result = WorkCapture.of(documents, opened, cursorMoves, documentRecords, SERVED)
                                        .and(QueryWorkCounters.GROUP_AGGREGATES)
                                        .and(EngineWorkCounters.TEMPORAL_READS)
                                        .call(() -> run(query, context));
          assertEquals(expected, result.result());
          result.work()
                .assertExactly(documents, bindings, "admission and fallback evaluate each source binding only once")
                .assertExactly(opened, bindings,
                    "each source binding opens one reader, including after admission declines")
                .assertBetween(cursorMoves, 0, 4L * bindings, "serving does not materialize the source's records")
                .assertBetween(documentRecords, kind.equals("covered")
                    ? bindings
                    : 0, 16L * bindings, "document node work must not grow with the fixture population")
                .assertExactly(SERVED, kind.equals("covered")
                    ? bindings
                    : 0, "a covering aggregate remains admitted")
                .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, kind.equals("covered")
                    ? 0
                    : bindings, "every declined aggregate retains masked serving, including an empty source")
                .assertExactly(EngineWorkCounters.TEMPORAL_GROUP_HEADS_DECODED, kind.equals("covered")
                    ? 4L * bindings
                    : 0, "uncovered and oversized dictionaries decode no group heads");
        }
      }
    }
  }

  private void build(final String kind, final int rows) {
    final var path = directory.toPath().resolve("source");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(path)) {
      database.createResource(ResourceConfiguration.newBuilder("records")
                                                   .validTimePaths("lo", "hi")
                                                   .storageType(StorageType.FILE_CHANNEL)
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
              .append(kind.equals("dictionary")
                  ? row
                  : row % 4)
              .append(",\"n\":")
              .append(row + 1)
              .append(",\"lo\":\"2024-01-01T00:00:00Z\",\"hi\":\"2025-01-01T00:00:00Z\"}");
        }
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json.append(']').toString()),
            JsonNodeTrx.Commit.NO);
        writer.moveToDocumentRoot();
        writer.moveToFirstChild();
        final long array = writer.getNodeKey();
        writer.moveToFirstChild();
        final long first = writer.getNodeKey();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, writer, "source");
        final List<Path<QNm>> fields = List.of("g", "n", "lo", "hi")
                                           .stream()
                                           .map(field -> Path.parse("/[]/" + field, PathParser.Type.JSON))
                                           .toList();
        final IndexDef projection = IndexDefs.createProjectionIdxDef(Path.parse("/[]", PathParser.Type.JSON), fields,
            List.of(Type.LON, Type.LON, Type.STR, Type.STR), 0, IndexDef.DbType.JSON);
        final JsonIndexController controller = session.getWtxIndexController(writer.getRevisionNumber());
        controller.createIndexes(Set.of(projection), writer);
        if (!kind.equals("none")) {
          TemporalAggregateDeclarations.declare(controller, writer, "/[]", List.of("g"), kind.equals("uncovered")
              ? List.of("count(*)")
              : List.of("count(*)", "sum(n)"));
        }
        writer.commit();
        if (kind.equals("dictionary") || kind.equals("empty")) {
          for (int row = 1; row < rows; row++) {
            writer.moveTo(first);
            assertTrue(writer.moveToRightSibling());
            writer.remove();
          }
          if (kind.equals("empty")) {
            writer.moveTo(first);
            writer.remove();
          }
          writer.moveTo(array);
          writer.commit();
        }
        DerivedIndexTestSupport.await(session);
      }
    }
  }

  private static String query(final boolean correlated, final int mode, final boolean empty) {
    final String point = empty
        ? "xs:dateTime('invalid-date')"
        : "$point";
    return "declare variable $point external; " + (correlated
        ? "for $d in [{\"day\":1},{\"day\":2}][] "
        : "") + "for $r in (for $x in jn:doc('source','records')[] where xs:dateTime($x.lo) "
        + ((mode & 1) == 0
            ? "le"
            : "lt")
        + " " + point + " and " + point + " " + ((mode & 2) == 0
            ? "le"
            : "lt")
        + " xs:dateTime($x.hi) return $x) let " + (correlated
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

  private static String expected(final int rows, final String kind, final boolean correlated) {
    if (kind.equals("empty")) {
      return "";
    }
    final StringBuilder result = new StringBuilder();
    for (int day = 1; day <= (correlated
        ? 2
        : 1); day++) {
      for (int group = 0; group < (kind.equals("dictionary")
          ? 1
          : 4); group++) {
        final int count = kind.equals("dictionary")
            ? 1
            : rows / 4;
        final long sum = kind.equals("dictionary")
            ? 1
            : (long) count * (group + 1) + 2L * count * (count - 1);
        result.append('{');
        if (correlated) {
          result.append("\"day\":").append(day).append(',');
        }
        result.append("\"g\":")
              .append(group)
              .append(",\"count\":")
              .append(count)
              .append(",\"sum\":")
              .append(sum)
              .append('}');
      }
    }
    return result.toString();
  }

  private static String run(final Query query, final SirixQueryContext context) throws Exception {
    try (ByteArrayOutputStream output = new ByteArrayOutputStream();
        PrintWriter writer = new PrintWriter(output, false, StandardCharsets.UTF_8)) {
      query.serialize(context, writer);
      writer.flush();
      return output.toString(StandardCharsets.UTF_8).trim().replaceAll("\\s+", "");
    }
  }
}
