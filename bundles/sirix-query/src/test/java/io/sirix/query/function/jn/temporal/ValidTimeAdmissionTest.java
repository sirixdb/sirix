package io.sirix.query.function.jn.temporal;

import io.brackit.query.Query;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.DateTime;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.brackit.query.jdm.json.Object;
import io.sirix.access.Databases;
import io.sirix.access.trx.node.json.JsonIndexController;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.index.IndexDef;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Isolated
final class ValidTimeAdmissionTest {
  private static final String POINT = "xs:dateTime('2024-01-01T00:00:00Z')";
  private static final WorkCapture POSTINGS = WorkCapture.of(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS);
  private static final String ROWS = """
      [{"id":1,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z",
        "nested":[{"vf":"invalid","vt":"2025-01-01T00:00:00Z"}]},
       {"id":2,"vf":"2023-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
      """;

  @TempDir
  Path directory;

  @Test
  void warmedProofsSeparateCohortsResourcesPointsAndIndexDefinitions() throws Exception {
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      create(chain, context, "exact", ROWS);
      create(chain, context, "inexact", ROWS.replace("2023-01-01T00:00:00Z", "2023-01-01T00:00:00.000500Z"));
      final var collection = store.lookup("admission");
      final JsonDBItem exact = collection.getDocument("exact");
      final JsonDBItem inexact = collection.getDocument("inexact");
      assertEquals(exact.getNodeKey(), inexact.getNodeKey());
      POSTINGS.run(() -> assertTrue(admitted(exact)))
              .assertAtLeast(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS, 1,
                  "first proof must perform real validation");
      POSTINGS.run(() -> assertTrue(admitted(exact)))
              .assertZero(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS, "the immutable cohort must reuse its proof");
      assertFalse(admitted(inexact));
      final JsonDBItem nested = (JsonDBItem) ((Object) ((Array) exact).at(0)).get(new QNm("nested"));
      assertFalse(admitted(nested));
      assertIds(chain, context, "exact", exact.getTrx().getRevisionNumber(), List.of(1L, 2L));
      assertEquals(0,
          ((Numeric) new Query(chain, "count(for $x in jn:doc('admission','exact')[] where xs:dateTime($x.other) le "
              + POINT + " and " + POINT + " lt xs:dateTime($x.vt) return $x)").evaluate(context)).intValue());
      final var config = exact.getResourceSession().getResourceConfig().getValidTimeConfig();
      assertEquals(0, Objects
                             .requireNonNull(ValidTimeIntervalIndex.comparisonSequence(exact,
                                 () -> new DateTime("2026-01-01T00:00:00Z"), config, false, true))
                             .size()
                             .intValue());
      assertNull(ValidTimeIntervalIndex.comparisonSequence(exact, () -> new DateTime("2024-01-01T00:00:00.000500Z"),
          config, false, true));
      assertThrows(IllegalStateException.class, () -> ValidTimeIntervalIndex.comparisonSequence(exact, () -> {
        throw new IllegalStateException("point");
      }, config, false, true));
      assertThrows(QueryException.class,
          () -> new Query(chain, "count(for $x in jn:doc('admission','exact')[0].nested[] where xs:dateTime($x.vf) le "
              + POINT + " and " + POINT + " lt xs:dateTime($x.vt) return $x)").evaluate(context));
      final int initial = exact.getTrx().getRevisionNumber();
      new Query(chain,
          "let $d := jn:doc('admission','exact') let $i := jn:drop-valid-time-index($d) return sdb:commit($d)").evaluate(
              context);
      final JsonDBItem dropped = collection.getDocument("exact");
      assertNull(ValidTimeIntervalIndex.comparisonSequence(dropped, () -> new DateTime("2024-01-01T00:00:00Z"), config,
          false, true));
      assertIds(chain, context, "exact", dropped.getTrx().getRevisionNumber(), List.of(1L, 2L));
      new Query(chain,
          "let $d := jn:doc('admission','exact') let $i := jn:create-valid-time-index($d) return sdb:commit($d)").evaluate(
              context);
      final JsonDBItem rebuilt = collection.getDocument("exact");
      POSTINGS.run(() -> assertTrue(admitted(rebuilt)))
              .assertAtLeast(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS, 1,
                  "a new index definition and revision must validate independently");
      assertTrue(admitted(collection.getDocument("exact", initial)));
      assertIds(chain, context, "exact", initial, List.of(1L, 2L));
    }
  }

  @Test
  void writersMovesUpdatesRevertsAndReopenedHistoryCannotReuseStaleProofs() throws Exception {
    final int original;
    final int moved;
    final int restored;
    final int changed;
    final int incomplete;
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      create(chain, context, "rows", ROWS);
      final var collection = store.lookup("admission");
      final JsonDBItem document = collection.getDocument("rows");
      original = document.getTrx().getRevisionNumber();
      assertTrue(admitted(document));
      final var cursor = document.getTrx();
      cursor.moveTo(document.getNodeKey());
      cursor.moveToFirstChild();
      cursor.moveToFirstChild();
      cursor.moveToRightSibling();
      final long fromKey = cursor.getNodeKey();
      cursor.moveTo(document.getNodeKey());
      cursor.moveToLastChild();
      final long last = cursor.getNodeKey();
      final var session = document.getResourceSession();
      final IndexDef definition = definition(document);
      final JsonIndexController controller = (JsonIndexController) session.getRtxIndexController(original);
      final var writer = session.getNodeTrx().orElseGet(session::beginNodeTrx);
      for (int i = 0; i < 2; i++) {
        POSTINGS.run(() -> assertFalse(
            controller.isExactValidTimeArray(writer.getStorageEngineReader(), definition, document.getNodeKey(), 2)))
                .assertZero(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS,
                    "writer-backed readers must decline immutable admission");
      }
      writer.moveTo(fromKey);
      writer.setStringValue("invalid");
      assertFalse(
          controller.isExactValidTimeArray(writer.getStorageEngineReader(), definition, document.getNodeKey(), 2));
      writer.setStringValue("2023-01-01T00:00:00Z");
      writer.moveTo(document.getNodeKey());
      writer.moveSubtreeToFirstChild(last);
      writer.commit();
      moved = session.getMostRecentRevisionNumber();
      final JsonDBItem reordered = collection.getDocument("rows", moved);
      assertFalse(admitted(reordered));
      POSTINGS.run(() -> assertFalse(admitted(reordered)))
              .assertZero(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS,
                  "declined immutable cohorts must also reuse their validated result");
      assertIds(chain, context, "rows", moved, List.of(2L, 1L));
      assertIds(chain, context, "rows", original, List.of(1L, 2L));
      writer.revertTo(original);
      writer.commit();
      restored = session.getMostRecentRevisionNumber();
      final JsonDBItem reverted = collection.getDocument("rows", restored);
      POSTINGS.run(() -> assertTrue(admitted(reverted)))
              .assertAtLeast(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS, 1,
                  "a restored head must validate its own immutable revision");
      assertIds(chain, context, "rows", restored, List.of(1L, 2L));
      writer.moveTo(fromKey);
      writer.setStringValue("invalid");
      writer.commit();
      changed = session.getMostRecentRevisionNumber();
      assertFalse(admitted(collection.getDocument("rows", changed)));
      for (int i = 0; i < 2; i++) {
        assertThrows(QueryException.class,
            () -> new Query(chain, "count(" + expression("rows", changed, false) + ")").evaluate(context));
      }
      writer.moveTo(fromKey);
      writer.remove();
      writer.commit();
      incomplete = session.getMostRecentRevisionNumber();
      assertFalse(admitted(collection.getDocument("rows", incomplete)));
      assertIds(chain, context, "rows", incomplete, List.of(2L));
      writer.close();
    }
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var collection = store.lookup("admission");
      POSTINGS.run(() -> assertTrue(admitted(collection.getDocument("rows", original))))
              .assertAtLeast(EngineWorkCounters.VALID_TIME_POSTING_CHUNKS, 1,
                  "reopening must not retain a closed session's proof");
      assertIds(chain, context, "rows", original, List.of(1L, 2L));
      assertIds(chain, context, "rows", moved, List.of(2L, 1L));
      assertIds(chain, context, "rows", restored, List.of(1L, 2L));
      assertFalse(admitted(collection.getDocument("rows", changed)));
      assertIds(chain, context, "rows", incomplete, List.of(2L));
    }
  }

  private static boolean admitted(final JsonDBItem document) {
    final JsonIndexController controller =
        (JsonIndexController) document.getResourceSession()
                                      .getRtxIndexController(document.getTrx().getRevisionNumber());
    return controller.isExactValidTimeArray(document.getTrx().getStorageEngineReader(), definition(document),
        document.getNodeKey(), ((Array) document).len());
  }

  private static IndexDef definition(final JsonDBItem document) {
    return document.getResourceSession()
                   .getRtxIndexController(document.getTrx().getRevisionNumber())
                   .getIndexes()
                   .getIndexDefs()
                   .stream()
                   .filter(index -> index.isValidTimeIndex())
                   .findFirst()
                   .orElseThrow();
  }

  private static void create(final SirixCompileChain chain, final SirixQueryContext context, final String resource,
      final String json) {
    final boolean createCollection = context.getJsonItemStore().lookup("admission") == null;
    new Query(chain,
        "jn:store('admission','" + resource + "','" + json + "'," + createCollection + "(),"
            + "{\"validFromPath\":\"vf\",\"validToPath\":\"vt\",\"autoCreateValidTimeIndex\":true()})").evaluate(
                context);
  }

  private static String expression(final String resource, final int revision, final boolean positional) {
    return "for $x " + (positional
        ? "at $position "
        : "") + "in jn:doc('admission','" + resource + "'," + revision + ")[] where xs:dateTime($x.vf) le " + POINT
        + " and " + POINT + " lt xs:dateTime($x.vt) return $x.id";
  }

  private static void assertIds(final SirixCompileChain chain, final SirixQueryContext context, final String resource,
      final int revision, final List<Long> expected) {
    for (final boolean positional : List.of(false, true, false)) {
      final Sequence result = new Query(chain, expression(resource, revision, positional)).execute(context);
      assertNotNull(result);
      final List<Long> actual = new ArrayList<>();
      try (var iterator = result.iterate()) {
        for (var item = iterator.next(); item != null; item = iterator.next()) {
          actual.add(((Numeric) item).longValue());
        }
      }
      assertEquals(expected, actual);
    }
  }
}
