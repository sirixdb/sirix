package io.sirix.query.budget;

import io.brackit.query.Query;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Numeric;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBStore;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Array;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.budget.WorkReport;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import io.sirix.io.StorageType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/** Guards against eager candidate verification/materialization, using the real transaction seam. */
@Isolated
final class ValidTimeSliceWorkBudgetTest {
  private static final WorkCapture INDEX_WORK =
      WorkCapture.of(EngineWorkCounters.VALID_TIME_INTERVAL_REFS, EngineWorkCounters.VALID_TIME_POSTING_REFS);
  @TempDir
  Path directory;

  @Test
  void noObjectReadsUntilDemandAndCountDoesNotReadTimestampFields() {
    assertSliceBudget(false);
  }

  @Test
  @Disabled("Awaiting Brackit UDF materialization fix; see docs/VALID_TIME_KEY_SLICES.md#brackit-dependency")
  void userFunctionCountDoesNotMaterializeTheSlice() {
    assertSliceBudget(true);
  }

  @Test
  void reorderedBitemporalCountsRemainKeyOnlyAcrossRevisions() {
    shredRows("""
        [{"id":1,"vf":"2020-01-01T00:00:00Z","vt":"2030-01-01T00:00:00Z"},
         {"id":2,"vf":"2020-01-01T00:00:00Z","vt":"2030-01-01T00:00:00Z"}]
        """);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "let $d := jn:doc('budget','rows') let $i := jn:create-valid-time-index($d) return sdb:commit($d)").evaluate(
              context);
      final JsonDBCollection realCollection = store.lookup("budget");
      final JsonDBItem original = realCollection.getDocument("rows");
      final int originalRevision = original.getTrx().getRevisionNumber();
      final var reader = original.getTrx();
      reader.moveTo(original.getNodeKey());
      reader.moveToFirstChild();
      final long first = reader.getNodeKey();
      reader.moveToLastChild();
      reader.moveToFirstChild();
      final long firstEnd = reader.getNodeKey();
      reader.moveTo(first);
      reader.moveToRightSibling();
      final long second = reader.getNodeKey();
      final var session = original.getResourceSession();
      final JsonNodeTrx writer = session.getNodeTrx().orElseGet(session::beginNodeTrx);
      writer.moveTo(original.getNodeKey());
      writer.moveSubtreeToFirstChild(second);
      writer.commit();
      final int movedRevision = session.getMostRecentRevisionNumber();
      writer.moveTo(firstEnd);
      writer.setStringValue("2024-01-01T00:00:00Z");
      writer.commit();
      final int changedRevision = session.getMostRecentRevisionNumber();
      final JsonDBCollection collection = mock(JsonDBCollection.class, delegatesTo(realCollection));
      final JsonDBStore observedStore = mock(JsonDBStore.class, delegatesTo(store));
      doReturn(collection).when(observedStore).lookup("budget");
      try (var observedContext = SirixQueryContext.createWithJsonStore(observedStore);
          var observedChain = SirixCompileChain.createWithJsonStore(observedStore)) {
        for (final int revision : new int[] {originalRevision, movedRevision, changedRevision}) {
          final JsonDBItem document = realCollection.getDocument("rows", revision);
          final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class, delegatesTo(document.getTrx()));
          final JsonDBItem observed =
              mock(JsonDBItem.class, withSettings().extraInterfaces(Array.class).defaultAnswer(delegatesTo(document)));
          doReturn(cursor).when(observed).getTrx();
          doReturn(observed).when(collection).getDocument(eq("rows"), any(Instant.class));
          final String point = "xs:dateTime('2024-01-01T00:00:00Z')";
          final String source = "jn:open-bitemporal('budget','rows',xs:dateTime('2099-01-01T00:00:00Z')," + point + ")";
          for (final boolean start : new boolean[] {false, true}) {
            for (final boolean strict : new boolean[] {false, true}) {
              for (final boolean general : new boolean[] {false, true}) {
                for (final boolean mirror : new boolean[] {false, true}) {
                  final String bound = "xs:dateTime($x." + (start
                      ? "vf"
                      : "vt") + ")";
                  final String operator = general
                      ? (strict
                          ? "<"
                          : "<=")
                      : (strict
                          ? "lt"
                          : "le");
                  final String swapped = general
                      ? (strict
                          ? ">"
                          : ">=")
                      : (strict
                          ? "gt"
                          : "ge");
                  final String predicate = mirror
                      ? (start
                          ? point + " " + swapped + " " + bound
                          : bound + " " + swapped + " " + point)
                      : (start
                          ? bound + " " + operator + " " + point
                          : point + " " + operator + " " + bound);
                  final String text = "for $x in " + source + " where " + predicate + " return $x";
                  final int expected = revision == changedRevision && !start && strict
                      ? 1
                      : 2;
                  clearInvocations(cursor);
                  final Sequence rows = new Query(observedChain, text).execute(observedContext);
                  assertNotNull(rows);
                  verify(cursor, never()).getFirstChildKey();
                  assertEquals(expected, rows.size().intValue(), text);
                  assertEquals(expected,
                      ((Numeric) new Query(observedChain, "count(" + text + ")").evaluate(observedContext)).intValue(),
                      text);
                  verify(cursor, never()).moveTo(anyLong());
                  verify(cursor, never()).getFirstChildKey();
                  verify(cursor, never()).getValue();
                  clearInvocations(cursor);
                  try (var iterator = rows.iterate()) {
                    final JsonDBItem item = (JsonDBItem) iterator.next();
                    assertNotNull(item);
                    verify(cursor, times(1)).moveTo(anyLong());
                    verify(cursor, times(1)).getFirstChildKey();
                    verify(cursor, never()).getValue();
                    assertEquals(expected == 1
                        ? second
                        : first, item.getNodeKey());
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /**
   * Guards the candidate-source invariant: records needing exact verification are registered in the
   * RI-tree, so a closed stab that no interval contains must not re-materialize any of them.
   *
   * <p>
   * Healthy: zero {@code moveTo} and zero {@code getValue} for the empty answer, and at least 64 of
   * each for the 64-record answer. Checked by mutation — making the verification-posting union
   * unconditional again fails the empty answer at its first {@code moveTo} inside
   * {@code ValidTimeIntervalIndex.keys}, while the other budget in this class stays green.
   * </p>
   */
  @Test
  void closedAndStrictStabsOutsideEveryIntervalReadNoObjectWhenEveryRecordNeedsVerification() throws Exception {
    assertInexactEmptyStabBudget(64);
  }

  @Test
  @Tag("heavy")
  @EnabledIfEnvironmentVariable(named = "SIRIX_VALID_TIME_LARGE_BUDGET", matches = "true")
  void oneHundredThousandInexactIntervalsHaveZeroReadEmptyStab() throws Exception {
    assertInexactEmptyStabBudget(100_000);
  }

  private void assertInexactEmptyStabBudget(final int count) throws Exception {
    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < count; i++) {
      if (i != 0) {
        json.append(',');
      }
      // Sub-millisecond bounds: every record is inexact, so every record carries a verification posting.
      json.append("{\"id\":")
          .append(i)
          .append(",\"vf\":\"2020-01-01T00:00:00.000500Z\",\"vt\":\"2020-12-31T23:59:59.000500Z\"}");
    }
    json.append(']');
    shredRows(json.toString());
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "let $d := jn:doc('budget','rows') let $i := jn:create-valid-time-index($d) return sdb:commit($d)").evaluate(
              context);
      final JsonDBItem document = store.lookup("budget").getDocument("rows");
      final var config = document.getResourceSession().getResourceConfig().getValidTimeConfig();
      final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class, delegatesTo(document.getTrx()));
      final JsonDBItem observed =
          mock(JsonDBItem.class, withSettings().extraInterfaces(Array.class).defaultAnswer(delegatesTo(document)));
      doReturn(cursor).when(observed).getTrx();

      final Instant[] outsidePoints = {Instant.parse("2019-01-01T00:00:00Z"), Instant.parse("2021-01-01T00:00:00Z")};
      for (final Instant point : outsidePoints) {
        for (int mode = 0; mode < 4; mode++) {
          clearInvocations(cursor);
          final boolean strictStart = (mode & 1) != 0;
          final boolean strictEnd = (mode & 2) != 0;
          final var setup = INDEX_WORK.call(() -> Objects.requireNonNull(
              ValidTimeIntervalIndex.sequence(observed, point, config, strictStart, strictEnd, null)));
          final Sequence outside = setup.result();
          assertNotNull(outside);
          assertZeroIndexWork(setup.work(), count, point, mode, "before-demand");
          assertZeroObjectReads(cursor, count, point, mode, "before-demand");
          final WorkReport work = INDEX_WORK.run(() -> {
            assertEquals(0, outside.size().intValue());
            assertNull(outside.get(Int32.ONE));
            assertFalse(outside.booleanValue());
            try (var iterator = outside.iterate()) {
              assertNull(iterator.next());
            }
          });
          assertZeroIndexWork(work, count, point, mode, "sequence");
          assertZeroObjectReads(cursor, count, point, mode, "sequence");
        }
        for (final boolean strictEnd : new boolean[] {false, true}) {
          clearInvocations(cursor);
          final var capture = INDEX_WORK.call(() -> ValidTimeIntervalIndex.keys(observed, point, strictEnd));
          assertEquals(0, capture.result().length);
          assertZeroIndexWork(capture.work(), count, point, strictEnd
              ? 2
              : 0, "keys");
          assertZeroObjectReads(cursor, count, point, strictEnd
              ? 2
              : 0, "keys");
        }
      }

      for (final Instant point : new Instant[] {Instant.parse("2020-06-01T00:00:00Z"),
          Instant.parse("2020-12-31T23:59:59Z")}) {
        for (int mode = 0; mode < 4; mode++) {
          clearInvocations(cursor);
          final Sequence inside =
              ValidTimeIntervalIndex.sequence(observed, point, config, (mode & 1) != 0, (mode & 2) != 0, null);
          assertNotNull(inside);
          final WorkReport positive = INDEX_WORK.run(() -> {
            if (count == 64) {
              assertEquals(count, inside.size().intValue());
              verify(cursor, atLeast(count)).moveTo(anyLong());
              verify(cursor, atLeast(count)).getValue();
            } else {
              assertNotNull(inside.get(Int32.ONE));
              verify(cursor, atLeast(1)).moveTo(anyLong());
              verify(cursor, atLeast(1)).getValue();
              verify(cursor, atLeast(1)).getFirstChildKey();
            }
          });
          assertPositiveIndexWork(positive, count);
          INDEX_WORK.run(() -> assertNotNull(inside.get(Int32.ONE)))
                    .assertZero(EngineWorkCounters.VALID_TIME_INTERVAL_REFS,
                        "repeat demand must reuse the candidate set")
                    .assertZero(EngineWorkCounters.VALID_TIME_POSTING_REFS,
                        "repeat demand must reuse posting evidence");
        }
      }

      final JsonDBCollection collection = mock(JsonDBCollection.class, delegatesTo(store.lookup("budget")));
      doReturn(observed).when(collection).getDocument(eq("rows"), any(Instant.class));
      final JsonDBStore observedStore = mock(JsonDBStore.class, delegatesTo(store));
      doReturn(collection).when(observedStore).lookup("budget");
      try (var observedContext = SirixQueryContext.createWithJsonStore(observedStore);
          var observedChain = SirixCompileChain.createWithJsonStore(observedStore)) {
        for (final Instant point : outsidePoints) {
          for (int mode = 0; mode < 4; mode++) {
            final String expression = bitemporalExpression(point, mode);
            clearInvocations(cursor);
            final var capture =
                INDEX_WORK.call(() -> ((Numeric) new Query(observedChain, "count(" + expression + ")").evaluate(
                    observedContext)).intValue());
            assertEquals(0, capture.result());
            assertZeroIndexWork(capture.work(), count, point, mode, "query-count");
            assertZeroObjectReads(cursor, count, point, mode, "query-count");
            clearInvocations(cursor);
            final WorkReport work = INDEX_WORK.run(() -> {
              final Sequence outside = new Query(observedChain, expression).execute(observedContext);
              if (outside != null) {
                try (var iterator = outside.iterate()) {
                  assertNull(iterator.next());
                }
              }
            });
            assertZeroIndexWork(work, count, point, mode, "query-first");
            assertZeroObjectReads(cursor, count, point, mode, "query-first");
          }
        }
        for (final Instant point : new Instant[] {Instant.parse("2020-06-01T00:00:00Z"),
            Instant.parse("2020-12-31T23:59:59Z")}) {
          for (int mode = 0; mode < 4; mode++) {
            final String expression = bitemporalExpression(point, mode);
            clearInvocations(cursor);
            final WorkReport positive = INDEX_WORK.run(() -> {
              if (count == 64) {
                assertEquals(count, ((Numeric) new Query(observedChain, "count(" + expression + ")").evaluate(
                    observedContext)).intValue());
              } else {
                final Sequence inside = new Query(observedChain, expression).execute(observedContext);
                try (var iterator = inside.iterate()) {
                  assertNotNull(iterator.next());
                }
              }
            });
            assertPositiveIndexWork(positive, count);
            verify(cursor, atLeast(1)).getValue();
          }
        }
      }
    }
  }

  private static String bitemporalExpression(final Instant point, final int mode) {
    final String dateTime = "xs:dateTime('" + point + "')";
    final String direct = "jn:open-bitemporal('budget','rows',xs:dateTime('2099-01-01T00:00:00Z')," + dateTime + ")";
    return switch (mode) {
      case 0 -> direct;
      case 1 -> "for $x in " + direct + " where xs:dateTime($x.vf) lt " + dateTime + " return $x";
      case 2 -> "for $x in " + direct + " where " + dateTime + " lt xs:dateTime($x.vt) return $x";
      default -> "for $x in " + direct + " where xs:dateTime($x.vf) lt " + dateTime + " and " + dateTime
          + " lt xs:dateTime($x.vt) return $x";
    };
  }

  private static void assertZeroIndexWork(final WorkReport work, final int count, final Instant point, final int mode,
      final String route) {
    work.assertZero(EngineWorkCounters.VALID_TIME_INTERVAL_REFS, "an empty closed stab must emit no interval refs")
        .assertZero(EngineWorkCounters.VALID_TIME_POSTING_REFS,
            "an empty closed stab must not expand posting evidence");
    System.out.printf("valid-time empty stab records=%d point=%s mode=%d route=%s intervalRefs=%d postingRefs=%d%n",
        count, point, mode, route, work.of(EngineWorkCounters.VALID_TIME_INTERVAL_REFS),
        work.of(EngineWorkCounters.VALID_TIME_POSTING_REFS));
  }

  private static void assertPositiveIndexWork(final WorkReport work, final int count) {
    work.assertAtLeast(EngineWorkCounters.VALID_TIME_INTERVAL_REFS, count,
        "a nonempty stab must enumerate its candidates")
        .assertExactly(EngineWorkCounters.VALID_TIME_POSTING_REFS, 2L * count,
            "one membership and one verification ref per candidate must be counted");
  }

  private static void assertZeroObjectReads(final JsonNodeReadOnlyTrx cursor, final int count, final Instant point,
      final int mode, final String route) {
    verify(cursor, never()).moveTo(anyLong());
    verify(cursor, never()).getValue();
    verify(cursor, never()).getFirstChildKey();
    for (final String method : new String[] {"moveTo", "getFirstChildKey", "getValue"}) {
      final long calls = mockingDetails(cursor).getInvocations()
                                               .stream()
                                               .filter(invocation -> method.equals(invocation.getMethod().getName()))
                                               .count();
      System.out.printf("valid-time empty stab records=%d point=%s mode=%d route=%s %s=%d%n", count, point, mode, route,
          method, calls);
      assertEquals(0, calls);
    }
  }

  private void shredRows(final String json) {
    final Path databasePath = directory.resolve("budget");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows")
                                                   .storageType(StorageType.FILE_CHANNEL)
                                                   .validTimePaths("vf", "vt")
                                                   .build());
      try (var session = database.beginResourceSession("rows"); var writer = session.beginNodeTrx()) {
        writer.insertSubtreeAsFirstChild(JsonShredder.createStringReader(json), JsonNodeTrx.Commit.NO);
        writer.commit();
      }
    }
  }

  private void assertSliceBudget(final boolean includeUserFunction) {
    final int count = 64;
    final StringBuilder json = new StringBuilder("[");
    for (int i = 0; i < count; i++) {
      if (i != 0) {
        json.append(',');
      }
      json.append("{\"id\":").append(i).append(",\"vf\":\"2020-01-01T00:00:00Z\",\"vt\":\"2030-01-01T00:00:00Z\"}");
    }
    json.append(']');
    shredRows(json.toString());
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var context = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      new Query(chain,
          "let $d := jn:doc('budget','rows') let $i := jn:create-valid-time-index($d) return sdb:commit($d)").evaluate(
              context);
      final JsonDBItem document = store.lookup("budget").getDocument("rows");
      final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class, delegatesTo(document.getTrx()));
      final JsonDBItem observed =
          mock(JsonDBItem.class, withSettings().extraInterfaces(Array.class).defaultAnswer(delegatesTo(document)));
      doReturn(cursor).when(observed).getTrx();
      final Sequence sequence = ValidTimeIntervalIndex.sequence(observed, Instant.parse("2024-01-01T00:00:00Z"),
          document.getResourceSession().getResourceConfig().getValidTimeConfig(), false, true, null);
      assertNotNull(sequence);
      try (var iterator = sequence.iterate()) {
        verify(cursor, never()).moveTo(anyLong());
        verify(cursor, never()).getValue();
        verify(cursor, never()).isObject();
        verify(cursor, never()).getFirstChildKey();
        assertEquals(count, sequence.size().intValue());
        verify(cursor, never()).moveTo(anyLong()); // parent membership is answered by index postings
        verify(cursor, never()).getValue();
        verify(cursor, never()).isObject();
        verify(cursor, never()).getFirstChildKey();
        clearInvocations(cursor);
        assertNotNull(iterator.next());
        verify(cursor, times(1)).moveTo(anyLong());
        verify(cursor, times(1)).getFirstChildKey();
        verify(cursor, never()).getValue();
        // A second iteration is independent and early-close never closes the borrowed transaction.
        try (var second = sequence.iterate()) {
          assertEquals(((JsonDBItem) sequence.get(Int32.ONE)).getNodeKey(), ((JsonDBItem) second.next()).getNodeKey());
        }
      }
      assertEquals(count, sequence.size().intValue());
      assertNull(sequence.get(new Int32(count + 1)));
      // Non-vacuity: the same decorated cursor observes a real field read on consumer demand.
      clearInvocations(cursor);
      cursor.moveToFirstChild();
      cursor.moveToRightSibling();
      cursor.getValue();
      verify(cursor, times(1)).getValue();
      final JsonDBCollection collection = mock(JsonDBCollection.class, delegatesTo(store.lookup("budget")));
      doReturn(observed).when(collection).getDocument(eq("rows"), any(Instant.class));
      doReturn(observed).when(collection).getDocument(eq("rows"), anyInt());
      final JsonDBStore observedStore = mock(JsonDBStore.class, delegatesTo(store));
      doReturn(collection).when(observedStore).lookup("budget");
      try (var observedContext = SirixQueryContext.createWithJsonStore(observedStore);
          var observedChain = SirixCompileChain.createWithJsonStore(observedStore)) {
        final String direct = "jn:open-bitemporal('budget','rows',xs:dateTime('2099-01-01T00:00:00Z'),"
            + "xs:dateTime('2024-01-01T00:00:00Z'))";
        clearInvocations(cursor);
        assertEquals(count,
            ((Numeric) new Query(observedChain, "count(" + direct + ")").evaluate(observedContext)).intValue());
        verify(cursor, never()).getFirstChildKey();
        verify(cursor, never()).getValue();
        final String directSlice =
            "count(for $x in " + direct + " where xs:dateTime('2024-01-01T00:00:00Z') lt xs:dateTime($x.vt) return $x)";
        clearInvocations(cursor);
        assertEquals(count, ((Numeric) new Query(observedChain, directSlice).evaluate(observedContext)).intValue());
        verify(cursor, never()).getFirstChildKey();
        verify(cursor, never()).getValue();
        final String plainSlice = "for $x in jn:doc('budget','rows')[] where "
            + "xs:dateTime($x.vf) le xs:dateTime('2024-01-01T00:00:00Z') and "
            + "xs:dateTime('2024-01-01T00:00:00Z') lt xs:dateTime($x.vt) return $x";
        clearInvocations(cursor);
        assertEquals(count,
            ((Numeric) new Query(observedChain, "count(" + plainSlice + ")").evaluate(observedContext)).intValue());
        verify(cursor, never()).getFirstChildKey();
        verify(cursor, never()).getValue();
        clearInvocations(cursor);
        final Sequence demanded = new Query(observedChain, plainSlice).execute(observedContext);
        try (var iterator = demanded.iterate()) {
          assertNotNull(iterator.next());
        }
        verify(cursor, atLeast(1)).getFirstChildKey();
        verify(cursor, never()).getValue();
        if (!includeUserFunction) {
          return;
        }
        final String wrapped = """
            declare function local:slice($p as xs:dateTime) {
              for $x in jn:open-bitemporal('budget','rows',xs:dateTime('2099-01-01T00:00:00Z'),$p)
              where $p lt xs:dateTime($x.vt) return $x
            };
            count(local:slice(xs:dateTime('2024-01-01T00:00:00Z')))
            """;
        clearInvocations(cursor);
        assertEquals(count, ((Numeric) new Query(observedChain, wrapped).evaluate(observedContext)).intValue());
        verify(cursor, never()).getFirstChildKey();
        verify(cursor, never()).getValue();
      }
    }
  }
}
