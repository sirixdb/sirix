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
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.function.jn.temporal.ValidTimeIntervalIndex;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBItem;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.anyLong;
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
  void aClosedStabOutsideEveryIntervalReadsNoObjectWhenEveryRecordNeedsVerification() {
    final int count = 64;
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
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
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

      final Sequence outside =
          ValidTimeIntervalIndex.sequence(observed, Instant.parse("2019-01-01T00:00:00Z"), config, false, false, null);
      assertNotNull(outside);
      assertEquals(0, outside.size().intValue());
      verify(cursor, never()).moveTo(anyLong());
      verify(cursor, never()).getValue();
      verify(cursor, never()).getFirstChildKey();

      // Non-vacuity: the same decorated cursor verifies every one of those records when the point
      // does fall inside their intervals, so the zero above is a gated union and not a dead route.
      clearInvocations(cursor);
      final Sequence inside =
          ValidTimeIntervalIndex.sequence(observed, Instant.parse("2020-06-01T00:00:00Z"), config, false, false, null);
      assertNotNull(inside);
      assertEquals(count, inside.size().intValue());
      verify(cursor, atLeast(count)).moveTo(anyLong());
      verify(cursor, atLeast(count)).getValue();
    }
  }

  private void shredRows(final String json) {
    final Path databasePath = directory.resolve("budget");
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (var database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder("rows").validTimePaths("vf", "vt").build());
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
    try (var store = BasicJsonDBStore.newBuilder().location(directory).build();
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
