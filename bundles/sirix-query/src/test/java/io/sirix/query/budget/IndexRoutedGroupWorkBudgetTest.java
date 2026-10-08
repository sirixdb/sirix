package io.sirix.query.budget;

import io.brackit.query.Query;
import io.brackit.query.jdm.json.Array;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.budget.EngineWorkCounters;
import io.sirix.budget.WorkCapture;
import io.sirix.io.StorageType;
import io.sirix.query.SirixCompileChain;
import io.sirix.query.SirixQueryContext;
import io.sirix.query.bench.bitemporal.BitemporalProjections;
import io.sirix.query.bench.bitemporal.BitemporalSchema;
import io.sirix.query.json.BasicJsonDBStore;
import io.sirix.query.json.JsonDBCollection;
import io.sirix.query.json.JsonDBItem;
import io.sirix.query.json.JsonDBStore;
import io.sirix.query.json.ValidTimeIndexes;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;

/**
 * The regression this guards: an index-routed grouped aggregate — a grouped FLWOR over
 * {@code jn:open-bitemporal} served from the projection under the valid-time index's row mask —
 * must materialise ZERO record objects (the generic route builds one {@code JsonDBObject} per valid
 * row and reads its fields), and must read ONLY the projection leaves that hold an admitted key
 * (the row source prunes the rest before any column segment is fetched).
 *
 * <p>
 * Fixture: 2,600 segments in three projection leaves; at the queried valid instant only the first
 * leaf's rows are valid, so two of the three leaves must be pruned. The document cursor the opener
 * would navigate is a decorated real transaction: the routed query makes no cursor move and reads
 * no child pointer through it, while the generic reference over the same decorated cursor is the
 * positive control that moves it once per valid row.
 * </p>
 *
 * <p>
 * Measured (2026-10-08): routed — groupAggregates 1, numericGroupBys 1, groupSliced 1, leavesPruned 4
 * (two prune passes over the same two leaves), cursor moves 0; generic reference — groupAggregates 0,
 * cursor moves ≥ 1,000. With the row source left out of the keep mask, leavesPruned reads 0 and
 * every leaf's columns are fetched.
 * </p>
 */
@Isolated
final class IndexRoutedGroupWorkBudgetTest {

  private static final String DB = "budgetrt";
  private static final String RES = BitemporalSchema.CONTRACTS;
  private static final int VALID_ROWS = 1_000; // exactly the first projection leaf
  private static final int ROWS = 2_600;

  @TempDir
  Path directory;

  @Test
  void routedGroupReadsOnlyMaskedLeavesAndMaterialisesNoObjects() throws Exception {
    build();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build()) {
      final JsonDBCollection realCollection = store.lookup(DB);
      final JsonDBCollection collection = mock(JsonDBCollection.class, delegatesTo(realCollection));
      final JsonDBStore observedStore = mock(JsonDBStore.class, delegatesTo(store));
      doReturn(collection).when(observedStore).lookup(DB);
      final JsonDBItem document = realCollection.getDocument(RES);
      final JsonNodeReadOnlyTrx cursor = mock(JsonNodeReadOnlyTrx.class, delegatesTo(document.getTrx()));
      final JsonDBItem observed =
          mock(JsonDBItem.class, withSettings().extraInterfaces(Array.class).defaultAnswer(delegatesTo(document)));
      doReturn(cursor).when(observed).getTrx();
      doReturn(observed).when(collection).getDocument(eq(RES), any(Instant.class));
      try (var ctx = SirixQueryContext.createWithJsonStore(observedStore);
          var chain = SirixCompileChain.createWithJsonStore(observedStore)) {
        final String prolog = "declare variable $T := xs:dateTime('2024-02-01T00:00:00Z');\n"
            + "declare variable $P := xs:dateTime('2024-06-01T00:00:00Z');\n";
        final String body = """
            for $c in SRC
            let $grade := $c.grade, $qty := $c.qty, $value := $c.cost * $c.qty
            group by $grade
            let $n := count($qty), $exposure := sum($value)
            order by $grade
            return {"grade":$grade,"n":$n,"exposure":$exposure}
            """;
        final String source = "jn:open-bitemporal('" + DB + "','" + RES + "',$T,$P)";
        // Positive control: the generic pipeline over the SAME decorated cursor materialises the
        // valid rows, one cursor move each.
        final WorkCapture.Captured<String> generic = WorkCapture.of(QueryWorkCounters.ROUTES)
                                                                .call(() -> run(chain, ctx,
                                                                    prolog + body.replace("SRC", "(" + source + ")")));
        verify(cursor, atLeast(VALID_ROWS)).moveTo(anyLong());
        generic.work().assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 0, "the reference takes the generic route");
        clearInvocations(cursor);

        final WorkCapture.Captured<String> routed = WorkCapture.of(QueryWorkCounters.ROUTES)
                                                               .and(EngineWorkCounters.PROJECTION_LEAVES_PRUNED)
                                                               .call(() -> run(chain, ctx,
                                                                   prolog + body.replace("SRC", source)));
        assertEquals(generic.result(), routed.result(), "the routed answer must equal the generic one");
        routed.work()
              .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1, "the grouped aggregate must be served")
              // The route prices its keep mask twice — once to bound the derived lane's evaluation
              // and once inside the arm's own predicate fill — and each pass drops the same two
              // leaves. A dedup of the two passes tightens this to 2; a mask that stopped pruning
              // reads 0 whichever pass runs.
              .assertExactly(EngineWorkCounters.PROJECTION_LEAVES_PRUNED, 4,
                  "two of the three leaves hold no valid row and must never be fetched (two prune passes)");
        // Zero objects: no cursor move and no child-pointer read on the document the opener returns.
        verify(cursor, never()).moveTo(anyLong());
        verify(cursor, never()).getFirstChildKey();
      }
    }
  }

  private static String run(final SirixCompileChain chain, final SirixQueryContext ctx, final String query)
      throws Exception {
    try (final ByteArrayOutputStream out = new ByteArrayOutputStream(); final PrintWriter pw = new PrintWriter(out)) {
      new Query(chain, query).serialize(ctx, pw);
      pw.flush();
      return out.toString().trim();
    }
  }

  private void build() {
    final Path databasePath = directory.resolve(DB);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RES)
                                                   .validTimePaths("vf", "vt")
                                                   .customCommitTimestamps(true)
                                                   .buildPathSummary(true)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(RES); JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(rows()), JsonNodeTrx.Commit.NO);
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, DB);
        BitemporalProjections.declare(session, wtx, RES);
        wtx.commit("E0", Instant.parse("2024-01-15T00:00:00Z"));
      }
    }
  }

  /** The first {@value #VALID_ROWS} rows are valid through 2025; every later row ended in February. */
  private static String rows() {
    final StringBuilder json = new StringBuilder(ROWS * 110).append('[');
    for (int i = 0; i < ROWS; i++) {
      if (i > 0) {
        json.append(',');
      }
      final String vt = i < VALID_ROWS
          ? "2025-01-01T00:00:00Z"
          : "2024-02-01T00:00:00Z";
      json.append("{\"id\":").append(i + 1).append(",\"sid\":").append(i % 7).append(",\"cost\":")
          .append(1_000 + (i * 37) % 900);
      if (i % 97 != 0) {
        json.append(",\"qty\":").append(1 + i % 23);
      }
      json.append(",\"grade\":").append(i % 4).append(",\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"").append(vt)
          .append("\"}");
    }
    return json.append(']').toString();
  }
}
