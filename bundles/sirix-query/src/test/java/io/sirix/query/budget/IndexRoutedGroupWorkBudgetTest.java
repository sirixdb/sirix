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
import io.sirix.index.projection.ProjectionIndexCatalog;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionRecordKeySet.Masks;
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
import io.sirix.settings.VersioningType;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    build(false);
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
        final WorkCapture.Captured<String> generic =
            WorkCapture.of(QueryWorkCounters.ROUTES)
                       .call(() -> run(chain, ctx, prolog + body.replace("SRC", "(" + source + ")")));
        verify(cursor, atLeast(VALID_ROWS)).moveTo(anyLong());
        generic.work().assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 0, "the reference takes the generic route");
        clearInvocations(cursor);

        final WorkCapture.Captured<String> routed =
            WorkCapture.of(QueryWorkCounters.ROUTES)
                       .and(EngineWorkCounters.PROJECTION_LEAVES_PRUNED)
                       .and(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                       .call(() -> run(chain, ctx, prolog + body.replace("SRC", source)));
        assertEquals(generic.result(), routed.result(), "the routed answer must equal the generic one");
        routed.work()
              .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1, "the grouped aggregate must be served")
              .assertExactly(EngineWorkCounters.PROJECTION_LEAVES_PRUNED, 2,
                  "one mask excludes the two leaves without valid rows")
              .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 4,
                  "only the kept leaf supplies grade, qty, cost and the derived qty operand");
        final WorkCapture.Captured<String> empty =
            WorkCapture.of(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                       .and(QueryWorkCounters.GROUP_AGGREGATES)
                       .call(() -> run(chain, ctx,
                           prolog.replace("2024-06-01", "2025-01-01") + body.replace("SRC", source)));
        assertEquals("", empty.result());
        empty.work()
             .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0,
                 "an empty source fills no key or operand columns")
             .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1, "the empty source is served");
        // Zero objects: no cursor move and no child-pointer read on the document the opener returns.
        verify(cursor, never()).moveTo(anyLong());
        verify(cursor, never()).getFirstChildKey();
      }
    }
  }

  @Test
  void persistedOrderExceptionPrunesLeavesWithoutSelectedRows() throws Exception {
    build(true);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String query =
          """
              for $c in jn:open-bitemporal('budgetrt','contracts',xs:dateTime('2024-02-01T00:00:00Z'),xs:dateTime('2024-06-01T00:00:00Z'))
              let $grade := $c.grade, $qty := $c.qty, $value := $c.cost * $c.qty
              group by $grade
              let $n := count($qty), $exposure := sum($value)
              order by $grade
              return {"grade":$grade,"n":$n,"exposure":$exposure}
              """;
      final WorkCapture.Captured<String> routed = WorkCapture.of(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                                                             .and(QueryWorkCounters.GROUP_AGGREGATES)
                                                             .call(() -> run(chain, ctx, query));
      assertEquals("{\"grade\":0,\"n\":1,\"exposure\":13200}", routed.result());
      routed.work()
            .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1, "the reordered source is served")
            .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 4,
                "only the leaf with the selected row supplies key and operand bodies");
      final String reference = query.replace("in jn:open-bitemporal(", "in (jn:open-bitemporal(")
                                    .replace("'2024-06-01T00:00:00Z'))", "'2024-06-01T00:00:00Z')))");
      assertEquals(run(chain, ctx, reference), routed.result());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void sparseColdAndWarmSelectionsUsePersistedLookup(final VersioningType versioning) throws Exception {
    final int rows = 32_000;
    build(true, rows, versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String source = "jn:open-bitemporal('budgetrt','contracts',xs:dateTime('2024-02-01T00:00:00Z'),"
          + "xs:dateTime('2024-06-01T00:00:00Z'))";
      final String query =
          "for $c in " + source + " let $grade := $c.grade, $qty := $c.qty, $value := $c.cost * $c.qty group by $grade"
              + " order by $grade return {'grade':$grade,'n':count($qty),'exposure':sum($value)}";
      for (int repeat = 0; repeat < 2; repeat++) {
        final WorkCapture.Captured<String> result = WorkCapture.of(EngineWorkCounters.PROJECTION_LOOKUP_DESCRIPTORS)
                                                               .and(EngineWorkCounters.PROJECTION_LOOKUP_KEYS)
                                                               .and(EngineWorkCounters.PROJECTION_KEY_SEGMENTS)
                                                               .and(EngineWorkCounters.PROJECTION_DENSE_ROWS)
                                                               .and(QueryWorkCounters.GROUP_AGGREGATES)
                                                               .call(() -> run(chain, ctx, query));
        assertEquals("{\"grade\":0,\"n\":1,\"exposure\":13200}", result.result());
        result.work()
              .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1, "the one-key grouping is served")
              .assertExactly(EngineWorkCounters.PROJECTION_LOOKUP_DESCRIPTORS, 1, "only the selected leaf is located")
              .assertExactly(EngineWorkCounters.PROJECTION_LOOKUP_KEYS, 1, "lookup reads one KEYS segment")
              .assertBetween(EngineWorkCounters.PROJECTION_KEY_SEGMENTS, 1, 1,
                  "predicate KEYS are confined to that leaf")
              .assertExactly(EngineWorkCounters.PROJECTION_DENSE_ROWS, 0,
                  "neither cold nor warm selection walks all rows");
      }
      assertEquals(run(chain, ctx, query.replace(source, "(" + source + ")")), run(chain, ctx, query));
      final var document = store.lookup(DB).getDocument(RES);
      final var session = document.getTrx().getResourceSession();
      final int revision = document.getTrx().getRevisionNumber();
      final var handle = ProjectionIndexCatalog.lookupCovering(session,
          session.getResourceConfig().getResource().toString(), revision, new String[] {"[]"}, new String[] {"cost"});
      assertNotNull(handle);
      final var columns = handle.columnStoreOrNull();
      assertNotNull(columns);
      assertTrue(columns.leafCount() >= 32);
      final var fetcher = ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
      final LongArrayList all = new LongArrayList(rows + 1);
      for (final long[] leaf : columns.recordKeys(fetcher)) {
        all.addElements(all.size(), leaf);
      }
      final long[] keys = all.toLongArray();
      Arrays.sort(keys);
      final WorkCapture.Captured<ColumnPredicate> dense =
          WorkCapture.of(EngineWorkCounters.PROJECTION_DENSE_ROWS)
                     .and(EngineWorkCounters.PROJECTION_KEY_SET_ADVANCES)
                     .call(() -> ColumnPredicate.recordKeysIn(keys, columns, fetcher));
      dense.work()
           .assertExactly(EngineWorkCounters.PROJECTION_DENSE_ROWS, rows + 1, "dense construction visits each row once")
           .assertAtMost(EngineWorkCounters.PROJECTION_KEY_SET_ADVANCES, rows + 1 + columns.leafCount(),
               "dense advances remain linear");
      final Masks sparse = fetcher.recordKeyMasks(handle.defId(), keys);
      assertEquals(sparse.physicalSlots(), dense.result().keyMasks.physicalSlots());
      assertEquals(sparse.byFirstKey().size(), dense.result().keyMasks.byFirstKey().size());
      for (final var entry : sparse.byFirstKey().long2ObjectEntrySet()) {
        assertArrayEquals(entry.getValue(), dense.result().keyMasks.byFirstKey().get(entry.getLongKey()));
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

  private void build(final boolean orderException) {
    build(orderException, ROWS, VersioningType.SLIDING_SNAPSHOT);
  }

  private void build(final boolean orderException, final int rowCount, final VersioningType versioning) {
    final Path databasePath = directory.resolve(DB);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      database.createResource(ResourceConfiguration.newBuilder(RES)
                                                   .validTimePaths("vf", "vt")
                                                   .customCommitTimestamps(true)
                                                   .buildPathSummary(true)
                                                   .versioningApproach(versioning)
                                                   .storeDiffs(false)
                                                   .build());
      try (JsonResourceSession session = database.beginResourceSession(RES); JsonNodeTrx wtx = session.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(rows(orderException, rowCount)),
            JsonNodeTrx.Commit.NO);
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, DB);
        BitemporalProjections.declare(session, wtx, RES);
        wtx.commit("E0", Instant.parse("2024-01-15T00:00:00Z"));
        if (orderException) {
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("""
              {"id":%d,"sid":0,"cost":1,"qty":1,"grade":0,
               "vf":"2024-01-01T00:00:00Z","vt":"2024-02-01T00:00:00Z"}
              """.formatted(rowCount + 1)), JsonNodeTrx.Commit.NO);
          wtx.commit("E1", Instant.parse("2024-01-16T00:00:00Z"));
        }
      }
    }
  }

  private static String rows(final boolean orderException, final int rowCount) {
    final StringBuilder json = new StringBuilder(rowCount * 110).append('[');
    for (int i = 0; i < rowCount; i++) {
      if (i > 0) {
        json.append(',');
      }
      final String vt = (orderException
          ? i == VALID_ROWS
          : i < VALID_ROWS)
              ? "2025-01-01T00:00:00Z"
              : "2024-02-01T00:00:00Z";
      json.append("{\"id\":")
          .append(i + 1)
          .append(",\"sid\":")
          .append(i % 7)
          .append(",\"cost\":")
          .append(1_000 + (i * 37) % 900);
      if (i % 97 != 0) {
        json.append(",\"qty\":").append(1 + i % 23);
      }
      json.append(",\"grade\":")
          .append(i % 4)
          .append(",\"vf\":\"2024-01-01T00:00:00Z\",\"vt\":\"")
          .append(vt)
          .append("\"}");
    }
    return json.append(']').toString();
  }
}
