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
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.FillBudgetExceededException;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexHOTStorage;
import io.sirix.index.projection.ProjectionIndexMetadata;
import io.sirix.index.projection.ProjectionSlotLayout;
import io.sirix.index.projection.RowGroupDescriptor;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexRegistry;
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
import io.sirix.query.scan.SirixVectorizedExecutor;
import io.sirix.query.scan.SirixVectorizedExecutor.ComputedLane;
import io.sirix.query.scan.SirixVectorizedExecutor.GroupRouting;
import io.sirix.query.scan.SirixVectorizedExecutor.ServedGroups;
import io.sirix.index.projection.ProjectionIndexByteScan;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.withSettings;
import static org.mockito.Mockito.CALLS_REAL_METHODS;

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
          var referenceChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(observedStore);
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
                       .call(() -> run(referenceChain, ctx, prolog + body.replace("SRC", source)));
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
        var referenceChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
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
      assertEquals(run(referenceChain, ctx, query), routed.result());
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void sparseColdAndWarmSelectionsUsePersistedLookup(final VersioningType versioning) throws Exception {
    final int rows = 32_000;
    build(true, rows, versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var referenceChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final String source = "jn:open-bitemporal('budgetrt','contracts',xs:dateTime('2024-02-01T00:00:00Z'),"
          + "xs:dateTime('2024-06-01T00:00:00Z'))";
      final String query =
          "for $c in " + source + " let $grade := $c.grade, $qty := $c.qty, $value := $c.cost * $c.qty group by $grade"
              + " order by $grade return {'grade':$grade,'n':count($qty),'exposure':sum($value)}";
      for (int repeat = 0; repeat < 4; repeat++) {
        final WorkCapture.Captured<String> result = WorkCapture.of(EngineWorkCounters.PROJECTION_LOOKUP_DESCRIPTORS)
                                                               .and(EngineWorkCounters.PROJECTION_LOOKUP_KEYS)
                                                               .and(EngineWorkCounters.PROJECTION_KEY_SEGMENTS)
                                                               .and(EngineWorkCounters.PROJECTION_DENSE_ROWS)
                                                               .and(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
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
                  "neither cold nor warm selection walks all rows")
              .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 4,
                  "repeated masked requests fetch only the selected key and operands");
      }
      assertEquals(run(referenceChain, ctx, query), run(chain, ctx, query));
      final var document = store.lookup(DB).getDocument(RES);
      final var session = document.getTrx().getResourceSession();
      final int revision = document.getTrx().getRevisionNumber();
      final var handle = ProjectionIndexCatalog.lookupCovering(session,
          session.getResourceConfig().getResource().toString(), revision, new String[] {"[]"}, new String[] {"cost"});
      assertNotNull(handle);
      assertEquals(0, handle.slicedRouteTick(), "masked requests never enter whole-projection promotion");
      assertFalse(handle.payloadsMaterialized());
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

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void correlatedGlobalKeyKeepsItsMaskOnDenseGrouping(final VersioningType versioning) throws Exception {
    final String previous = System.getProperty("sirix.projection.globalDict");
    System.setProperty("sirix.projection.globalDict", "always");
    try {
      build(true, ROWS, versioning);
      try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
          var ctx = SirixQueryContext.createWithJsonStore(store);
          var referenceChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final String source =
            "jn:open-bitemporal('budgetrt','contracts',xs:dateTime($e.ts)," + "xs:dateTime('2024-06-01T00:00:00Z'))";
        final String query = "for $e in [{\"epoch\":0,\"ts\":\"2024-02-01T00:00:00Z\"}][] for $c in " + source
            + " let $epoch := $e.epoch, $until := $c.vt, $cost := $c.cost group by $epoch, $until"
            + " let $total := sum($cost) order by $epoch, $until return {'epoch':$epoch,'until':$until,'total':$total}";
        final String expected = run(referenceChain, ctx, query);
        assertEquals("{\"epoch\":0,\"until\":\"2025-01-01T00:00:00Z\",\"total\":1100}", expected);
        for (int repeat = 0; repeat < 4; repeat++) {
          final WorkCapture.Captured<String> result = WorkCapture.of(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                                                                 .and(QueryWorkCounters.GROUP_DENSE)
                                                                 .call(() -> run(chain, ctx, query));
          assertEquals(expected, result.result());
          result.work()
                .assertExactly(QueryWorkCounters.GROUP_DENSE, 1, "the correlated inner grouping takes the dense arm")
                .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 2,
                    "only the selected leaf supplies the global key and cost");
        }
      }
    } finally {
      restoreProperty("sirix.projection.globalDict", previous);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void cappedCompositeMasksDeclineBeforeWholeLeafMaterialization(final VersioningType versioning) throws Exception {
    final String previous = System.getProperty("sirix.projection.globalDict");
    System.setProperty("sirix.projection.globalDict", "never");
    try {
      build(true, ROWS, versioning);
      try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
          var ctx = SirixQueryContext.createWithJsonStore(store);
          var referenceChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final var document = store.lookup(DB).getDocument(RES);
        final var session = document.getTrx().getResourceSession();
        final int revision = document.getTrx().getRevisionNumber();
        final var handle = ProjectionIndexCatalog.lookupCovering(session,
            session.getResourceConfig().getResource().toString(), revision, new String[] {"[]"}, new String[] {"cost"});
        assertNotNull(handle);
        final var columns = handle.columnStoreOrNull();
        assertNotNull(columns);
        final var fetcher = ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
        final long[] keys = {columns.recordKeysMasked(fetcher, new long[] {2L})[1][0]};
        final var executor = new SirixVectorizedExecutor(session, revision, 1);
        try {
          for (final String[] fields : new String[][] {{"grade", "sid"}, {"vf", "vt"}}) {
            final boolean dictionary = fields[0].equals("vf");
            if (dictionary) {
              assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
                  handle.columnKindOf(handle.columnOf(fields[0])));
              assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
                  handle.columnKindOf(handle.columnOf(fields[1])));
            }
            final String source = "jn:open-bitemporal('budgetrt','contracts',xs:dateTime('2024-02-01T00:00:00Z'),"
                + "xs:dateTime('2024-06-01T00:00:00Z'))";
            final String body = "for $c in " + source + " let $k1 := $c." + fields[0] + ", $k2 := $c." + fields[1]
                + ", $cost := $c.cost group by $k1,$k2 let $total := sum($cost) order by $k1,$k2"
                + " return {'k1':$k1,'k2':$k2,'total':$total}";
            final String query = "subsequence(" + body + ",1,10)";
            final String expected = run(referenceChain, ctx, query);
            assertEquals(dictionary
                ? "{\"k1\":\"2024-01-01T00:00:00Z\",\"k2\":\"2025-01-01T00:00:00Z\",\"total\":1100}"
                : "{\"k1\":0,\"k2\":6,\"total\":1100}", expected);
            for (int repeat = 0; repeat < 4; repeat++) {
              final WorkCapture.Captured<ServedGroups> direct =
                  WorkCapture.of(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                             .and(QueryWorkCounters.GROUP_AGGREGATES_DECLINED)
                             .and(QueryWorkCounters.GROUP_AGGREGATES_FAILED)
                             .call(() -> executor.executeGroupByAggregate(ctx, new String[] {"[]"}, null, fields,
                                 new String[] {"k1", "k2"}, new String[] {"sum"}, new String[] {"cost"},
                                 new String[] {"total"}, new int[] {0, 1}, new boolean[] {true, true},
                                 new boolean[] {true, true}, 10L, null, null, null, null, null, null, null, null, null,
                                 null, new GroupRouting(keys, null)));
              assertNull(direct.result());
              direct.work()
                    .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0,
                        "decline precedes whole-leaf fetches")
                    .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_DECLINED, 1, "capped composite mask declines")
                    .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_FAILED, 0,
                        "decline is an ordinary route decision");
              final WorkCapture.Captured<String> fallback = WorkCapture.of(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                                                                       .and(QueryWorkCounters.GROUP_AGGREGATES_DECLINED)
                                                                       .and(QueryWorkCounters.GROUP_AGGREGATES)
                                                                       .call(() -> run(chain, ctx, query));
              assertEquals(expected, fallback.result());
              fallback.work()
                      .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0,
                          "fallback fetches no projection bodies")
                      .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_DECLINED, 1, "compiled capped mask declines")
                      .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 0, "generic fallback supplies the result");
              assertFalse(handle.payloadsMaterialized());
            }
          }
        } finally {
          executor.close();
        }
      }
    } finally {
      restoreProperty("sirix.projection.globalDict", previous);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void maskedDictionaryDistinctDeclinesBeforeWholeLeafFallback(final VersioningType versioning) throws Exception {
    final String previous = System.getProperty("sirix.projection.globalDict");
    System.setProperty("sirix.projection.globalDict", "never");
    try {
      build(true, ROWS, versioning);
      try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
          var ctx = SirixQueryContext.createWithJsonStore(store)) {
        final var document = store.lookup(DB).getDocument(RES);
        final var session = document.getTrx().getResourceSession();
        final int revision = document.getTrx().getRevisionNumber();
        final var handle = ProjectionIndexCatalog.lookupCovering(session,
            session.getResourceConfig().getResource().toString(), revision, new String[] {"[]"}, new String[] {"vf"});
        assertNotNull(handle);
        assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, handle.columnKindOf(handle.columnOf("vf")));
        final var fetcher = ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
        final long[] keys = {handle.columnStoreOrNull().recordKeysMasked(fetcher, new long[] {2L})[1][0]};
        final var executor = new SirixVectorizedExecutor(session, revision, 1);
        final long previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(1L);
        try {
          for (int repeat = 0; repeat < 4; repeat++) {
            final WorkCapture.Captured<ServedGroups> result =
                WorkCapture.of(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                           .and(QueryWorkCounters.GROUP_AGGREGATES_DECLINED)
                           .and(QueryWorkCounters.GROUP_AGGREGATES_FAILED)
                           .call(() -> group(executor, ctx, "count-distinct", "vf", new GroupRouting(keys, null)));
            assertNull(result.result());
            result.work()
                  .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "no column body is fetched")
                  .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_DECLINED, 1, "the unsupported masked arm declines")
                  .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_FAILED, 0,
                      "a budget refusal is an ordinary decline");
          }
          assertFalse(handle.payloadsMaterialized());
          assertEquals(0, handle.slicedRouteTick());
        } finally {
          ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
          executor.close();
        }
      }
    } finally {
      restoreProperty("sirix.projection.globalDict", previous);
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void maskedWindowBudgetDeclinesBeforeActualBodyRequests(final VersioningType versioning) throws Exception {
    build(true, 32_000, versioning);
    ProjectionIndexRegistry.clear();
    ProjectionIndexCatalog.clearCache();
    Databases.clearGlobalCaches();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store);
        var referenceChain = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
        var chain = SirixCompileChain.createWithJsonStore(store)) {
      final var document = store.lookup(DB).getDocument(RES);
      final var session = document.getTrx().getResourceSession();
      final int revision = document.getTrx().getRevisionNumber();
      final var handle = ProjectionIndexCatalog.lookupCovering(session,
          session.getResourceConfig().getResource().toString(), revision, new String[] {"[]"}, new String[] {"cost"});
      assertNotNull(handle);
      final var columns = handle.columnStoreOrNull();
      assertNotNull(columns);
      assertTrue(columns.leafCount() >= 32);
      final var realFetcher = ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
      final long[] keys = {columns.recordKeysMasked(realFetcher, new long[] {2L})[1][0]};
      final AtomicInteger bodyRequests = new AtomicInteger();
      final ColumnSegmentFetcher observedFetcher = mock(ColumnSegmentFetcher.class, delegatesTo(realFetcher));
      doAnswer(invocation -> {
        final int from = invocation.getArgument(2);
        final int to = invocation.getArgument(3);
        final byte[][] out = invocation.getArgument(4);
        realFetcher.fetchSlotRange(invocation.getArgument(0), invocation.getArgument(1), from, to, out);
        countBodyRequests(out, from, to, bodyRequests);
        return null;
      }).when(observedFetcher).fetchSlotRange(anyInt(), any(), anyInt(), anyInt(), any());
      doAnswer(invocation -> {
        final int from = invocation.getArgument(1);
        final int to = invocation.getArgument(2);
        final byte[][] out = invocation.getArgument(3);
        realFetcher.fetchRange(invocation.getArgument(0), from, to, out);
        countBodyRequests(out, from, to, bodyRequests);
        return null;
      }).when(observedFetcher).fetchRange(any(), anyInt(), anyInt(), any());
      doAnswer(invocation -> {
        final byte[][] out = realFetcher.fetchAll(invocation.getArgument(0));
        countBodyRequests(out, 0, out.length, bodyRequests);
        return out;
      }).when(observedFetcher).fetchAll(any());
      // Positive control: a real BODY fetch is visible even when the work counter misses it.
      columns.windowedLeafAccess(observedFetcher, null, 1).slice(handle.columnOf("cost"), 1);
      assertEquals(1, bodyRequests.get());
      final var observedColumns = spy(columns);
      final var observedHandle = mock(handle.getClass(), delegatesTo(handle));
      doReturn(observedColumns).when(observedHandle).columnStoreOrNull();
      // Static mocks are caller-thread scoped; bind the probe to worker accesses as well.
      doAnswer(invocation -> columns.windowedLeafAccess(observedFetcher, invocation.getArgument(1),
          invocation.getArgument(2), invocation.getArgument(3), invocation.getArgument(4))).when(observedColumns)
                                                                                           .windowedLeafAccess(any(),
                                                                                               any(), anyInt(),
                                                                                               anyInt(), anyBoolean());
      final String source = "jn:open-bitemporal('budgetrt','contracts',xs:dateTime('2024-02-01T00:00:00Z'),"
          + "xs:dateTime('2024-06-01T00:00:00Z'))";
      final String query = "for $c in " + source + " let $grade := $c.grade group by $grade"
          + " order by $grade return {'grade':$grade,'n':sum($c.cost)}";
      final String expected = run(referenceChain, ctx, query);
      assertEquals("{\"grade\":0,\"n\":1100}", expected);
      final var executor = new SirixVectorizedExecutor(session, revision, 1);
      final long previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(1L);
      try (var catalog = mockStatic(ProjectionIndexCatalog.class, CALLS_REAL_METHODS)) {
        catalog.when(() -> ProjectionIndexCatalog.lookupCovering(eq(session), anyString(), eq(revision), any(), any()))
               .thenReturn(observedHandle);
        catalog.when(
            () -> ProjectionIndexCatalog.lookupCovering(eq(session), anyString(), eq(revision), any(), any(), eq(true)))
               .thenReturn(observedHandle);
        catalog.when(() -> ProjectionIndexCatalog.columnSegmentFetcher(session, revision)).thenReturn(observedFetcher);
        for (int repeat = 0; repeat < 4; repeat++) {
          bodyRequests.set(0);
          final WorkCapture.Captured<ServedGroups> result =
              WorkCapture.of(QueryWorkCounters.GROUP_AGGREGATES_DECLINED)
                         .and(QueryWorkCounters.GROUP_AGGREGATES_FAILED)
                         .call(() -> group(executor, ctx, "sum", "cost", new GroupRouting(keys, null)));
          assertAll(() -> assertNull(result.result(), "a masked request must decline the unmasked window decoder"),
              () -> assertEquals(0, bodyRequests.get(), "decline must precede actual BODY requests"));
          result.work()
                .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_DECLINED, 1, "the unsupported masked arm declines")
                .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_FAILED, 0, "a budget refusal is an ordinary decline");
          final WorkCapture.Captured<String> fallback = WorkCapture.of(QueryWorkCounters.GROUP_AGGREGATES_DECLINED)
                                                                   .and(QueryWorkCounters.GROUP_AGGREGATES)
                                                                   .call(() -> run(chain, ctx, query));
          assertEquals(expected, fallback.result(), "generic fallback preserves the query result");
          fallback.work()
                  .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_DECLINED, 1, "the compiled request declines")
                  .assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 0, "the generic fallback supplies the result");
          assertEquals(0, bodyRequests.get(), "fallback fetches no projection BODY segments");
        }
        assertFalse(handle.payloadsMaterialized());
        assertEquals(0, handle.slicedRouteTick());
      } finally {
        ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
        executor.close();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void maskedBudgetReentryStopsBeforeWholeLeafFetch(final VersioningType versioning) throws Exception {
    build(true, ROWS, versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store)) {
      final var document = store.lookup(DB).getDocument(RES);
      final var session = document.getTrx().getResourceSession();
      final int revision = document.getTrx().getRevisionNumber();
      final var handle = ProjectionIndexCatalog.lookupCovering(session,
          session.getResourceConfig().getResource().toString(), revision, new String[] {"[]"}, new String[] {"cost"});
      assertNotNull(handle);
      final var realFetcher = ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
      final var columns = handle.columnStoreOrNull();
      final long[] keys = {columns.recordKeysMasked(realFetcher, new long[] {2L})[1][0]};
      final var refusing = spy(columns);
      final var observedHandle = mock(handle.getClass(), delegatesTo(handle));
      doReturn(refusing).when(observedHandle).columnStoreOrNull();
      doThrow(mock(FillBudgetExceededException.class)).when(refusing).columnMaskedView(anyInt(), any(), any());
      final var executor = new SirixVectorizedExecutor(session, revision, 1);
      try (var catalog = mockStatic(ProjectionIndexCatalog.class, CALLS_REAL_METHODS)) {
        catalog.when(() -> ProjectionIndexCatalog.lookupCovering(eq(session), anyString(), eq(revision), any(), any()))
               .thenReturn(observedHandle);
        catalog.when(
            () -> ProjectionIndexCatalog.lookupCovering(eq(session), anyString(), eq(revision), any(), any(), eq(true)))
               .thenReturn(observedHandle);
        for (int repeat = 0; repeat < 4; repeat++) {
          clearInvocations(refusing);
          final WorkCapture.Captured<ServedGroups> result =
              WorkCapture.of(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                         .and(QueryWorkCounters.GROUP_AGGREGATES_DECLINED)
                         .and(QueryWorkCounters.GROUP_AGGREGATES_FAILED)
                         .call(() -> group(executor, ctx, "sum", "cost", new GroupRouting(keys, null)));
          assertNull(result.result());
          result.work()
                .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_DECLINED, 1,
                    "the resident fill refusal declines once")
                .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_FAILED, 0, "budget refusals do not count as defects")
                .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "neither retry reads a body");
          verify(refusing).columnMaskedView(anyInt(), any(), any());
          verify(refusing, never()).windowedLeafAccess(any(), any(), anyInt(), anyInt(), anyBoolean());
        }
        assertFalse(handle.payloadsMaterialized());
        assertEquals(0, handle.slicedRouteTick());
      } finally {
        executor.close();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void computedResidencyPricesEveryOperandAndDerivedBuffer(final VersioningType versioning) throws Exception {
    build(true, ROWS, versioning);
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
        var ctx = SirixQueryContext.createWithJsonStore(store)) {
      final var document = store.lookup(DB).getDocument(RES);
      final var session = document.getTrx().getResourceSession();
      final int revision = document.getTrx().getRevisionNumber();
      final var handle =
          ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(), revision,
              new String[] {"[]"}, new String[] {"cost", "qty"});
      assertNotNull(handle);
      final var columns = handle.columnStoreOrNull();
      final var fetcher = ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
      columns.column(handle.columnOf("grade"), fetcher);
      columns.column(handle.columnOf("cost"), fetcher);
      final long qtyBytes = columns.projectedColumnFillBytes(handle.columnOf("qty"));
      long derivedBytes = 0L;
      for (int leaf = 0; leaf < columns.rowGroupCount(); leaf++) {
        final long rows = columns.rowCount(leaf);
        derivedBytes += (rows + ((rows + 63L) >>> 6)) * Long.BYTES;
      }
      final var observedColumns = spy(columns);
      final var observedHandle = mock(handle.getClass(), delegatesTo(handle));
      doReturn(observedColumns).when(observedHandle).columnStoreOrNull();
      final ComputedLane lane = new ComputedLane(new String[] {"cost", "qty"},
          new int[] {0, 1, ProjectionIndexByteScan.COMPUTED_OP_MUL}, new long[0]);
      final long retained = columns.retainedFillBytes();
      final var executor = new SirixVectorizedExecutor(session, revision, 1);
      final long previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(retained + qtyBytes - 1);
      try (var catalog = mockStatic(ProjectionIndexCatalog.class, CALLS_REAL_METHODS)) {
        catalog.when(() -> ProjectionIndexCatalog.lookupCovering(eq(session), anyString(), eq(revision), any(), any()))
               .thenReturn(observedHandle);
        catalog.when(
            () -> ProjectionIndexCatalog.lookupCovering(eq(session), anyString(), eq(revision), any(), any(), eq(true)))
               .thenReturn(observedHandle);
        for (final long remainder : new long[] {qtyBytes - 1, qtyBytes + derivedBytes - 1}) {
          ProjectionColumnStore.setColumnFillBudgetBytesForTesting(retained + remainder);
          clearInvocations(observedColumns);
          final WorkCapture.Captured<ServedGroups> result =
              WorkCapture.of(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                         .and(QueryWorkCounters.GROUP_AGGREGATES_DECLINED)
                         .call(() -> group(executor, ctx, "sum", "prog:0",
                             new GroupRouting(null, new ComputedLane[] {lane})));
          assertNull(result.result());
          result.work()
                .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_DECLINED, 1,
                    "one decision declines the resident route")
                .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "decline precedes any operand fill");
          verify(observedColumns).columnsFitWithinBudget(any(), eq(-1), eq(derivedBytes));
          verify(observedColumns, never()).columnMaskedView(anyInt(), any(), any());
          assertFalse(columns.columnFilled(handle.columnOf("qty")));
        }
        ProjectionColumnStore.setColumnFillBudgetBytesForTesting(retained + qtyBytes + derivedBytes);
        final WorkCapture.Captured<ServedGroups> accepted =
            WorkCapture.of(QueryWorkCounters.GROUP_AGGREGATES)
                       .call(() -> group(executor, ctx, "sum", "prog:0",
                           new GroupRouting(null, new ComputedLane[] {lane})));
        assertNotNull(accepted.result());
        accepted.work().assertExactly(QueryWorkCounters.GROUP_AGGREGATES, 1, "the complete computed footprint fits");
      } finally {
        ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
        executor.close();
      }
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void coldOpenTailPrunesBeforeExcludedBodyReads(final VersioningType versioning) throws Exception {
    final long tailKey = buildOpenTail(versioning);
    final String main = tailSource(RES, "2024-06-01T00:00:00Z");
    final String partner = tailSource("suppliers", "2024-06-01T00:00:00Z");
    final String empty = tailSource(RES, "2030-01-01T00:00:00Z");
    final String group =
        " let $grade := $a.grade, $qty := $a.qty, $cost := $a.cost group by $grade let $n := count($qty), $total := sum($cost)"
            + " order by $grade return {'grade':$grade,'n':$n,'total':$total}";
    final String[] queries = {"for $a in " + main + group,
        "for $e in [{'epoch':1,'ts':'2024-02-01T00:00:00Z'}][] for $a in "
            + main.replace("xs:dateTime('2024-02-01T00:00:00Z')", "xs:dateTime($e.ts)")
            + " let $epoch := $e.epoch, $grade := $a.grade, $qty := $a.qty, $cost := $a.cost group by $epoch,$grade"
            + " let $n := count($qty), $total := sum($cost) order by $epoch,$grade"
            + " return {'epoch':$epoch,'grade':$grade,'n':$n,'total':$total}",
        "for $a in " + main + " for $b in " + partner + " where $a.id eq $b.id" + group,
        "for $b in " + partner + " for $a in " + main + " where $a.id eq $b.id" + group,
        "let $new := " + main + " for $a in " + partner + " where exists(for $b in $new where $b.id eq $a.id return $b)"
            + " let $region := $a.region, $tier := $a.tier group by $region order by $region return {'region':$region,'n':count($tier)}",
        "let $new := " + partner + " for $a in " + main + " where exists(for $b in $new where $b.id eq $a.id return $b)"
            + group,
        "let $new := " + partner + " for $a in " + main
            + " where empty(for $b in $new where $b.tier eq $a.id return $b)" + group,
        "let $new := " + empty + " for $a in " + main + " where empty(for $b in $new where $b.id eq $a.id return $b)"
            + group};
    for (final String query : queries) {
      clearColdProjectionState();
      try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
          var ctx = SirixQueryContext.createWithJsonStore(store);
          var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final String expected = run(generic, ctx, query);
        final long joins = SirixVectorizedExecutor.joinGroupServedCount();
        final WorkCapture.Captured<String> actual = WorkCapture.of(EngineWorkCounters.PROJECTION_MASKED_TAIL_DEFERRALS)
                                                               .and(EngineWorkCounters.PROJECTION_TAIL_BODY_READS)
                                                               .and(QueryWorkCounters.GROUP_AGGREGATES)
                                                               .and(QueryWorkCounters.GROUP_AGGREGATES_FAILED)
                                                               .call(() -> run(chain, ctx, query));
        assertEquals(expected, actual.result(), query);
        assertEquals(1, (SirixVectorizedExecutor.joinGroupServedCount() - joins)
            + actual.work().of(QueryWorkCounters.GROUP_AGGREGATES), query);
        actual.work()
              .assertExactly(EngineWorkCounters.PROJECTION_MASKED_TAIL_DEFERRALS, 1,
                  query + " reaches the open-tail directory boundary")
              .assertExactly(EngineWorkCounters.PROJECTION_TAIL_BODY_READS, 0,
                  "the excluded tail's base BODY segments are never read")
              .assertExactly(QueryWorkCounters.GROUP_AGGREGATES_FAILED, 0, "admission declines without an arm failure");
      }
    }
    clearColdProjectionState();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build()) {
      final var document = store.lookup(DB).getDocument(RES);
      final var session = document.getResourceSession();
      final int revision = document.getTrx().getRevisionNumber();
      final int index = session.getRtxIndexController(revision)
                               .getIndexes()
                               .getIndexDefs()
                               .stream()
                               .filter(def -> def.isProjectionIndex())
                               .findFirst()
                               .orElseThrow()
                               .getID();
      final var reader = document.getTrx().getStorageEngineReader();
      final var metadata = ProjectionIndexMetadata.parse(ProjectionIndexHOTStorage.readMetadataBlob(reader, index));
      assertNotNull(metadata);
      assertEquals(ProjectionSlotLayout.ROW_GROUP_MAJOR, metadata.slotLayout());
      assertEquals(3, metadata.rowGroupCount());
      final byte[] descriptor = ProjectionIndexHOTStorage.readBlob(reader, index, 3L << 16);
      assertTrue(RowGroupDescriptor.isTailed(descriptor), "the reopened last leaf has an appended row tail");
      final var fetcher = ProjectionIndexCatalog.columnSegmentFetcher(session, revision);
      final var keys = WorkCapture.of(EngineWorkCounters.PROJECTION_TAIL_BODY_READS)
                                  .and(EngineWorkCounters.PROJECTION_LOOKUP_KEYS)
                                  .call(() -> fetcher.recordKeyMasks(index, new long[] {tailKey}));
      assertTrue(keys.result().physicalSlots().contains(3));
      keys.work()
          .assertExactly(EngineWorkCounters.PROJECTION_LOOKUP_KEYS, 1,
              "persisted KEYS lookup resolves the appended tail identity")
          .assertExactly(EngineWorkCounters.PROJECTION_TAIL_BODY_READS, 0, "KEYS lookup reads no BODY segments");
      final var positive =
          WorkCapture.of(EngineWorkCounters.PROJECTION_TAIL_BODY_READS)
                     .call(() -> ProjectionIndexHOTStorage.readRowGroupFromColumnSegmentSlots(reader, index, 3));
      assertNotNull(positive.result());
      assertEquals(ROWS - 2 * ProjectionIndexRowGroupPage.MAX_ROWS + 1,
          ProjectionIndexRowGroupPage.deserialize(positive.result()).getRowCount());
      positive.work()
              .assertBetween(EngineWorkCounters.PROJECTION_TAIL_BODY_READS, 1, 8,
                  "an authorized whole-tail read is visible at the storage source");
      assertNotNull(ProjectionIndexCatalog.lookupCovering(session, session.getResourceConfig().getResource().toString(),
          revision, new String[] {"[]"}, new String[] {"cost"}));
      final var masked = WorkCapture.of(EngineWorkCounters.PROJECTION_TAIL_BODY_READS)
                                    .and(EngineWorkCounters.PROJECTION_MASKED_TAIL_DEFERRALS)
                                    .call(() -> ProjectionIndexCatalog.lookupCovering(session,
                                        session.getResourceConfig().getResource().toString(), revision,
                                        new String[] {"[]"}, new String[] {"cost"}, true));
      assertNotNull(masked.result(), "masked admission retains lazy tail payloads despite an unmasked cache entry");
      masked.work()
            .assertExactly(EngineWorkCounters.PROJECTION_MASKED_TAIL_DEFERRALS, 1,
                "masked admission still defers the tail")
            .assertExactly(EngineWorkCounters.PROJECTION_TAIL_BODY_READS, 0, "masked admission reads no tail body");
    }
  }

  @ParameterizedTest
  @EnumSource(VersioningType.class)
  void emptyMasksSkipEveryOpenTailLookup(final VersioningType versioning) throws Exception {
    buildOpenTail(versioning);
    final String main = tailSource(RES, "2024-06-01T00:00:00Z");
    final String empty = tailSource(RES, "2030-01-01T00:00:00Z");
    final String group = " let $grade := $a.grade, $qty := $a.qty group by $grade let $n := count($qty) order by $grade"
        + " return {'grade':$grade,'n':$n}";
    final String[] queries = {"for $a in " + empty + group,
        "for $e in [{'epoch':1,'ts':'2024-02-01T00:00:00Z'}][] for $a in "
            + empty.replace("xs:dateTime('2024-02-01T00:00:00Z')", "xs:dateTime($e.ts)")
            + " let $epoch := $e.epoch, $grade := $a.grade, $qty := $a.qty group by $epoch,$grade"
            + " let $n := count($qty) order by $epoch,$grade return {'epoch':$epoch,'grade':$grade,'n':$n}",
        "for $a in " + empty + " for $b in " + main + " where $a.id eq $b.id" + group,
        "for $a in " + main + " for $b in " + empty + " where $a.id eq $b.id" + group,
        "let $new := " + main + " for $a in " + empty + " where exists(for $b in $new where $b.id eq $a.id return $b)"
            + group,
        "let $new := " + empty + " for $a in " + main + " where exists(for $b in $new where $b.id eq $a.id return $b)"
            + group};
    for (final String query : queries) {
      clearColdProjectionState();
      try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build();
          var ctx = SirixQueryContext.createWithJsonStore(store);
          var generic = SirixCompileChain.createWithJsonStoreWithoutAutoWiring(store);
          var chain = SirixCompileChain.createWithJsonStore(store)) {
        final String expected = run(generic, ctx, query);
        assertEquals("", expected);
        final long catalog = ProjectionIndexCatalog.servedCount();
        final long joins = SirixVectorizedExecutor.joinGroupServedCount();
        final var actual = WorkCapture.of(EngineWorkCounters.PROJECTION_TAIL_BODY_READS)
                                      .and(EngineWorkCounters.PROJECTION_MASKED_TAIL_DEFERRALS)
                                      .and(EngineWorkCounters.PROJECTION_BODY_SEGMENTS)
                                      .and(QueryWorkCounters.GROUP_AGGREGATES)
                                      .call(() -> run(chain, ctx, query));
        assertEquals(expected, actual.result());
        assertEquals(catalog, ProjectionIndexCatalog.servedCount(), "empty masks make no handle lookup");
        assertEquals(1, (SirixVectorizedExecutor.joinGroupServedCount() - joins)
            + actual.work().of(QueryWorkCounters.GROUP_AGGREGATES), "the empty result stays routed: " + query);
        actual.work()
              .assertExactly(EngineWorkCounters.PROJECTION_MASKED_TAIL_DEFERRALS, 0, "no tail admission is attempted")
              .assertExactly(EngineWorkCounters.PROJECTION_TAIL_BODY_READS, 0, "no tail body is read")
              .assertExactly(EngineWorkCounters.PROJECTION_BODY_SEGMENTS, 0, "no column fill is attempted");
      }
    }
    clearColdProjectionState();
    try (var store = BasicJsonDBStore.newBuilder().location(directory).storageType(StorageType.FILE_CHANNEL).build()) {
      final var document = store.lookup(DB).getDocument(RES);
      final var executor =
          new SirixVectorizedExecutor(document.getResourceSession(), document.getTrx().getRevisionNumber(), 1);
      try (var catalog = mockStatic(ProjectionIndexCatalog.class, CALLS_REAL_METHODS)) {
        assertNull(executor.maskedColumns(new String[] {"[]"}, new long[0], new String[] {"cost"}));
        catalog.verify(() -> ProjectionIndexCatalog.lookupCovering(any(), anyString(), anyInt(), any(), any()),
            never());
        catalog.verify(
            () -> ProjectionIndexCatalog.lookupCovering(any(), anyString(), anyInt(), any(), any(), anyBoolean()),
            never());
      } finally {
        executor.close();
      }
    }
  }

  private static String tailSource(final String resource, final String valid) {
    return "jn:open-bitemporal('" + DB + "','" + resource + "',xs:dateTime('2024-02-01T00:00:00Z'),xs:dateTime('"
        + valid + "'))";
  }

  private static void clearColdProjectionState() {
    ProjectionIndexCatalog.clearCache();
    ProjectionIndexRegistry.clear();
    Databases.clearGlobalCaches();
  }

  private long buildOpenTail(final VersioningType versioning) {
    final String previousLayout = System.setProperty("sirix.projection.columnMajorSlots", "false");
    final String previousDictionary = System.setProperty("sirix.projection.globalDict", "never");
    final Path databasePath = directory.resolve(DB);
    Databases.createJsonDatabase(new DatabaseConfiguration(databasePath));
    try (Database<JsonResourceSession> database = Databases.openJsonDatabase(databasePath)) {
      for (final String resource : new String[] {RES, "suppliers"}) {
        database.createResource(ResourceConfiguration.newBuilder(resource)
                                                     .validTimePaths("vf", "vt")
                                                     .customCommitTimestamps(true)
                                                     .buildPathSummary(true)
                                                     .versioningApproach(versioning)
                                                     .storeDiffs(false)
                                                     .build());
        try (var session = database.beginResourceSession(resource); var wtx = session.beginNodeTrx()) {
          final String input = resource.equals(RES)
              ? rows(true, ROWS)
              : """
                  [{"id":1001,"region":0,"tier":1,
                    "vf":"2024-01-01T00:00:00Z","vt":"2025-01-01T00:00:00Z"}]
                  """;
          wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader(input), JsonNodeTrx.Commit.NO);
          wtx.moveToDocumentRoot();
          wtx.moveToFirstChild();
          ValidTimeIndexes.createValidTimeIndexesIfConfigured(session, wtx, DB);
          BitemporalProjections.declare(session, wtx, resource);
          wtx.commit("E0", Instant.parse("2024-01-15T00:00:00Z"));
        }
      }
      try (var session = database.beginResourceSession(RES); var wtx = session.beginNodeTrx()) {
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.insertSubtreeAsLastChild(JsonShredder.createStringReader("""
            {"id":2601,"sid":0,"cost":1,"qty":1,"grade":0,
             "vf":"2024-01-01T00:00:00Z","vt":"2024-02-01T00:00:00Z"}
            """), JsonNodeTrx.Commit.NO);
        wtx.moveToDocumentRoot();
        wtx.moveToFirstChild();
        wtx.moveToLastChild();
        final long key = wtx.getNodeKey();
        wtx.commit("E1", Instant.parse("2024-01-16T00:00:00Z"));
        return key;
      }
    } finally {
      restoreProperty("sirix.projection.columnMajorSlots", previousLayout);
      restoreProperty("sirix.projection.globalDict", previousDictionary);
    }
  }

  private static ServedGroups group(final SirixVectorizedExecutor executor, final SirixQueryContext ctx,
      final String func, final String field, final GroupRouting routing) {
    return executor.executeGroupByAggregate(ctx, new String[] {"[]"}, null, new String[] {"grade"},
        new String[] {"grade"}, new String[] {func}, new String[] {field}, new String[] {"n"}, null, null, null, -1L,
        null, null, null, null, null, null, null, null, null, null, routing);
  }

  private static void countBodyRequests(final byte[][] segments, final int from, final int to,
      final AtomicInteger requests) {
    for (int i = from; i < to; i++) {
      final byte[] segment = segments[i];
      if (segment != null && segment[ProjectionIndexColumnSegmentCodec.SEGMENT_HEADER_BYTES
          - 1] == ProjectionIndexColumnSegmentCodec.SEG_KIND_BODY) {
        requests.incrementAndGet();
      }
    }
  }

  private static void restoreProperty(final String name, final String previous) {
    if (previous == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, previous);
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
