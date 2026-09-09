/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.Database;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.ZoneIndex;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import io.sirix.service.json.shredder.JsonShredder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Executable witnesses over sealed dictionaries and nonresident column payloads. */
final class SegmentTopKBoundsTest {
  private static final String RESOURCE = "bounds";
  private Path path;
  private Database<JsonResourceSession> database;
  private JsonResourceSession session;

  @BeforeEach
  void createDatabase() throws IOException {
    path = Files.createTempDirectory(Path.of("build"), "segment-bounds-").resolve("db");
    Databases.createJsonDatabase(new DatabaseConfiguration(path));
    database = Databases.openJsonDatabase(path);
    database.createResource(ResourceConfiguration.newBuilder(RESOURCE).build());
    session = database.beginResourceSession(RESOURCE);
  }

  @AfterEach
  void closeDatabase() throws IOException {
    session.close();
    database.close();
    Databases.removeDatabase(path);
    Files.delete(path.getParent());
  }

  @Test
  void parallelPlanOrdersValuesAndSkipsWithoutFetchingLosingLeaves() {
    // Both mint order and segment order disagree with value order. Supplementary characters sort
    // before E000 under UTF-16, while unsigned UTF-8 byte order would put them after it.
    final String[][] mints = {{"zz", "", "m"}, {"\uE000", "", "\uD800\uDC00"}, {"yy", "", "a"}, {"zz", "", "a"}};
    final Fixture fixture = fixture(mints, 16);
    final ColumnPredicate excluded = exclusion(mints, "", Op.NE);
    for (final boolean descending : new boolean[] {false, true}) {
      fixture.reads().clear();
      final ProjectionColumnStore store = fixture.store();
      final long skipped = ProjectionColumnScan.topKLeavesSkippedCount();
      final ConcurrentLinkedQueue<JsonNodeReadOnlyTrx> opened = new ConcurrentLinkedQueue<>();
      try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        final long[] answer = ProjectionColumnScan.topKRecordKeys(store, new ColumnPredicate[] {excluded},
            new int[] {0}, new boolean[] {descending}, 40, fixture.fetcher(),
            new GlobalValueDictionary.ReadView[] {view(fixture, rtx)}, () -> {
              final JsonNodeReadOnlyTrx worker = session.beginNodeReadOnlyTrx();
              opened.add(worker);
              return new GlobalValueDictionary.ReadView[] {view(fixture, worker)};
            });
        assertArrayEquals(expected(fixture, "", descending, 40), answer,
            "value order, multiplicity and document ties must all survive bound pruning");
        assertTrue(ProjectionColumnScan.topKLeavesSkippedCount() > skipped, "the bound must actually prune");
        assertFalse(store.columnFilled(0), "planning must not materialize the sort column");
        assertFalse(store.recordKeysFilled(), "planning must not materialize record keys");
        assertTrue(fixture.reads().size() < fixture.payloadCount(), "some payloads must remain unfetched");
        assertTrue(fixture.reads().values().stream().allMatch(count -> count.get() == 1),
            "a fetched payload must be read once, with no speculative scan and restart");
      } finally {
        opened.forEach(JsonNodeReadOnlyTrx::close);
      }
    }
  }

  @Test
  void endpointsAreCollationPositionsAndAreCachedAcrossLeaves() {
    final String[][] mints = {{"z", "", "b"}, {"z", "", "a"}};
    final Fixture fixture = fixture(mints, 16);
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final GlobalValueDictionary.ReadView view = view(fixture, rtx);
      for (final boolean descending : new boolean[] {false, true}) {
        final ColumnPredicate excluded = exclusion(mints, descending
            ? "z"
            : "", Op.NE);
        final SegmentTopKBounds bounds =
            SegmentTopKBounds.create(view, new ColumnPredicate[] {excluded}, 0, descending);
        assertNotNull(bounds);
        final ZoneIndex zone = fixture.store().zoneIndex(0);
        for (int leaf = 0; leaf < fixture.leaves().size(); leaf++) {
          assertEquals(leaf < 16
              ? "b"
              : "a", view.valueOfCell(bounds.forLeaf(zone, leaf)));
        }
        assertEquals(4, bounds.positionLookups(), "two positions per segment, independent of leaf count");
      }
    }
  }

  @Test
  void uncertainMembershipAndUnsupportedRefinementsDoNotReadAnEndpoint() {
    final String[][] mints = {{"z", "", "b"}, {"z", "", "a"}};
    final Fixture fixture = fixture(mints, 1);
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final GlobalValueDictionary.ReadView view = view(fixture, rtx);
      final ColumnPredicate ne = exclusion(mints, "", Op.NE);
      assertNull(SegmentTopKBounds.create(view, new ColumnPredicate[0], 0, false),
          "an unrefined ordering is refused: its bound would need a whole-column all-present pass");
      assertNull(SegmentTopKBounds.create(view, new ColumnPredicate[] {exclusion(mints, "b", Op.EQ)}, 0, false));
      assertNull(SegmentTopKBounds.create(view, new ColumnPredicate[] {ne, ne}, 0, false));
      final SegmentTopKBounds bounds = SegmentTopKBounds.create(view, new ColumnPredicate[] {ne}, 0, false);
      assertNotNull(bounds);
      final long s1 = ProjectionIndexRowGroupPage.packSegmentCell(1, 1);
      final ZoneIndex zone =
          new ZoneIndex(new long[] {1, 1, 0, 5}, new long[] {3, s1, 3, 4}, new byte[4], new long[] {0b1110}); // unknown,
                                                                                                              // mixed,
                                                                                                              // invalid
                                                                                                              // id,
                                                                                                              // all-missing
      for (int leaf = 0; leaf < 4; leaf++) {
        assertEquals(SegmentTopKBounds.UNKNOWN, bounds.forLeaf(zone, leaf));
      }
      assertEquals(0, bounds.positionLookups());
    }
  }

  @Test
  void excludingTheOnlyDictionaryValueProvesNoMatchingRows() {
    final String[][] mints = {{""}};
    final Fixture fixture = fixture(mints, 2);
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final SegmentTopKBounds bounds =
          SegmentTopKBounds.create(view(fixture, rtx), new ColumnPredicate[] {exclusion(mints, "", Op.NE)}, 0, false);
      assertNotNull(bounds);
      assertEquals(SegmentTopKBounds.EMPTY, bounds.forLeaf(fixture.store().zoneIndex(0), 0));
      assertEquals(SegmentTopKBounds.EMPTY, bounds.forLeaf(fixture.store().zoneIndex(0), 1));
      assertEquals(1, bounds.positionLookups());
      assertTrue(fixture.reads().isEmpty());
    }
  }

  @Test
  void anUnrefinedOrderingEvaluatesUnboundedRatherThanProveTheColumnAllPresent() {
    final String[][] mints = {{"zz", "m"}, {"\uE000", "\uD800\uDC00"}, {"yy", "a"}, {"zz", "b"}};
    final Fixture fixture = fixture(mints, 8, new boolean[mints.length]);
    final ProjectionColumnStore store = fixture.store();
    final long skipped = ProjectionColumnScan.topKLeavesSkippedCount();
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final long[] answer = ProjectionColumnScan.topKRecordKeys(store, new ColumnPredicate[0], new int[] {0},
          new boolean[] {false}, 6, fixture.fetcher(), new GlobalValueDictionary.ReadView[] {view(fixture, rtx)});
      assertArrayEquals(expected(fixture, null, false, 6), answer,
          "the unbounded fallback still answers in dictionary value order, with ties in document order");
      assertTrue(fixture.reads().values().stream().allMatch(count -> count.get() == 1),
          "one read per payload: no all-present proof pass over the column, and no refetch after one");
      assertEquals(skipped, ProjectionColumnScan.topKLeavesSkippedCount(),
          "no predicate names the key, so no bound is produced and no leaf is skipped");
      assertFalse(store.columnFilled(0), "planning must not materialize the sort column");
    }
  }

  @Test
  void anUnrefinedOrderingDeclinesWhereALeafHidesAMissingSortKey() {
    // Segment 0 would bound better and is all-present; segment 1's leaves hold a row with no sort
    // key. Whichever leaf is reached first, such a row is the interpreter's to place, never one a
    // bound may skip past — the property planTopK's all-present guard keeps true for every arm.
    final Fixture fixture = fixture(new String[][] {{"a"}, {"z"}}, 7, new boolean[] {false, true});
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      assertNull(
          ProjectionColumnScan.topKRecordKeys(fixture.store(), new ColumnPredicate[0], new int[] {0},
              new boolean[] {false}, 4, fixture.fetcher(), new GlobalValueDictionary.ReadView[] {view(fixture, rtx)}),
          "a matching row without an order key must decline, never be bounded away");
    }
  }

  @Test
  void mergingGlobalKeysComparesThroughTheReceivingHeapsView() throws Exception {
    final Fixture fixture = fixture(new String[][] {{"z", "a"}}, 1);
    final byte[] kinds = {TopKHeap.KEY_STRING_GLOBAL};
    final boolean[] ascending = new boolean[1];
    final ConcurrentLinkedQueue<JsonNodeReadOnlyTrx> opened = new ConcurrentLinkedQueue<>();
    final ExecutorService pool = Executors.newSingleThreadExecutor();
    try (final JsonNodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
      final GlobalValueDictionary.ReadView receiving = globalView(fixture, rtx);
      assertNotNull(receiving);
      assertFalse(receiving.fullyOrdered(), "mint order must disagree with value order to need a view");
      final TopKHeap global = new TopKHeap(1, kinds, ascending, new GlobalValueDictionary.ReadView[] {receiving});
      global.offer(new ColumnSlice[] {onlyRow(1)}, 0, 100L, 0L);
      final TopKHeap local = pool.submit(() -> {
        final JsonNodeReadOnlyTrx worker = session.beginNodeReadOnlyTrx();
        opened.add(worker);
        final TopKHeap heap =
            new TopKHeap(1, kinds, ascending, new GlobalValueDictionary.ReadView[] {globalView(fixture, worker)});
        heap.offer(new ColumnSlice[] {onlyRow(2)}, 0, 200L, 1L);
        return heap;
      }).get();
      global.mergeFrom(local);
      assertArrayEquals(new long[] {200L}, global.sortedRecordKeys(),
          "the smaller VALUE wins, resolved through the merging thread's own view");
    } finally {
      pool.shutdownNow();
      opened.forEach(JsonNodeReadOnlyTrx::close);
    }
  }

  /** One row carrying {@code id} in the numeric lane — a global string key's tuple. */
  private static ColumnSlice onlyRow(final int id) {
    return new ColumnSlice(1, (byte) 0, id, id, new long[] {1L}, new long[] {id}, null, null, null, null);
  }

  private static ColumnPredicate exclusion(final String[][] mints, final String value, final Op op) {
    final long[] literals = new long[mints.length];
    Arrays.fill(literals, ColumnPredicate.SEGMENT_LITERAL_ABSENT);
    for (int segment = 0; segment < mints.length; segment++) {
      for (int id = 0; id < mints[segment].length; id++) {
        if (mints[segment][id].equals(value)) {
          literals[segment] = ProjectionIndexRowGroupPage.packSegmentCell(segment, id + 1);
        }
      }
    }
    return ColumnPredicate.segmentScopedEquality(0, op, literals);
  }

  private static GlobalValueDictionary.ReadView view(final Fixture fixture, final JsonNodeReadOnlyTrx rtx) {
    return GlobalValueDictionary.segmentUnionReadView(fixture.headers(), ownedReader(rtx));
  }

  private static GlobalValueDictionary.ReadView globalView(final Fixture fixture, final JsonNodeReadOnlyTrx rtx) {
    return GlobalValueDictionary.readView(fixture.headers()[0], ownedReader(rtx));
  }

  /** A reader that refuses every thread but the one that opened the view over it. */
  private static StorageEngineReader ownedReader(final JsonNodeReadOnlyTrx rtx) {
    final Thread owner = Thread.currentThread();
    final StorageEngineReader delegate = rtx.getStorageEngineReader();
    final StorageEngineReader reader =
        (StorageEngineReader) Proxy.newProxyInstance(StorageEngineReader.class.getClassLoader(),
            new Class<?>[] {StorageEngineReader.class}, (proxy, method, args) -> {
              assertEquals(owner, Thread.currentThread(), "dictionary reads must use the calling thread's reader");
              try {
                return method.invoke(delegate, args);
              } catch (final InvocationTargetException error) {
                throw error.getCause();
              }
            });
    return reader;
  }

  private record Row(long key, String value) {
  }

  private record Fixture(List<RowGroupDirectory> leaves, ColumnSegmentFetcher fetcher, long[] headers, List<Row> rows,
      Map<Long, AtomicInteger> reads, int payloadCount) {
    ProjectionColumnStore store() {
      return new ProjectionColumnStore(leaves);
    }
  }

  private static long[] expected(final Fixture fixture, final String exclusion, final boolean descending, final int k) {
    final Comparator<String> order = descending
        ? Comparator.reverseOrder()
        : Comparator.naturalOrder();
    return fixture.rows()
                  .stream()
                  .filter(row -> row.value() != null && !row.value().equals(exclusion))
                  .sorted(Comparator.comparing(Row::value, order).thenComparingLong(Row::key))
                  .limit(k)
                  .mapToLong(Row::key)
                  .toArray();
  }

  private Fixture fixture(final String[][] mints, final int leavesPerSegment) {
    final boolean[] missing = new boolean[mints.length];
    Arrays.fill(missing, true);
    return fixture(mints, leavesPerSegment, missing);
  }

  private Fixture fixture(final String[][] mints, final int leavesPerSegment, final boolean[] missingPerSegment) {
    final Map<Long, byte[]> payloads = new HashMap<>();
    final Map<Long, AtomicInteger> reads = new ConcurrentHashMap<>();
    final List<RowGroupDirectory> leaves = new ArrayList<>();
    final List<Row> rows = new ArrayList<>();
    long offset = 1_000L;
    for (int segment = 0; segment < mints.length; segment++) {
      final List<String> values = Arrays.asList(mints[segment]);
      final GlobalValueDictionaryEncoder encoder = new GlobalValueDictionaryEncoder() {
        @Override
        public int intern(final byte[] source, final int start, final int length) {
          return intern(new String(source, start, length, StandardCharsets.UTF_8));
        }

        @Override
        public int intern(final String value) {
          final int id = values.indexOf(value) + 1;
          assertTrue(id > 0);
          return id;
        }
      };
      for (int repeat = 0; repeat < leavesPerSegment; repeat++) {
        final ProjectionIndexRowGroupPage page =
            new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT});
        // Include every mint, duplicate the last value, and — where asked — a missing key.
        final int rowCount = values.size() + (missingPerSegment[segment]
            ? 2
            : 1);
        for (int row = 0; row < rowCount; row++) {
          final String value = row == values.size() + 1
              ? null
              : values.get(Math.min(row, values.size() - 1));
          final long key = leaves.size() * 1_000L + row + 1;
          rows.add(new Row(key, value));
          assertTrue(page.appendRow(key, new long[1], new boolean[1], new String[] {value},
              new boolean[] {value != null}, new boolean[1], new boolean[1], new boolean[1]));
        }
        page.convertStringDictColumnToSegment(0, encoder, segment);
        final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
            ProjectionIndexColumnSegmentCodec.encode(page.serialize());
        final long[] offsets = new long[encoded.columnSegmentIds().length];
        for (int i = 0; i < offsets.length; i++) {
          offsets[i] = offset;
          payloads.put(offset, encoded.segments()[i]);
          offset += encoded.segments()[i].length + 1L;
        }
        leaves.add(new RowGroupDirectory(leaves.size() + 1, encoded.descriptor(), encoded.columnSegmentIds(), offsets,
            new byte[offsets.length][]));
      }
    }
    final long[] headers = new long[mints.length];
    try (final JsonNodeTrx wtx = session.beginNodeTrx()) {
      wtx.insertSubtreeAsFirstChild(JsonShredder.createStringReader("{}"), JsonNodeTrx.Commit.NO);
      for (int segment = 0; segment < mints.length; segment++) {
        final byte[][] bytes =
            Arrays.stream(mints[segment]).map(value -> value.getBytes(StandardCharsets.UTF_8)).toArray(byte[][]::new);
        headers[segment] = SegmentDictionarySeal.write(wtx.getStorageEngineWriter(), 0, bytes).headerKey();
      }
      wtx.commit();
    }
    final ColumnSegmentFetcher fetcher = new ColumnSegmentFetcher() {
      @Override
      public byte[][] fetchAll(final long[] offsets) {
        final byte[][] bytes = new byte[offsets.length][];
        for (int i = 0; i < offsets.length; i++) {
          bytes[i] = payloads.get(offsets[i]);
          if (bytes[i] != null) {
            reads.computeIfAbsent(offsets[i], unused -> new AtomicInteger()).incrementAndGet();
          }
        }
        return bytes;
      }

      @Override
      public boolean rangedFetchIsConcurrent() {
        return true;
      }
    };
    return new Fixture(leaves, fetcher, headers, rows, reads, payloads.size());
  }
}
