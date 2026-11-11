package io.sirix.query.scan;

import io.sirix.api.json.JsonResourceSession;
import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.projection.NumericLane;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.LaneTarget;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.node.SegmentAccess;
import io.sirix.query.scan.DerivedCacheBudget.Reservation;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The cached masked selection: lanes decoded straight into a reservation of the numeric share (or
 * the store's resident columns, pinned) and the record keys laid out in the same slab, served
 * through leased views, freed exactly once when the last lease drops — on close, on a background
 * task's release, never under a reader.
 */
@Tag("native-image")
final class NumericMaskedColumnCacheTest {
  private static final String[] FIELDS = {"value"};
  private static final long[] KEYS = {10, 11};
  private static final long[] KEEP = {1L};
  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};
  /** Inline segments: the store never calls the fetcher. */
  private static final ColumnSegmentFetcher NO_FETCH = offsets -> new byte[offsets.length][];

  private final FrameSlotAllocator allocator = new FrameSlotAllocator(16L << 20);
  private final DerivedCacheBudget budget = new DerivedCacheBudget(allocator, 8L << 20, -1);
  private final NumericMaskedColumnCache cache = new NumericMaskedColumnCache(budget.numeric());
  private long processSlabBytesBefore;

  @BeforeEach
  void rememberProcessSlabs() {
    processSlabBytesBefore = Allocators.getInstance().getSlabBytes();
  }

  @AfterEach
  void releaseEverySlab() {
    budget.numeric().clear();
    budget.packed().clear();
    assertEquals(0, allocator.getSlabBytes(), "no slab outlives the views and entries of a test");
    allocator.shutdown();
    assertEquals(processSlabBytesBefore, Allocators.getInstance().getSlabBytes(),
        "resident columns and query-local slabs went back to the process allocator");
  }

  @Test
  void aReservedSelectionIsPublishedOnceAndServedThroughLeasedViews() {
    final ProjectionColumnStore store = store();
    final Reservation reservation = reserveLanes(store);
    final MaskedColumns view = decodeInto(store, reservation.slab());
    assertFalse(view.isCached());
    assertNull(view.acquireTaskLease(), "a view over nothing cached hands out no task lease");
    final long inFlight = budget.metrics().inFlightBytes();
    assertTrue(inFlight > 0);
    assertSame(view, cache.publish(reservation, store, FIELDS, KEYS, view, new int[0]));
    assertTrue(view.isCached());
    assertEquals(0, budget.metrics().inFlightBytes());
    assertEquals(inFlight, budget.metrics().retainedBytes(), "the reservation became the entry's bytes");
    try (MaskedColumns cached = cache.lookup(store, FIELDS, KEYS)) {
      assertNotNull(cached);
      assertTrue(cached.isCached());
      assertSame(view.rowIdentity(), cached.rowIdentity(), "the publisher's view took the entry's identity");
      assertEquals(2, cached.longValue(0, 0, 1));
      assertEquals(11, cached.recordKey(0, 1));
      assertEquals(1, cached.minimum(0));
      assertEquals(2, cached.maximum(0));
      assertTrue(cached.allSelectedPresent(0));
      assertTrue(cached.present(0, 0, 0));
      assertTrue(cached.isImmutableRevision());
      assertEquals(1, SegmentAccess.getLong(cached.laneSegment(0, 0), cached.laneBase(0, 0)),
          "lanes are read in place from the entry's slab");
      assertTrue(cached.laneSegment(0, 0).isNative());
      Objects.requireNonNull(cached.rowMask(0))[0] = 0;
      assertFalse(cached.isImmutableRevision(), "a filtered view no longer matches the entry's masks");
      Objects.requireNonNull(view.rowMask(0))[0] = 0;
      try (MaskedColumns repeated = cache.lookup(store, new String[] {"value"}, new long[] {10, 11})) {
        assertNotNull(repeated);
        assertEquals(3, Objects.requireNonNull(repeated.rowMask(0))[0],
            "every view owns its masks: the entry's never changed");
        assertSame(cached.rowIdentity(), repeated.rowIdentity());
      }
      assertNull(cache.lookup(store, new String[] {"changed"}, KEYS));
      assertNull(cache.lookup(store, FIELDS, new long[] {10}));
      assertNull(cache.lookup(store(), FIELDS, KEYS), "another store identity cannot reuse the selection");
    }
    assertEquals(1, budget.metrics().numeric().entries());
    view.close();
    view.close();
    assertEquals(inFlight, budget.metrics().retainedBytes(), "closing views keeps the published entry");
    budget.numeric().clear();
    assertEquals(0, allocator.getSlabBytes(), "the last lease was the cache's own");
    assertNull(cache.lookup(store, FIELDS, KEYS));
  }

  @Test
  void aViewOverResidentColumnsPinsThemThroughItsEntryUntilTheLastLeaseDrops() {
    final ProjectionColumnStore store = store();
    assertNotNull(store.column(0, NO_FETCH));
    assertTrue(Allocators.getInstance().getSlabBytes() > processSlabBytesBefore, "the resident column has a slab");
    final LaneTarget target = new LaneTarget();
    final ColumnSlice[] slices = store.columnMaskedView(0, NO_FETCH, KEEP, target);
    assertTrue(target.resident());
    // Nothing to decode: the reservation holds the selection's record keys alone.
    final long[] keyBases = new long[1];
    final long keyBytes = KeyLanes.layout(store, KEEP, keyBases, 0L);
    assertEquals(64L, keyBytes, "two keys, one leaf: one 64-byte aligned key lane");
    final Reservation reservation = cache.reserve(keyBytes);
    assertNotNull(reservation);
    final MaskedColumns view = new MaskedColumns(store, new long[][] {{3}},
        KeyLanes.copyInto(reservation.slab(), keyBases, store.recordKeysMasked(NO_FETCH, KEEP), store),
        new ColumnSlice[][] {slices}, KINDS, 2);
    view.markImmutableRevision();
    assertSame(view, cache.publish(reservation, store, FIELDS, KEYS, view, new int[] {0}));
    assertTrue(view.isCached());
    assertEquals(1, store.residencyPins(0), "the entry pins the column it reads in place");
    assertEquals(keyBytes, budget.metrics().retainedBytes(), "the entry is charged its key lanes");
    final MaskedColumns reader = cache.lookup(store, FIELDS, KEYS);
    assertNotNull(reader);
    assertEquals(2, reader.longValue(0, 0, 1));
    assertSame(store.column(0, NO_FETCH)[0].lane(), reader.slices()[0][0].lane(), "no copy: the store's lane");
    assertSame(reservation.slab(), reader.keySegment(0), "the keys are read from the entry's slab");
    assertEquals(11, SegmentAccess.getLong(reader.keySegment(0), reader.keyBase(0) + 8L));
    assertEquals(11, reader.recordKey(0, 1));
    budget.numeric().clear();
    assertEquals(1, store.residencyPins(0), "evicted, but two views still read: the pin stays");
    view.close();
    assertEquals(1, store.residencyPins(0));
    assertEquals(keyBytes, budget.metrics().retiredLeasedBytes(), "the reader keeps the key slab mapped");
    reader.close();
    assertEquals(0, store.residencyPins(0), "the last lease dropped: the column is unpinned");
    assertEquals(0, allocator.getSlabBytes(), "the key slab left with the last lease");
    store.retire();
  }

  @Test
  void residentViewsOutliveTheExecutorWhenCachingIsDisabledOrRefused() throws Exception {
    for (final long bytes : new long[] {0L, 8L}) {
      final DerivedCacheBudget allowance = withNumericShare(bytes);
      final DerivedCacheBudget previous = DerivedCacheBudget.setForTesting(allowance);
      try {
        for (final boolean ownLanes : new boolean[] {false, true}) {
          final ProjectionColumnStore store = store();
          final ProjectionColumnStore replacement = store(1, 2, KINDS[0], 100L);
          MaskedColumns view = null;
          try {
            assertNotNull(store.column(0, NO_FETCH));
            final LaneTarget target = new LaneTarget();
            store.columnMaskedView(0, NO_FETCH, ownLanes
                ? KEEP
                : null, target);
            assertTrue(target.resident());
            view = executorView(store, KEYS, KINDS, ownLanes);
            assertNotNull(view);
            assertFalse(view.isCached());
            assertEquals(1, store.residencyPins(0));
            store.retire();
            assertTrue(store.columnFilled(0));
            assertNotNull(replacement.column(0, NO_FETCH));
            assertEquals(1L, view.longValue(0, 0, 0));
            assertEquals(2L, view.longValue(0, 0, 1));
            assertEquals(11L, view.recordKey(0, 1));
            assertEquals(0L, allowance.metrics().retainedBytes());
            view.close();
            view.close();
            assertEquals(0, store.residencyPins(0));
            assertFalse(store.columnFilled(0));
          } finally {
            if (view != null) {
              view.close();
            }
            store.retire();
            replacement.retire();
          }
        }
      } finally {
        allowance.numeric().clear();
        allowance.packed().clear();
        DerivedCacheBudget.setForTesting(previous);
      }
    }
  }

  @Test
  void onlyNumericAndTimestampKindsAreSnapshotEligible() {
    assertTrue(SirixVectorizedExecutor.numericSnapshotKinds(KINDS));
    assertTrue(SirixVectorizedExecutor.numericSnapshotKinds(new byte[] {
        ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP}));
    for (final byte kind : new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
        ProjectionIndexRowGroupPage.COLUMN_KIND_DATE, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL,
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT}) {
      assertFalse(SirixVectorizedExecutor.numericSnapshotKinds(new byte[] {KINDS[0], kind}));
    }
  }

  @Test
  void snapshotDensityRequiresAQuarterOfRowsAndThreeQuartersOfLeaves() {
    final ProjectionColumnStore store = store(4, 16, KINDS[0]);
    try {
      assertTrue(SirixVectorizedExecutor.numericSnapshotDense(store, new long[][] {{31L}, {31L}, {63L}, null}, 16));
      assertFalse(SirixVectorizedExecutor.numericSnapshotDense(store, new long[][] {{31L}, {31L}, {31L}, null}, 15));
      assertFalse(
          SirixVectorizedExecutor.numericSnapshotDense(store, new long[][] {{0xFFFFL}, {0xFFFFL}, null, null}, 32));
    } finally {
      store.retire();
    }
  }

  @Test
  void sparseNumericAndStringSelectionsStayQueryLocal() throws Exception {
    final DerivedCacheBudget previous = DerivedCacheBudget.setForTesting(budget);
    final ProjectionColumnStore sparse = store(4, 16, KINDS[0]);
    final ProjectionColumnStore strings = store(1, 2, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT);
    try {
      final long[] sparseKeys = new long[16];
      for (int row = 0; row < sparseKeys.length; row++) {
        sparseKeys[row] = 10L + row;
      }
      try (MaskedColumns numeric = executorView(sparse, sparseKeys, KINDS, true);
          MaskedColumns text =
              executorView(strings, KEYS, new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT}, true)) {
        assertNotNull(numeric);
        assertNotNull(text);
        assertFalse(numeric.isCached());
        assertFalse(text.isCached());
        assertEquals(1L, numeric.longValue(0, 0, 0));
        assertEquals(0L, budget.metrics().retainedBytes());
        assertEquals(0, budget.metrics().numeric().entries());
      }
    } finally {
      sparse.retire();
      strings.retire();
      budget.numeric().clear();
      DerivedCacheBudget.setForTesting(previous);
    }
  }

  @Test
  void denseNumericExecutorSelectionsAreCached() throws Exception {
    final DerivedCacheBudget previous = DerivedCacheBudget.setForTesting(budget);
    final ProjectionColumnStore store = store();
    try {
      try (MaskedColumns columns = executorView(store, KEYS, KINDS, true)) {
        assertNotNull(columns);
        assertTrue(columns.isCached());
        assertEquals(1L, columns.longValue(0, 0, 0));
        assertEquals(2L, columns.longValue(0, 0, 1));
        assertEquals(1, budget.metrics().numeric().entries());
      }
    } finally {
      budget.numeric().clear();
      store.retire();
      DerivedCacheBudget.setForTesting(previous);
    }
  }

  private static MaskedColumns executorView(final ProjectionColumnStore store, final long[] keys, final byte[] kinds,
      final boolean ownLanes) throws Exception {
    final JsonResourceSession session =
        (JsonResourceSession) Proxy.newProxyInstance(JsonResourceSession.class.getClassLoader(),
            new Class<?>[] {JsonResourceSession.class}, (proxy, method, args) -> null);
    final Method read =
        SirixVectorizedExecutor.class.getDeclaredMethod("readOrCacheMaskedColumns", ProjectionColumnStore.class,
            ColumnSegmentFetcher.class, long[].class, String[].class, int[].class, byte[].class, boolean.class);
    read.setAccessible(true);
    final SirixVectorizedExecutor executor = new SirixVectorizedExecutor(session, 1, 1);
    try {
      executor.enterExecution();
      try {
        return (MaskedColumns) read.invoke(executor, store, NO_FETCH, keys, FIELDS, new int[] {0}, kinds, ownLanes);
      } finally {
        executor.leaveExecution();
      }
    } finally {
      executor.close();
    }
  }

  @Test
  void aRefusedReservationLeavesTheSelectionUncachedAndUnpinned() {
    final ProjectionColumnStore store = store();
    assertNotNull(store.column(0, NO_FETCH));
    final DerivedCacheBudget tight = withNumericShare(8);
    final NumericMaskedColumnCache refusing = new NumericMaskedColumnCache(tight.numeric());
    assertNull(refusing.reserve(64L), "the key lanes alone exceed the share");
    assertEquals(0, store.residencyPins(0), "nothing was published, nothing is pinned");
    assertNull(refusing.lookup(store, FIELDS, KEYS));
    assertTrue(tight.metrics().rejectedBytes() > 0);
    assertEquals(0, tight.metrics().retainedBytes());
    assertEquals(0, tight.metrics().inFlightBytes());
    store.retire();
  }

  @Test
  void aQueryLocalViewOwnsItsSlabAndReleasesItOnceOnClose() {
    final ProjectionColumnStore store = store();
    final MemorySegment slab = Allocators.getInstance().allocateSlab(64);
    assertNotNull(slab);
    final MaskedColumns view = decodeInto(store, slab);
    view.ownResources(slab, new int[0]);
    assertThrows(IllegalStateException.class, () -> view.ownResources(slab, new int[0]), "a view owns one thing");
    assertFalse(view.isCached());
    assertEquals(2, view.longValue(0, 0, 1));
    assertTrue(Allocators.getInstance().getSlabBytes() > processSlabBytesBefore);
    view.close();
    assertEquals(processSlabBytesBefore, Allocators.getInstance().getSlabBytes(), "closing returned the slab");
    view.close(); // idempotent: no second release
    assertEquals(processSlabBytesBefore, Allocators.getInstance().getSlabBytes());
  }

  @Test
  void mutableSelectionsCannotBePublished() {
    final ProjectionColumnStore store = store();
    final Reservation reservation = reserveLanes(store);
    final long[] bases = new long[1];
    store.maskedLaneLayout(0, KEEP, bases, 0L);
    final MaskedColumns view =
        view(store, store.columnMaskedView(0, NO_FETCH, KEEP, new LaneTarget(reservation.slab(), bases)));
    assertFalse(view.isImmutableRevision(), "never marked: a mutable revision's view");
    assertThrows(IllegalStateException.class, () -> cache.publish(reservation, store, FIELDS, KEYS, view, new int[0]));
    assertThrows(IllegalArgumentException.class,
        () -> cache.publish(reservation, store(), FIELDS, KEYS, view, new int[0]), "a view belongs to its store");
    reservation.close();
    assertEquals(0, budget.metrics().inFlightBytes(), "a reservation closed unpublished refunds the share");
    assertEquals(0, allocator.getSlabBytes());
  }

  @Test
  void concurrentReadersCannotMutateAnotherReadersMask() throws Exception {
    final ProjectionColumnStore store = store();
    publish(store);
    final CountDownLatch start = new CountDownLatch(1);
    try (var pool = Executors.newFixedThreadPool(2)) {
      final var first = pool.submit(() -> readAndMutate(store, start));
      final var second = pool.submit(() -> readAndMutate(store, start));
      start.countDown();
      first.get();
      second.get();
    }
  }

  private void readAndMutate(final ProjectionColumnStore store, final CountDownLatch start) {
    try {
      start.await();
      for (int i = 0; i < 1000; i++) {
        try (MaskedColumns view = cache.lookup(store, FIELDS, KEYS)) {
          assertNotNull(view);
          final long[] rowMask = Objects.requireNonNull(view.rowMask(0));
          assertEquals(3, rowMask[0]);
          assertEquals(2, view.longValue(0, 0, 1));
          rowMask[0] = 0;
        }
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  @Test
  void readersOfAnEvictedEntryKeepValidMemoryUntilTheyClose() {
    final ProjectionColumnStore store = store();
    final long bytes = laneBytes(store);
    final DerivedCacheBudget tight = withNumericShare(bytes);
    final NumericMaskedColumnCache one = new NumericMaskedColumnCache(tight.numeric());
    final Reservation reservation = one.reserve(bytes);
    assertNotNull(reservation);
    one.publish(reservation, store, FIELDS, KEYS, decodeInto(store, reservation.slab()), new int[0]).close();
    final MaskedColumns reader = one.lookup(store, FIELDS, KEYS);
    assertNotNull(reader);
    // Pressure cannot evict a leased entry: a second roster of the same size is refused instead.
    assertNull(one.reserve(bytes));
    assertEquals(bytes, tight.metrics().rejectedBytes());
    try (MaskedColumns again = one.lookup(store, FIELDS, KEYS)) {
      assertNotNull(again, "the leased entry is still served");
    }
    // An explicit eviction retires the entry but defers freeing to the reader.
    tight.numeric().clear();
    assertNull(one.lookup(store, FIELDS, KEYS));
    assertEquals(bytes, tight.metrics().retiredLeasedBytes(), "the evicted slab stays mapped for its reader");
    assertEquals(2, reader.longValue(0, 0, 1), "the reader still reads valid memory");
    assertEquals(11, reader.recordKey(0, 1));
    assertTrue(reader.isImmutableRevision());
    reader.close();
    reader.close();
    assertEquals(0, tight.metrics().retiredLeasedBytes(), "closing twice releases once");
    assertEquals(0, allocator.getSlabBytes());
    assertNull(one.lookup(store, FIELDS, KEYS), "a freed entry never returns");
    final Reservation replacement = one.reserve(bytes);
    assertNotNull(replacement, "the share is free again");
    one.publish(replacement, store, new String[] {"other"}, KEYS, decodeInto(store, replacement.slab()), new int[0])
       .close();
    final MaskedColumns other = one.lookup(store, new String[] {"other"}, KEYS);
    assertNotNull(other);
    other.close();
    tight.numeric().clear();
    assertEquals(0, allocator.getSlabBytes());
  }

  @Test
  void aTaskLeaseOutlivesTheViewThatTookIt() {
    final ProjectionColumnStore store = store();
    publish(store);
    final MaskedColumns view = cache.lookup(store, FIELDS, KEYS);
    assertNotNull(view);
    final DerivedCacheBudget.Lease task = view.acquireTaskLease();
    assertNotNull(task);
    view.close();
    assertNull(view.acquireTaskLease(), "a closed view holds no entry any more");
    budget.numeric().clear();
    assertTrue(allocator.getSlabBytes() > 0, "the background task still holds the slab");
    task.run();
    task.run();
    assertEquals(0, allocator.getSlabBytes(), "released once, when the task is done");
  }

  @Test
  void membershipPredicatesFilterCachedMasksWithoutChangingTheEntry() {
    final ProjectionColumnStore store = store();
    final MaskedColumns heap = columns(store, 1, null);
    assertNull(heap.slices()[0][0].lane(), "the comparison fixture holds heap values");
    publish(store);
    for (final boolean anti : new boolean[] {false, true}) {
      try (MaskedColumns cached = cache.lookup(store, FIELDS, KEYS)) {
        assertNotNull(cached);
        final ColumnPredicate predicate =
            cached.recordKeyPredicateByMembership(0, KEYS, new LongOpenHashSet(new long[] {1}), anti);
        final ColumnPredicate expected =
            heap.recordKeyPredicateByMembership(0, KEYS, new LongOpenHashSet(new long[] {1}), anti);
        assertEquals(anti
            ? 2L
            : 1L, Objects.requireNonNull(predicate.keyMasks).byFirstKey().get(10)[0]);
        assertArrayEquals(Objects.requireNonNull(expected.keyMasks).byFirstKey().get(10),
            Objects.requireNonNull(predicate.keyMasks).byFirstKey().get(10), "lane and heap filters agree");
        assertArrayEquals(heap.recordKeysByMembership(0, new LongOpenHashSet(new long[] {2}), anti),
            cached.recordKeysByMembership(0, new LongOpenHashSet(new long[] {2}), anti));
        assertEquals(heap.presentLongValues(0), cached.presentLongValues(0));
        assertEquals(3L, Objects.requireNonNull(cached.rowMask(0))[0]);
      }
      try (MaskedColumns repeated = cache.lookup(store, FIELDS, KEYS)) {
        assertNotNull(repeated);
        assertEquals(3L, Objects.requireNonNull(repeated.rowMask(0))[0]);
      }
    }
    heap.close();
  }

  @Test
  void multiFieldRostersReadEveryFieldFromItsLane() {
    final ProjectionColumnStore store = store();
    for (final int fieldCount : new int[] {3, 5}) {
      final String[] fields = new String[fieldCount];
      for (int field = 0; field < fieldCount; field++) {
        fields[field] = "f" + field;
      }
      final Reservation reservation = cache.reserve(64L * fieldCount);
      assertNotNull(reservation);
      final MaskedColumns heap = columns(store, fieldCount, null);
      final MaskedColumns lanes = columns(store, fieldCount, reservation.slab());
      cache.publish(reservation, store, fields, KEYS, lanes, new int[0]).close();
      try (MaskedColumns view = cache.lookup(store, fields, KEYS)) {
        assertNotNull(view);
        for (int field = 0; field < fieldCount; field++) {
          assertEquals(64L * field, view.laneBase(field, 0), "lanes are laid out one after another");
          assertEquals(heap.minimum(field), view.minimum(field));
          assertEquals(heap.maximum(field), view.maximum(field));
          assertEquals(heap.isTimestamp(field), view.isTimestamp(field));
          for (int row = 0; row < 2; row++) {
            assertEquals(heap.present(field, 0, row), view.present(field, 0, row));
            assertEquals(heap.longValue(field, 0, row), view.longValue(field, 0, row),
                "field " + field + " row " + row);
          }
          assertEquals(heap.presentLongValues(field), view.presentLongValues(field));
        }
        final LongOpenHashSet members = new LongOpenHashSet(new long[] {heap.longValue(fieldCount - 1, 0, 1)});
        assertArrayEquals(heap.recordKeysByMembership(fieldCount - 1, members, false),
            view.recordKeysByMembership(fieldCount - 1, members, false));
        assertArrayEquals(heap.recordKeysByMembership(fieldCount - 1, members, true),
            view.recordKeysByMembership(fieldCount - 1, members, true));
        final ColumnPredicate expected = heap.recordKeyPredicateByMembership(0, KEYS, members, false);
        final ColumnPredicate actual = view.recordKeyPredicateByMembership(0, KEYS, members, false);
        assertArrayEquals(Objects.requireNonNull(expected.keyMasks).byFirstKey().get(10),
            Objects.requireNonNull(actual.keyMasks).byFirstKey().get(10));
      }
      heap.close();
    }
    assertEquals(2, budget.metrics().numeric().entries(), "the two rosters are separate entries");
  }

  @Test
  void immutableEntryPresenceProofsDoNotFollowMutableViews() {
    final ProjectionColumnStore store = store();
    final Reservation reservation = cache.reserve(64);
    assertNotNull(reservation);
    final MemorySegment slab = reservation.slab();
    SegmentAccess.putLong(slab, 0L, 1L);
    SegmentAccess.putLong(slab, 8L, 2L);
    final ColumnSlice sparse = new ColumnSlice(2, (byte) 0, 1, 2, new long[] {1}, new NumericLane(slab, 0L, 2));
    final MaskedColumns original = new MaskedColumns(store, new long[][] {{3}},
        KeyLanes.onHeap(new long[][] {{10, 11}}), new ColumnSlice[][] {{sparse}}, KINDS, 2);
    original.markImmutableRevision();
    assertFalse(original.allSelectedPresent(0));
    cache.publish(reservation, store, FIELDS, KEYS, original, new int[0]);
    try (MaskedColumns first = cache.lookup(store, FIELDS, KEYS);
        MaskedColumns second = cache.lookup(store, FIELDS, KEYS)) {
      assertNotNull(first);
      assertNotNull(second);
      assertSame(original.rowIdentity(), first.rowIdentity());
      assertFalse(first.allSelectedPresent(0));
      assertFalse(first.present(0, 0, 1));
      Objects.requireNonNull(first.rowMask(0))[0] = 1;
      assertTrue(first.allSelectedPresent(0));
      assertFalse(first.isImmutableRevision());
      assertFalse(second.allSelectedPresent(0));
      Objects.requireNonNull(second.rowMask(0))[0] = 1;
      assertTrue(second.allSelectedPresent(0));
      Objects.requireNonNull(first.rowMask(0))[0] = 2;
      assertFalse(first.allSelectedPresent(0));
      assertTrue(second.allSelectedPresent(0));
      assertFalse(original.allSelectedPresent(0));
    }
    original.close();
  }

  @Test
  void aRetiredStoreMakesItsEntryStaleAndItsPinsReturn() {
    final ProjectionColumnStore store = store();
    assertNotNull(store.column(0, NO_FETCH));
    final MaskedColumns view = view(store, store.columnMaskedView(0, NO_FETCH, KEEP, new LaneTarget()));
    view.markImmutableRevision();
    final Reservation reservation = cache.reserve(64L);
    assertNotNull(reservation);
    cache.publish(reservation, store, FIELDS, KEYS, view, new int[] {0});
    final MaskedColumns reader = cache.lookup(store, FIELDS, KEYS);
    assertNotNull(reader);
    store.retire();
    assertTrue(store.columnFilled(0), "the entry's pin keeps the retired store's column for its readers");
    try (MaskedColumns served = cache.lookup(store, FIELDS, KEYS)) {
      assertNotNull(served, "a query still holding the retired handle is served until the entry is reaped");
    }
    Objects.requireNonNull(cache.reserve(64)).close();
    assertNull(cache.lookup(store, FIELDS, KEYS), "a retired store's entry is reaped on the next admission");
    assertEquals(0, budget.metrics().numeric().entries());
    assertEquals(2, reader.longValue(0, 0, 1), "readers still read valid memory");
    view.close();
    reader.close();
    assertFalse(store.columnFilled(0), "the last lease released the pin and the retired column with it");
  }

  @Test
  void aViewReservesExactlyTheSurvivingLeavesLanes() {
    final ProjectionColumnStore store = store();
    final long[] bases = new long[1];
    final long bytes = store.maskedLaneLayout(0, KEEP, bases, 0L);
    assertEquals(64L, bytes, "two rows, one leaf: one 64-byte aligned lane");
    assertEquals(0L, bases[0]);
    assertThrows(IllegalArgumentException.class, () -> store.maskedLaneLayout(0, KEEP, new long[2], 0L),
        "one base per leaf");
    final Reservation reservation = cache.reserve(bytes);
    assertNotNull(reservation);
    final LaneTarget target = new LaneTarget(reservation.slab(), bases);
    final ColumnSlice[] slices = store.columnMaskedView(0, NO_FETCH, KEEP, target);
    assertTrue(target.filled());
    assertFalse(store.columnFilled(0), "a masked fill retains nothing in the store");
    assertEquals(2, SegmentAccess.getLong(reservation.slab(), 8L));
    assertNotNull(slices[0].lane());
    reservation.close();
    assertEquals(0, allocator.getSlabBytes());
  }

  @Test
  void keyLanesLayOutKeptLeavesInTheSlabAndReadInPlace() {
    final ProjectionColumnStore store = store();
    final long[] bases = new long[1];
    assertEquals(192L, KeyLanes.layout(store, KEEP, bases, 128L), "one 64-byte aligned lane behind the offset");
    assertEquals(128L, bases[0]);
    assertEquals(128L, KeyLanes.layout(store, new long[] {0L}, bases, 128L), "a pruned leaf takes no bytes");
    assertEquals(-1L, bases[0]);
    assertThrows(IllegalArgumentException.class, () -> KeyLanes.layout(store, KEEP, new long[2], 0L),
        "one base per leaf");
    assertThrows(IllegalArgumentException.class, () -> KeyLanes.layout(store, KEEP, new long[1], 8L), "aligned start");
    final MemorySegment slab = Allocators.getInstance().allocateSlab(192L);
    assertNotNull(slab);
    try {
      KeyLanes.layout(store, KEEP, bases, 128L);
      final KeyLanes lanes = KeyLanes.copyInto(slab, bases, store.recordKeysMasked(NO_FETCH, KEEP), store);
      assertTrue(lanes.has(0));
      assertSame(slab, lanes.segment(0));
      assertEquals(128L, lanes.base(0));
      assertEquals(2, lanes.rows(0));
      assertEquals(10L, lanes.key(0, 0));
      assertEquals(11L, lanes.key(0, 1));
      assertEquals(11L, SegmentAccess.getLong(slab, 128L + 8L), "the keys live in the slab");
      assertThrows(IndexOutOfBoundsException.class, () -> lanes.key(0, 2));
      assertArrayEquals(new long[] {10L}, lanes.firstKeyArrays()[0], "the membership filter's per-leaf identity");
      assertSame(lanes.firstKeyArrays(), lanes.firstKeyArrays(), "built once");
      assertThrows(IllegalStateException.class, () -> KeyLanes.copyInto(slab, bases, new long[][] {{10L}}, store),
          "a lane holds every row of its leaf");
      assertThrows(IllegalArgumentException.class,
          () -> KeyLanes.copyInto(slab, new long[] {slab.byteSize()}, store.recordKeysMasked(NO_FETCH, KEEP), store),
          "a lane outside the slab (slabs round up to whole pages)");
      final KeyLanes pruned = KeyLanes.copyInto(slab, new long[] {-1L}, new long[][] {new long[0]}, store);
      assertFalse(pruned.has(0));
      assertThrows(IllegalStateException.class, () -> pruned.segment(0));
      assertEquals(0, pruned.firstKeyArrays()[0].length);
    } finally {
      Allocators.getInstance().releaseSlab(slab);
    }
    final KeyLanes heap = KeyLanes.onHeap(new long[][] {{10L, 11L}, new long[0], null});
    assertTrue(heap.has(0));
    assertFalse(heap.segment(0).isNative(), "the allocator-refused fallback views the array");
    assertEquals(11L, heap.key(0, 1));
    assertTrue(heap.has(1), "an empty array is an empty lane");
    assertEquals(0, heap.rows(1));
    assertFalse(heap.has(2));
  }

  // --- fixtures

  /** Reserve the lanes of the one surviving leaf of {@link #store()} in the test's numeric share. */
  private Reservation reserveLanes(final ProjectionColumnStore store) {
    final Reservation reservation = cache.reserve(laneBytes(store));
    assertNotNull(reservation);
    return reservation;
  }

  private static long laneBytes(final ProjectionColumnStore store) {
    return store.maskedLaneLayout(0, KEEP, new long[1], 0L);
  }

  /** The store's one column decoded into {@code slab} under the keep mask: a query-local view. */
  private static MaskedColumns decodeInto(final ProjectionColumnStore store, final MemorySegment slab) {
    final long[] bases = new long[1];
    store.maskedLaneLayout(0, KEEP, bases, 0L);
    final LaneTarget target = new LaneTarget(slab, bases);
    final ColumnSlice[] slices = store.columnMaskedView(0, NO_FETCH, KEEP, target);
    assertTrue(target.filled());
    final MaskedColumns view = view(store, slices);
    view.markImmutableRevision();
    return view;
  }

  /** A view whose record keys stay in their decoded arrays (the allocator-refused shape). */
  private static MaskedColumns view(final ProjectionColumnStore store, final ColumnSlice[] slices) {
    return new MaskedColumns(store, new long[][] {{3}}, KeyLanes.onHeap(store.recordKeysMasked(NO_FETCH, KEEP)),
        new ColumnSlice[][] {slices}, KINDS, 2);
  }

  /**
   * Publish {@link #FIELDS}/{@link #KEYS} of {@code store} from a reservation; the publisher's view
   * closed.
   */
  private void publish(final ProjectionColumnStore store) {
    final Reservation reservation = reserveLanes(store);
    cache.publish(reservation, store, FIELDS, KEYS, decodeInto(store, reservation.slab()), new int[0]).close();
  }

  /**
   * {@code fieldCount} lanes over two rows; field {@code f} holds {@code 10 * f + row + 1}, field 1 a
   * timestamp. With a slab the lanes live in it 64 bytes apart, else in heap arrays.
   */
  private static MaskedColumns columns(final ProjectionColumnStore store, final int fieldCount,
      final MemorySegment slab) {
    final ColumnSlice[][] slices = new ColumnSlice[fieldCount][1];
    final byte[] kinds = new byte[fieldCount];
    for (int field = 0; field < fieldCount; field++) {
      final long first = 10L * field + 1;
      if (slab == null) {
        slices[field][0] = new ColumnSlice(2, (byte) 0, first, first + 1, new long[] {3}, new long[] {first, first + 1},
            null, null, null, null, null, null);
      } else {
        SegmentAccess.putLong(slab, 64L * field, first);
        SegmentAccess.putLong(slab, 64L * field + 8L, first + 1);
        slices[field][0] =
            new ColumnSlice(2, (byte) 0, first, first + 1, new long[] {3}, new NumericLane(slab, 64L * field, 2));
      }
      kinds[field] = field == 1
          ? ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP
          : ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
    }
    final MaskedColumns columns =
        new MaskedColumns(store, new long[][] {{3}}, KeyLanes.onHeap(new long[][] {{10, 11}}), slices, kinds, 2);
    columns.markImmutableRevision();
    return columns;
  }

  private DerivedCacheBudget withNumericShare(final long bytes) {
    final long total = (4 * bytes + 2) / 3;
    return new DerivedCacheBudget(allocator, total * 8, total);
  }

  /**
   * One NUMERIC_LONG column, one leaf, rows (key 10, value 1) and (key 11, value 2), inline segments.
   */
  private static ProjectionColumnStore store() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
    page.appendRow(10, new long[] {1}, new boolean[1], new String[1]);
    page.appendRow(11, new long[] {2}, new boolean[1], new String[1]);
    final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    assertNotNull(encoded);
    final long[] offsets = new long[encoded.columnSegmentIds().length];
    Arrays.fill(offsets, Constants.NULL_ID_LONG);
    return new ProjectionColumnStore(List.of(
        new RowGroupDirectory(1, encoded.descriptor(), encoded.columnSegmentIds(), offsets, encoded.segments())));
  }

  private static ProjectionColumnStore store(final int leaves, final int rows, final byte kind) {
    return store(leaves, rows, kind, 0L);
  }

  private static ProjectionColumnStore store(final int leaves, final int rows, final byte kind,
      final long valueOffset) {
    final List<RowGroupDirectory> directories = new ArrayList<>(leaves);
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {kind});
      for (int row = 0; row < rows; row++) {
        page.appendRow(10L + (long) leaf * rows + row, new long[] {valueOffset + row + 1L}, new boolean[1],
            new String[] {kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
                ? "value" + row
                : null});
      }
      final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      assertNotNull(encoded);
      final long[] offsets = new long[encoded.columnSegmentIds().length];
      Arrays.fill(offsets, Constants.NULL_ID_LONG);
      directories.add(new RowGroupDirectory(leaf + 1L, encoded.descriptor(), encoded.columnSegmentIds(), offsets,
          encoded.segments()));
    }
    return new ProjectionColumnStore(directories);
  }

}
