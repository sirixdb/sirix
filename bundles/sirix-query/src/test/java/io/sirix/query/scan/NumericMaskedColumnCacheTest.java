package io.sirix.query.scan;

import io.sirix.cache.FrameSlotAllocator;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("native-image")
final class NumericMaskedColumnCacheTest {
  private final FrameSlotAllocator allocator = new FrameSlotAllocator(16L << 20);
  private final DerivedCacheBudget budget = new DerivedCacheBudget(allocator, 8L << 20, -1);
  /** Copies run inline so every admission is observable right after it returns. */
  private final NumericMaskedColumnCache cache = new NumericMaskedColumnCache(budget.numeric(), Runnable::run);

  @AfterEach
  void releaseEverySlab() {
    budget.numeric().clear();
    budget.packed().clear();
    assertEquals(0, allocator.getSlabBytes(), "no slab outlives the views and entries of a test");
    allocator.shutdown();
  }

  @Test
  void signaturesAndRevisionIdentityFenceReuseAndEveryViewOwnsItsMasks() {
    final ProjectionColumnStore store = store();
    final String[] fields = {"value"};
    final long[] keys = {10, 11};
    final MaskedColumns original = columns(store);
    cache.admit(store, fields, keys, original);
    assertFalse(cache.copying(store, fields, keys));
    try (MaskedColumns cached = cache.lookup(store, fields, keys)) {
      assertNotNull(cached);
      assertTrue(cached.isNative());
      assertEquals(2, cached.longValue(0, 0, 1));
      assertEquals(11, cached.recordKey(0, 1));
      assertEquals(1, cached.minimum(0));
      assertEquals(2, cached.maximum(0));
      assertTrue(cached.allSelectedPresent(0));
      assertTrue(cached.present(0, 0, 0));
      assertTrue(cached.isImmutableRevision());
      Objects.requireNonNull(cached.rowMask(0))[0] = 0;
      assertFalse(cached.isImmutableRevision(), "a filtered view no longer matches the resident masks");
      Objects.requireNonNull(original.rowMask(0))[0] = 0;
      fields[0] = "changed";
      keys[0] = 9;
      try (MaskedColumns repeated = cache.lookup(store, new String[] {"value"}, new long[] {10, 11})) {
        assertNotNull(repeated);
        assertEquals(3, Objects.requireNonNull(repeated.rowMask(0))[0], "the slab masks never change");
        assertSame(cached.rowIdentity(), repeated.rowIdentity());
      }
      assertNull(cache.lookup(store, fields, keys));
      assertNull(cache.lookup(store, new String[] {"value"}, new long[] {10}));
      assertNull(cache.lookup(store(), new String[] {"value"}, new long[] {10, 11}),
          "a new revision/store identity cannot reuse old values even with identical keys");
      assertThrows(IllegalStateException.class, () -> cached.numericLane(0, 0), "native lanes are read via the slab");
    }
  }

  @Test
  void denseNumericAdmissionChecksPresenceOnlyWithinSelectedRows() {
    final ProjectionColumnStore store = store();
    final ColumnSlice sparseSlice =
        new ColumnSlice(2, (byte) 0, 1, 2, new long[] {1}, new long[] {1, 2}, null, null, null, null, null, null);
    final MaskedColumns both = new MaskedColumns(store, new long[][] {{3}}, new long[][] {{10, 11}},
        new ColumnSlice[][] {{sparseSlice}}, new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 2);
    assertFalse(both.allSelectedPresent(0));
    Objects.requireNonNull(both.rowMask(0))[0] = 1;
    assertTrue(both.allSelectedPresent(0));
    assertEquals(1, both.numericLane(0, 0)[0]);
  }

  @Test
  void immutableSnapshotPresenceProofsDoNotFollowMutableViews() {
    final ProjectionColumnStore store = store();
    final ColumnSlice sparseSlice =
        new ColumnSlice(2, (byte) 0, 1, 2, new long[] {1}, new long[] {1, 2}, null, null, null, null, null, null);
    final MaskedColumns original = new MaskedColumns(store, new long[][] {{3}}, new long[][] {{10, 11}},
        new ColumnSlice[][] {{sparseSlice}}, new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 2);
    original.markImmutableRevision();
    assertFalse(original.allSelectedPresent(0));
    final String[] fields = {"value"};
    final long[] keys = {10, 11};
    cache.admit(store, fields, keys, original);
    try (MaskedColumns first = cache.lookup(store, fields, keys);
        MaskedColumns second = cache.lookup(store, fields, keys)) {
      assertNotNull(first);
      assertNotNull(second);
      assertSame(original.rowIdentity(), first.rowIdentity());
      assertSame(first.rowIdentity(), second.rowIdentity());
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
  }

  @Test
  void snapshotsWithSharedRecordKeysKeepSeparateIdentitiesAndFieldRosters() {
    final ProjectionColumnStore store = store();
    final MaskedColumns first = columns(store);
    final MaskedColumns second = new MaskedColumns(store, new long[][] {{2}}, new long[][] {{10, 11}},
        new ColumnSlice[][] {{slice()}}, new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 1);
    second.markImmutableRevision();
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, first);
    cache.admit(store, new String[] {"value"}, new long[] {11}, second);
    cache.admit(store, new String[] {"other"}, new long[] {11}, columns(store));
    try (MaskedColumns cachedFirst = cache.lookup(store, new String[] {"value"}, new long[] {10, 11});
        MaskedColumns cachedSecond = cache.lookup(store, new String[] {"value"}, new long[] {11});
        MaskedColumns otherField = cache.lookup(store, new String[] {"other"}, new long[] {11})) {
      assertNotNull(cachedFirst);
      assertNotNull(cachedSecond);
      assertNotNull(otherField);
      assertSame(first.rowIdentity(), cachedFirst.rowIdentity());
      assertNotSame(cachedFirst.rowIdentity(), cachedSecond.rowIdentity());
      assertNotSame(cachedSecond.rowIdentity(), otherField.rowIdentity());
      assertEquals(3, Objects.requireNonNull(cachedFirst.rowMask(0))[0]);
      assertEquals(2, Objects.requireNonNull(cachedSecond.rowMask(0))[0]);
      assertEquals(3, budget.metrics().numeric().entries());
    }
  }

  @Test
  void concurrentReadersCannotMutateAnotherReadersMask() throws Exception {
    final ProjectionColumnStore store = store();
    final String[] fields = {"value"};
    final long[] keys = {10, 11};
    cache.admit(store, fields, keys, columns(store));
    final CountDownLatch start = new CountDownLatch(1);
    try (var pool = Executors.newFixedThreadPool(2)) {
      final var first = pool.submit(() -> readAndMutate(store, fields, keys, start));
      final var second = pool.submit(() -> readAndMutate(store, fields, keys, start));
      start.countDown();
      first.get();
      second.get();
    }
  }

  private void readAndMutate(final ProjectionColumnStore store, final String[] fields, final long[] keys,
      final CountDownLatch start) {
    try {
      start.await();
      for (int i = 0; i < 1000; i++) {
        try (MaskedColumns view = cache.lookup(store, fields, keys)) {
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
  void readersOfAnEvictedSnapshotKeepValidMemoryUntilTheyClose() {
    final ProjectionColumnStore store = store();
    final MaskedColumns original = columns(store);
    final long bytes = Objects.requireNonNull(NativeNumericSnapshot.layout(original, 2)).bytes();
    final DerivedCacheBudget tight = withNumericShare(bytes);
    final NumericMaskedColumnCache one = new NumericMaskedColumnCache(tight.numeric(), Runnable::run);
    one.admit(store, new String[] {"value"}, new long[] {10, 11}, original);
    final MaskedColumns reader = one.lookup(store, new String[] {"value"}, new long[] {10, 11});
    assertNotNull(reader);
    // Pressure cannot evict a leased snapshot: a second roster of the same size is refused instead.
    one.admit(store, new String[] {"other"}, new long[] {10, 11}, columns(store));
    assertNull(one.lookup(store, new String[] {"other"}, new long[] {10, 11}));
    assertEquals(bytes, tight.metrics().rejectedBytes());
    try (MaskedColumns again = one.lookup(store, new String[] {"value"}, new long[] {10, 11})) {
      assertNotNull(again, "the leased snapshot is still served");
    }
    // An explicit eviction retires the snapshot but defers freeing to the reader.
    tight.numeric().clear();
    assertNull(one.lookup(store, new String[] {"value"}, new long[] {10, 11}));
    assertEquals(bytes, tight.metrics().retiredLeasedBytes(), "the evicted slab stays mapped for its reader");
    assertEquals(2, reader.longValue(0, 0, 1), "the reader still reads valid memory");
    assertEquals(11, reader.recordKey(0, 1));
    assertTrue(reader.isImmutableRevision());
    reader.close();
    reader.close();
    assertEquals(0, tight.metrics().retiredLeasedBytes(), "closing twice releases once");
    assertEquals(0, allocator.getSlabBytes());
    assertNull(one.lookup(store, new String[] {"value"}, new long[] {10, 11}), "a freed snapshot never returns");
    one.admit(store, new String[] {"other"}, new long[] {10, 11}, columns(store));
    try (MaskedColumns replacement = one.lookup(store, new String[] {"other"}, new long[] {10, 11})) {
      assertNotNull(replacement, "the share is free again");
    }
    tight.numeric().clear();
    assertEquals(0, allocator.getSlabBytes());
  }

  @Test
  void aTaskLeaseOutlivesTheViewThatTookIt() {
    final ProjectionColumnStore store = store();
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, columns(store));
    final MaskedColumns view = cache.lookup(store, new String[] {"value"}, new long[] {10, 11});
    assertNotNull(view);
    final DerivedCacheBudget.Lease task = view.acquireTaskLease();
    view.close();
    budget.numeric().clear();
    assertTrue(allocator.getSlabBytes() > 0, "the background task still pins the slab");
    task.run();
    assertEquals(0, allocator.getSlabBytes());
  }

  @Test
  void membershipPredicatesFilterCachedMasksWithoutChangingTheSnapshot() {
    final ProjectionColumnStore store = store();
    final String[] fields = {"value"};
    final long[] keys = {10, 11};
    final MaskedColumns heap = columns(store);
    cache.admit(store, fields, keys, heap);
    for (final boolean anti : new boolean[] {false, true}) {
      try (MaskedColumns cached = cache.lookup(store, fields, keys)) {
        assertNotNull(cached);
        final ColumnPredicate predicate =
            cached.recordKeyPredicateByMembership(0, keys, new LongOpenHashSet(new long[] {1}), anti);
        final ColumnPredicate expected =
            heap.recordKeyPredicateByMembership(0, keys, new LongOpenHashSet(new long[] {1}), anti);
        assertEquals(anti
            ? 2L
            : 1L, Objects.requireNonNull(predicate.keyMasks).byFirstKey().get(10)[0]);
        assertArrayEquals(Objects.requireNonNull(expected.keyMasks).byFirstKey().get(10),
            Objects.requireNonNull(predicate.keyMasks).byFirstKey().get(10), "native and heap filters agree");
        assertArrayEquals(heap.recordKeysByMembership(0, new LongOpenHashSet(new long[] {2}), anti),
            cached.recordKeysByMembership(0, new LongOpenHashSet(new long[] {2}), anti));
        assertEquals(heap.presentLongValues(0), cached.presentLongValues(0));
        assertEquals(3L, Objects.requireNonNull(cached.rowMask(0))[0]);
      }
      try (MaskedColumns repeated = cache.lookup(store, fields, keys)) {
        assertNotNull(repeated);
        assertEquals(3L, Objects.requireNonNull(repeated.rowMask(0))[0]);
      }
    }
  }

  @Test
  void sparseMasksDoNotAcquireDenseReuse() {
    final ProjectionColumnStore store = store();
    final MaskedColumns sparse = new MaskedColumns(store, new long[][] {{0}}, new long[][] {{10, 11}},
        new ColumnSlice[][] {{slice()}}, new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 0);
    sparse.markImmutableRevision();
    assertNull(NativeNumericSnapshot.layout(sparse, 1));
    cache.admit(store, new String[] {"value"}, new long[] {10}, sparse);
    assertNull(cache.lookup(store, new String[] {"value"}, new long[] {10}));
    assertEquals(0, budget.metrics().retainedBytes());
  }

  @Test
  void aSingleOversizedSnapshotIsRefusedBeforeCopyingAndFitsAtTheExactThreshold() {
    final ProjectionColumnStore store = store();
    final MaskedColumns columns = columns(store);
    final long bytes = Objects.requireNonNull(NativeNumericSnapshot.layout(columns, 2)).bytes();
    final DerivedCacheBudget below = withNumericShare(bytes - 1);
    final NumericMaskedColumnCache refused = new NumericMaskedColumnCache(below.numeric(), Runnable::run);
    refused.admit(store, new String[] {"value"}, new long[] {10, 11}, columns);
    assertNull(refused.lookup(store, new String[] {"value"}, new long[] {10, 11}));
    assertEquals(0, below.metrics().retainedBytes());
    assertEquals(0, below.metrics().inFlightBytes());
    assertEquals(bytes, below.metrics().rejectedBytes());
    assertEquals(0, allocator.getSlabBytes(), "a refusal maps nothing");

    final DerivedCacheBudget exact = withNumericShare(bytes);
    final NumericMaskedColumnCache admitted = new NumericMaskedColumnCache(exact.numeric(), Runnable::run);
    admitted.admit(store, new String[] {"value"}, new long[] {10, 11}, columns);
    try (MaskedColumns view = admitted.lookup(store, new String[] {"value"}, new long[] {10, 11})) {
      assertNotNull(view);
    }
    assertEquals(bytes, exact.metrics().retainedBytes());
    assertEquals(0, exact.metrics().inFlightBytes());
    exact.numeric().clear();
  }

  @Test
  void combinedRostersEvictIndividualSnapshotsAndAlternatingSelectionsReuseWhenTheyFit() {
    final ProjectionColumnStore store = store();
    final String[] rosterNames = {"diffs", "costs", "joins"};
    final long bytes = Objects.requireNonNull(NativeNumericSnapshot.layout(columns(store), 2)).bytes();
    final DerivedCacheBudget roomy = withNumericShare(3 * bytes);
    final NumericMaskedColumnCache all = new NumericMaskedColumnCache(roomy.numeric(), Runnable::run);
    for (final String name : rosterNames) {
      all.admit(store, new String[] {name}, new long[] {10, 11}, columns(store));
    }
    for (int repeat = 0; repeat < 4; repeat++) {
      for (final String name : rosterNames) {
        try (MaskedColumns view = all.lookup(store, new String[] {name}, new long[] {10, 11})) {
          assertNotNull(view);
          assertEquals(2, view.longValue(0, 0, 1));
          assertEquals(3, Objects.requireNonNull(view.rowMask(0))[0]);
        }
      }
    }
    assertEquals(3 * bytes, roomy.metrics().retainedBytes());
    roomy.numeric().clear();

    final DerivedCacheBudget tight = withNumericShare(2 * bytes);
    final NumericMaskedColumnCache two = new NumericMaskedColumnCache(tight.numeric(), Runnable::run);
    for (final String name : rosterNames) {
      two.admit(store, new String[] {name}, new long[] {10, 11}, columns(store));
    }
    int hits = 0;
    for (final String name : rosterNames) {
      try (MaskedColumns view = two.lookup(store, new String[] {name}, new long[] {10, 11})) {
        if (view != null) {
          hits++;
        }
      }
    }
    assertEquals(2, hits, "one oversized roster list must not evict all this store's snapshots");
    assertEquals(2 * bytes, tight.metrics().retainedBytes());
    assertEquals(0, tight.metrics().inFlightBytes());
    tight.numeric().clear();
  }

  @Test
  void mutableTransactionsAndChangedMasksCannotAcquireSnapshotResidency() {
    final ProjectionColumnStore store = store();
    final MaskedColumns immutable = columns(store);
    final MaskedColumns writer = new MaskedColumns(store, new long[][] {{3}}, new long[][] {{10, 11}},
        new ColumnSlice[][] {{slice()}}, new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 2);
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, writer);
    assertNull(cache.lookup(store, new String[] {"value"}, new long[] {10, 11}));
    Objects.requireNonNull(immutable.rowMask(0))[0] = 1;
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, immutable);
    assertNull(cache.lookup(store, new String[] {"value"}, new long[] {10, 11}));
    assertEquals(0, budget.metrics().retainedBytes());
    assertEquals(0, allocator.getSlabBytes());
  }

  @Test
  void aRefusedBackgroundCopyReleasesItsReservationAndTheSelectionCanBeAdmittedLater() {
    final ProjectionColumnStore store = store();
    final NumericMaskedColumnCache rejecting = new NumericMaskedColumnCache(budget.numeric(), task -> {
      throw new RejectedExecutionException("no copy thread");
    });
    assertThrows(RejectedExecutionException.class,
        () -> rejecting.admit(store, new String[] {"value"}, new long[] {10, 11}, columns(store)));
    assertEquals(0, budget.metrics().inFlightBytes());
    assertEquals(0, budget.metrics().retainedBytes());
    assertEquals(0, allocator.getSlabBytes(), "the reserved slab is released with the failed reservation");
    assertFalse(rejecting.copying(store, new String[] {"value"}, new long[] {10, 11}));
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, columns(store));
    try (MaskedColumns view = cache.lookup(store, new String[] {"value"}, new long[] {10, 11})) {
      assertNotNull(view);
    }
  }

  @Test
  void admissionOfAResidentOrCopyingSelectionIsANoOp() {
    final ProjectionColumnStore store = store();
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, columns(store));
    final long retained = budget.metrics().retainedBytes();
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, columns(store));
    assertEquals(retained, budget.metrics().retainedBytes());
    assertEquals(1, budget.metrics().numeric().entries());
    try (MaskedColumns view = cache.lookup(store, new String[] {"value"}, new long[] {10, 11})) {
      assertNotNull(view);
      cache.admit(store, new String[] {"value"}, new long[] {10, 11}, view);
      assertEquals(1, budget.metrics().numeric().entries(), "a native view is already resident");
    }
  }

  @Test
  void multiFieldRostersReadEveryFieldFromItsResidentLane() {
    final ProjectionColumnStore store = store();
    for (final int fieldCount : new int[] {3, 5}) {
      final MaskedColumns heap = columns(store, fieldCount);
      final String[] fields = new String[fieldCount];
      for (int field = 0; field < fieldCount; field++) {
        fields[field] = "f" + field;
      }
      cache.admit(store, fields, new long[] {10, 11}, heap);
      try (MaskedColumns view = cache.lookup(store, fields, new long[] {10, 11})) {
        assertNotNull(view);
        for (int field = 0; field < fieldCount; field++) {
          assertTrue(view.laneBase(field, 0) >= 0, "field " + field + " has a resident lane");
          assertTrue(field == 0 || view.laneBase(field, 0) >= view.laneBase(field - 1, 0) + 16L,
              "lanes are laid out one after another, eight bytes per row");
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
        final ColumnPredicate expected = heap.recordKeyPredicateByMembership(0, new long[] {10, 11}, members, false);
        final ColumnPredicate actual = view.recordKeyPredicateByMembership(0, new long[] {10, 11}, members, false);
        assertArrayEquals(Objects.requireNonNull(expected.keyMasks).byFirstKey().get(10),
            Objects.requireNonNull(actual.keyMasks).byFirstKey().get(10));
      }
    }
  }

  /**
   * {@code fieldCount} lanes over two rows; field {@code f} holds {@code 10 * f + row + 1}, field 1 a
   * timestamp.
   */
  private static MaskedColumns columns(final ProjectionColumnStore store, final int fieldCount) {
    final ColumnSlice[][] slices = new ColumnSlice[fieldCount][1];
    final byte[] kinds = new byte[fieldCount];
    for (int field = 0; field < fieldCount; field++) {
      final long first = 10L * field + 1;
      slices[field][0] = new ColumnSlice(2, (byte) 0, first, first + 1, new long[] {3}, new long[] {first, first + 1},
          null, null, null, null, null, null);
      kinds[field] = field == 1
          ? ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP
          : ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
    }
    final MaskedColumns columns =
        new MaskedColumns(store, new long[][] {{3}}, new long[][] {{10, 11}}, slices, kinds, 2);
    columns.markImmutableRevision();
    return columns;
  }

  private DerivedCacheBudget withNumericShare(final long bytes) {
    final long total = (4 * bytes + 2) / 3;
    return new DerivedCacheBudget(allocator, total * 8, total);
  }

  private static ColumnSlice slice() {
    return new ColumnSlice(2, (byte) 0, 1, 2, new long[] {3}, new long[] {1, 2}, null, null, null, null, null, null);
  }

  private static MaskedColumns columns(final ProjectionColumnStore store) {
    final MaskedColumns columns = new MaskedColumns(store, new long[][] {{3}}, new long[][] {{10, 11}},
        new ColumnSlice[][] {{slice()}}, new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 2);
    columns.markImmutableRevision();
    return columns;
  }

  private static ProjectionColumnStore store() {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    page.appendRow(10, new long[] {1}, new boolean[1], new String[1]);
    page.appendRow(11, new long[] {2}, new boolean[1], new String[1]);
    final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    assertNotNull(encoded);
    final long[] offsets = new long[encoded.columnSegmentIds().length];
    Arrays.fill(offsets, Constants.NULL_ID_LONG);
    return new ProjectionColumnStore(List.of(
        new RowGroupDirectory(1, encoded.descriptor(), encoded.columnSegmentIds(), offsets, encoded.segments())));
  }
}
