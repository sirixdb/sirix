package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.settings.Constants;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("native-image")
final class NumericMaskedColumnCacheTest {
  private final DerivedCacheBudget budget = new DerivedCacheBudget(1L << 20, 1L << 20, 1L << 20, -1);
  private final NumericMaskedColumnCache cache = new NumericMaskedColumnCache(budget.numeric());
  @Test
  void signaturesAndRevisionIdentityFenceReuseAndEveryViewOwnsItsMasks() {
    final ProjectionColumnStore store = store();
    final String[] fields = {"value"};
    final long[] keys = {10, 11};
    final MaskedColumns original = columns(store);
    cache.admit(store, fields, keys, original);
    final MaskedColumns cached = cache.lookup(store, fields, keys);
    assertNotNull(cached);
    assertEquals(2, cached.longValue(0, 0, 1));
    assertEquals(1, cached.minimum(0));
    assertEquals(2, cached.maximum(0));
    assertTrue(cached.allSelectedPresent(0));
    Objects.requireNonNull(cached.rowMask(0))[0] = 0;
    Objects.requireNonNull(original.rowMask(0))[0] = 0;
    fields[0] = "changed";
    keys[0] = 9;
    final MaskedColumns repeated = cache.lookup(store, new String[] {"value"}, new long[] {10, 11});
    assertNotNull(repeated);
    assertEquals(3, Objects.requireNonNull(repeated.rowMask(0))[0]);
    assertNull(cache.lookup(store, fields, keys));
    assertNull(cache.lookup(store, new String[] {"value"}, new long[] {10}));
    assertNull(cache.lookup(store(), new String[] {"value"}, new long[] {10, 11}),
        "a new revision/store identity cannot reuse old values even with identical keys");
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
    final MaskedColumns first = cache.lookup(store, fields, keys);
    final MaskedColumns second = cache.lookup(store, fields, keys);
    assertNotNull(first);
    assertNotNull(second);
    assertSame(original.rowIdentity(), first.rowIdentity());
    assertSame(first.rowIdentity(), second.rowIdentity());
    assertFalse(first.allSelectedPresent(0));
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

  @Test
  void snapshotsWithSharedRecordKeysKeepSeparateIdentitiesAndFieldRosters() {
    final ProjectionColumnStore store = store();
    final MaskedColumns first = columns(store);
    final MaskedColumns.NumericSnapshot snapshot =
        first.numericSnapshot(new String[] {"value"}, new long[] {10, 11}, 1);
    final MaskedColumns second = new MaskedColumns(store, new long[][] {{2}}, snapshot.recordKeys(), snapshot.slices(),
        new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 1);
    second.markImmutableRevision();
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, first);
    cache.admit(store, new String[] {"value"}, new long[] {11}, second);
    cache.admit(store, new String[] {"other"}, new long[] {11}, columns(store));
    final MaskedColumns cachedFirst = cache.lookup(store, new String[] {"value"}, new long[] {10, 11});
    final MaskedColumns cachedSecond = cache.lookup(store, new String[] {"value"}, new long[] {11});
    final MaskedColumns otherField = cache.lookup(store, new String[] {"other"}, new long[] {11});
    assertNotNull(cachedFirst);
    assertNotNull(cachedSecond);
    assertNotNull(otherField);
    assertSame(first.rowIdentity(), cachedFirst.rowIdentity());
    assertNotSame(cachedFirst.rowIdentity(), cachedSecond.rowIdentity());
    assertNotSame(cachedSecond.rowIdentity(), otherField.rowIdentity());
    assertEquals(3, Objects.requireNonNull(cachedFirst.rowMask(0))[0]);
    assertEquals(2, Objects.requireNonNull(cachedSecond.rowMask(0))[0]);
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
        final MaskedColumns view = cache.lookup(store, fields, keys);
        assertNotNull(view);
        final long[] rowMask = Objects.requireNonNull(view.rowMask(0));
        assertEquals(3, rowMask[0]);
        assertEquals(2, view.longValue(0, 0, 1));
        rowMask[0] = 0;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
    }
  }

  @Test
  void membershipPredicatesFilterCachedMasksWithoutChangingTheSnapshot() {
    final ProjectionColumnStore store = store();
    final String[] fields = {"value"};
    final long[] keys = {10, 11};
    cache.admit(store, fields, keys, columns(store));
    for (final boolean anti : new boolean[] {false, true}) {
      final MaskedColumns cached = cache.lookup(store, fields, keys);
      assertNotNull(cached);
      final ColumnPredicate predicate =
          cached.recordKeyPredicateByMembership(0, keys, new LongOpenHashSet(new long[] {1}), anti);
      assertEquals(anti
          ? 2L
          : 1L, Objects.requireNonNull(predicate.keyMasks).byFirstKey().get(10)[0]);
      assertEquals(3L, Objects.requireNonNull(cached.rowMask(0))[0]);
      final MaskedColumns repeated = cache.lookup(store, fields, keys);
      assertNotNull(repeated);
      assertEquals(3L, Objects.requireNonNull(repeated.rowMask(0))[0]);
    }
  }

  @Test
  void sparseMasksDoNotAcquireDenseReuse() {
    final ProjectionColumnStore store = store();
    final MaskedColumns sparse = new MaskedColumns(store, new long[][] {{0}}, new long[][] {{10, 11}},
        columns(store).numericSnapshot(new String[] {"value"}, new long[] {10, 11}, 1).slices(),
        new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 0);
    cache.admit(store, new String[] {"value"}, new long[] {10}, sparse);
    assertNull(cache.lookup(store, new String[] {"value"}, new long[] {10}));
  }

  @Test
  void aSingleOversizedSnapshotIsRefusedBeforeCopyingAndFitsAtTheExactThreshold() {
    final ProjectionColumnStore store = store();
    final MaskedColumns columns = columns(store);
    final long bytes = columns.numericSnapshotWeight(2) + 64 + 2 * "value".length();
    final DerivedCacheBudget below = withNumericShare(bytes - 1);
    final NumericMaskedColumnCache refused = new NumericMaskedColumnCache(below.numeric());
    refused.admit(store, new String[] {"value"}, new long[] {10, 11}, columns);
    assertNull(refused.lookup(store, new String[] {"value"}, new long[] {10, 11}));
    assertEquals(0, below.metrics().retainedBytes());
    assertEquals(0, below.metrics().inFlightBytes());
    assertEquals(bytes, below.metrics().rejectedBytes());

    final DerivedCacheBudget exact = withNumericShare(bytes);
    final NumericMaskedColumnCache admitted = new NumericMaskedColumnCache(exact.numeric());
    admitted.admit(store, new String[] {"value"}, new long[] {10, 11}, columns);
    assertNotNull(admitted.lookup(store, new String[] {"value"}, new long[] {10, 11}));
    assertEquals(bytes, exact.metrics().retainedBytes());
    assertEquals(0, exact.metrics().inFlightBytes());
  }

  @Test
  void combinedRostersEvictIndividualSnapshotsAndAlternatingSelectionsReuseWhenTheyFit() {
    final ProjectionColumnStore store = store();
    final String[] rosterNames = {"diffs", "costs", "joins"};
    final long bytes = columns(store).numericSnapshotWeight(2) + 64 + 10;
    final DerivedCacheBudget roomy = withNumericShare(3 * bytes);
    final NumericMaskedColumnCache all = new NumericMaskedColumnCache(roomy.numeric());
    for (final String name : rosterNames) {
      all.admit(store, new String[] {name}, new long[] {10, 11}, columns(store));
    }
    for (int repeat = 0; repeat < 4; repeat++) {
      for (final String name : rosterNames) {
        final MaskedColumns view = all.lookup(store, new String[] {name}, new long[] {10, 11});
        assertNotNull(view);
        assertEquals(2, view.longValue(0, 0, 1));
        assertEquals(3, Objects.requireNonNull(view.rowMask(0))[0]);
      }
    }
    assertEquals(3 * bytes, roomy.metrics().retainedBytes());

    final DerivedCacheBudget tight = withNumericShare(2 * bytes);
    final NumericMaskedColumnCache two = new NumericMaskedColumnCache(tight.numeric());
    for (final String name : rosterNames) {
      two.admit(store, new String[] {name}, new long[] {10, 11}, columns(store));
    }
    int hits = 0;
    for (final String name : rosterNames) {
      if (two.lookup(store, new String[] {name}, new long[] {10, 11}) != null) {
        hits++;
      }
    }
    assertEquals(2, hits, "one oversized roster list must not evict all this store's snapshots");
    assertEquals(2 * bytes, tight.metrics().retainedBytes());
    assertEquals(0, tight.metrics().inFlightBytes());
  }

  @Test
  void mutableTransactionsAndChangedMasksCannotAcquireSnapshotResidency() {
    final ProjectionColumnStore store = store();
    final MaskedColumns immutable = columns(store);
    final MaskedColumns.NumericSnapshot data = immutable.numericSnapshot(new String[] {"value"}, new long[] {10, 11}, 1);
    final MaskedColumns writer = new MaskedColumns(store, new long[][] {{3}}, data.recordKeys(), data.slices(),
        data.kinds(), 2);
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, writer);
    assertNull(cache.lookup(store, new String[] {"value"}, new long[] {10, 11}));
    Objects.requireNonNull(immutable.rowMask(0))[0] = 1;
    cache.admit(store, new String[] {"value"}, new long[] {10, 11}, immutable);
    assertNull(cache.lookup(store, new String[] {"value"}, new long[] {10, 11}));
    assertEquals(0, budget.metrics().retainedBytes());
  }

  private static DerivedCacheBudget withNumericShare(final long bytes) {
    final long total = (4 * bytes + 2) / 3;
    return new DerivedCacheBudget(total * 8, total * 8, total, total);
  }

  private static MaskedColumns columns(final ProjectionColumnStore store) {
    final ColumnSlice slice =
        new ColumnSlice(2, (byte) 0, 1, 2, new long[] {3}, new long[] {1, 2}, null, null, null, null, null, null);
    final MaskedColumns columns = new MaskedColumns(store, new long[][] {{3}}, new long[][] {{10, 11}},
        new ColumnSlice[][] {{slice}}, new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 2);
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
