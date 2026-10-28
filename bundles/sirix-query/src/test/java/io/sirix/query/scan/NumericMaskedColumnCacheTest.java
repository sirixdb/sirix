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

final class NumericMaskedColumnCacheTest {
  @Test
  void signaturesAndRevisionIdentityFenceReuseAndEveryViewOwnsItsMasks() {
    final ProjectionColumnStore store = store();
    final String[] fields = {"value"};
    final long[] keys = {10, 11};
    final MaskedColumns original = columns(store);
    NumericMaskedColumnCache.put(store, fields, keys, original);
    final MaskedColumns cached = NumericMaskedColumnCache.get(store, fields, keys);
    assertNotNull(cached);
    assertEquals(2, cached.longValue(0, 0, 1));
    assertEquals(1, cached.minimum(0));
    assertEquals(2, cached.maximum(0));
    assertTrue(cached.allSelectedPresent(0));
    Objects.requireNonNull(cached.rowMask(0))[0] = 0;
    Objects.requireNonNull(original.rowMask(0))[0] = 0;
    fields[0] = "changed";
    keys[0] = 9;
    final MaskedColumns repeated = NumericMaskedColumnCache.get(store, new String[] {"value"}, new long[] {10, 11});
    assertNotNull(repeated);
    assertEquals(3, Objects.requireNonNull(repeated.rowMask(0))[0]);
    assertNull(NumericMaskedColumnCache.get(store, fields, keys));
    assertNull(NumericMaskedColumnCache.get(store, new String[] {"value"}, new long[] {10}));
    assertNull(NumericMaskedColumnCache.get(store(), new String[] {"value"}, new long[] {10, 11}),
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
    NumericMaskedColumnCache.put(store, fields, keys, original);
    final MaskedColumns first = NumericMaskedColumnCache.get(store, fields, keys);
    final MaskedColumns second = NumericMaskedColumnCache.get(store, fields, keys);
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
    NumericMaskedColumnCache.put(store, new String[] {"value"}, new long[] {10, 11}, first);
    NumericMaskedColumnCache.put(store, new String[] {"value"}, new long[] {11}, second);
    NumericMaskedColumnCache.put(store, new String[] {"other"}, new long[] {11}, columns(store));
    final MaskedColumns cachedFirst = NumericMaskedColumnCache.get(store, new String[] {"value"}, new long[] {10, 11});
    final MaskedColumns cachedSecond = NumericMaskedColumnCache.get(store, new String[] {"value"}, new long[] {11});
    final MaskedColumns otherField = NumericMaskedColumnCache.get(store, new String[] {"other"}, new long[] {11});
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
    NumericMaskedColumnCache.put(store, fields, keys, columns(store));
    final CountDownLatch start = new CountDownLatch(1);
    try (var pool = Executors.newFixedThreadPool(2)) {
      final var first = pool.submit(() -> readAndMutate(store, fields, keys, start));
      final var second = pool.submit(() -> readAndMutate(store, fields, keys, start));
      start.countDown();
      first.get();
      second.get();
    }
  }

  private static void readAndMutate(final ProjectionColumnStore store, final String[] fields, final long[] keys,
      final CountDownLatch start) {
    try {
      start.await();
      for (int i = 0; i < 1000; i++) {
        final MaskedColumns view = NumericMaskedColumnCache.get(store, fields, keys);
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
    NumericMaskedColumnCache.put(store, fields, keys, columns(store));
    for (final boolean anti : new boolean[] {false, true}) {
      final MaskedColumns cached = NumericMaskedColumnCache.get(store, fields, keys);
      assertNotNull(cached);
      final ColumnPredicate predicate =
          cached.recordKeyPredicateByMembership(0, keys, new LongOpenHashSet(new long[] {1}), anti);
      assertEquals(anti
          ? 2L
          : 1L, Objects.requireNonNull(predicate.keyMasks).byFirstKey().get(10)[0]);
      assertEquals(3L, Objects.requireNonNull(cached.rowMask(0))[0]);
      final MaskedColumns repeated = NumericMaskedColumnCache.get(store, fields, keys);
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
    NumericMaskedColumnCache.put(store, new String[] {"value"}, new long[] {10}, sparse);
    assertNull(NumericMaskedColumnCache.get(store, new String[] {"value"}, new long[] {10}));
  }

  private static MaskedColumns columns(final ProjectionColumnStore store) {
    final ColumnSlice slice =
        new ColumnSlice(2, (byte) 0, 1, 2, new long[] {3}, new long[] {1, 2}, null, null, null, null, null, null);
    return new MaskedColumns(store, new long[][] {{3}}, new long[][] {{10, 11}}, new ColumnSlice[][] {{slice}},
        new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG}, 2);
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
