package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.CountDownLatch;
import java.util.Arrays;
import io.sirix.settings.Constants;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
    cached.rowMask(0)[0] = 0;
    original.rowMask(0)[0] = 0;
    fields[0] = "changed";
    keys[0] = 9;
    final MaskedColumns repeated = NumericMaskedColumnCache.get(store, new String[] {"value"}, new long[] {10, 11});
    assertNotNull(repeated);
    assertEquals(3, repeated.rowMask(0)[0]);
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
    both.rowMask(0)[0] = 1;
    assertTrue(both.allSelectedPresent(0));
    assertEquals(1, both.numericLane(0, 0)[0]);
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
        assertEquals(3, view.rowMask(0)[0]);
        assertEquals(2, view.longValue(0, 0, 1));
        view.rowMask(0)[0] = 0;
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IllegalStateException(e);
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
    final long[] offsets = new long[encoded.columnSegmentIds().length];
    Arrays.fill(offsets, Constants.NULL_ID_LONG);
    return new ProjectionColumnStore(List.of(
        new RowGroupDirectory(1, encoded.descriptor(), encoded.columnSegmentIds(), offsets, encoded.segments())));
  }
}
