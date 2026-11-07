/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A cached masked selection whose numeric lanes were decoded straight into this entry's slab (a
 * reservation of the numeric partition) or are the store's own resident columns, which the entry
 * pins for as long as a lease exists, and whose record keys live in the same slab as lanes: no heap
 * arrays, no copy per query. The entry keeps the selection's original masks, its key lanes and the
 * slices that point into the lanes; every reader gets a view with its own mask copy and a lease on
 * this entry. Packed join sides are keyed by the entry's row identity and leave with it.
 *
 * <p>
 * The store is held strongly: the pins must be returned to it when the final lease drops, and a
 * store whose handle left the catalog ({@link ProjectionColumnStore#retire()}) makes the entry
 * stale, so the partition reaps it on its next admission.
 * </p>
 */
final class LaneSnapshot extends DerivedCacheBudget.Entry {
  private final ProjectionColumnStore store;
  private final String[] fields;
  private final long[] keys;
  private final long[][] masks;
  private final KeyLanes recordKeys;
  private final ColumnSlice[][] slices;
  private final byte[] kinds;
  private final long rows;
  private final byte[] presenceMemo;
  private final Object rowIdentity = new Object();
  /** Store columns this entry pinned resident because its views read their slabs in place. */
  private final int[] pinnedColumns;
  private final AtomicBoolean unpinned = new AtomicBoolean();

  /**
   * @param pinnedColumns the store columns whose resident slabs the slices read; pinned here,
   *        unpinned when the final lease drops ({@link #released()}) or the entry is
   *        {@link #discard()}ed
   */
  LaneSnapshot(final ProjectionColumnStore store, final String[] fields, final long[] keys, final long[][] masks,
      final KeyLanes recordKeys, final ColumnSlice[][] slices, final byte[] kinds, final long rows,
      final byte[] presenceMemo, final int[] pinnedColumns) {
    this.store = Objects.requireNonNull(store);
    this.fields = fields.clone();
    this.keys = keys.clone();
    this.masks = Objects.requireNonNull(masks);
    this.recordKeys = Objects.requireNonNull(recordKeys);
    this.slices = Objects.requireNonNull(slices);
    this.kinds = Objects.requireNonNull(kinds);
    this.rows = rows;
    this.presenceMemo = Objects.requireNonNull(presenceMemo);
    this.pinnedColumns = pinnedColumns.clone();
    for (final int col : this.pinnedColumns) {
      store.retainColumn(col);
    }
  }

  /** Store by identity (a revision's store, never an equal one), roster and keys by content. */
  boolean matches(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    return this.store == store && Arrays.equals(this.fields, fields) && Arrays.equals(this.keys, keys);
  }

  /**
   * A view holding {@code lease} on this entry: shared slices and key lanes, this view's own masks.
   */
  MaskedColumns view(final DerivedCacheBudget.Lease lease) {
    final MaskedColumns view = new MaskedColumns(store, MaskedColumns.copyMasks(masks), recordKeys, slices, kinds, rows,
        rowIdentity, masks, presenceMemo);
    view.adoptLease(this, lease);
    return view;
  }

  Object rowIdentity() {
    return rowIdentity;
  }

  @Override
  protected boolean isStale() {
    return store.retired();
  }

  @Override
  protected void evicted() {
    // Packed sides are keyed by this entry's row identity; they leave with it.
    partition().budget().packed().evictDependents(rowIdentity);
  }

  /** The final lease dropped: no view reads the resident columns any more, unpin them. */
  @Override
  protected void released() {
    unpin();
  }

  /** Undo the constructor's pins for an entry that was never published. */
  void discard() {
    unpin();
  }

  private void unpin() {
    if (unpinned.compareAndSet(false, true)) {
      for (final int col : pinnedColumns) {
        store.releaseColumn(col);
      }
    }
  }
}
