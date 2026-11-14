/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import static io.sirix.cache.ShardedPageCache.retainCleanupFailure;
import static io.sirix.cache.ShardedPageCache.rethrowCleanupFailure;

import io.sirix.cache.Allocators;
import java.lang.foreign.MemorySegment;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.jspecify.annotations.Nullable;

/**
 * The allocator slabs a {@link ProjectionColumnStore} decoded its numeric columns into, one per
 * column. Held apart from the store so the same object can serve as the store's
 * {@link java.lang.ref.Cleaner} action: a store that becomes unreachable with slabs still mapped (a
 * bounded handle the catalog never cached and therefore never retired) returns them through
 * {@link #run()}, which never references the store. Every deterministic path — the residency
 * release at a query boundary, the fit door making room, retirement, the last pin of a retired
 * column — takes a slab out through {@link #take} first, so the backstop finds nothing on those
 * stores.
 */
final class ColumnSlabs implements Runnable {

  private final AtomicReferenceArray<MemorySegment> slabs;

  ColumnSlabs(final int columns) {
    if (columns < 0) {
      throw new IllegalArgumentException("negative column count " + columns);
    }
    slabs = new AtomicReferenceArray<>(columns);
  }

  /** The slab {@code col}'s lanes live in, or {@code null} when the column owns none. */
  @Nullable
  MemorySegment get(final int col) {
    return slabs.get(col);
  }

  /** Record the slab {@code col}'s lanes live in; the column must not already own one. */
  void set(final int col, final MemorySegment slab) {
    if (!slabs.compareAndSet(col, null, slab)) {
      throw new IllegalStateException("column " + col + " already owns a slab");
    }
  }

  /** Remove and return {@code col}'s slab, or {@code null} when it owns none. */
  @Nullable
  MemorySegment take(final int col) {
    return slabs.getAndSet(col, null);
  }

  /** Release every slab still mapped — the backstop for a store that was never retired. */
  // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
  // failure is rethrown with later ones attached.
  @SuppressWarnings("java:S1181")
  @Override
  public void run() {
    Throwable failure = null;
    for (int col = 0; col < slabs.length(); col++) {
      final MemorySegment slab = slabs.getAndSet(col, null);
      if (slab != null) {
        try {
          Allocators.getInstance().releaseSlab(slab);
        } catch (final Throwable cleanup) {
          failure = retainCleanupFailure(failure, cleanup);
        }
      }
    }
    rethrowCleanupFailure(failure);
  }
}
