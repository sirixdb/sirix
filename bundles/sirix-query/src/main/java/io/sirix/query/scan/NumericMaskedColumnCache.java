package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.query.scan.DerivedCacheBudget.Entry;
import io.sirix.query.scan.DerivedCacheBudget.Lease;
import io.sirix.query.scan.DerivedCacheBudget.Partition;
import io.sirix.query.scan.DerivedCacheBudget.Reservation;
import org.jspecify.annotations.Nullable;

/**
 * Immutable masked selections ({@link LaneSnapshot}) within the numeric share of the derived
 * allowance. A selection is cached without a copy: the executor reserves the share for the lanes
 * the store does not hold resident plus the selection's record keys, decodes straight into that
 * slab, and publishes the view over it; a later execution reads the entry through a leased view and
 * releases it with {@link MaskedColumns#close()}. Every entry owns its slab: a selection whose
 * columns are all resident still reserves the share for its key lanes and pins those columns.
 */
public final class NumericMaskedColumnCache {
  private static final NumericMaskedColumnCache INSTANCE = new NumericMaskedColumnCache(null);

  private final @Nullable Partition injected;

  /** @param injected the partition to serve from, or {@code null} for the global numeric share */
  NumericMaskedColumnCache(final @Nullable Partition injected) {
    this.injected = injected;
  }

  /** The cache over the global numeric partition. */
  public static NumericMaskedColumnCache global() {
    return INSTANCE;
  }

  private Partition partition() {
    return injected == null
        ? DerivedCacheBudget.global().numeric()
        : injected;
  }

  /** A leased view over the cached selection, or {@code null}; the caller closes the view. */
  @Nullable
  MaskedColumns lookup(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    final Entry entry =
        partition().acquire(candidate -> candidate instanceof LaneSnapshot lanes && lanes.matches(store, fields, keys));
    if (entry == null) {
      return null;
    }
    return ((LaneSnapshot) entry).view(new Lease(entry));
  }

  /**
   * Reserve {@code bytes} of the numeric share for a selection about to be decoded straight into the
   * returned slab, or {@code null} when the share or the allocator refuses it; the caller then
   * decodes into query-local memory and nothing is cached.
   */
  DerivedCacheBudget.@Nullable Reservation reserve(final long bytes) {
    return partition().reserve(bytes);
  }

  /**
   * Publish {@code view}, whose non-resident lanes and record keys live in {@code reservation}'s
   * slab, as the cached selection of {@code (store, fields, keys)}. The view must be marked an
   * immutable revision; it adopts the publisher's lease and reads the entry until it is closed.
   * {@code pinnedColumns} are the store columns the view reads resident; the entry pins them until
   * its final lease drops.
   */
  MaskedColumns publish(final Reservation reservation, final ProjectionColumnStore store, final String[] fields,
      final long[] keys, final MaskedColumns view, final int[] pinnedColumns) {
    if (view.store() != store) {
      throw new IllegalArgumentException("the view was read from another store");
    }
    final long[][] masks = view.immutableMasks();
    if (masks == null) {
      throw new IllegalStateException("only an immutable-revision selection can be published");
    }
    final LaneSnapshot entry = new LaneSnapshot(store, fields, keys, masks, view.keyLanes(), view.slices(),
        view.kinds(), view.rows(), view.presenceMemo(), pinnedColumns);
    try {
      reservation.publish(entry);
    } catch (final RuntimeException | Error failure) {
      entry.discard(); // the pins of an entry that was never published go back
      throw failure;
    }
    // The publisher's view adopts its lease (the publisher's, held by the entry) and the identity.
    view.adoptLease(entry, new Lease(entry));
    view.adoptIdentity(entry.rowIdentity());
    return view;
  }
}
