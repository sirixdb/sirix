package io.sirix.query.scan;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.query.scan.DerivedCacheBudget.Partition;
import io.sirix.query.scan.DerivedCacheBudget.Reservation;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

/** Immutable numeric snapshots, independently evictable within one process-wide derived allowance. */
final class NumericMaskedColumnCache {
  private static final NumericMaskedColumnCache INSTANCE = new NumericMaskedColumnCache(null);

  // The index owns only handles, never arrays. Eviction removes a handle; collection of a store
  // makes its handles weakly reachable in the weighted cache. Neither values nor handles own stores.
  private final Cache<ProjectionColumnStore, ConcurrentLinkedQueue<Handle>> index =
      Caffeine.newBuilder().weakKeys().build();
  private final @Nullable Partition injected;

  NumericMaskedColumnCache(final @Nullable Partition injected) {
    this.injected = injected;
  }

  private static final class Handle implements DerivedCacheBudget.EvictionKey {
    private final ConcurrentLinkedQueue<Handle> siblings;

    private Handle(final ConcurrentLinkedQueue<Handle> siblings) {
      this.siblings = siblings;
    }

    @Override
    public void evicted() {
      siblings.remove(this);
    }
  }

  private Partition partition() {
    return injected == null ? DerivedCacheBudget.global().numeric() : injected;
  }

  static @Nullable MaskedColumns get(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    return INSTANCE.lookup(store, fields, keys);
  }

  @Nullable MaskedColumns lookup(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    final ConcurrentLinkedQueue<Handle> handles = index.getIfPresent(store);
    if (handles == null) {
      return null;
    }
    final Partition partition = partition();
    for (final Handle handle : handles) {
      final MaskedColumns.NumericSnapshot data = (MaskedColumns.NumericSnapshot) partition.get(handle);
      if (data != null && Arrays.equals(fields, data.fields()) && Arrays.equals(keys, data.keys())) {
        return data.view(store);
      }
    }
    return null;
  }

  static void put(final ProjectionColumnStore store, final String[] fields, final long[] keys,
      final MaskedColumns columns) {
    INSTANCE.admit(store, fields, keys, columns);
  }

  void admit(final ProjectionColumnStore store, final String[] fields, final long[] keys,
      final MaskedColumns columns) {
    if (columns.store() != store || !columns.isImmutableRevision()) {
      return;
    }
    long bytes = columns.numericSnapshotWeight(keys.length);
    if (bytes <= 0) {
      return;
    }
    for (final String field : fields) {
      bytes = DerivedCacheBudget.saturatedAdd(bytes, 64L + 2L * field.length());
    }
    final Partition partition = partition();
    final ConcurrentLinkedQueue<Handle> handles = index.get(store, ignored -> new ConcurrentLinkedQueue<>());
    // Serialize only admission for this store; readers and other stores never take this monitor.
    synchronized (handles) {
      for (final Handle handle : handles) {
        final MaskedColumns.NumericSnapshot existing = (MaskedColumns.NumericSnapshot) partition.get(handle);
        if (existing != null && Arrays.equals(fields, existing.fields()) && Arrays.equals(keys, existing.keys())) {
          return;
        }
      }
      try (final Reservation reservation = partition.reserve(bytes)) {
        if (reservation == null) {
          return;
        }
        final MaskedColumns.NumericSnapshot snapshot = columns.numericSnapshot(fields, keys, bytes);
        final Handle handle = new Handle(handles);
        handles.add(handle);
        try {
          reservation.publish(handle, snapshot, bytes);
        } catch (final RuntimeException | Error e) {
          handles.remove(handle);
          throw e;
        }
      }
    }
  }
}
