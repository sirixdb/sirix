package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.query.scan.DerivedCacheBudget.Entry;
import io.sirix.query.scan.DerivedCacheBudget.Lease;
import io.sirix.query.scan.DerivedCacheBudget.Partition;
import io.sirix.query.scan.DerivedCacheBudget.Reservation;
import io.sirix.query.scan.NativeNumericSnapshot.Layout;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Cleaner;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

/**
 * Immutable numeric snapshots in allocator slabs, independently evictable within the numeric share
 * of the derived allowance. Admission reserves synchronously and copies the lanes on a background
 * task, so the query that decoded them never pays the copy; a later execution reads the slab
 * through a leased view and releases it with {@link MaskedColumns#close()}.
 */
public final class NumericMaskedColumnCache {
  private static final Logger LOGGER = LoggerFactory.getLogger(NumericMaskedColumnCache.class);
  private static final NumericMaskedColumnCache INSTANCE = new NumericMaskedColumnCache(null, null);
  /** Backstop for a view that was never closed: the lease, never the view, is the cleaning action. */
  private static final Cleaner CLEANER = Cleaner.create();
  private static volatile @Nullable Executor copyExecutorForTesting;

  private final @Nullable Partition injected;
  private final @Nullable Executor injectedExecutor;
  private final ConcurrentHashMap<InFlightKey, Boolean> inFlight = new ConcurrentHashMap<>();

  NumericMaskedColumnCache(final @Nullable Partition injected, final @Nullable Executor executor) {
    this.injected = injected;
    this.injectedExecutor = executor;
  }

  /** Store by identity, roster and keys by content; dedupes background copies of one selection. */
  private record InFlightKey(WeakReference<ProjectionColumnStore> store, int storeHash, String[] fields, long[] keys) {
    @Override
    @SuppressWarnings("ReferenceEquality") // Store identity scopes a snapshot; equal stores are not the same revision.
    public boolean equals(final Object other) {
      if (other == this) {
        return true;
      }
      if (!(other instanceof InFlightKey key)) {
        return false;
      }
      final ProjectionColumnStore mine = store.get();
      return mine != null && key.store.get() == mine && Arrays.equals(fields, key.fields)
          && Arrays.equals(keys, key.keys);
    }

    @Override
    public int hashCode() {
      return 31 * (31 * storeHash + Arrays.hashCode(fields)) + Arrays.hashCode(keys);
    }
  }

  /** Test seam: run background copies on a held executor instead of the common pool. */
  public static @Nullable Executor setCopyExecutorForTesting(final @Nullable Executor executor) {
    final Executor previous = copyExecutorForTesting;
    copyExecutorForTesting = executor;
    return previous;
  }

  private Partition partition() {
    return injected == null
        ? DerivedCacheBudget.global().numeric()
        : injected;
  }

  private Executor executor() {
    if (injectedExecutor != null) {
      return injectedExecutor;
    }
    final Executor override = copyExecutorForTesting;
    return override == null
        ? ForkJoinPool.commonPool()
        : override;
  }

  static @Nullable MaskedColumns get(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    return INSTANCE.lookup(store, fields, keys);
  }

  /** A leased view over the resident snapshot, or {@code null}; the caller closes the view. */
  @Nullable
  MaskedColumns lookup(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    final Entry entry = partition().acquire(
        candidate -> candidate instanceof NativeNumericSnapshot snapshot && snapshot.matches(store, fields, keys));
    if (entry == null) {
      return null;
    }
    final NativeNumericSnapshot snapshot = (NativeNumericSnapshot) entry;
    final Lease lease = new Lease(snapshot);
    final MaskedColumns view = snapshot.view(store, lease);
    CLEANER.register(view, lease);
    return view;
  }

  static void put(final ProjectionColumnStore store, final String[] fields, final long[] keys,
      final MaskedColumns columns) {
    INSTANCE.admit(store, fields, keys, columns);
  }

  /**
   * Reserve the snapshot's slab now and fill it in the background. Only immutable, dense, numeric
   * selections read from a heap view qualify; a view over a resident snapshot is already cached.
   */
  void admit(final ProjectionColumnStore store, final String[] fields, final long[] keys, final MaskedColumns columns) {
    if (columns.store() != store || columns.isNative() || !columns.isImmutableRevision()) {
      return;
    }
    final Layout layout = NativeNumericSnapshot.layout(columns, keys.length);
    if (layout == null) {
      return;
    }
    final Partition partition = partition();
    final InFlightKey key =
        new InFlightKey(new WeakReference<>(store), System.identityHashCode(store), fields.clone(), keys.clone());
    if (partition.contains(
        candidate -> candidate instanceof NativeNumericSnapshot snapshot && snapshot.matches(store, fields, keys))
        || inFlight.putIfAbsent(key, Boolean.TRUE) != null) {
      return;
    }
    boolean scheduled = false;
    try {
      final Reservation reservation = partition.reserve(layout.bytes());
      if (reservation == null) {
        return;
      }
      // Masks are the only mutable input: copy them now, before the query filters its view further.
      final long[][] masks = columns.copyRowMasks();
      final Runnable copy = () -> {
        try (reservation) {
          final NativeNumericSnapshot snapshot =
              NativeNumericSnapshot.build(reservation.slab(), layout, masks, columns, store, key.fields(), key.keys());
          reservation.publish(snapshot);
          snapshot.release();
        } catch (final RuntimeException | Error failure) {
          LOGGER.warn("Numeric snapshot copy failed; the selection stays uncached", failure);
        } finally {
          inFlight.remove(key);
        }
      };
      try {
        CompletableFuture.runAsync(copy, executor());
        scheduled = true;
      } catch (final RuntimeException | Error failure) {
        reservation.close();
        throw failure;
      }
    } finally {
      if (!scheduled) {
        inFlight.remove(key);
      }
    }
  }

  /** Whether a background copy for this selection is still running. */
  boolean copying(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    return inFlight.containsKey(
        new InFlightKey(new WeakReference<>(store), System.identityHashCode(store), fields, keys));
  }
}
