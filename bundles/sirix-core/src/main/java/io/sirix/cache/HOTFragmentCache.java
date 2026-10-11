/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * The raw HOT fragment cache, split by where its images actually live.
 *
 * <p>
 * A committed fragment is decoded either into an allocator frame (every backend that does not
 * implement the compact fragment reads, and every byte pipeline without memory-segment support) or
 * into a packed Java-heap {@code byte[]}. One budget cannot bound both honestly: a heap-derived
 * ceiling would shrink a cache of native frames that costs no heap, and an off-heap-derived ceiling
 * would let heap images grow past what the heap can take. So each residency gets its own weighted
 * cache, and a lookup consults both — a durable offset is decoded the same way by every reader of a
 * resource, so in practice a key only ever occupies one of them.
 * </p>
 */
public final class HOTFragmentCache implements Cache<PageReference, HOTLeafPage> {
  private final ShardedPageCache<HOTLeafPage> nativeImages;
  private final ShardedPageCache<HOTLeafPage> compactImages;

  public HOTFragmentCache(final long nativeMaxWeightBytes, final long heapMaxWeightBytes) {
    nativeImages = new ShardedPageCache<>(nativeMaxWeightBytes);
    compactImages = new ShardedPageCache<>(heapMaxWeightBytes);
  }

  /** The allocator-frame half, bounded by the off-heap-derived fragment budget. */
  public ShardedPageCache<HOTLeafPage> nativeImages() {
    return nativeImages;
  }

  /** The compact-decode half, bounded by the retained-heap ceiling. */
  /**
   * The images allocator pressure cannot reclaim: compact decoded point-lookup fragments (GC-owned
   * packed bytes, see HOTLeafPage#isCompactImage) and any heap-resident image. This half keeps its
   * own budget and sweeper, as the retained-heap half it used to be.
   */
  public ShardedPageCache<HOTLeafPage> compactImages() {
    return compactImages;
  }

  private ShardedPageCache<HOTLeafPage> cacheFor(final HOTLeafPage page) {
    // The compact half holds what allocator pressure cannot reclaim: a compact decoded image (native
    // memory it owns outright, see HOTLeafPage#isCompactImage) and any heap-resident image.
    return page.isCompactImage() || !page.slots().isNative()
        ? compactImages
        : nativeImages;
  }

  @Override
  public @Nullable HOTLeafPage getAndGuard(final PageReference key) {
    // Compact images first: they answer the requested-slot reads, which is the latency-sensitive path.
    final HOTLeafPage compact = compactImages.getAndGuard(key);
    return compact != null
        ? compact
        : nativeImages.getAndGuard(key);
  }

  @Override
  public @Nullable HOTLeafPage getOrLoadAndGuard(final PageReference key,
      final Function<PageReference, HOTLeafPage> loader) {
    Objects.requireNonNull(loader);
    final HOTLeafPage cached = getAndGuard(key);
    if (cached != null) {
      return cached;
    }
    final HOTLeafPage loaded = loader.apply(key);
    if (loaded == null) {
      return null;
    }
    // The delegate re-probes under its own admission lock, so a concurrent winner is still returned
    // guarded and this caller's image is still reported as the loser.
    return cacheFor(loaded).getOrLoadAndGuard(key, _ -> loaded);
  }

  @Override
  public @Nullable HOTLeafPage get(final PageReference key) {
    final HOTLeafPage compact = compactImages.get(key);
    return compact != null
        ? compact
        : nativeImages.get(key);
  }

  @Override
  public void put(final PageReference key, final HOTLeafPage value) {
    cacheFor(value).put(key, value);
  }

  @Override
  public void putAll(final Map<? extends PageReference, ? extends HOTLeafPage> map) {
    map.forEach(this::put);
  }

  @Override
  public Map<PageReference, HOTLeafPage> getAll(final Iterable<? extends PageReference> keys) {
    final Map<PageReference, HOTLeafPage> found = new ConcurrentHashMap<>();
    for (final PageReference key : keys) {
      final HOTLeafPage page = get(key);
      if (page != null) {
        found.put(key, page);
      }
    }
    return found;
  }

  @Override
  public void remove(final PageReference key) {
    final HOTLeafPage removed = removeAndGet(key);
    if (removed != null) {
      removed.retire();
    }
  }

  @Override
  public @Nullable HOTLeafPage removeAndGet(final PageReference key) {
    HOTLeafPage compact = null;
    HOTLeafPage resident = null;
    Throwable failure = null;
    try {
      compact = compactImages.removeAndGet(key);
    } catch (final RuntimeException | Error e) {
      failure = e;
    }
    try {
      resident = nativeImages.removeAndGet(key);
    } catch (final RuntimeException | Error e) {
      failure = ShardedPageCache.retainCleanupFailure(failure, e);
    }
    final HOTLeafPage result = compact != null
        ? compact
        : resident;
    if (resident != null && resident != result) {
      try {
        resident.retire();
      } catch (final RuntimeException | Error e) {
        failure = ShardedPageCache.retainCleanupFailure(failure, e);
      }
    }
    if (failure != null && result != null) {
      try {
        result.retire();
      } catch (final RuntimeException | Error e) {
        failure = ShardedPageCache.retainCleanupFailure(failure, e);
      }
    }
    ShardedPageCache.rethrowCleanupFailure(failure);
    return result;
  }

  @Override
  public void removePage(final HOTLeafPage value) {
    cacheFor(value).removePage(value);
  }

  @Override
  public ConcurrentMap<PageReference, HOTLeafPage> asMap() {
    // A snapshot, not a live view: the two halves have independent weight accounting, so one mutable
    // union would have to pick a residency for every write it accepted. Callers that sweep take the
    // keys from here and go back through remove/removeAndGet, which route by residency.
    final ConcurrentMap<PageReference, HOTLeafPage> union = new ConcurrentHashMap<>();
    union.putAll(nativeImages.asMap());
    union.putAll(compactImages.asMap());
    return union;
  }

  @Override
  public void clear() {
    Throwable failure = null;
    try {
      compactImages.clear();
    } catch (final RuntimeException | Error e) {
      failure = e;
    }
    try {
      nativeImages.clear();
    } catch (final RuntimeException | Error e) {
      failure = ShardedPageCache.retainCleanupFailure(failure, e);
    }
    ShardedPageCache.rethrowCleanupFailure(failure);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException("The HOT fragment cache has no second tier");
  }

  @Override
  public void close() {
    clear();
  }

  public long getCurrentWeightBytes() {
    return nativeImages.getCurrentWeightBytes() + compactImages.getCurrentWeightBytes();
  }

  public long getMaxWeightBytes() {
    return nativeImages.getMaxWeightBytes() + compactImages.getMaxWeightBytes();
  }

  public long size() {
    return nativeImages.size() + compactImages.size();
  }

  void evictUnderPressure() {
    // Allocator pressure concerns the resident half only; a compact image holds no frame to release.
    nativeImages.evictUnderPressure();
  }
}
