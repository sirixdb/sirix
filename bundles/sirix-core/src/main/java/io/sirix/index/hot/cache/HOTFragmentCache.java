/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.hot.cache;

import io.sirix.cache.Cache;
import io.sirix.cache.ShardedPageCache;
import io.sirix.index.hot.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

/**
 * The raw HOT fragment cache, split by allocation ownership.
 *
 * <p>
 * Allocator-frame images can release frames under allocator pressure; compact images own their
 * native storage independently and need a separate budget and sweeper. Lookups consult both halves.
 * Backend selection, byte charges and budget controls are documented in
 * {@code docs/PROJECTION_READ_PERFORMANCE.md}, under Versioned HOT projection slot reads.
 * </p>
 */
public final class HOTFragmentCache implements Cache<PageReference, HOTLeafPage> {
  private final ShardedPageCache<HOTLeafPage> nativeImages;
  private final ShardedPageCache<HOTLeafPage> compactImages;
  private final boolean compactEnabled;

  public HOTFragmentCache(final long nativeMaxWeightBytes, final long heapMaxWeightBytes) {
    nativeImages = new ShardedPageCache<>(nativeMaxWeightBytes);
    if (heapMaxWeightBytes < 0) {
      throw new IllegalArgumentException("Negative compact-image allowance");
    }
    compactImages = new ShardedPageCache<>(heapMaxWeightBytes);
    compactEnabled = heapMaxWeightBytes != 0;
  }

  /** The allocator-frame half, bounded by the off-heap-derived fragment budget. */
  public ShardedPageCache<HOTLeafPage> nativeImages() {
    return nativeImages;
  }

  /**
   * The images allocator pressure cannot reclaim: {@link HOTLeafPage#isCompactImage() compact images}
   * and any heap-resident image. See the class documentation for budget ownership.
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

  /**
   * Prefer a fragment that just left the writer's sliding window when its partition needs space.
   * Historical readers can still reload the immutable image, and guards keep current readers alive.
   * Below the sweeper's pressure threshold, retain it for those historical reads as usual.
   */
  public boolean evictSupersededFragment(final HOTLeafPage page) {
    Objects.requireNonNull(page);
    final ShardedPageCache<HOTLeafPage> cache = cacheFor(page);
    final long maximum = cache.getMaxWeightBytes();
    if (maximum <= 0 || cache.getCurrentWeightBytes() < maximum - maximum / 10) {
      return false;
    }
    final PageReference key = page.lastCacheKey();
    return key != null && cache.evictIfSame(key, page);
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
    if (!compactEnabled && cacheFor(loaded) == compactImages) {
      if (!loaded.acquireGuard()) {
        loaded.retire();
        return null;
      }
      // The reader guard owns this uncached image; retirement closes it on the final release.
      loaded.retire();
      return loaded;
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
    final ShardedPageCache<HOTLeafPage> destination = cacheFor(value);
    if (compactEnabled || destination != compactImages) {
      destination.put(key, value);
    }
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

  public void evictUnderPressure() {
    // Allocator pressure concerns the resident half only; a compact image holds no frame to release.
    nativeImages.evictUnderPressure();
  }
}
