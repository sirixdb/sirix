/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Scheduler;
import io.sirix.page.KeyValueLeafPage;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Interface for all upcoming cache implementations. Can be a weak one, a LRU-based one or a
 * persistent. However, clear, put and get must to be provided. Instances of this class are used
 * with {@code StorageEngineReader} as well as with {@code StorageEngineWriter}.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 *
 * @param <K> the key
 * @param <V> the value
 */
public interface Cache<K, V> {
  Scheduler scheduler = Scheduler.systemScheduler();

  default void putIfAbsent(K key, V value) {
    if (get(key) == null) {
      put(key, value);
    }
  }

  /**
   * Load-if-absent: return what is cached under {@code key}, otherwise compute it, store it and
   * return it. The mapping function must NOT run on a hit — every caller in the codebase is a pure
   * loader (read a page, rebuild a name dictionary), so invoking it on a hit turns a cache into a
   * recompute-and-overwrite that keeps the value alive but never saves the work it exists to save.
   *
   * <p>
   * Implementations backed by a {@link ConcurrentMap} override this to compute under the key's bin
   * lock, so concurrent misses on one key load once. This default cannot: it deliberately does NOT go
   * through {@link #asMap()}, whose own default throws, which used to make this method unusable on
   * every implementation that keeps its entries somewhere other than a concurrent map
   * ({@code LRUCache}, {@code EmptyCache}). Two threads racing a miss here may both load; for a cache
   * that is waste, not incorrectness.
   */
  default V get(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) {
    final V hit = get(key);
    if (hit != null) {
      return hit;
    }
    final V computed = mappingFunction.apply(key, null);
    if (computed != null) {
      put(key, computed);
    }
    return computed;
  }

  /**
   * Clearing the cache. That is removing all elements.
   */
  void clear();

  /**
   * Getting a value related to a given key.
   *
   * @param key the key for the requested {@link PageContainer}
   * @return {@link PageContainer} instance related to this key
   */
  V get(K key);

  /**
   * Lookup a value by key, accepting any object with compatible hashCode/equals. This follows the
   * Java Collections pattern where Map.get() accepts Object, enabling zero-allocation lookups with
   * mutable lookup key objects.
   * <p>
   * The default implementation casts to K and delegates to {@link #get(Object)}. Implementations
   * backed by Caffeine or ConcurrentHashMap should override to directly call getIfPresent(Object) or
   * get(Object) for efficiency.
   *
   * @param key the key for lookup (must have compatible hashCode/equals with K)
   * @return the value, or null if not present
   */
  @SuppressWarnings("unchecked")
  default V lookup(Object key) {
    return get((K) key);
  }

  /**
   * Putting a key/value into the cache.
   *
   * @param key for putting the page in the cache
   * @param value should be putted in the cache as well
   */
  void put(K key, V value);

  /**
   * Put all entries from a map into the cache.
   *
   * @param map map with entries to put into the cache
   */
  void putAll(Map<? extends K, ? extends V> map);

  /**
   * Save all entries of this cache in the secondary cache without removing them.
   */
  void toSecondCache();

  /**
   * Return a concurrent observational view when this implementation has one.
   *
   * <p>
   * Mutation support is implementation-specific: lifecycle-owning caches may return an unmodifiable
   * view so callers cannot bypass retirement, accounting, or synchronization. Callers must use the
   * cache's mutation methods rather than assume this map supports optional mutating operations.
   * </p>
   *
   * @return the implementation's live concurrent view
   * @throws UnsupportedOperationException if this cache has no map view
   */
  default ConcurrentMap<K, V> asMap() {
    throw new UnsupportedOperationException();
  }

  /**
   * Test whether the given value instance is currently held by this cache.
   *
   * <p>
   * Membership is by reference identity for value types that do not override {@code equals} (e.g.
   * {@link io.sirix.page.HOTLeafPage}). Used by transaction-teardown code to avoid releasing off-heap
   * memory of a page that is still owned (and shared) by a buffer cache.
   * </p>
   *
   * @param value the value instance to look for (may be {@code null}, in which case {@code false} is
   *        returned)
   * @return {@code true} if the cache currently holds this exact instance
   */
  default boolean containsPage(V value) {
    if (value == null) {
      return false;
    }
    return asMap().containsValue(value);
  }

  /**
   * Remove the given value instance from this cache by reference identity, without releasing its
   * resources — the caller retains ownership.
   *
   * <p>
   * Used to take a transaction-private page out of the shared cache so that background
   * ({@code ClockSweeper}) and pressure-driven eviction cannot reclaim its off-heap memory while the
   * page is still needed (e.g. a dirty {@link io.sirix.page.HOTLeafPage} owned by the
   * transaction-intent log until commit). The default implementation is a no-op; caches that hold
   * evictable, instance-identified pages override it.
   * </p>
   *
   * @param value the value instance to remove (no-op if {@code null} or not present)
   */
  default void removePage(V value) {
    // No-op by default — overridden by caches with instance-granular removal.
  }

  /**
   * Get all entries corresponding to the keys.
   *
   * @param keys {@link Iterable} of keys
   * @return {@link Map} instance with corresponding values
   */
  Map<K, V> getAll(Iterable<? extends K> keys);

  /**
   * Remove key from storage.
   *
   * @param key key to remove
   */
  void remove(K key);

  /**
   * Remove {@code key} and return whatever mapping was actually removed.
   *
   * <p>
   * Atomic where the implementation can be: a separate {@code get} then {@code remove} is a race, and
   * for page caches a lost race is a leak — the caller closes the page it saw while a different page,
   * inserted in between, is evicted with no owner and no close. The default is the racy two-step for
   * caches that hold no off-heap frames.
   * </p>
   *
   * @param key the key to remove
   * @return the removed value, or {@code null} if there was no mapping
   */
  default V removeAndGet(K key) {
    final V previous = get(key);
    remove(key);
    return previous;
  }

  /**
   * Force synchronous completion of pending maintenance operations. For Caffeine caches, this
   * processes the async removal listener queue. Critical for preventing race conditions when pages
   * are removed from cache and immediately closed by TIL.
   */
  default void cleanUp() {
    // Default: no-op for caches that don't need it
  }

  /**
   * Get a page and atomically acquire a guard on it (if V is KeyValueLeafPage). This prevents the
   * race where ClockSweeper evicts a page between cache lookup and guard acquisition.
   * <p>
   * Default implementation assumes V is KeyValueLeafPage and uses asMap().compute() for atomicity.
   * Implementations can override for better performance.
   *
   * @param key the page reference key
   * @return page with guard already acquired, or null if not in cache or closed
   * @throws UnsupportedOperationException if V is not KeyValueLeafPage
   */
  default V getAndGuard(K key) {
    try {
      return asMap().compute(key, (k, existingValue) -> {
        if (existingValue != null) {
          KeyValueLeafPage page = (KeyValueLeafPage) existingValue;
          // ATOMIC: mark accessed AND acquire guard while holding map lock for this key.
          // acquireGuard() subsumes the isClosed() check and closes its race: it also returns false
          // WITHOUT incrementing on an ORPHANED page, so an isClosed()-only test would hand back a
          // page the caller believes is guarded, and the caller's release would then take someone
          // else's guard. Never return a page whose guard was not actually acquired.
          if (page.acquireGuard()) {
            page.markAccessed();
            return existingValue;
          }
        }
        // Not in cache, or the mapping is dead (closed/orphaned) — drop it and report a miss.
        return null;
      });
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("getAndGuard() only supports KeyValueLeafPage values", e);
    }
  }

  /**
   * Get page from cache or load via loader, atomically acquiring a guard. Prevents race between cache
   * lookup and guard acquisition. Also handles weight tracking for ShardedPageCache.
   * <p>
   * This is the preferred method for page access when a loader is available.
   *
   * @param key the page reference key
   * @param loader function to load page on cache miss (may return null)
   * @return guarded page, or null if not found and loader returns null
   * @throws UnsupportedOperationException if V is not KeyValueLeafPage
   */
  default V getOrLoadAndGuard(K key, Function<K, V> loader) {
    throw new UnsupportedOperationException("getOrLoadAndGuard() only for KeyValueLeafPage caches");
  }

  /** Close a cache, might be a file handle for persistent caches. */
  void close();
}
