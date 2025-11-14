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
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * Interface for all upcoming cache implementations. Can be a weak one, a LRU-based one or a
 * persistent. However, clear, put and get must to be provided. Instances of this class are used
 * with {@code PageReadTrx} as well as with {@code PageWriteTrx}.
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

  default V get(K key, BiFunction<? super K, ? super V, ? extends V> mappingFunction) {
    V value = get(key);
    if (value == null) {
      value = asMap().compute(key, mappingFunction);
      if (value != null) {
        put(key, value);
      }
    }
    return value;
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
   * Putting a key/value into the cache.
   *
   * @param key for putting the page in the cache
   * @param value should be putted in the cache as well
   */
  void put(K key, @NonNull V value);

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

  default ConcurrentMap<K, V> asMap() {
    throw new UnsupportedOperationException();
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
   * Force synchronous completion of pending maintenance operations.
   * For Caffeine caches, this processes the async removal listener queue.
   * Critical for preventing race conditions when pages are removed from cache
   * and immediately closed by TIL.
   */
  default void cleanUp() {
    // Default: no-op for caches that don't need it
  }

  /**
   * Get a page and atomically acquire a guard on it (if V is KeyValueLeafPage).
   * This prevents the race where ClockSweeper evicts a page between
   * cache lookup and guard acquisition.
   * <p>
   * Default implementation assumes V is KeyValueLeafPage and uses asMap().compute()
   * for atomicity. Implementations can override for better performance.
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
          if (!page.isClosed()) {
            // ATOMIC: mark accessed AND acquire guard while holding map lock for this key
            page.markAccessed();
            page.acquireGuard();
            return existingValue;
          }
        }
        // Not in cache or closed - return null (don't return closed page!)
        // CRITICAL: Must NOT acquire guard on closed pages and must NOT return them
        return null;
      });
    } catch (ClassCastException e) {
      throw new UnsupportedOperationException("getAndGuard() only supports KeyValueLeafPage values", e);
    }
  }

  /** Close a cache, might be a file handle for persistent caches. */
  void close();
}
