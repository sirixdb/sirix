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

package org.sirix.cache;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.*;

import static java.util.Objects.requireNonNull;

/**
 * An LRU cache, based on {@code LinkedHashMap}. This cache can hold an possible second cache as a
 * second layer for example for storing data in a persistent way.
 *
 * @author Sebastian Graf, University of Konstanz
 */
public final class LRUCache<K, V> implements Cache<K, V> {

  /**
   * Capacity of the cache. Number of stored pages.
   */
  static final int CACHE_CAPACITY = 1;

  /**
   * The collection to hold the maps.
   */
  private final Map<K, V> map;

  /**
   * The reference to the second cache.
   */
  private final Cache<K, V> secondCache;

  /**
   * Creates a new LRU cache.
   *
   * @param secondCache the reference to the second {@link Cache} where the data is stored when it
   *        gets removed from the first one.
   */
  public LRUCache(final Cache<K, V> secondCache) {
    // Assertion instead of requireNonNull(...).
    assert secondCache != null;
    this.secondCache = secondCache;
    map = new LinkedHashMap<>(CACHE_CAPACITY) {
      private static final long serialVersionUID = 1;

      @Override
      protected boolean removeEldestEntry(final Map.@Nullable Entry<K, V> eldest) {
        boolean returnVal = false;
        if (size() > CACHE_CAPACITY) {
          if (eldest != null) {
            final K key = eldest.getKey();
            final V value = eldest.getValue();
            if (key != null && value != null) {
              LRUCache.this.secondCache.put(key, value);
            }
          }
          returnVal = true;
        }
        return returnVal;
      }
    };
  }

  /** Constructor which initializes the cache backed by an empty second cache. */
  public LRUCache() {
    this(new EmptyCache<K, V>());
  }

  /**
   * Retrieves an entry from the cache.<br>
   * The retrieved entry becomes the MRU (most recently used) entry.
   *
   * @param key the key whose associated value is to be returned.
   * @return the value associated to this key, or {@code null} if no value with this key exists in
   *         the cache
   */
  @Override
  public V get(final K key) {
    V value = map.get(key);
    if (value == null) {
      value = secondCache.get(key);
      if (value != null) {
        map.put(key, value);
      }
    }
    return value;
  }

  /**
   *
   * Adds an entry to this cache. If the cache is full, the LRU (least recently used) entry is
   * dropped.
   *
   * @param key the key with which the specified value is to be associated
   * @param value a value to be associated with the specified key
   */
  @Override
  public void put(final K key, final V value) {
    map.put(key, value);
  }

  /**
   * Clears the cache.
   */
  @Override
  public void clear() {
    map.clear();
    secondCache.clear();
  }

  /**
   * Returns the number of used entries in the cache.
   *
   * @return the number of entries currently in the cache.
   */
  public int usedEntries() {
    return map.size();
  }

  /**
   * Returns a {@code Collection} that contains a copy of all cache entries.
   *
   * @return a {@code Collection} with a copy of the cache content
   */
  public Collection<Map.Entry<? super K, ? super V>> getAll() {
    return new ArrayList<Map.Entry<? super K, ? super V>>(map.entrySet());
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("First Cache", map)
                      .add("Second Cache", secondCache)
                      .toString();
  }

  @Override
  public ImmutableMap<K, V> getAll(final Iterable<? extends K> keys) {
    final ImmutableMap.Builder<K, V> builder = new ImmutableMap.Builder<>();
    for (final K key : keys) {
      if (map.get(key) != null) {
        builder.put(key, map.get(key));
      }
    }
    return builder.build();
  }

  @Override
  public void putAll(final Map<? extends K, ? extends V> map) {
    this.map.putAll(requireNonNull(map));
  }

  @Override
  public void toSecondCache() {
    secondCache.putAll(map);
  }

  /**
   * Get a view of the underlying map.
   *
   * @return an unmodifiable view of all entries in the cache
   */
  public Map<K, V> getMap() {
    return Collections.unmodifiableMap(map);
  }

  @Override
  public void remove(final K key) {
    map.remove(key);
    if (secondCache.get(key) != null) {
      secondCache.remove(key);
    }
  }

  @Override
  public void close() {
    map.clear();
    secondCache.close();
  }
}
