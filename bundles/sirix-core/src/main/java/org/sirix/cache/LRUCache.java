/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * An LRU cache, based on {@code LinkedHashMap}. This cache can hold an
 * possible second cache as a second layer for example for storing data in a
 * persistent way.
 * 
 * @author Sebastian Graf, University of Konstanz
 */
public final class LRUCache<K, V> implements ICache<K, V> {

  /**
   * Capacity of the cache. Number of stored pages.
   */
  static final int CACHE_CAPACITY = 10_000;

  /**
   * The collection to hold the maps.
   */
  private final Map<K, V> mMap;

  /**
   * The reference to the second cache.
   */
  private final ICache<K, V> mSecondCache;

  /**
   * Creates a new LRU cache.
   * 
   * @param pSecondCache
   *          the reference to the second {@link ICache} where the data is stored
   *          when it gets removed from the first one.
   */
  public LRUCache(@Nonnull final ICache<K, V> pSecondCache) {
    mSecondCache = checkNotNull(pSecondCache);
    mMap = new LinkedHashMap<K, V>(CACHE_CAPACITY) {
      private static final long serialVersionUID = 1;

      @Override
      protected boolean removeEldestEntry(
        @Nullable final Map.Entry<K, V> pEldest) {
        boolean returnVal = false;
        if (size() > CACHE_CAPACITY) {
          if (pEldest != null) {
            final K key = pEldest.getKey();
            final V value = pEldest.getValue();
            if (key != null && value != null) {
              mSecondCache.put(key, value);
            }
          }
          returnVal = true;
        }
        return returnVal;
      }
    };
  }

  public LRUCache() {
    this(new EmptyCache<K, V>());
  }

  /**
   * Retrieves an entry from the cache.<br>
   * The retrieved entry becomes the MRU (most recently used) entry.
   * 
   * @param pKey
   *          the key whose associated value is to be returned.
   * @return the value associated to this key, or {@code null} if no value with this
   *         key exists in the cache
   */
  @Override
  public V get(@Nonnull final K pKey) {
    V page = mMap.get(pKey);
    if (page == null) {
      page = mSecondCache.get(pKey);
    }
    return page;
  }

  /**
   * 
   * Adds an entry to this cache. If the cache is full, the LRU (least
   * recently used) entry is dropped.
   * 
   * @param pKey
   *          the key with which the specified value is to be associated
   * @param pValue
   *          a value to be associated with the specified key
   */
  @Override
  public void put(@Nonnull final K pKey, @Nonnull final V pValue) {
    mMap.put(pKey, pValue);
  }

  /**
   * Clears the cache.
   */
  @Override
  public void clear() {
    mMap.clear();
    mSecondCache.clear();
  }

  /**
   * Returns the number of used entries in the cache.
   * 
   * @return the number of entries currently in the cache.
   */
  public int usedEntries() {
    return mMap.size();
  }

  /**
   * Returns a {@code Collection} that contains a copy of all cache
   * entries.
   * 
   * @return a {@code Collection} with a copy of the cache content
   */
  public Collection<Map.Entry<K, V>> getAll() {
    return new ArrayList<>(mMap.entrySet());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("First Cache", mMap).add(
      "Second Cache", mSecondCache).toString();
  }

  @Override
  public ImmutableMap<K, V> getAll(final @Nonnull Iterable<? extends K> pKeys) {
    final ImmutableMap.Builder<K, V> builder = new ImmutableMap.Builder<>();
    for (final K key : pKeys) {
      if (mMap.get(key) != null) {
        builder.put(key, mMap.get(key));
      }
    }
    return builder.build();
  }

  @Override
  public void putAll(final @Nonnull Map<K, V> pMap) {
    mMap.putAll(checkNotNull(pMap));
  }

  @Override
  public void toSecondCache() {
    mSecondCache.putAll(mMap);
  }

  /**
   * Get a view of the underlying map.
   * 
   * @return an unmodifiable view of all entries in the cache
   */
  public Map<K, V> getMap() {
    return Collections.unmodifiableMap(mMap);
  }
  
  @Override
  public void remove(final @Nonnull K pKey) {
    mMap.remove(pKey);
    if (mSecondCache.get(pKey) != null) {
      mSecondCache.remove(pKey);
    }
  }
}
