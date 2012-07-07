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

import com.google.common.collect.ImmutableMap;

import javax.annotation.Nonnull;

import org.sirix.utils.FastWeakHashMap;

/**
 * Simple RAM implementation with the help of a {@link FastWeakHashMap}.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class RAMCache<K, V> implements ICache<K, V> {

  /**
   * Local instance.
   */
  private final FastWeakHashMap<K, V> mMap;

  /**
   * Simple constructor.
   */
  public RAMCache() {
    mMap = new FastWeakHashMap<>();
  }

  @Override
  public void clear() {
    mMap.clear();
  }

  @Override
  public V get(@Nonnull final K pKey) {
    return mMap.get(pKey);
  }

  @Override
  public void put(@Nonnull final K pKey, @Nonnull final V pPage) {
    mMap.put(pKey, pPage);
  }

  @Override
  public String toString() {
    return mMap.toString();
  }

  @Override
  public ImmutableMap<K, V> getAll(@Nonnull final Iterable<? extends K> pKeys) {
    final ImmutableMap.Builder<K, V> builder = new ImmutableMap.Builder<>();
    for (final K key : pKeys) {
      if (mMap.get(key) != null) {
        builder.put(key, mMap.get(key));
      }
    }
    return builder.build();
  }

}
