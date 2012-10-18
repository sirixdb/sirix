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

import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Interface for all upcoming cache implementations. Can be a weak one, a
 * LRU-based one or a persistent. However, clear, put and get must to be
 * provided. Instances of this class are used with {@code IReadTransactions} as
 * well as with {@code IWriteTransactions}.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 * @param K
 *          the key
 * @param V
 *          the value
 */
public interface Cache<K, V> {
	/**
	 * Clearing the cache. That is removing all elements.
	 */
	void clear();

	/**
	 * Getting a value related to a given key.
	 * 
	 * @param key
	 *          the key for the requested {@link NodePageContainer}
	 * @return {@link NodePageContainer} instance related to this key
	 */
	V get(@Nonnull K key);

	/**
	 * Putting an key/value into the cache.
	 * 
	 * @param key
	 *          for putting the page in the cache
	 * @param value
	 *          should be putted in the cache as well
	 */
	void put(@Nonnull K key, @Nonnull V value);

	/**
	 * Put all entries from a map into the cache.
	 * 
	 * @param map
	 *          map with entries to put into the cache
	 */
	void putAll(@Nonnull Map<K, V> map);

	/**
	 * Save all entries of this cache in the secondary cache without removing
	 * them.
	 */
	void toSecondCache();

	/**
	 * Get all entries corresponding to the keys.
	 * 
	 * @param keys
	 *          {@link Iterable} of keys
	 * @return {@link ImmutableMap} instance with corresponding values
	 */
	ImmutableMap<K, V> getAll(@Nonnull Iterable<? extends K> keys);

	/**
	 * Remove key from storage.
	 * 
	 * @param key
	 *          key to remove
	 */
	void remove(@Nonnull K key);

	/** Close a cache, might be a file handle for persistent caches. */
	void close();
}
