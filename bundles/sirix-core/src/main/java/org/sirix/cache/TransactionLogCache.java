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

import java.io.File;
import java.util.Map;

import javax.annotation.Nonnegative;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.page.interfaces.KeyValuePage;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;

/**
 * Thread safe transaction-log for storing all upcoming nodes in either the ram
 * cache or a persistent second cache.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class TransactionLogCache<T extends KeyValuePage<?, ?>> implements
		Cache<Long, RecordPageContainer<T>> {

	/** RAM-Based first cache. */
	private final LRUCache<Long, RecordPageContainer<T>> mFirstCache;

	/** Persistend second cache. */
	private final BerkeleyPersistenceCache<T> mSecondCache;

	/**
	 * Constructor including the {@link DatabaseConfiguration} for persistent
	 * storage.
	 * 
	 * @param file
	 *          the config for having a storage-place
	 * @param revision
	 *          revision number
	 * @param logType
	 *          type of log
	 * @param pageReadTrx
	 *          page reading transaction
	 * @throws SirixIOException
	 *           if a database error occurs
	 */
	public TransactionLogCache(final File file,
			final @Nonnegative int revision, final String logType,
			final PageReadTrx pageReadTrx) throws SirixIOException {
		mSecondCache = new BerkeleyPersistenceCache<>(file, revision, logType,
				pageReadTrx);
		mFirstCache = new LRUCache<>(mSecondCache);
	}

	@Override
	public void close() {
		mFirstCache.close();
	}

	@Override
	public String toString() {
		return Objects.toStringHelper(this).add("cache", mFirstCache).toString();
	}

	@Override
	public ImmutableMap<Long, RecordPageContainer<T>> getAll(
			final Iterable<? extends Long> pKeys) {
		final ImmutableMap.Builder<Long, RecordPageContainer<T>> builder = new ImmutableMap.Builder<>();
		for (final Long key : pKeys) {
			if (mFirstCache.get(key) != null) {
				builder.put(key, mFirstCache.get(key));
			}
		}
		return builder.build();
	}

	@Override
	public void clear() {
		mFirstCache.clear();
	}

	@Override
	public RecordPageContainer<T> get(final Long key) {
		@SuppressWarnings("unchecked")
		RecordPageContainer<T> container = (RecordPageContainer<T>) RecordPageContainer.EMPTY_INSTANCE;
		if (mFirstCache.get(key) != null) {
			container = mFirstCache.get(key);
		}
		return container;
	}

	@Override
	public void put(final Long key,
			final RecordPageContainer<T> value) {
		mFirstCache.put(key, value);
	}

	@Override
	public void putAll(
			final Map<? extends Long, ? extends RecordPageContainer<T>> map) {
		mFirstCache.putAll(map);
	}

	@Override
	public void toSecondCache() {
		mSecondCache.putAll(mFirstCache.getMap());
	}

	@Override
	public void remove(final Long key) {
		mFirstCache.remove(key);
		if (mSecondCache.get(key) != null) {
			mSecondCache.remove(key);
		}
	}
}
