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
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.exception.SirixIOException;

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
public final class TransactionLogCache implements
		Cache<Long, NodePageContainer> {

	/** RAM-Based first cache. */
	private final LRUCache<Long, NodePageContainer> mFirstCache;

	/** Persistend second cache. */
	private final BerkeleyPersistenceCache mSecondCache;

	/** {@link ReadWriteLock} instance. */
	private final ReadWriteLock mLock = new ReentrantReadWriteLock();

	/** Shared read lock. */
	private final Lock mReadLock = mLock.readLock();

	/** Write lock. */
	private final Lock mWriteLock = mLock.writeLock();

	/**
	 * Constructor including the {@link DatabaseConfiguration} for persistent
	 * storage.
	 * 
	 * @param pPageWriteTransaction
	 *          page write transaction
	 * @param file
	 *          the config for having a storage-place
	 * @param revision
	 *          revision number
	 * @throws SirixIOException
	 *           if a database error occurs
	 */
	public TransactionLogCache(final @Nonnull File file,
			final @Nonnegative int revision, final @Nonnull String logType,
			final @Nonnull ResourceConfiguration resourceConfig)
			throws SirixIOException {
		mSecondCache = new BerkeleyPersistenceCache(file, revision, logType, resourceConfig);
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
	public ImmutableMap<Long, NodePageContainer> getAll(
			final @Nonnull Iterable<? extends Long> pKeys) {
		final ImmutableMap.Builder<Long, NodePageContainer> builder = new ImmutableMap.Builder<>();
		try {
			mReadLock.lock();
			for (final Long key : pKeys) {
				if (mFirstCache.get(key) != null) {
					builder.put(key, mFirstCache.get(key));
				}
			}
		} finally {
			mReadLock.unlock();
		}
		return builder.build();
	}

	@Override
	public void clear() {
		try {
			mWriteLock.lock();
			mFirstCache.clear();
		} finally {
			mWriteLock.unlock();
		}
	}

	@Override
	public NodePageContainer get(final @Nonnull Long pKey) {
		NodePageContainer container = NodePageContainer.EMPTY_INSTANCE;
		try {
			mReadLock.lock();
			container = mFirstCache.get(pKey);
			if (container == null) {
				container = NodePageContainer.EMPTY_INSTANCE;
			}
		} finally {
			mReadLock.unlock();
		}
		return container;
	}

	@Override
	public void put(final @Nonnull Long key,
			final @Nonnull NodePageContainer value) {
		try {
			mWriteLock.lock();
			mFirstCache.put(key, value);
		} finally {
			mWriteLock.unlock();
		}
	}

	@Override
	public void putAll(final @Nonnull Map<Long, NodePageContainer> map) {
		try {
			mWriteLock.lock();
			mFirstCache.putAll(map);
		} finally {
			mWriteLock.unlock();
		}
	}

	@Override
	public void toSecondCache() {
		try {
			mWriteLock.lock();
			mSecondCache.putAll(mFirstCache.getMap());
		} finally {
			mWriteLock.unlock();
		}
	}

	@Override
	public void remove(final @Nonnull Long key) {
		try {
			mWriteLock.lock();
			mFirstCache.remove(key);
			if (mSecondCache.get(key) != null) {
				mSecondCache.remove(key);
			}
		} finally {
			mWriteLock.unlock();
		}
	}
}
