package org.sirix.cache;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.annotation.Nullable;

import org.sirix.page.PageReference;
import org.sirix.settings.Constants;

import com.google.common.base.MoreObjects;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;

public final class TransactionIntentLog implements Cache<PageReference, PageContainer> {
	/**
	 * Capacity of the cache. Number of stored pages.
	 */
	private static final int CACHE_CAPACITY = 1500;

	/**
	 * The collection to hold the maps.
	 */
	private final Map<PageReference, PageContainer> mMap;

	/**
	 * Maps in-memory key to persistent key and vice versa.
	 */
	private final BiMap<Integer, Long> mMapToPersistentLogKey;

	/**
	 * The reference to the second cache.
	 */
	private final PersistentFileCache mSecondCache;

	/**
	 * The log key.
	 */
	private int mLogKey;

	/**
	 * Creates a new LRU cache.
	 *
	 * @param secondCache the reference to the second {@link Cache} where the data is stored when it
	 *        gets removed from the first one.
	 */
	public TransactionIntentLog(final PersistentFileCache secondCache) {
		// Assertion instead of checkNotNull(...).
		assert secondCache != null;
		mLogKey = 0;
		mSecondCache = secondCache;
		mMapToPersistentLogKey = HashBiMap.create();
		mMap = new LinkedHashMap<PageReference, PageContainer>(CACHE_CAPACITY) {
			private static final long serialVersionUID = 1;

			@Override
			protected boolean removeEldestEntry(
					final @Nullable Map.Entry<PageReference, PageContainer> eldest) {
				boolean returnVal = false;
				if (size() > CACHE_CAPACITY) {
					if (eldest != null) {
						final PageReference key = eldest.getKey();
						assert key.getLogKey() != Constants.NULL_ID_INT;
						final PageContainer value = eldest.getValue();
						if (key != null && value != null) {
							mSecondCache.put(key, value);
							mMapToPersistentLogKey.put(key.getLogKey(), key.getPersistentLogKey());
						}
					}
					returnVal = true;
				}
				return returnVal;
			}
		};
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
	public PageContainer get(final PageReference key) {
		PageContainer value = mMap.get(key);
		if (value == null) {
			if (key.getLogKey() != Constants.NULL_ID_INT) {
				final Long persistentKey = mMapToPersistentLogKey.get(key.getLogKey());
				if (persistentKey != null)
					key.setPersistentLogKey(persistentKey);
			}
			value = mSecondCache.get(key);
			if (value != null && !PageContainer.emptyInstance().equals(value)) {
				key.setPersistentLogKey(Constants.NULL_ID_LONG);
				put(key, value);
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
	public void put(final PageReference key, final PageContainer value) {
		key.setKey(Constants.NULL_ID_LONG);
		key.setLogKey(mLogKey++);
		mMap.put(key, value);
	}

	/**
	 * Clears the cache.
	 */
	@Override
	public void clear() {
		mLogKey = 0;
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
	 * Returns a {@code Collection} that contains a copy of all cache entries.
	 *
	 * @return a {@code Collection} with a copy of the cache content
	 */
	public Collection<Map.Entry<? super PageReference, ? super PageContainer>> getAll() {
		return new ArrayList<Map.Entry<? super PageReference, ? super PageContainer>>(mMap.entrySet());
	}

	@Override
	public String toString() {
		return MoreObjects.toStringHelper(this).add("First Cache", mMap)
				.add("Second Cache", mSecondCache).toString();
	}

	@Override
	public ImmutableMap<PageReference, PageContainer> getAll(
			final Iterable<? extends PageReference> keys) {
		final ImmutableMap.Builder<PageReference, PageContainer> builder = new ImmutableMap.Builder<>();
		for (final PageReference key : keys) {
			if (mMap.get(key) != null) {
				builder.put(key, mMap.get(key));
			}
		}
		return builder.build();
	}

	@Override
	public void putAll(final Map<? extends PageReference, ? extends PageContainer> map) {
		mMap.putAll(checkNotNull(map));
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
	public Map<PageReference, PageContainer> getMap() {
		return Collections.unmodifiableMap(mMap);
	}

	@Override
	public void remove(final PageReference key) {
		mMap.remove(key);
		if (mSecondCache.get(key) != null) {
			mSecondCache.remove(key);
		}
	}

	@Override
	public void close() {
		mMap.clear();
		mSecondCache.close();
	}

}
