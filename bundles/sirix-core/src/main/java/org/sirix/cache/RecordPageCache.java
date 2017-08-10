package org.sirix.cache;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.sirix.page.PageReference;
import org.sirix.page.interfaces.KeyValuePage;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;

public final class RecordPageCache
		implements Cache<PageReference, RecordPageContainer<? extends KeyValuePage<?, ?>>> {

	private final com.google.common.cache.Cache<PageReference, RecordPageContainer<? extends KeyValuePage<?, ?>>> mPageCache;

	public RecordPageCache() {
		final RemovalListener<PageReference, RecordPageContainer<? extends KeyValuePage<?, ?>>> removalListener;

		removalListener =
				new RemovalListener<PageReference, RecordPageContainer<? extends KeyValuePage<?, ?>>>() {
					@Override
					public void onRemoval(
							final RemovalNotification<PageReference, RecordPageContainer<? extends KeyValuePage<?, ?>>> removal) {
						removal.getKey().setPage(null);
					}
				};

		mPageCache =
				CacheBuilder.newBuilder().maximumSize(1000).expireAfterWrite(5000, TimeUnit.SECONDS)
						.expireAfterAccess(5000, TimeUnit.SECONDS).removalListener(removalListener).build();
	}

	@Override
	public void clear() {
		mPageCache.invalidateAll();
	}

	@Override
	public RecordPageContainer<? extends KeyValuePage<?, ?>> get(PageReference key) {
		return mPageCache.getIfPresent(key);
	}

	@Override
	public void put(PageReference key, RecordPageContainer<? extends KeyValuePage<?, ?>> value) {
		mPageCache.put(key, value);
	}

	@Override
	public void putAll(
			Map<? extends PageReference, ? extends RecordPageContainer<? extends KeyValuePage<?, ?>>> map) {
		mPageCache.putAll(map);
	}

	@Override
	public void toSecondCache() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ImmutableMap<PageReference, RecordPageContainer<? extends KeyValuePage<?, ?>>> getAll(
			Iterable<? extends PageReference> keys) {
		return mPageCache.getAllPresent(keys);
	}

	@Override
	public void remove(PageReference key) {
		mPageCache.invalidate(key);
	}

	@Override
	public void close() {}
}
