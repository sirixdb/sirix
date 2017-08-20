package org.sirix.cache;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.sirix.page.PageReference;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;

public final class RecordPageCache implements Cache<PageReference, PageContainer> {

	private final com.google.common.cache.Cache<PageReference, PageContainer> mPageCache;

	public RecordPageCache() {
		final RemovalListener<PageReference, PageContainer> removalListener;

		removalListener = new RemovalListener<PageReference, PageContainer>() {
			@Override
			public void onRemoval(final RemovalNotification<PageReference, PageContainer> removal) {
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
	public PageContainer get(PageReference key) {
		return mPageCache.getIfPresent(key);
	}

	@Override
	public void put(PageReference key, PageContainer value) {
		mPageCache.put(key, value);
	}

	@Override
	public void putAll(Map<? extends PageReference, ? extends PageContainer> map) {
		mPageCache.putAll(map);
	}

	@Override
	public void toSecondCache() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ImmutableMap<PageReference, PageContainer> getAll(
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
