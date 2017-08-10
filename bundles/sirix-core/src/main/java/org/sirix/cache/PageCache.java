package org.sirix.cache;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.ImmutableMap;

public final class PageCache implements Cache<PageReference, Page> {

	private final com.google.common.cache.Cache<PageReference, Page> mPageCache;

	public PageCache() {
		RemovalListener<PageReference, Page> removalListener =
				new RemovalListener<PageReference, Page>() {
					@Override
					public void onRemoval(final RemovalNotification<PageReference, Page> removal) {
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
	public Page get(PageReference key) {
		return mPageCache.getIfPresent(key);
	}

	@Override
	public void put(PageReference key, Page value) {
		mPageCache.put(key, value);
	}

	@Override
	public void putAll(Map<? extends PageReference, ? extends Page> map) {
		mPageCache.putAll(map);
	}

	@Override
	public void toSecondCache() {
		throw new UnsupportedOperationException();
	}

	@Override
	public ImmutableMap<PageReference, Page> getAll(Iterable<? extends PageReference> keys) {
		return mPageCache.getAllPresent(keys);
	}

	@Override
	public void remove(PageReference key) {
		mPageCache.invalidate(key);
	}

	@Override
	public void close() {}
}
