package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.sirix.page.*;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.util.Map;
import java.util.function.Function;

public final class PageCache implements Cache<PageReference, Page> {

	private final com.github.benmanes.caffeine.cache.Cache<PageReference, Page> pageCache;

	public PageCache(final int maxSize) {
		RemovalListener<PageReference, Page> removalListener = (PageReference key, Page value, RemovalCause cause) -> {
			key.setPage(null);
		};

		pageCache = Caffeine.newBuilder().initialCapacity(maxSize).maximumSize(maxSize).executor(Runnable::run)
				.scheduler(scheduler).removalListener(removalListener).build();
	}

	@Override
	public Page get(PageReference key, Function<? super PageReference, ? extends @PolyNull Page> mappingFunction) {
		return pageCache.get(key, mappingFunction);
	}

	@Override
	public void clear() {
		pageCache.invalidateAll();
	}

	@Override
	public Page get(PageReference key) {
		var page = pageCache.getIfPresent(key);
		return page;
	}

	@Override
	public void put(PageReference key, Page value) {
		if (!(value instanceof RevisionRootPage) && !(value instanceof PathSummaryPage) && !(value instanceof PathPage)
				&& !(value instanceof CASPage) && !(value instanceof NamePage)) {
			assert key.getKey() != Constants.NULL_ID_LONG;
			pageCache.put(key, value);
		}
	}

	@Override
	public void putAll(Map<? extends PageReference, ? extends Page> map) {
		pageCache.putAll(map);
	}

	@Override
	public void toSecondCache() {
		throw new UnsupportedOperationException();
	}

	@Override
	public Map<PageReference, Page> getAll(Iterable<? extends PageReference> keys) {
		return pageCache.getAllPresent(keys);
	}

	@Override
	public void remove(PageReference key) {
		pageCache.invalidate(key);
	}

	@Override
	public void close() {
	}
}
