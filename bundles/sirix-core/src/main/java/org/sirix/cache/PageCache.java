package org.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class PageCache implements Cache<PageReference, Page> {

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, Page> mPageCache;

  public PageCache() {
    RemovalListener<PageReference, Page> removalListener =
        (PageReference key, Page value, RemovalCause cause) -> key.setPage(null);

    mPageCache = Caffeine.newBuilder()
                         .maximumSize(1000)
                         .expireAfterWrite(5000, TimeUnit.SECONDS)
                         .expireAfterAccess(5000, TimeUnit.SECONDS)
                         .removalListener(removalListener)
                         .build();
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
  public Map<PageReference, Page> getAll(Iterable<? extends PageReference> keys) {
    return mPageCache.getAllPresent(keys);
  }

  @Override
  public void remove(PageReference key) {
    mPageCache.invalidate(key);
  }

  @Override
  public void close() {}
}
