package org.sirix.cache;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.Page;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

public final class PageCache implements Cache<PageReference, Page> {

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, Page> mPageCache;

  public PageCache() {
    RemovalListener<PageReference, Page> removalListener =
        (PageReference key, Page value, RemovalCause cause) -> key.setPage(null);

    mPageCache = Caffeine.newBuilder()
                         .maximumSize(5_000)
                         .expireAfterWrite(5, TimeUnit.MINUTES)
                         .expireAfterAccess(5, TimeUnit.MINUTES)
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
