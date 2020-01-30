package org.sirix.cache;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.sirix.page.PageReference;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;

public final class RecordPageCache implements Cache<PageReference, PageContainer> {

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, PageContainer> mPageCache;

  public RecordPageCache() {
    final RemovalListener<PageReference, PageContainer> removalListener;

    removalListener =
        (PageReference key, PageContainer value, RemovalCause cause) -> key.setPage(null);

    mPageCache = Caffeine.newBuilder()
                         .maximumSize(1_000)
                         .expireAfterWrite(20, TimeUnit.SECONDS)
                         .expireAfterAccess(20, TimeUnit.SECONDS)
                         .removalListener(removalListener)
                         .build();
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
  public Map<PageReference, PageContainer> getAll(Iterable<? extends PageReference> keys) {
    return mPageCache.getAllPresent(keys);
  }

  @Override
  public void remove(PageReference key) {
    mPageCache.invalidate(key);
  }

  @Override
  public void close() {}
}
