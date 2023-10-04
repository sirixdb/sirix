package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;
import io.sirix.page.interfaces.Page;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class RecordPageCache implements Cache<PageReference, Page> {

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, Page> pageCache;

  public RecordPageCache(final int maxSize) {
    final RemovalListener<PageReference, Page> removalListener =
        (PageReference key, Page value, RemovalCause cause) -> {
          key.setPage(null);
          //      if (value instanceof KeyValueLeafPage keyValueLeafPage) {
          //        keyValueLeafPage.clearPage();
          //      }
        };

    pageCache = Caffeine.newBuilder()
                        .maximumSize(maxSize)
                        .expireAfterAccess(5, TimeUnit.MINUTES)
                        .scheduler(scheduler)
                        .removalListener(removalListener)
                        .build();
  }

  @Override
  public void clear() {
    pageCache.invalidateAll();
  }

  @Override
  public Page get(PageReference key) {
    return pageCache.getIfPresent(key);
  }

  @Override
  public void put(PageReference key, @NonNull Page value) {
    pageCache.put(key, value);
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
