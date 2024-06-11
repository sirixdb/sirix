package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class RecordPageCache implements Cache<PageReference, KeyValueLeafPage> {

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, KeyValueLeafPage> pageCache;

  public RecordPageCache(final int maxSize) {
    final RemovalListener<PageReference, KeyValueLeafPage> removalListener =
        (PageReference key, KeyValueLeafPage _, RemovalCause _) -> {
          key.setPage(null);
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
  public KeyValueLeafPage get(PageReference key) {
    var keyValueLeafPage = pageCache.getIfPresent(key);

    //    if (keyValueLeafPage != null) {
    //      keyValueLeafPage = new KeyValueLeafPage(keyValueLeafPage);
    //    }

    return keyValueLeafPage;
  }

  @Override
  public void put(PageReference key, @NonNull KeyValueLeafPage value) {
    pageCache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends PageReference, ? extends KeyValueLeafPage> map) {
    pageCache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<PageReference, KeyValueLeafPage> getAll(Iterable<? extends PageReference> keys) {
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
