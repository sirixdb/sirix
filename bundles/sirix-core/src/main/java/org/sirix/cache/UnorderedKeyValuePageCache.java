package org.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.sirix.page.interfaces.Page;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class UnorderedKeyValuePageCache implements Cache<IndexLogKey, Page> {

  private final com.github.benmanes.caffeine.cache.Cache<IndexLogKey, Page> pageCache;

  public UnorderedKeyValuePageCache() {
    pageCache = Caffeine.newBuilder()
                        .maximumSize(1_000)
                        .expireAfterWrite(10, TimeUnit.SECONDS)
                        .expireAfterAccess(10, TimeUnit.SECONDS)
                        .build();
  }

  @Override
  public void clear() {
    pageCache.invalidateAll();
  }

  @Override
  public Page get(IndexLogKey key) {
    return pageCache.getIfPresent(key);
  }

  @Override
  public void put(IndexLogKey key, @Nonnull Page value) {
    pageCache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends IndexLogKey, ? extends Page> map) {
    pageCache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<IndexLogKey, Page> getAll(Iterable<? extends IndexLogKey> keys) {
    return pageCache.getAllPresent(keys);
  }

  @Override
  public void remove(IndexLogKey key) {
    pageCache.invalidate(key);
  }

  @Override
  public void close() {
  }
}
