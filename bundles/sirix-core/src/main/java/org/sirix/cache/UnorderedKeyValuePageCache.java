package org.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.sirix.page.interfaces.Page;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public final class UnorderedKeyValuePageCache implements Cache<IndexLogKey, Page> {

  private final com.github.benmanes.caffeine.cache.Cache<IndexLogKey, Page> mPageCache;

  public UnorderedKeyValuePageCache() {
    mPageCache = Caffeine.newBuilder()
                         .maximumSize(1_000)
                         .expireAfterWrite(20, TimeUnit.SECONDS)
                         .expireAfterAccess(20, TimeUnit.SECONDS)
                         .build();
  }

  @Override
  public void clear() {
    mPageCache.invalidateAll();
  }

  @Override
  public Page get(IndexLogKey key) {
    return mPageCache.getIfPresent(key);
  }

  @Override
  public void put(IndexLogKey key, @Nonnull Page value) {
    mPageCache.put(key, value);
  }

  @Override
  public void putAll(Map<? extends IndexLogKey, ? extends Page> map) {
    mPageCache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<IndexLogKey, Page> getAll(Iterable<? extends IndexLogKey> keys) {
    return mPageCache.getAllPresent(keys);
  }

  @Override
  public void remove(IndexLogKey key) {
    mPageCache.invalidate(key);
  }

  @Override
  public void close() {
  }
}
