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

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, Page> cache;

  public PageCache(final int maxSize) {
    RemovalListener<PageReference, Page> removalListener = (PageReference key, Page value, RemovalCause cause) -> {
      key.setPage(null);
    };

    cache = Caffeine.newBuilder()
                    .initialCapacity(maxSize)
                    .maximumSize(maxSize)
                    .scheduler(scheduler)
                    .removalListener(removalListener)
                    .build();
  }

  @Override
  public void putIfAbsent(PageReference key, Page value) {
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public Page get(PageReference key, Function<? super PageReference, ? extends @PolyNull Page> mappingFunction) {
    return cache.get(key, mappingFunction);
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public Page get(PageReference key) {
    var page = cache.getIfPresent(key);
    return page;
  }

  @Override
  public void put(PageReference key, Page value) {
    if (!(value instanceof RevisionRootPage) && !(value instanceof PathSummaryPage) && !(value instanceof PathPage)
        && !(value instanceof CASPage) && !(value instanceof NamePage)) {
      assert key.getKey() != Constants.NULL_ID_LONG;
      cache.put(key, value);
    }
  }

  @Override
  public void putAll(Map<? extends PageReference, ? extends Page> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<PageReference, Page> getAll(Iterable<? extends PageReference> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void remove(PageReference key) {
    cache.invalidate(key);
  }

  @Override
  public void close() {
  }
}
