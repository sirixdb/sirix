package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.sirix.page.*;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public final class PageCache implements Cache<PageReference, Page> {

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, Page> cache;

  public PageCache(final int maxWeight) {
    final RemovalListener<PageReference, Page> removalListener = (PageReference key, Page page, RemovalCause cause) -> {
      assert key != null;
      key.setPage(null);
      assert page != null;

      if (page instanceof KeyValueLeafPage keyValueLeafPage) {
        assert keyValueLeafPage.getPinCount() == 0 : "Page must not be pinned: " + keyValueLeafPage.getPinCount();
      }
      page.clear();
    };

    cache = Caffeine.newBuilder().maximumWeight(maxWeight).weigher((PageReference _, Page value) -> {
      if (value instanceof KeyValueLeafPage keyValueLeafPage) {
        if (keyValueLeafPage.getPinCount() > 0) {
          return 0;
        } else {
          return keyValueLeafPage.getUsedSlotsSize();
        }
      } else {
        return 1000;
      }
    }).scheduler(Scheduler.systemScheduler()).evictionListener(removalListener).executor(Runnable::run).build();
  }

  @Override
  public ConcurrentMap<PageReference, Page> asMap() {
    return cache.asMap();
  }

  @Override
  public void putIfAbsent(PageReference key, Page value) {
    assert !(value instanceof KeyValueLeafPage) || !value.isClosed();
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public Page get(PageReference key, BiFunction<? super PageReference, ? super Page, ? extends Page> mappingFunction) {
    return cache.asMap().compute(key, mappingFunction);
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
    assert !(value instanceof KeyValueLeafPage) || !value.isClosed();
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
