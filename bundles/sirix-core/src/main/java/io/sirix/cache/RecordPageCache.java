package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

public final class RecordPageCache implements Cache<PageReference, KeyValueLeafPage> {

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, KeyValueLeafPage> cache;

  public RecordPageCache(final int maxWeight) {
    final RemovalListener<PageReference, KeyValueLeafPage> removalListener =
        (PageReference key, KeyValueLeafPage page, RemovalCause cause) -> {
          assert cause.wasEvicted();
          assert key != null;
          key.setPage(null);
          assert page != null;
          assert page.getPinCount() == 0 : "Page must not be pinned: " + page.getPinCount();
          page.clear();
          KeyValueLeafPagePool.getInstance().returnPage(page);
        };

    cache = Caffeine.newBuilder()
                    .maximumWeight(maxWeight)
                    .weigher((PageReference _, KeyValueLeafPage value) -> value.getPinCount() > 0
                        ? 0
                        : value.getUsedSlotsSize())
                    .scheduler(scheduler)
                    .evictionListener(removalListener)
                    .build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public KeyValueLeafPage get(PageReference key) {
    return cache.getIfPresent(key);
  }

  @Override
  public KeyValueLeafPage get(PageReference key,
      BiFunction<? super PageReference, ? super KeyValueLeafPage, ? extends KeyValueLeafPage> mappingFunction) {
    return cache.asMap().compute(key, mappingFunction);
  }

  @Override
  public void put(PageReference key, @NonNull KeyValueLeafPage value) {
    cache.put(key, value);
  }

  @Override
  public void putIfAbsent(PageReference key, KeyValueLeafPage value) {
    cache.asMap().putIfAbsent(key, value);
  }

  @Override
  public void putAll(Map<? extends PageReference, ? extends KeyValueLeafPage> map) {
    cache.putAll(map);
  }

  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public ConcurrentMap<PageReference, KeyValueLeafPage> asMap() {
    return cache.asMap();
  }

  @Override
  public Map<PageReference, KeyValueLeafPage> getAll(Iterable<? extends PageReference> keys) {
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
