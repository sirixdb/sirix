package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.util.Map;
import java.util.function.Function;

public final class RecordPageCache implements Cache<PageReference, KeyValueLeafPage> {

  private final com.github.benmanes.caffeine.cache.Cache<PageReference, KeyValueLeafPage> cache;

  public RecordPageCache(final int maxSize) {
    final RemovalListener<PageReference, KeyValueLeafPage> removalListener =
        (PageReference key, KeyValueLeafPage page, RemovalCause _) -> {
          assert key != null;
          key.setPage(null);
          assert page != null;
          page.close();
        };

    cache = Caffeine.newBuilder()
                    .initialCapacity(maxSize)
                    .maximumSize(maxSize)
                    .scheduler(scheduler)
                    .removalListener(removalListener)
                    .build();
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public KeyValueLeafPage get(PageReference key) {
    var keyValueLeafPage = cache.getIfPresent(key);
    //    if (keyValueLeafPage != null) {
    //      keyValueLeafPage = new KeyValueLeafPage(keyValueLeafPage);
    //    }

    return keyValueLeafPage;
  }

  @Override
  public KeyValueLeafPage get(PageReference key,
      Function<? super PageReference, ? extends @PolyNull KeyValueLeafPage> mappingFunction) {
    return cache.get(key, mappingFunction);
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
