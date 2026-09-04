package io.sirix.cache;

import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiFunction;

/**
 * Which dictionaries have already been warmed, so the walk runs once rather than once per query.
 *
 * <p>
 * Keyed by {@link GlobalDictionaryRecordCacheKey} with the dictionary's HEADER key in the node-key
 * slot. It lives on the buffer manager, not in a static, so that a resource deleted and recreated
 * with the same ids cannot find a surviving marker and conclude its dictionaries are warm when the
 * caches holding them were swept — a marker that outlives its data disables the warmer permanently,
 * and it fails that way only in a long-lived single-resource process, which is exactly what a
 * benchmark is.
 * </p>
 *
 * <p>
 * Bounded by count because an entry is one identity: losing one costs a redundant walk, never a
 * wrong answer.
 * </p>
 */
public final class GlobalDictionaryWarmMarkerCache implements Cache<GlobalDictionaryRecordCacheKey, Boolean> {

  /** Distinct (resource, revision, dictionary) triples remembered; far more than any live set. */
  private static final int MAX_MARKERS = 1024;

  private final com.github.benmanes.caffeine.cache.Cache<GlobalDictionaryRecordCacheKey, Boolean> cache =
      Caffeine.newBuilder().maximumSize(MAX_MARKERS).scheduler(scheduler).build();

  /**
   * Claims the right to warm {@code key}.
   *
   * @return {@code true} for the caller that should walk, {@code false} if another already has
   */
  public boolean claim(final GlobalDictionaryRecordCacheKey key) {
    return cache.asMap().putIfAbsent(key, Boolean.TRUE) == null;
  }

  @Override
  public void clear() {
    cache.invalidateAll();
  }

  @Override
  public Boolean get(final GlobalDictionaryRecordCacheKey key) {
    return cache.getIfPresent(key);
  }

  @Override
  public Boolean get(final GlobalDictionaryRecordCacheKey key,
      final BiFunction<? super GlobalDictionaryRecordCacheKey, ? super Boolean, ? extends Boolean> mappingFunction) {
    return cache.asMap().compute(key, mappingFunction);
  }

  /** No second tier: a marker is one boolean. */
  @Override
  public void toSecondCache() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Map<GlobalDictionaryRecordCacheKey, Boolean> getAll(
      final Iterable<? extends GlobalDictionaryRecordCacheKey> keys) {
    return cache.getAllPresent(keys);
  }

  @Override
  public void put(final GlobalDictionaryRecordCacheKey key, final Boolean value) {
    cache.put(key, value);
  }

  @Override
  public void putAll(final Map<? extends GlobalDictionaryRecordCacheKey, ? extends Boolean> map) {
    cache.putAll(map);
  }

  @Override
  public void remove(final GlobalDictionaryRecordCacheKey key) {
    cache.invalidate(key);
  }

  @Override
  public ConcurrentMap<GlobalDictionaryRecordCacheKey, Boolean> asMap() {
    return cache.asMap();
  }

  @Override
  public void close() {
    cache.invalidateAll();
  }
}
