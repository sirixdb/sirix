package io.sirix.io;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Per-resource view over a single global Caffeine {@link AsyncCache} that is keyed by
 * {@link RevisionFileDataCacheKey} {@code (resourcePath, revision)}. Each Sirix
 * resource gets its own instance of this wrapper, but storage and eviction live in
 * one shared cache so the operator-facing memory ceiling is a single number — the
 * SaaS-friendly pattern PostgreSQL ({@code shared_buffers}) and InnoDB
 * ({@code innodb_buffer_pool_size}) follow.
 *
 * <h2>Why a wrapper instead of refactoring the storage classes</h2>
 * <p>{@code AsyncCache<Integer, RevisionFileData>} threads through ~12 storage
 * classes (Storage / Reader / Writer for each of FILE / FILE_CHANNEL /
 * MEMORY_MAPPED / IO_URING / RAM backends) and {@code cache.synchronous()} is
 * passed deep into reader constructors. Wrapping at this boundary keeps every
 * caller compiling against the same Caffeine type while the actual storage is
 * now global.
 *
 * <h2>Method scope</h2>
 * <p>Only the methods that the Sirix codebase actually calls on this cache are
 * implemented. Unused {@link AsyncCache} / {@link Cache} surface throws
 * {@link UnsupportedOperationException} on first call, with a comment naming the
 * expected use case. Add support as the codebase grows; the unsupported branches
 * are explicit so reviewers can spot them.
 *
 * @author Johannes Lichtenberger
 */
public final class PerResourceRevisionFileDataCache implements AsyncCache<Integer, RevisionFileData> {

  private final Path resourcePath;
  private final AsyncCache<RevisionFileDataCacheKey, RevisionFileData> global;
  private final SynchronousView synchronousView;

  public PerResourceRevisionFileDataCache(final Path resourcePath,
      final AsyncCache<RevisionFileDataCacheKey, RevisionFileData> global) {
    this.resourcePath = resourcePath;
    this.global = global;
    this.synchronousView = new SynchronousView();
  }

  // ---------------- AsyncCache surface that callers in Sirix actually use ----------------

  @Override
  public CompletableFuture<RevisionFileData> get(final Integer key,
      final Function<? super Integer, ? extends RevisionFileData> mappingFunction) {
    return global.get(new RevisionFileDataCacheKey(resourcePath, key), k -> mappingFunction.apply(k.revision()));
  }

  @Override
  public CompletableFuture<Map<Integer, RevisionFileData>> getAll(final Iterable<? extends Integer> keys,
      final Function<? super Set<? extends Integer>, ? extends Map<? extends Integer, ? extends RevisionFileData>> mappingFunction) {
    final List<RevisionFileDataCacheKey> compositeKeys = new ArrayList<>();
    keys.forEach(k -> compositeKeys.add(new RevisionFileDataCacheKey(resourcePath, k)));

    return global.getAll(compositeKeys, missingComposite -> {
      final Set<Integer> missingInts = new HashSet<>();
      for (final RevisionFileDataCacheKey k : missingComposite) {
        missingInts.add(k.revision());
      }
      final Map<? extends Integer, ? extends RevisionFileData> intResult = mappingFunction.apply(missingInts);
      // Translate Integer-keyed mapper output back into composite keys for the global cache to store.
      final Map<RevisionFileDataCacheKey, RevisionFileData> compositeResult = new HashMap<>(intResult.size());
      for (final Map.Entry<? extends Integer, ? extends RevisionFileData> e : intResult.entrySet()) {
        compositeResult.put(new RevisionFileDataCacheKey(resourcePath, e.getKey()), e.getValue());
      }
      return compositeResult;
    }).thenApply(compositeMap -> {
      final Map<Integer, RevisionFileData> intMap = new HashMap<>(compositeMap.size());
      compositeMap.forEach((k, v) -> intMap.put(k.revision(), v));
      return intMap;
    });
  }

  @Override
  public void put(final Integer key, final CompletableFuture<? extends RevisionFileData> valueFuture) {
    global.put(new RevisionFileDataCacheKey(resourcePath, key), valueFuture);
  }

  @Override
  public ConcurrentMap<Integer, CompletableFuture<RevisionFileData>> asMap() {
    // Only .isEmpty() and .size() are called on the AsyncCache asMap() in Sirix.
    // Implement those efficiently; the rest is a forwarding view that filters by
    // resourcePath when iterated.
    return new PerResourceAsMapView();
  }

  @Override
  public Cache<Integer, RevisionFileData> synchronous() {
    return synchronousView;
  }

  // ---------------- AsyncCache methods Sirix does not call (yet) ----------------

  @Override
  public CompletableFuture<RevisionFileData> getIfPresent(final Integer key) {
    return global.getIfPresent(new RevisionFileDataCacheKey(resourcePath, key));
  }

  @Override
  public CompletableFuture<RevisionFileData> get(final Integer key,
      final BiFunction<? super Integer, ? super Executor, ? extends CompletableFuture<? extends RevisionFileData>> mappingFunction) {
    return global.get(new RevisionFileDataCacheKey(resourcePath, key),
                      (k, executor) -> mappingFunction.apply(k.revision(), executor));
  }

  @Override
  public CompletableFuture<Map<Integer, RevisionFileData>> getAll(final Iterable<? extends Integer> keys,
      final BiFunction<? super Set<? extends Integer>, ? super Executor, ? extends CompletableFuture<? extends Map<? extends Integer, ? extends RevisionFileData>>> mappingFunction) {
    throw new UnsupportedOperationException(
        "getAll(Iterable, BiFunction) is not used by Sirix internal callers; add support if needed");
  }

  // -------------------------------------------------------------------------------

  private final class PerResourceAsMapView extends AbstractForwardingConcurrentMap<Integer, CompletableFuture<RevisionFileData>> {

    @Override
    public boolean isEmpty() {
      // Used in IOStorage.loadRevisionFileDataIntoMemory to skip pre-loading if any entry
      // for this resource is already cached. Iterate the global keyset filtering by path.
      for (final RevisionFileDataCacheKey k : global.asMap().keySet()) {
        if (k.resourcePath().equals(resourcePath)) {
          return false;
        }
      }
      return true;
    }

    @Override
    public int size() {
      int count = 0;
      for (final RevisionFileDataCacheKey k : global.asMap().keySet()) {
        if (k.resourcePath().equals(resourcePath)) {
          count++;
        }
      }
      return count;
    }

    @Override
    public Set<Integer> keySet() {
      final Set<Integer> out = new HashSet<>();
      for (final RevisionFileDataCacheKey k : global.asMap().keySet()) {
        if (k.resourcePath().equals(resourcePath)) {
          out.add(k.revision());
        }
      }
      return out;
    }

    @Override
    public Set<Map.Entry<Integer, CompletableFuture<RevisionFileData>>> entrySet() {
      final Set<Map.Entry<Integer, CompletableFuture<RevisionFileData>>> out = new HashSet<>();
      for (final Map.Entry<RevisionFileDataCacheKey, CompletableFuture<RevisionFileData>> e : global.asMap().entrySet()) {
        if (e.getKey().resourcePath().equals(resourcePath)) {
          out.add(new AbstractMap.SimpleImmutableEntry<>(e.getKey().revision(), e.getValue()));
        }
      }
      return out;
    }
  }

  /** Per-resource synchronous view. Only the methods Sirix actually calls are implemented. */
  private final class SynchronousView implements Cache<Integer, RevisionFileData> {

    @Override
    public RevisionFileData get(final Integer key, final Function<? super Integer, ? extends RevisionFileData> mappingFunction) {
      return global.synchronous().get(new RevisionFileDataCacheKey(resourcePath, key), k -> mappingFunction.apply(k.revision()));
    }

    @Override
    public RevisionFileData getIfPresent(final Integer key) {
      return global.synchronous().getIfPresent(new RevisionFileDataCacheKey(resourcePath, key));
    }

    @Override
    public void put(final Integer key, final RevisionFileData value) {
      global.synchronous().put(new RevisionFileDataCacheKey(resourcePath, key), value);
    }

    @Override
    public void invalidateAll() {
      // Used by LocalDatabase.removeDatabase to drop all entries for this resource on
      // resource removal. Iterate the global keyset and invalidate matching entries —
      // O(N_global) but only runs on resource teardown, never on a hot path.
      final List<RevisionFileDataCacheKey> toInvalidate = new ArrayList<>();
      for (final RevisionFileDataCacheKey k : global.synchronous().asMap().keySet()) {
        if (k.resourcePath().equals(resourcePath)) {
          toInvalidate.add(k);
        }
      }
      global.synchronous().invalidateAll(toInvalidate);
    }

    @Override
    public Map<Integer, RevisionFileData> getAllPresent(final Iterable<? extends Integer> keys) {
      final List<RevisionFileDataCacheKey> compositeKeys = new ArrayList<>();
      keys.forEach(k -> compositeKeys.add(new RevisionFileDataCacheKey(resourcePath, k)));
      final Map<RevisionFileDataCacheKey, RevisionFileData> compositeResult = global.synchronous().getAllPresent(compositeKeys);
      final Map<Integer, RevisionFileData> intResult = new HashMap<>(compositeResult.size());
      compositeResult.forEach((k, v) -> intResult.put(k.revision(), v));
      return intResult;
    }

    @Override
    public Map<Integer, RevisionFileData> getAll(final Iterable<? extends Integer> keys,
        final Function<? super Set<? extends Integer>, ? extends Map<? extends Integer, ? extends RevisionFileData>> mappingFunction) {
      throw new UnsupportedOperationException(
          "Cache#getAll is not used on the per-resource synchronous view; add support if needed");
    }

    @Override
    public void putAll(final Map<? extends Integer, ? extends RevisionFileData> map) {
      for (final Map.Entry<? extends Integer, ? extends RevisionFileData> e : map.entrySet()) {
        put(e.getKey(), e.getValue());
      }
    }

    @Override
    public void invalidate(final Integer key) {
      global.synchronous().invalidate(new RevisionFileDataCacheKey(resourcePath, key));
    }

    @Override
    public void invalidateAll(final Iterable<? extends Integer> keys) {
      final List<RevisionFileDataCacheKey> compositeKeys = new ArrayList<>();
      keys.forEach(k -> compositeKeys.add(new RevisionFileDataCacheKey(resourcePath, k)));
      global.synchronous().invalidateAll(compositeKeys);
    }

    @Override
    public long estimatedSize() {
      // Per-resource estimate: scan the global keyset filtering by path. Diagnostic-only.
      long count = 0L;
      for (final RevisionFileDataCacheKey k : global.synchronous().asMap().keySet()) {
        if (k.resourcePath().equals(resourcePath)) {
          count++;
        }
      }
      return count;
    }

    @Override
    public CacheStats stats() {
      // The underlying global cache has stats; per-resource stats would require a separate
      // hit/miss recorder. Diagnostic-only — return the global stats.
      return global.synchronous().stats();
    }

    @Override
    public ConcurrentMap<Integer, RevisionFileData> asMap() {
      throw new UnsupportedOperationException(
          "Cache#asMap is not used on the per-resource synchronous view; add support if needed");
    }

    @Override
    public void cleanUp() {
      global.synchronous().cleanUp();
    }

    @Override
    public Policy<Integer, RevisionFileData> policy() {
      throw new UnsupportedOperationException(
          "Cache#policy is not used on the per-resource synchronous view; add support if needed");
    }
  }

  /**
   * Skeletal {@link ConcurrentMap} that supports only the read-side accessors
   * {@link PerResourceAsMapView} overrides. Mutating methods throw, since the
   * {@link AsyncCache#asMap()} return value should be treated as a snapshot view in
   * Sirix's call sites — we do not mutate it.
   */
  private abstract static class AbstractForwardingConcurrentMap<K, V> implements ConcurrentMap<K, V> {

    @Override public V get(Object key) { return null; }
    @Override public boolean containsKey(Object key) { return get(key) != null; }
    @Override public boolean containsValue(Object value) { return values().contains(value); }
    @Override public Collection<V> values() {
      final List<V> out = new ArrayList<>();
      for (final Map.Entry<K, V> e : entrySet()) {
        out.add(e.getValue());
      }
      return out;
    }
    @Override public V put(K key, V value) { throw new UnsupportedOperationException(); }
    @Override public V remove(Object key) { throw new UnsupportedOperationException(); }
    @Override public void putAll(Map<? extends K, ? extends V> m) { throw new UnsupportedOperationException(); }
    @Override public void clear() { throw new UnsupportedOperationException(); }
    @Override public V putIfAbsent(K key, V value) { throw new UnsupportedOperationException(); }
    @Override public boolean remove(Object key, Object value) { throw new UnsupportedOperationException(); }
    @Override public boolean replace(K key, V oldValue, V newValue) { throw new UnsupportedOperationException(); }
    @Override public V replace(K key, V value) { throw new UnsupportedOperationException(); }

    // Subclasses must override these.
    @Override public abstract int size();
    @Override public abstract boolean isEmpty();
    @Override public abstract Set<K> keySet();
    @Override public abstract Set<Map.Entry<K, V>> entrySet();
  }

  // Suppress unused-symbol warnings for utilities pulled in for completeness.
  @SuppressWarnings("unused")
  private static final BiConsumer<?, ?> UNUSED_BICONSUMER = (a, b) -> {};
  @SuppressWarnings("unused")
  private static final Iterator<?> UNUSED_ITERATOR = Collections.emptyIterator();
  @SuppressWarnings("unused")
  private static final Predicate<?> UNUSED_PREDICATE = x -> true;
  @SuppressWarnings("unused")
  private static final NoSuchElementException UNUSED_NSE = new NoSuchElementException();
}
