package io.sirix.query;

import io.brackit.query.compiler.AST;
import io.sirix.query.compiler.optimizer.mesh.Mesh;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Strictly bounded, chain-owned optimized-plan LRU. The chain's compile monitor protects plan
 * lookup and translation. Expression evaluation never takes that monitor. Only detached plans and
 * immutable source hints are retained, never an expression, query context, tuple, result or
 * transaction. Prolog values are owned by Brackit's QueryExecution.
 */
final class CompiledQueryCache {
  static final int MAX_ENTRIES = 128;
  static final int MAX_QUERY_LENGTH = 65_536;

  record Key(String text, long schemaVersion, StoreBoundExecutorCache.DocumentSource hint, boolean morsel) {
    Key {
      Objects.requireNonNull(text);
    }
  }

  record Entry(OptimizedQueryPlan plan, AST parsed, AST optimized, Mesh mesh) {
  }

  private final Map<Key, Entry> entries = new LinkedHashMap<>(MAX_ENTRIES, 0.75f, true) {
    @Override
    protected boolean removeEldestEntry(final Map.Entry<Key, Entry> eldest) {
      return size() > MAX_ENTRIES;
    }
  };

  Entry get(final Key key) {
    return entries.get(key);
  }

  void put(final Key key, final Entry entry) {
    entries.put(key, entry);
  }

  void clear() {
    entries.clear();
  }

  int size() {
    return entries.size();
  }
}
