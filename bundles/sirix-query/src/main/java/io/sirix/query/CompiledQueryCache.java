package io.sirix.query;

import io.brackit.query.compiler.AST;
import io.brackit.query.module.Module;
import io.sirix.query.compiler.optimizer.mesh.Mesh;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Strictly bounded, chain-owned LRU. The chain's compile monitor protects all operations; execution
 * never takes that monitor. Only translated code and immutable source hints are retained, never a
 * query context, tuple, result or transaction. Prolog values are owned by Brackit's QueryExecution.
 */
final class CompiledQueryCache {
  static final int MAX_ENTRIES = 128;
  static final int MAX_QUERY_LENGTH = 65_536;

  record Key(String text, long schemaVersion, StoreBoundExecutorCache.DocumentSource hint, boolean morsel) {
    Key {
      Objects.requireNonNull(text);
    }
  }

  record Entry(Module module, AST parsed, AST optimized, Mesh mesh) {
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
