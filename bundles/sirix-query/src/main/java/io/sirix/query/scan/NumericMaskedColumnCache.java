package io.sirix.query.scan;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.index.projection.ProjectionColumnStore;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

/** Bounded reuse of immutable numeric masks, scoped by the revision's column-store identity. */
final class NumericMaskedColumnCache {
  private static final long MAX_BYTES = Math.max(1L << 20, Math.min(64L << 20, Runtime.getRuntime().maxMemory() / 64));
  private static final Cache<ProjectionColumnStore, MaskedColumns.NumericSnapshot> CACHE =
      Caffeine.newBuilder()
              .weakKeys()
              .maximumWeight(MAX_BYTES)
              .weigher((ProjectionColumnStore store, MaskedColumns.NumericSnapshot data) -> data.weight())
              .build();

  private NumericMaskedColumnCache() {}

  static @Nullable MaskedColumns get(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    final MaskedColumns.NumericSnapshot data = CACHE.getIfPresent(store);
    return data != null && Arrays.equals(fields, data.fields()) && Arrays.equals(keys, data.keys())
        ? data.view(store)
        : null;
  }

  static void put(final ProjectionColumnStore store, final String[] fields, final long[] keys,
      final MaskedColumns columns) {
    long bytes = columns.numericSnapshotWeight(keys.length);
    if (bytes <= 0) {
      return;
    }
    for (final String field : fields) {
      bytes += 64L + 2L * field.length();
    }
    if (bytes > 0 && bytes <= MAX_BYTES) {
      CACHE.put(store, columns.numericSnapshot(fields, keys, (int) bytes));
    }
  }
}
