package io.sirix.query.scan;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.index.projection.ProjectionColumnStore;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Bounded reuse of immutable numeric masks, scoped by the revision's column-store identity. */
final class NumericMaskedColumnCache {
  private static final long MAX_BYTES = Math.max(1L << 20, Math.min(64L << 20, Runtime.getRuntime().maxMemory() / 64));
  private static final Cache<ProjectionColumnStore, List<MaskedColumns.NumericSnapshot>> CACHE =
      Caffeine.newBuilder()
              .weakKeys()
              .maximumWeight(MAX_BYTES)
              .weigher(
                  (ProjectionColumnStore store, List<MaskedColumns.NumericSnapshot> snapshots) -> weight(snapshots))
              .build();

  private NumericMaskedColumnCache() {}

  static @Nullable MaskedColumns get(final ProjectionColumnStore store, final String[] fields, final long[] keys) {
    final List<MaskedColumns.NumericSnapshot> snapshots = CACHE.getIfPresent(store);
    if (snapshots != null) {
      for (final MaskedColumns.NumericSnapshot data : snapshots) {
        if (Arrays.equals(fields, data.fields()) && Arrays.equals(keys, data.keys())) {
          return data.view(store);
        }
      }
    }
    return null;
  }

  private static int weight(final List<MaskedColumns.NumericSnapshot> snapshots) {
    int bytes = 32 + 8 * snapshots.size();
    for (final MaskedColumns.NumericSnapshot snapshot : snapshots) {
      bytes += snapshot.weight();
    }
    return bytes;
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
      final MaskedColumns.NumericSnapshot snapshot = columns.numericSnapshot(fields, keys, (int) bytes);
      CACHE.asMap().compute(store, (key, existing) -> {
        final List<MaskedColumns.NumericSnapshot> snapshots = new ArrayList<>(existing == null
            ? 1
            : existing.size() + 1);
        if (existing != null) {
          for (final MaskedColumns.NumericSnapshot data : existing) {
            if (!Arrays.equals(fields, data.fields()) || !Arrays.equals(keys, data.keys())) {
              snapshots.add(data);
            }
          }
        }
        snapshots.add(snapshot);
        return List.copyOf(snapshots);
      });
    }
  }
}
