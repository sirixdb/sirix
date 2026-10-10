package io.sirix.query.function.jn.temporal;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.api.json.JsonResourceSession;
import org.jspecify.annotations.Nullable;

/** Bounded reuse of one immutable, membership-proved range per resource session. */
final class ValidTimeRangeCache {
  private static final long MAX_BYTES = Math.max(1L << 20, Math.min(16L << 20, Runtime.getRuntime().maxMemory() / 64));
  private static final Cache<JsonResourceSession, Snapshot> CACHE = Caffeine.newBuilder()
      .weakKeys()
      .maximumWeight(MAX_BYTES)
      .weigher((JsonResourceSession session, Snapshot snapshot) -> snapshot.weight())
      .build();

  private record Snapshot(int revision, long array, int index, long lower, long upper, int mode, long[] keys,
      int weight) {
    boolean matches(final int revision, final long array, final int index, final long lower, final long upper,
        final int mode) {
      return this.revision == revision && this.array == array && this.index == index && this.lower == lower
          && this.upper == upper && this.mode == mode;
    }
  }

  private ValidTimeRangeCache() {
  }

  /** Borrow immutable keys; the sequence exposes only copies to callers. */
  static long @Nullable [] get(final JsonResourceSession session, final int revision, final long array,
      final int index, final long lower, final long upper, final int mode) {
    final Snapshot snapshot = CACHE.getIfPresent(session);
    return snapshot != null && snapshot.matches(revision, array, index, lower, upper, mode)
        ? snapshot.keys()
        : null;
  }

  static void put(final JsonResourceSession session, final int revision, final long array, final int index,
      final long lower, final long upper, final int mode, final long[] keys) {
    final long bytes = 128L + 8L * keys.length;
    if (bytes <= MAX_BYTES) {
      CACHE.put(session, new Snapshot(revision, array, index, lower, upper, mode, keys.clone(), (int) bytes));
    }
  }
}
