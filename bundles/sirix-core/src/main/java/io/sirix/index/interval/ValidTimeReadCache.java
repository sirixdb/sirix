package io.sirix.index.interval;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.BufferManager;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Session-scoped memo of immutable companion posting masks.
 *
 * <p>
 * Only masks are memoized. Trie readers are deliberately NOT parked across transactions:
 * re-pointing one at the current transaction needs a forwarding {@code StorageEngineReader}, a
 * second loaded implementation of the interface every record read goes through. On a JVM that only
 * reads, the JIT otherwise binds those calls statically; loading the wrapper at the first stab
 * invalidated that assumption for the whole cold read path and cost more than the readers saved
 * (measured on cold Q4/Q9). Each walk constructs its own trie reader, two small objects bound to
 * the transaction.
 */
public final class ValidTimeReadCache {
  private static final LongAdder MASK_HITS = new LongAdder();

  public static long maskHits() {
    return MASK_HITS.sum();
  }

  private record MaskKey(int revision, int index, byte store, long fork, long endpoint, long base, long generation) {
  }

  private final Cache<MaskKey, PostingMask> masks =
      Caffeine.newBuilder()
              .maximumWeight(16L << 20)
              .weigher((MaskKey key, PostingMask mask) -> mask.cacheWeight())
              .build();

  /**
   * Only immutable companion masks are memoized; no transaction or mutable candidate set is retained.
   */
  public static PostingMask mask(final HOTIndexReader<ValidTimeKey> reader, final byte store, final long fork,
      final long endpoint, final long base, final Supplier<@Nullable NodeReferences> load) {
    Objects.requireNonNull(reader);
    Objects.requireNonNull(load);
    final StorageEngineReader storage = reader.getStorageEngineReader();
    final BufferManager buffers = storage.getBufferManager();
    final ValidTimeReadCache cache =
        storage.hasTrxIntentLog() || buffers == null || !buffers.getHOTLookupCache().isEnabled()
            ? null
            : storage.getResourceSession().getValidTimeReadCache();
    if (cache == null) {
      return PostingMask.from(load.get());
    }
    final MaskKey key = new MaskKey(storage.getRevisionNumber(), reader.getIndexNumber(), store, fork, endpoint, base,
        buffers.getHOTLookupCache().generation());
    final PostingMask hit = cache.masks.getIfPresent(key);
    if (hit != null) {
      if (HotOrderedStore.scanDiagnosticsEnabled()) {
        MASK_HITS.increment();
      }
      return hit;
    }
    return cache.masks.get(key, _ -> PostingMask.from(load.get()));
  }

  public void clear() {
    masks.invalidateAll();
  }
}
