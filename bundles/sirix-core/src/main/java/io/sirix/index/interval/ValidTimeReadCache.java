package io.sirix.index.interval;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.BufferManager;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * Session-scoped memo of immutable companion posting masks, plus per-walk trie readers.
 *
 * <p>
 * Readers are deliberately NOT cached across transactions. Doing so needs a forwarding
 * {@code StorageEngineReader} to re-point a parked trie at the current transaction, and that
 * wrapper is a second loaded implementation of the interface every record read goes through. On a
 * JVM that only reads, the JIT otherwise binds those calls statically; loading the wrapper at the
 * first stab invalidated that assumption for the whole cold read path and cost more than the
 * readers saved (measured on cold Q4/Q9). Constructing a trie reader is two small objects per walk.
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

  /** One trie reader for one walk, bound to the transaction's own storage reader. */
  public static Lease borrow(final HOTIndexReader<ValidTimeKey> reader) {
    final StorageEngineReader storage = reader.getStorageEngineReader();
    return new Lease(new State(storage, reader.getIndexNumber()));
  }

  public void clear() {
    masks.invalidateAll();
  }

  private static final class State {
    private final HOTIndexReader<ValidTimeKey> index;
    private final HOTTrieReader trie;

    private State(final StorageEngineReader storage, final int indexNumber) {
      index = HOTIndexReader.create(storage, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, indexNumber);
      trie = new HOTTrieReader(storage);
    }
  }

  public static final class Lease implements AutoCloseable {
    private final State state;
    private boolean closed;

    private Lease(final State state) {
      this.state = state;
    }

    public HOTTrieReader trie() {
      return state.trie;
    }

    public @Nullable PageReference root() {
      return state.index.getRootReference();
    }

    @Override
    public void close() {
      if (!closed) {
        closed = true;
        state.trie.close();
      }
    }
  }
}
