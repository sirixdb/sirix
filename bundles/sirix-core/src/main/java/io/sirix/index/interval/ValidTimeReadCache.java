package io.sirix.index.interval;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Cache;
import io.sirix.access.trx.page.AbstractForwardingStorageEngineReader;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.BufferManager;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import java.util.function.Supplier;
import io.sirix.index.IndexType;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/** Session-scoped, bounded scratch readers. Parked readers retain no transaction or leaf guard. */
public final class ValidTimeReadCache {
  private static final LongAdder MASK_HITS = new LongAdder();

  public static long maskHits() {
    return MASK_HITS.sum();
  }

  private record Key(int revision, int index, long generation) {
  }

  private final Cache<Key, AtomicReference<State>> readers = Caffeine.newBuilder().maximumSize(64).build();

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
    final ValidTimeReadCache cache = storage.hasTrxIntentLog() || buffers == null
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

  /**
   * Borrow one reader for a walk. Concurrent walks get independent stacks; committed roots are reused
   * only within a revision and cache generation. Intent-log readers are never parked.
   */
  public static Lease borrow(final HOTIndexReader<ValidTimeKey> reader) {
    final StorageEngineReader storage = reader.getStorageEngineReader();
    final BufferManager buffers = storage.getBufferManager();
    final ValidTimeReadCache cache = storage.hasTrxIntentLog() || buffers == null
        ? null
        : storage.getResourceSession().getValidTimeReadCache();
    final AtomicReference<State> slot = cache == null
        ? null
        : cache.readers.get(
            new Key(storage.getRevisionNumber(), reader.getIndexNumber(), buffers.getHOTLookupCache().generation()),
            _ -> new AtomicReference<>());
    State state = slot == null
        ? null
        : slot.getAndSet(null);
    if (state == null) {
      state = new State(storage, reader.getIndexNumber());
    } else {
      state.binding.storage = storage;
    }
    return new Lease(state, slot);
  }

  public void clear() {
    readers.invalidateAll();
    masks.invalidateAll();
  }

  private static final class Binding extends AbstractForwardingStorageEngineReader {
    private @Nullable StorageEngineReader storage;

    private Binding(final StorageEngineReader storage) {
      this.storage = storage;
    }

    @Override
    protected StorageEngineReader delegate() {
      return Objects.requireNonNull(storage, "unbound valid-time reader");
    }
  }

  private static final class State {
    private final Binding binding;
    private final HOTIndexReader<ValidTimeKey> index;
    private final HOTTrieReader trie;

    private State(final StorageEngineReader storage, final int indexNumber) {
      binding = new Binding(storage);
      index = HOTIndexReader.create(binding, ValidTimeKeySerializer.INSTANCE, IndexType.VALIDTIME, indexNumber);
      trie = new HOTTrieReader(binding);
    }
  }

  public static final class Lease implements AutoCloseable {
    private final State state;
    private final @Nullable AtomicReference<State> slot;
    private boolean closed;

    private Lease(final State state, final @Nullable AtomicReference<State> slot) {
      this.state = state;
      this.slot = slot;
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
        try {
          state.trie.close();
        } finally {
          state.binding.storage = null;
        }
        if (slot != null) {
          slot.compareAndSet(null, state);
        }
      }
    }
  }
}
