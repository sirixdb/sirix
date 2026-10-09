/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.HOTKeySerializer;
import io.sirix.index.hot.NodeReferencesSerializer.ChunkAccumulator;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.hot.PostingDeltas;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Iterator;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Persistent {@link OrderedStore} backed by a SirixDB HOT (Height-Optimized Trie) sub-tree.
 *
 * <p>
 * This is the SirixDB realisation of the storage SPI the {@link RelationalIntervalTree} drives. One
 * logical ordered map defined by {@link OrderedStore} is encoded in a single HOT sub-tree shared by
 * BOTH RI-tree stores; a per-instance one-byte {@link #store} discriminator
 * ({@link ValidTimeKey#STORE_LOWER} / {@link ValidTimeKey#STORE_UPPER}) keeps the two stores in
 * disjoint, contiguous key ranges. Companion evidence stores use the same SPI over a separate root
 * with their own discriminators. The record references (node keys) are stored as the HOT slot VALUE
 * — a chunked Roaring bitmap, exactly as the CAS index stores node keys under a CAS value.
 * </p>
 *
 * <p>
 * A {@code (forkNode, [endpointLo, endpointHi])} {@link #scan} is one contiguous HOT range scan
 * over {@code [(store,fork,endpointLo), (store,fork,endpointHi)]} thanks to the order-preserving
 * {@code [store][fork][endpoint]} key encoding (see {@link ValidTimeKeySerializer}).
 * </p>
 *
 * <p>
 * The {@code reader} may be {@code null} on a writer-only store; scans are then no-ops. Factories
 * that need read-your-writes provide both. The {@code writer} may be {@code null} on a read-only
 * store (the query path never mutates); {@code insert}/{@code remove} then throw.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class HotOrderedStore implements OrderedStore {

  private static final boolean SCAN_DIAGNOSTICS = Boolean.getBoolean("sirix.validTime.scanDiag");
  private static final LongAdder INTERVAL_REFS_EMITTED = new LongAdder();
  private static final LongAdder POSTING_REFS_EMITTED = new LongAdder();
  private static final LongAdder POSTING_LOOKUPS = new LongAdder();
  private static final LongAdder POSTING_CHUNKS_READ = new LongAdder();

  public static boolean scanDiagnosticsEnabled() {
    return SCAN_DIAGNOSTICS;
  }

  public static long intervalRefsEmitted() {
    return INTERVAL_REFS_EMITTED.sum();
  }

  public static long postingRefsEmitted() {
    return POSTING_REFS_EMITTED.sum();
  }

  public static long postingLookups() {
    return POSTING_LOOKUPS.sum();
  }

  public static long postingChunksRead() {
    return POSTING_CHUNKS_READ.sum();
  }

  private final byte store;
  private final @Nullable HOTIndexWriter<ValidTimeKey> writer;
  private final @Nullable HOTIndexReader<ValidTimeKey> reader;

  /**
   * @param store the interval or evidence discriminator defined by {@link ValidTimeKey}
   * @param writer the HOT index writer (mutations); may be {@code null} for a read-only store
   * @param reader the HOT index reader (scans); may be {@code null} for a writer-only store
   */
  public HotOrderedStore(final byte store, final @Nullable HOTIndexWriter<ValidTimeKey> writer,
      final @Nullable HOTIndexReader<ValidTimeKey> reader) {
    this.store = store;
    this.writer = writer;
    this.reader = reader;
  }

  @Override
  public void insert(final long forkNode, final long endpoint, final long ref) {
    final HOTIndexWriter<ValidTimeKey> w = requireNonNull(writer, "writer-only operation on a read-only store");
    final ValidTimeKey key = new ValidTimeKey(store, forkNode, endpoint);
    w.indexNodeKey(key, ref);
  }

  @Override
  public void remove(final long forkNode, final long endpoint, final long ref) {
    final HOTIndexWriter<ValidTimeKey> w = requireNonNull(writer, "writer-only operation on a read-only store");
    final ValidTimeKey key = new ValidTimeKey(store, forkNode, endpoint);
    w.remove(key, ref);
  }

  @Override
  public void scan(final long forkNode, final long endpointLo, final long endpointHi, final LongConsumer out) {
    if (reader == null || endpointLo > endpointHi) {
      return;
    }
    final ValidTimeKey from = new ValidTimeKey(store, forkNode, endpointLo);
    final ValidTimeKey to = new ValidTimeKey(store, forkNode, endpointHi);
    scan(from, to, out);
  }

  public @Nullable NodeReferences chunk(final long forkNode, final long endpoint, final long ref) {
    if (ref < 0 || (ref >>> 48) != 0) {
      throw new IllegalArgumentException("Reference exceeds the HOT chunk domain: " + ref);
    }
    final HOTIndexReader<ValidTimeKey> r = requireNonNull(reader);
    if (SCAN_DIAGNOSTICS) {
      POSTING_LOOKUPS.increment();
    }
    final var root = r.getRootReference();
    if (root == null) {
      return null;
    }
    final byte[] key = new byte[ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES];
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(new ValidTimeKey(store, forkNode, endpoint),
        (int) (ref >>> 16), key, 0);
    try (final PostingChunkCursor chunks = new PostingChunkCursor(r, root, key, key)) {
      return chunks.hasNext()
          ? chunks.next()
          : null;
    }
  }

  public long cardinality(final long forkNode, final long endpoint) {
    return cardinality(forkNode, endpoint, false);
  }

  public boolean hasReferences(final long forkNode, final long endpoint) {
    return cardinality(forkNode, endpoint, true) != 0;
  }

  public boolean intersects(final long forkNode, final long endpoint, final HotOrderedStore other,
      final long otherForkNode, final long otherEndpoint) {
    requireNonNull(other);
    final HOTIndexReader<ValidTimeKey> r = requireNonNull(reader);
    if (SCAN_DIAGNOSTICS) {
      POSTING_LOOKUPS.increment();
    }
    final var root = r.getRootReference();
    if (root == null) {
      return false;
    }
    final ValidTimeKey key = new ValidTimeKey(store, forkNode, endpoint);
    final byte[] from = new byte[ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES];
    final byte[] to = new byte[from.length];
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, 0, from, 0);
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, -1, to, 0);
    try (final PostingChunkCursor chunks = new PostingChunkCursor(r, root, from, to)) {
      while (chunks.hasNext()) {
        final NodeReferences refs = chunks.next();
        if (refs == null) {
          continue;
        }
        final NodeReferences otherRefs = other.chunk(otherForkNode, otherEndpoint, chunks.chunkBase());
        if (otherRefs != null && Roaring64Bitmap.intersects(refs.getNodeKeys(), otherRefs.getNodeKeys())) {
          return true;
        }
      }
    }
    return false;
  }

  private long cardinality(final long forkNode, final long endpoint, final boolean stopAtFirst) {
    final HOTIndexReader<ValidTimeKey> r = requireNonNull(reader);
    if (SCAN_DIAGNOSTICS) {
      POSTING_LOOKUPS.increment();
    }
    final var root = r.getRootReference();
    if (root == null) {
      return 0;
    }
    final ValidTimeKey key = new ValidTimeKey(store, forkNode, endpoint);
    final byte[] from = new byte[ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES];
    final byte[] to = new byte[from.length];
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, 0, from, 0);
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, -1, to, 0);
    long count = 0;
    try (final PostingChunkCursor chunks = new PostingChunkCursor(r, root, from, to)) {
      while (chunks.hasNext()) {
        final NodeReferences refs = chunks.next();
        if (refs != null) {
          count += refs.cardinality();
        }
        if (stopAtFirst && count != 0) {
          break;
        }
      }
    }
    return count;
  }

  /** Reads one bounded chunk at a time, including its chronological deltas and side payload. */
  private static final class PostingChunkCursor implements AutoCloseable {
    private static final int COMPOSITE_BYTES = ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES;
    private final HOTTrieReader trie;
    private final HOTRangeCursor cursor;
    private final byte[] composite;
    private final ChunkAccumulator accumulator = ChunkAccumulator.forChunkLookup();
    private long chunkBase;

    private PostingChunkCursor(final HOTIndexReader<ValidTimeKey> reader, final PageReference root, final byte[] from,
        final byte[] to) {
      trie = new HOTTrieReader(reader.getStorageEngineReader());
      composite = Arrays.copyOf(from, COMPOSITE_BYTES);
      // A point bound ending at the base excludes every suffix delta. Include the complete
      // suffix domain of the last chunk without admitting the next chunk or logical key.
      final byte[] upper = Arrays.copyOf(to, COMPOSITE_BYTES + PostingDeltas.SUFFIX_BYTES);
      Arrays.fill(upper, COMPOSITE_BYTES, upper.length, (byte) 0xFF);
      try {
        cursor = trie.range(root, from, upper);
      } catch (RuntimeException | Error e) {
        trie.close();
        throw e;
      }
    }

    private boolean hasNext() {
      return cursor.hasNext();
    }

    private long chunkBase() {
      return chunkBase;
    }

    private @Nullable NodeReferences next() {
      boolean positioned = false;
      for (int attempt = 0; !positioned; attempt++) {
        checkRetries(attempt);
        try {
          final HOTLeafPage leaf = cursor.currentLeafPage();
          final int index = cursor.currentEntryIndex();
          if (leaf.getKeyLength(index) < COMPOSITE_BYTES
              || leaf.compareKeyPrefix(index, composite, ValidTimeKeySerializer.KEY_BYTES) != 0) {
            throw new IllegalArgumentException("Unreadable VALIDTIME posting chunk key");
          }
          final int chunkIndex = leaf.readKeyIntBE(index, ValidTimeKeySerializer.KEY_BYTES);
          if (cursor.validateLeaf()) {
            HOTKeySerializer.writeChunkIdxBE(composite, ValidTimeKeySerializer.KEY_BYTES, chunkIndex);
            positioned = true;
          } else {
            cursor.recoverTorn(attempt + 1, "VALIDTIME posting chunk");
          }
        } catch (RuntimeException e) {
          if (cursor.validateLeaf()) {
            throw e;
          }
          cursor.recoverTorn(attempt + 1, "VALIDTIME posting chunk");
        }
      }

      merge: for (int attempt = 0;; attempt++) {
        checkRetries(attempt);
        while (cursor.hasNext()) {
          final HOTLeafPage leaf = cursor.currentLeafPage();
          final int index = cursor.currentEntryIndex();
          final boolean end;
          try {
            end = leaf.compareKeyPrefix(index, composite, COMPOSITE_BYTES) != 0;
            if (!end) {
              final int length = leaf.getKeyLength(index);
              final boolean ok;
              if (length == COMPOSITE_BYTES) {
                ok = accumulator.addChunk(leaf, leaf.valueRef(index), 0, trie);
              } else if (length == COMPOSITE_BYTES + PostingDeltas.SUFFIX_BYTES
                  && PostingDeltas.isDelta(leaf.readKeyIntBE(index, COMPOSITE_BYTES) & 0xFFFFFFFFL)) {
                // OrderedStore.chunk returns chunk-local low-16-bit references on both sides
                // of an intersection; expanding either side to document keys would miscompare.
                ok = accumulator.applyDelta(leaf, leaf.valueRef(index), 0,
                    leaf.readKeyIntBE(index, COMPOSITE_BYTES) & 0xFFFFFFFFL, trie);
              } else {
                throw new IllegalArgumentException("Malformed VALIDTIME posting chunk key length: " + length);
              }
              if (!ok) {
                accumulator.reset();
                cursor.restartAtComposite(composite);
                continue merge;
              }
            }
          } catch (RuntimeException e) {
            if (cursor.validateLeaf()) {
              throw e;
            }
            accumulator.reset();
            cursor.restartAtComposite(composite);
            continue merge;
          }
          if (!cursor.validateLeaf()) {
            accumulator.reset();
            cursor.restartAtComposite(composite);
            continue merge;
          }
          if (end) {
            break;
          }
          cursor.advance();
        }
        break;
      }
      chunkBase = (HOTKeySerializer.readChunkIdx(composite, 0, COMPOSITE_BYTES) & 0xFFFFFFFFL) << 16;
      if (SCAN_DIAGNOSTICS) {
        POSTING_CHUNKS_READ.increment();
      }
      return accumulator.toNodeReferencesAndReset();
    }

    private static void checkRetries(final int attempt) {
      if (attempt > HOTTrieReader.MAX_STAMP_RETRIES) {
        throw HOTTrieReader.stampRetriesExhausted("VALIDTIME posting chunk");
      }
    }

    @Override
    public void close() {
      try {
        cursor.close();
      } finally {
        trie.close();
      }
    }
  }

  @Override
  public void forEachRef(final LongConsumer out) {
    if (reader == null) {
      return;
    }
    scan(new ValidTimeKey(store, Long.MIN_VALUE, Long.MIN_VALUE),
        new ValidTimeKey(store, Long.MAX_VALUE, Long.MAX_VALUE), out);
  }

  private void scan(final ValidTimeKey from, final ValidTimeKey to, final LongConsumer out) {
    final Iterator<Map.Entry<ValidTimeKey, NodeReferences>> it = requireNonNull(reader).range(from, to);
    while (it.hasNext()) {
      final NodeReferences refs = it.next().getValue();
      if (refs == null) {
        continue;
      }
      final LongIterator longIt = refs.getNodeKeys().getLongIterator();
      while (longIt.hasNext()) {
        if (SCAN_DIAGNOSTICS) {
          if (store == ValidTimeKey.STORE_LOWER || store == ValidTimeKey.STORE_UPPER) {
            INTERVAL_REFS_EMITTED.increment();
          } else {
            POSTING_REFS_EMITTED.increment();
          }
        }
        out.accept(longIt.next());
      }
    }
  }
}
