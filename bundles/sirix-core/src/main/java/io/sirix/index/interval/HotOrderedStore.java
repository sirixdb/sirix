/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.HOTKeySerializer;
import io.sirix.index.hot.NodeReferencesSerializer;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.Roaring64Bitmap;

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

  private static final ValueLayout.OfInt INT_BE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
  private static final ValueLayout.OfLong LONG_BE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);

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

  @Override
  public void scanChunks(final long forkNode, final long endpointLo, final long endpointHi, final long exclusiveEnd,
      final PostingChunkConsumer out) {
    requireNonNull(out);
    if (reader == null || endpointLo > endpointHi) {
      return;
    }
    walkChunks(new ValidTimeKey(store, forkNode, endpointLo), new ValidTimeKey(store, forkNode, endpointHi),
        exclusiveEnd, out);
  }

  private void walkChunks(final ValidTimeKey lower, final ValidTimeKey upper, final long exclusiveEnd,
      final PostingChunkConsumer out) {
    final byte[] from = new byte[ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES];
    final byte[] to = new byte[from.length];
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(lower, 0, from, 0);
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(upper, -1, to, 0);
    try (final ValidTimeReadCache.Lease lease = ValidTimeReadCache.borrow(requireNonNull(reader))) {
      final var root = lease.root();
      if (root == null) {
        return;
      }
      try (final HOTRangeCursor cursor = lease.trie().range(root, from, to)) {
        while (cursor.hasNext()) {
          byte[] payload = null;
          long base = 0;
          long endpoint = 0;
          boolean stable = false;
          for (int round = 0; round < HOTTrieReader.MAX_STAMP_RETRIES; round++) {
            try {
              final MemorySegment key = cursor.currentKeySlice();
              final MemorySegment value = cursor.currentValueSlice();
              if (key.byteSize() != ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES) {
                throw new IllegalStateException("Invalid valid-time composite key length: " + key.byteSize());
              }
              final long size = value.byteSize();
              if (size > cursor.currentLeafPage().slotCapacity()) {
                throw new IllegalStateException("Posting payload exceeds the leaf slot capacity");
              }
              // A tombstone has no keys. Confirm its stamp before skipping it, without allocating.
              if (size == 1 && value.get(ValueLayout.JAVA_BYTE, 0) == (byte) 0xFE) {
                payload = null;
              } else {
                payload = value.toArray(ValueLayout.JAVA_BYTE);
                base = (key.get(INT_BE, ValidTimeKeySerializer.KEY_BYTES) & 0xFFFFFFFFL) << 16;
                endpoint = key.get(LONG_BE, 9) ^ Long.MIN_VALUE;
              }
            } catch (final RuntimeException exception) {
              if (cursor.validateLeaf()) {
                throw exception;
              }
              cursor.refreshLeaf();
              continue;
            }
            if (cursor.validateLeaf()) {
              stable = true;
              break;
            }
            cursor.refreshLeaf();
          }
          if (!stable) {
            throw HOTTrieReader.stampRetriesExhausted("valid-time posting scan");
          }
          if (payload != null) {
            final NodeReferences refs = NodeReferencesSerializer.deserializeReadChunk(payload);
            if (SCAN_DIAGNOSTICS) {
              if (store == ValidTimeKey.STORE_LOWER || store == ValidTimeKey.STORE_UPPER) {
                INTERVAL_REFS_EMITTED.add(refs.cardinality());
              } else {
                POSTING_REFS_EMITTED.add(refs.cardinality());
              }
            }
            out.accept(base, refs, exclusiveEnd >= 0 && endpoint == exclusiveEnd);
          }
          cursor.advance();
        }
      }
    }
  }

  public @Nullable NodeReferences chunk(final long forkNode, final long endpoint, final long ref) {
    if (ref < 0 || (ref >>> 48) != 0) {
      throw new IllegalArgumentException("Reference exceeds the HOT chunk domain: " + ref);
    }
    final HOTIndexReader<ValidTimeKey> r = requireNonNull(reader);
    if (SCAN_DIAGNOSTICS) {
      POSTING_LOOKUPS.increment();
    }
    return readChunk(forkNode, endpoint, ref);
  }

  private @Nullable NodeReferences readChunk(final long forkNode, final long endpoint, final long ref) {
    final HOTIndexReader<ValidTimeKey> r = requireNonNull(reader);
    final byte[] key = new byte[ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES];
    ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(new ValidTimeKey(store, forkNode, endpoint),
        (int) (ref >>> 16), key, 0);
    try (final ValidTimeReadCache.Lease lease = ValidTimeReadCache.borrow(r)) {
      final var root = lease.root();
      if (root == null) {
        return null;
      }
      try (final HOTRangeCursor cursor = lease.trie().range(root, key, key)) {
        return cursor.hasNext()
            ? postingChunk(cursor.next())
            : null;
      }
    }
  }

  /** The same chunk lookup as {@link #chunk}, with a revision-cached immutable mask result. */
  public PostingMask chunkMask(final long forkNode, final long endpoint, final long ref) {
    if (ref < 0 || (ref >>> 48) != 0) {
      throw new IllegalArgumentException("Reference exceeds the HOT chunk domain: " + ref);
    }
    if (SCAN_DIAGNOSTICS) {
      POSTING_LOOKUPS.increment();
    }
    return ValidTimeReadCache.mask(requireNonNull(reader), store, forkNode, endpoint, ref & ~0xFFFFL,
        () -> readChunk(forkNode, endpoint, ref));
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
    try (final ValidTimeReadCache.Lease lease = ValidTimeReadCache.borrow(r)) {
      final var root = lease.root();
      if (root == null) {
        return false;
      }
      final ValidTimeKey key = new ValidTimeKey(store, forkNode, endpoint);
      final byte[] from = new byte[ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES];
      final byte[] to = new byte[from.length];
      ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, 0, from, 0);
      ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, -1, to, 0);
      try (final HOTRangeCursor cursor = lease.trie().range(root, from, to)) {
        while (cursor.hasNext()) {
          final HOTRangeCursor.Entry entry = cursor.next();
          final NodeReferences refs = postingChunk(entry);
          if (!refs.hasNodeKeys()) {
            continue;
          }
          final byte[] composite = entry.keyBytes();
          final long chunkBase = (HOTKeySerializer.readChunkIdx(composite, 0, composite.length) & 0xFFFFFFFFL) << 16;
          final NodeReferences otherRefs = other.chunk(otherForkNode, otherEndpoint, chunkBase);
          if (otherRefs != null && Roaring64Bitmap.intersects(refs.getNodeKeys(), otherRefs.getNodeKeys())) {
            return true;
          }
        }
      }
      return false;
    }
  }

  private long cardinality(final long forkNode, final long endpoint, final boolean stopAtFirst) {
    final HOTIndexReader<ValidTimeKey> r = requireNonNull(reader);
    if (SCAN_DIAGNOSTICS) {
      POSTING_LOOKUPS.increment();
    }
    try (final ValidTimeReadCache.Lease lease = ValidTimeReadCache.borrow(r)) {
      final var root = lease.root();
      if (root == null) {
        return 0;
      }
      final ValidTimeKey key = new ValidTimeKey(store, forkNode, endpoint);
      final byte[] from = new byte[ValidTimeKeySerializer.KEY_BYTES + HOTKeySerializer.CHUNK_IDX_BYTES];
      final byte[] to = new byte[from.length];
      ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, 0, from, 0);
      ValidTimeKeySerializer.INSTANCE.serializeWithChunkIdx(key, -1, to, 0);
      long count = 0;
      try (final HOTRangeCursor cursor = lease.trie().range(root, from, to)) {
        while (cursor.hasNext()) {
          count += postingChunk(cursor.next()).cardinality();
          if (stopAtFirst && count != 0) {
            break;
          }
        }
      }
      return count;
    }
  }

  private static NodeReferences postingChunk(final HOTRangeCursor.Entry entry) {
    if (SCAN_DIAGNOSTICS) {
      POSTING_CHUNKS_READ.increment();
    }
    return NodeReferencesSerializer.deserializeReadChunk(entry.valueBytes());
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
    walkChunks(from, to, -1, (base, refs, _) -> refs.forEachNodeKey(key -> out.accept(base | key)));
  }
}
