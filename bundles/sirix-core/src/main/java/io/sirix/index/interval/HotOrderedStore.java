/*
 * [New BSD License]
 * Copyright (c) 2026, SirixDB Contributors
 * All rights reserved.
 */
package io.sirix.index.interval;

import io.sirix.index.hot.HOTIndexReader;
import io.sirix.index.hot.HOTIndexWriter;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;

import java.util.Iterator;
import java.util.Map;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Persistent {@link OrderedStore} backed by a SirixDB HOT (Height-Optimized Trie) sub-tree.
 *
 * <p>This is the SirixDB realisation of the storage SPI the {@link RelationalIntervalTree} drives.
 * One {@code (forkNode, endpoint) -> multiset(ref)} logical ordered map is encoded in a single HOT
 * sub-tree shared by BOTH RI-tree stores; a per-instance one-byte {@link #store} discriminator
 * ({@link ValidTimeKey#STORE_LOWER} / {@link ValidTimeKey#STORE_UPPER}) keeps the two stores in
 * disjoint, contiguous key ranges. The record references (node keys) are stored as the HOT slot
 * VALUE — a chunked Roaring bitmap, exactly as the CAS index stores node keys under a CAS value.</p>
 *
 * <p>A {@code (forkNode, [endpointLo, endpointHi])} {@link #scan} is one contiguous HOT range scan
 * over {@code [(store,fork,endpointLo), (store,fork,endpointHi)]} thanks to the order-preserving
 * {@code [store][fork][endpoint]} key encoding (see {@link ValidTimeKeySerializer}).</p>
 *
 * <p>The {@code reader} may be {@code null} on a writer-only store (the build / maintain path never
 * scans); {@code scan} is then a no-op. The {@code writer} may be {@code null} on a read-only store
 * (the query path never mutates); {@code insert}/{@code remove} then throw.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class HotOrderedStore implements OrderedStore {

  private final byte store;
  private final @Nullable HOTIndexWriter<ValidTimeKey> writer;
  private final @Nullable HOTIndexReader<ValidTimeKey> reader;

  /**
   * @param store  the discriminator byte ({@link ValidTimeKey#STORE_LOWER} or
   *               {@link ValidTimeKey#STORE_UPPER})
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
    w.index(key, new NodeReferences().addNodeKey(ref), RBTreeReader.MoveCursor.NO_MOVE);
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
    final Iterator<Map.Entry<ValidTimeKey, NodeReferences>> it = reader.range(from, to);
    while (it.hasNext()) {
      final NodeReferences refs = it.next().getValue();
      if (refs == null) {
        continue;
      }
      final LongIterator longIt = refs.getNodeKeys().getLongIterator();
      while (longIt.hasNext()) {
        out.accept(longIt.next());
      }
    }
  }
}
