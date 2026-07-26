/*
 * Copyright (c) 2024, SirixDB
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.index.hot;

import io.sirix.access.trx.page.HOTRangeCursor;
import io.sirix.access.trx.page.HOTTrieReader;
import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/**
 * Generic HOT index reader for object keys (CASValue, QNm).
 *
 * <p>
 * Replaces {@link io.sirix.index.redblacktree.RBTreeReader} for HOT-based secondary indexes.
 * Provides read-only access with optimistic concurrency for lock-free reads.
 * </p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 * <li>Thread-local byte buffers for key serialization</li>
 * <li>No Optional - uses @Nullable returns</li>
 * <li>Lock-free reads with version validation</li>
 * </ul>
 *
 * @param <K> the key type (must implement Comparable)
 * @author Johannes Lichtenberger
 */
public final class HOTIndexReader<K extends Comparable<? super K>> extends AbstractHOTIndexReader<K> {

  /**
   * Thread-local buffer for key serialization. Sized to fit the largest CAS prefix (10-byte
   * header + {@code MAX_STRING_VALUE_BYTES = 246}) PLUS
   * {@link HOTKeySerializer#CHUNK_IDX_BYTES} (= 4) chunkIdx trailer; rounded to 512 for headroom.
   * Mirrors {@link HOTIndexWriter#KEY_BUFFER}'s sizing to avoid 4-byte overflow on max-length
   * string CAS values.
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[512]);

  private final HOTKeySerializer<K> keySerializer;

  /**
   * Private constructor.
   *
   * @param storageEngineReader the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  private HOTIndexReader(StorageEngineReader storageEngineReader, HOTKeySerializer<K> keySerializer, IndexType indexType,
      int indexNumber) {
    super(storageEngineReader, indexType, indexNumber);
    this.keySerializer = requireNonNull(keySerializer);
  }

  /**
   * Creates a new HOTIndexReader.
   *
   * @param storageEngineReader the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType the index type
   * @param indexNumber the index number
   * @param <K> the key type
   * @return a new HOTIndexReader instance
   */
  public static <K extends Comparable<? super K>> HOTIndexReader<K> create(StorageEngineReader storageEngineReader,
      HOTKeySerializer<K> keySerializer, IndexType indexType, int indexNumber) {
    return new HOTIndexReader<>(storageEngineReader, keySerializer, indexType, indexNumber);
  }

  /**
   * Reassemble all chunks of {@code key} into one logical {@link NodeReferences}.
   *
   * <p>Chunked-bitmap storage (Phase 1+2): the logical bitmap for {@code key} is split across
   * multiple HOT slots, one per chunkIdx = {@code (int)(nodeKey >>> 16)}. This method
   * range-scans {@code [(prefix, 0), (prefix, 0xFFFFFFFF)]} via Phase 0b's
   * {@link HOTTrieReader#lowerBound} and merges every chunk's low-16-bit bitmap into a
   * single 64-bit {@code Roaring64Bitmap}, expanding bit16 → {@code (chunkIdx << 16) | bit16}.</p>
   *
   * @param key the logical index key
   * @param mode reserved (only {@code EQUAL} is meaningful for {@code get}); range modes go via
   *             {@link #range(Comparable, Comparable)} / {@link #iteratorFrom(Comparable)}
   * @return reassembled NodeReferences, or {@code null} if no chunks exist for {@code key}
   */
  public @Nullable NodeReferences get(K key, SearchMode mode) {
    requireNonNull(key);

    byte[] keyBuf = getKeyBuffer();
    int prefixLen = serializeKey(key, keyBuf, 0);
    if (prefixLen > keyBuf.length) {
      keyBuf = new byte[prefixLen];
      setKeyBuffer(keyBuf);
      prefixLen = serializeKey(key, keyBuf, 0);
    }
    return reassembleChunksForPrefix(keyBuf, prefixLen);
  }

  /**
   * Internal helper: reassemble all chunk slots whose composite key starts with
   * {@code prefixBuf[0..prefixLen)}. Shared by {@link #get} and the range iterators.
   */
  private @Nullable NodeReferences reassembleChunksForPrefix(byte[] prefixBuf, int prefixLen) {
    final PageReference rootRef = getRootReference();
    if (rootRef == null) {
      return null;
    }

    final byte[] fromBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(prefixBuf, 0, fromBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(fromBytes, prefixLen, 0);

    final byte[] toBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(prefixBuf, 0, toBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(toBytes, prefixLen, 0xFFFFFFFF);

    Roaring64Bitmap merged = collectViaCursor(rootRef, prefixBuf, prefixLen, fromBytes, toBytes);

    if ((merged == null || merged.isEmpty())
        && !Boolean.getBoolean("hot.cas.leftmostfallback.disable")) {
      // Phase 7v retry: the PEXT-routed lowerBound may misroute under I6 violations
      // (writer-side structural bugs from byte-10 encoder discontinuity). Retry with a
      // full leaf-walk scan that's robust against non-lex-order leaves. Only fires when
      // the fast path returned 0 chunks — zero overhead for queries that succeeded.
      merged = collectViaLeafWalk(rootRef, prefixBuf, prefixLen);
    }

    if (merged == null || merged.isEmpty()) {
      return null;
    }
    return new NodeReferences(merged);
  }

  private @Nullable Roaring64Bitmap collectViaCursor(PageReference rootRef, byte[] prefixBuf,
      int prefixLen, byte[] fromBytes, byte[] toBytes) {
    Roaring64Bitmap merged = null;
    try (HOTTrieReader reader = new HOTTrieReader(getStorageEngineReader());
        HOTRangeCursor cursor = reader.range(rootRef, fromBytes, toBytes)) {
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int idx = cursor.currentEntryIndex();
        final byte[] composite = leaf.getKey(idx);
        if (composite.length != prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES
            || Arrays.compareUnsigned(composite, 0, prefixLen, prefixBuf, 0, prefixLen) != 0) {
          cursor.advance();
          continue;
        }
        merged = mergeChunk(merged, leaf, idx, composite);
        cursor.advance();
      }
    }
    return merged;
  }

  /**
   * Phase 7v fallback: walk every leaf in the trie (left-to-right traversal order, NOT lex order
   * — robust against I8 violations), filter each entry by exact prefix match. Used only when the
   * primary PEXT-routed cursor returns 0 chunks for a key that is in fact stored. O(total trie
   * entries) per call; only triggered on miss.
   */
  private @Nullable Roaring64Bitmap collectViaLeafWalk(PageReference rootRef, byte[] prefixBuf,
      int prefixLen) {
    Roaring64Bitmap merged = null;
    try (HOTTrieReader reader = new HOTTrieReader(getStorageEngineReader())) {
      HOTLeafPage leaf = reader.navigateToLeftmostLeaf(rootRef);
      while (leaf != null) {
        final int entryCount = leaf.getEntryCount();
        for (int idx = 0; idx < entryCount; idx++) {
          final byte[] composite = leaf.getKey(idx);
          if (composite.length != prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES
              || Arrays.compareUnsigned(composite, 0, prefixLen, prefixBuf, 0, prefixLen) != 0) {
            continue;
          }
          merged = mergeChunk(merged, leaf, idx, composite);
        }
        leaf = reader.advanceToNextLeaf();
      }
    }
    return merged;
  }

  private @Nullable Roaring64Bitmap mergeChunk(@Nullable Roaring64Bitmap merged, HOTLeafPage leaf,
      int idx, byte[] composite) {
    final int chunkIdx = HOTKeySerializer.readChunkIdx(composite, 0, composite.length);
    final byte[] chunkBytes = leaf.getValue(idx);
    if (NodeReferencesSerializer.isTombstone(chunkBytes, 0, chunkBytes.length)) {
      return merged;
    }
    final NodeReferences chunkRefs = NodeReferencesSerializer.deserialize(chunkBytes);
    final Roaring64Bitmap chunkBitmap = chunkRefs.getNodeKeys();
    if (chunkBitmap.isEmpty()) {
      return merged;
    }
    if (merged == null) {
      merged = new Roaring64Bitmap();
    }
    final long high = ((long) chunkIdx) << 16;
    final LongIterator bIt = chunkBitmap.getLongIterator();
    while (bIt.hasNext()) {
      merged.add(high | (bIt.next() & 0xFFFFL));
    }
    return merged;
  }

  /**
   * Create a range iterator over logical entries with keys in {@code [fromKey, toKey]}.
   *
   * <p>Composite-key range scan with chunk grouping. Walks composite keys
   * {@code [(fromKey, 0), (toKey, 0xFFFFFFFF)]} and groups consecutive same-prefix slots into one
   * logical {@link Map.Entry}{@code <K, NodeReferences>} — chunks of one prefix lex-cluster
   * because composite keys share prefix bytes and the chunkIdx_be4 trailer determines order
   * within that range.</p>
   *
   * @param fromKey start key (inclusive)
   * @param toKey   end key (inclusive)
   */
  public Iterator<Map.Entry<K, NodeReferences>> range(K fromKey, K toKey) {
    requireNonNull(fromKey);
    requireNonNull(toKey);

    byte[] keyBuf = getKeyBuffer();
    int fromLen = serializeKey(fromKey, keyBuf, 0);
    byte[] fromPrefix = Arrays.copyOf(keyBuf, fromLen);
    int toLen = serializeKey(toKey, keyBuf, 0);
    byte[] toPrefix = Arrays.copyOf(keyBuf, toLen);

    final byte[] fromComposite = new byte[fromLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(fromPrefix, 0, fromComposite, 0, fromLen);
    HOTKeySerializer.writeChunkIdxBE(fromComposite, fromLen, 0);

    final byte[] toComposite = new byte[toLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(toPrefix, 0, toComposite, 0, toLen);
    HOTKeySerializer.writeChunkIdxBE(toComposite, toLen, 0xFFFFFFFF);

    return new ChunkAggregatingIterator(fromComposite, toComposite, fromPrefix);
  }

  /**
   * Create an iterator that starts from {@code fromKey} (inclusive) with no upper bound. Used
   * for {@code GREATER} / {@code GREATER_OR_EQUAL} CAS queries.
   */
  /**
   * Iterate every logical entry in the index. Overrides the abstract base's per-slot iterator —
   * with chunked-bitmap storage, "all entries" means one logical {@link Map.Entry} per prefix,
   * not per chunk slot. Implemented by walking the entire composite-key range with no bounds
   * and grouping consecutive same-prefix slots.
   */
  @Override
  public Iterator<Map.Entry<K, NodeReferences>> iterator() {
    return new ChunkAggregatingIterator(new byte[0], null, null);
  }

  public Iterator<Map.Entry<K, NodeReferences>> iteratorFrom(K fromKey) {
    requireNonNull(fromKey);

    byte[] keyBuf = getKeyBuffer();
    int fromLen = serializeKey(fromKey, keyBuf, 0);
    byte[] fromPrefix = Arrays.copyOf(keyBuf, fromLen);

    final byte[] fromComposite = new byte[fromLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(fromPrefix, 0, fromComposite, 0, fromLen);
    HOTKeySerializer.writeChunkIdxBE(fromComposite, fromLen, 0);

    return new ChunkAggregatingIterator(fromComposite, null, fromPrefix);
  }

  /**
   * Iterator that walks a chunked composite-key range and groups consecutive same-prefix slots
   * into logical {@link Map.Entry}{@code <K, NodeReferences>} records.
   *
   * <p>Per group: deserialize each chunk's bitmap, expand bit16 → {@code chunkIdx<<16|bit16},
   * accumulate into a fresh {@link Roaring64Bitmap}, then emit. Crosses to the next group when
   * the composite key's prefix bytes change.</p>
   */
  private final class ChunkAggregatingIterator implements Iterator<Map.Entry<K, NodeReferences>> {
    private final @Nullable HOTTrieReader trieReader;
    private final @Nullable HOTRangeCursor cursor;
    /**
     * Lex-prefix lower bound. Groups whose prefix is lex-less than {@code fromPrefixFilter} are
     * skipped during {@link #advance()}. Required because HOT sibling subtrees can have
     * overlapping lex ranges, so a path-stack forward walk is not strictly lex-monotonic across
     * the whole trie even after the BE partial-key encoding refactor.
     *
     * <p>The overlap arises whenever a high-order key bit varies at the parent level <em>and</em>
     * varies within some sibling subtree: HOT can only capture a bit as a parent disc bit when
     * it is constant in every sibling's subtree (see {@code HOTTrieWriter#computeDiscBits},
     * {@link HOTTrieWriter#bitConstantValueInSubtree}). Bits that fail that test must live at a
     * deeper level, which means two sibling subtrees can share <em>some</em> lex prefixes while
     * differing on lower bits. Forward sweep therefore can emit a leaf in subtree i whose key is
     * lex-less than {@code fromKey} after the PEXT-routed seek already positioned us beyond it.
     *
     * <p>The filter is a per-entry forward-only prefix compare — it never seeks backwards and
     * never falls back to a full scan. The cursor still skips the bulk of the trie via the
     * lower-bound seek; this only suppresses the small interleaving residue at the head of the
     * sweep so {@code GREATER}/{@code GREATER_OR_EQUAL} CAS semantics are exact.
     *
     * <p>{@code null} disables filtering — used by full-trie iteration ({@link #iterator()}).</p>
     */
    private final byte @Nullable [] fromPrefixFilter;
    private Map.@Nullable Entry<K, NodeReferences> nextEntry;

    ChunkAggregatingIterator(byte[] fromComposite, byte @Nullable [] toComposite,
        byte @Nullable [] fromPrefixFilter) {
      this.fromPrefixFilter = fromPrefixFilter;
      final PageReference rootRef = getRootReference();
      if (rootRef == null) {
        // Empty trie — no entries.
        this.trieReader = null;
        this.cursor = null;
        this.nextEntry = null;
        return;
      }
      this.trieReader = new HOTTrieReader(getStorageEngineReader());
      this.cursor = trieReader.range(rootRef, fromComposite, toComposite);
      advance();
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<K, NodeReferences> next() {
      if (nextEntry == null) {
        throw new NoSuchElementException();
      }
      final Map.Entry<K, NodeReferences> result = nextEntry;
      advance();
      if (nextEntry == null) {
        closeQuietly();
      }
      return result;
    }

    private void closeQuietly() {
      if (cursor != null) {
        cursor.close();
      }
      if (trieReader != null) {
        trieReader.close();
      }
    }

    private void advance() {
      if (cursor == null) {
        nextEntry = null;
        return;
      }
      while (true) {
        while (cursor.hasNext()) {
          final byte[] composite = cursor.currentLeafPage().getKey(cursor.currentEntryIndex());
          if (composite.length < HOTKeySerializer.CHUNK_IDX_BYTES) {
            cursor.advance();
            continue;
          }
          if (fromPrefixFilter != null) {
            final int candidatePrefixLen = composite.length - HOTKeySerializer.CHUNK_IDX_BYTES;
            if (Arrays.compareUnsigned(composite, 0, candidatePrefixLen,
                fromPrefixFilter, 0, fromPrefixFilter.length) < 0) {
              cursor.advance();
              continue;
            }
          }
          break;
        }
        if (!cursor.hasNext()) {
          nextEntry = null;
          return;
        }

        final byte[] groupComposite = cursor.currentLeafPage().getKey(cursor.currentEntryIndex());
        final int prefixLen = groupComposite.length - HOTKeySerializer.CHUNK_IDX_BYTES;

        Roaring64Bitmap merged = null;

        while (cursor.hasNext()) {
          final HOTLeafPage leaf = cursor.currentLeafPage();
          final int idx = cursor.currentEntryIndex();
          final byte[] composite = leaf.getKey(idx);
          if (composite.length != prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES
              || Arrays.compareUnsigned(composite, 0, prefixLen, groupComposite, 0, prefixLen) != 0) {
            break;
          }
          final int chunkIdx = HOTKeySerializer.readChunkIdx(composite, 0, composite.length);
          final byte[] chunkBytes = leaf.getValue(idx);
          if (!NodeReferencesSerializer.isTombstone(chunkBytes, 0, chunkBytes.length)) {
            final NodeReferences chunkRefs = NodeReferencesSerializer.deserialize(chunkBytes);
            final Roaring64Bitmap chunkBitmap = chunkRefs.getNodeKeys();
            if (!chunkBitmap.isEmpty()) {
              if (merged == null) {
                merged = new Roaring64Bitmap();
              }
              final long high = ((long) chunkIdx) << 16;
              final LongIterator bIt = chunkBitmap.getLongIterator();
              while (bIt.hasNext()) {
                merged.add(high | (bIt.next() & 0xFFFFL));
              }
            }
          }
          cursor.advance();
        }

        if (merged != null && !merged.isEmpty()) {
          final K logicalKey = deserializeKey(groupComposite, 0, prefixLen);
          nextEntry = new AbstractMap.SimpleImmutableEntry<>(logicalKey, new NodeReferences(merged));
          return;
        }
      }
    }
  }

  @Override
  protected int serializeKey(K key, byte[] buffer, int offset) {
    return keySerializer.serialize(key, buffer, offset);
  }

  @Override
  protected @Nullable K deserializeKey(byte[] buffer, int offset, int length) {
    return keySerializer.deserialize(buffer, offset, length);
  }

  @Override
  protected int compareKeys(byte[] key1, int offset1, int length1, byte[] key2, int offset2, int length2) {
    return keySerializer.compare(key1, offset1, length1, key2, offset2, length2);
  }

  @Override
  protected byte[] getKeyBuffer() {
    return KEY_BUFFER.get();
  }

  @Override
  protected void setKeyBuffer(byte[] newBuffer) {
    KEY_BUFFER.set(newBuffer);
  }
}
