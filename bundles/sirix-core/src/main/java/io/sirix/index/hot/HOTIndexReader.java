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

import java.lang.foreign.MemorySegment;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

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
    return NodeReferences.adopt(merged);
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

  /**
   * Merge one chunk into {@code merged}, reading its keys straight off the page.
   *
   * <p>This is the point-lookup ({@code get}) path, so it is the hottest of the three. It used to
   * copy the chunk value to the heap and build a {@link NodeReferences} — hence a
   * {@link Roaring64Bitmap}, which that constructor clones — purely to walk it into {@code merged}.
   * {@code mergeChunkInto} reads the packed form in place instead.
   */
  private @Nullable Roaring64Bitmap mergeChunk(@Nullable Roaring64Bitmap merged, HOTLeafPage leaf,
      int idx, byte[] composite) {
    final int chunkIdx = HOTKeySerializer.readChunkIdx(composite, 0, composite.length);
    final MemorySegment chunkValue = leaf.getValueSlice(idx);
    if (NodeReferencesSerializer.isTombstone(chunkValue)) {
      return merged;
    }
    final Roaring64Bitmap dest = merged == null ? new Roaring64Bitmap() : merged;
    final boolean added = NodeReferencesSerializer.mergeChunkInto(chunkValue,
        ((long) chunkIdx) << 16, dest);
    // Preserve the old contract exactly: an empty chunk must not turn a null merged into a
    // non-null empty one, or callers testing `merged == null` would change behaviour.
    return added || merged != null ? dest : null;
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

    return new ChunkAggregatingIterator(fromComposite, toComposite, fromPrefix, null);
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
    return new ChunkAggregatingIterator(new byte[0], null, null, null);
  }

  /**
   * Full-trie iterator that discards non-matching groups <em>before</em> materializing their values.
   *
   * <p>Every consumer of {@link #iterator()} in the tree (name/CAS index scans) pulls an entry and
   * then tests its KEY, discarding the value untouched on a miss. Doing that outside the iterator
   * means each rejected group still pays the full value cost: one heap copy plus one
   * {@link NodeReferences} plus one {@link Roaring64Bitmap} <em>per chunk</em>, merged into another
   * bitmap, which {@code new NodeReferences(merged)} then clones. All of it immediately garbage.
   *
   * <p>Passing the predicate in lets the group be skipped as soon as its logical key is known, so a
   * rejected group costs one key deserialization and a cursor walk — no value bytes are read and no
   * bitmap is built. Unfiltered scans are unaffected: the key is deserialized once per group either
   * way, just earlier.
   *
   * @param keyFilter predicate on the logical key; {@code null} accepts everything
   * @return iterator over the matching key-value pairs
   */
  public Iterator<Map.Entry<K, NodeReferences>> iterator(final @Nullable Predicate<? super K> keyFilter) {
    return new ChunkAggregatingIterator(new byte[0], null, null, keyFilter);
  }

  /**
   * Values-only scan: emits each matching group's {@link NodeReferences} without ever building a
   * {@link Map.Entry}.
   *
   * <p>Every production consumer of {@link #iterator(Predicate)} — the name and CAS index scans —
   * uses the key solely to decide whether to keep the group, then calls {@code getValue()} and
   * discards the key. With the predicate already applied inside the iterator, the entry object and
   * the escaping key are both pure plumbing: one {@code SimpleImmutableEntry} allocated per
   * emitted group, plus a key that is garbage the moment the filter returns.
   *
   * <p>This yields the values directly. The key is still deserialized (the predicate needs it) but
   * never escapes, and no entry is allocated. Callers also stop needing their own unwrapping
   * iterator.
   *
   * @param keyFilter predicate on the logical key; {@code null} accepts everything
   * @return iterator over the matching groups' references
   */
  public Iterator<NodeReferences> valueIterator(final @Nullable Predicate<? super K> keyFilter) {
    final ChunkAggregatingIterator entries =
        new ChunkAggregatingIterator(new byte[0], null, null, keyFilter);
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return entries.hasNext();
      }

      @Override
      public NodeReferences next() {
        return entries.takeValue();
      }
    };
  }

  public Iterator<Map.Entry<K, NodeReferences>> iteratorFrom(K fromKey) {
    requireNonNull(fromKey);

    byte[] keyBuf = getKeyBuffer();
    int fromLen = serializeKey(fromKey, keyBuf, 0);
    byte[] fromPrefix = Arrays.copyOf(keyBuf, fromLen);

    final byte[] fromComposite = new byte[fromLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(fromPrefix, 0, fromComposite, 0, fromLen);
    HOTKeySerializer.writeChunkIdxBE(fromComposite, fromLen, 0);

    return new ChunkAggregatingIterator(fromComposite, null, fromPrefix, null);
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
    /** Key predicate applied BEFORE a group's values are read; {@code null} accepts everything. */
    private final @Nullable Predicate<? super K> keyFilter;
    /**
     * The pending group, held as key + value rather than a {@link Map.Entry}: the values-only
     * consumers must not pay for an entry object they immediately unwrap. The entry is built
     * lazily, only by {@link #next()}.
     */
    private @Nullable K nextKey;
    private @Nullable NodeReferences nextValue;

    ChunkAggregatingIterator(byte[] fromComposite, byte @Nullable [] toComposite,
        byte @Nullable [] fromPrefixFilter, @Nullable Predicate<? super K> keyFilter) {
      this.fromPrefixFilter = fromPrefixFilter;
      this.keyFilter = keyFilter;
      final PageReference rootRef = getRootReference();
      if (rootRef == null) {
        // Empty trie — no entries.
        this.trieReader = null;
        this.cursor = null;
        this.nextKey = null;
        this.nextValue = null;
        return;
      }
      this.trieReader = new HOTTrieReader(getStorageEngineReader());
      this.cursor = trieReader.range(rootRef, fromComposite, toComposite);
      advance();
    }

    @Override
    public boolean hasNext() {
      return nextValue != null;
    }

    @Override
    public Map.Entry<K, NodeReferences> next() {
      final NodeReferences value = takeValue();
      return new AbstractMap.SimpleImmutableEntry<>(lastKey, value);
    }

    /** The key of the group most recently returned by {@link #takeValue()}. */
    private @Nullable K lastKey;

    /** Consume the pending group's value, advancing the scan. No entry object is allocated. */
    NodeReferences takeValue() {
      if (nextValue == null) {
        throw new NoSuchElementException();
      }
      final NodeReferences result = nextValue;
      lastKey = nextKey;
      advance();
      if (nextValue == null) {
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
        nextKey = null;
        nextValue = null;
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
          nextKey = null;
          nextValue = null;
          return;
        }

        final byte[] groupComposite = cursor.currentLeafPage().getKey(cursor.currentEntryIndex());
        final int prefixLen = groupComposite.length - HOTKeySerializer.CHUNK_IDX_BYTES;

        // Resolve the logical key FIRST. A rejected group is then skipped without reading a single
        // value byte or building a single bitmap — the whole point of taking the predicate here
        // rather than letting the caller filter the emitted entries.
        final K logicalKey = deserializeKey(groupComposite, 0, prefixLen);
        if (keyFilter != null && (logicalKey == null || !keyFilter.test(logicalKey))) {
          skipGroup(groupComposite, prefixLen);
          continue;
        }

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
          // Zero-copy: read the chunk's keys straight off the page and add them to the merge
          // bitmap. The old path copied the value to the heap, built a NodeReferences (allocating
          // a Roaring64Bitmap, which its constructor then cloned), and walked a LongIterator to
          // re-add every key — three allocations per chunk to move a handful of longs.
          final MemorySegment chunkValue = leaf.getValueSlice(idx);
          if (!NodeReferencesSerializer.isTombstone(chunkValue)) {
            if (merged == null) {
              merged = new Roaring64Bitmap();
            }
            NodeReferencesSerializer.mergeChunkInto(chunkValue, ((long) chunkIdx) << 16, merged);
          }
          cursor.advance();
        }

        if (merged != null && !merged.isEmpty()) {
          nextKey = logicalKey;
          nextValue = NodeReferences.adopt(merged);
          return;
        }
      }
    }

    /** Advance the cursor past every remaining chunk of the group identified by its prefix. */
    private void skipGroup(final byte[] groupComposite, final int prefixLen) {
      while (cursor.hasNext()) {
        final byte[] composite = cursor.currentLeafPage().getKey(cursor.currentEntryIndex());
        if (composite.length != prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES
            || Arrays.compareUnsigned(composite, 0, prefixLen, groupComposite, 0, prefixLen) != 0) {
          return;
        }
        cursor.advance();
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
