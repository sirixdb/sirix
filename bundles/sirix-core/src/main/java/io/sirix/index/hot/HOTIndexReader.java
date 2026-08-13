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

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

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
   * Thread-local buffer for key serialization. Sized to fit the largest CAS prefix (10-byte header +
   * {@code MAX_STRING_VALUE_BYTES = 246}), rounded to 512 for headroom. Only the LOGICAL key is ever
   * written here — the chunkIdx trailer is appended into a right-sized seek array by
   * {@link AbstractHOTIndexReader#collectChunksViaLowerBoundWalk}, never in place, because the trie
   * descent takes the search key's length from the array it is handed.
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[512]);

  /**
   * Whether a lookup miss retries with the O(index) leftmost leaf walk. Resolved once at class load
   * (it was a synchronized system-Properties lookup on every miss).
   *
   * <p>
   * Historically this was ON by default and switched off via
   * {@code hot.cas.leftmostfallback.disable}; it is now opt-in through
   * {@code hot.cas.leftmostfallback.enable}, because the writer enforces I8/I12 pre-commit and the
   * walk costs a whole-index scan on EVERY miss (~2.5 ms against ~2 us for the primary path on a
   * 33K-key index). The retired {@code .disable} property is deliberately NOT consulted: reading it
   * would make {@code .disable=false} — the value that used to mean "leave the default alone" —
   * silently turn the whole-index scan back on.
   */
  private static final boolean LEFTMOST_FALLBACK_ENABLED = Boolean.getBoolean("hot.cas.leftmostfallback.enable");

  private final HOTKeySerializer<K> keySerializer;

  /**
   * Private constructor.
   *
   * @param storageEngineReader the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  private HOTIndexReader(StorageEngineReader storageEngineReader, HOTKeySerializer<K> keySerializer,
      IndexType indexType, int indexNumber) {
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
   * <p>
   * Chunked-bitmap storage (Phase 1+2): the logical bitmap for {@code key} is split across multiple
   * HOT slots, one per chunkIdx = {@code (int)(nodeKey >>> 16)}. This method range-scans
   * {@code [(prefix, 0), (prefix, 0xFFFFFFFF)]} via Phase 0b's {@link HOTTrieReader#lowerBound} and
   * merges every chunk's low-16-bit bitmap into a single 64-bit {@code Roaring64Bitmap}, expanding
   * bit16 → {@code (chunkIdx << 16) | bit16}.
   * </p>
   *
   * @param key the logical index key
   * @param mode reserved (only {@code EQUAL} is meaningful for {@code get}); range modes go via
   *        {@link #range(Comparable, Comparable)} / {@link #iteratorFrom(Comparable, boolean)}
   * @return reassembled NodeReferences, or {@code null} if no chunks exist for {@code key}
   */
  public @Nullable NodeReferences get(K key, SearchMode mode) {
    requireNonNull(key);

    // Size the buffer BEFORE serializing: checking the returned length afterwards is too late,
    // the write past the end has already happened.
    byte[] keyBuf = getKeyBuffer();
    final int required = maxSerializedKeyLength(key);
    if (required > keyBuf.length) {
      keyBuf = new byte[required];
      setKeyBuffer(keyBuf);
    }
    final int prefixLen = serializeKey(key, keyBuf, 0);
    return reassembleChunksForPrefix(keyBuf, prefixLen);
  }

  /**
   * Internal helper: reassemble all chunk slots whose composite key starts with
   * {@code prefixBuf[0..prefixLen)}. Shared by {@link #get} and the range iterators.
   */
  private @Nullable NodeReferences reassembleChunksForPrefix(byte[] prefixBuf, int prefixLen) {
    final NodeReferences collected = collectChunksViaLowerBoundWalk(prefixBuf, prefixLen);
    if (collected != null) {
      return collected;
    }

    if (!LEFTMOST_FALLBACK_ENABLED) {
      return null;
    }
    // Phase 7v retry for tries written before the writer enforced I8/I12: a full leaf-walk scan
    // robust against non-lex-order leaves.
    final PageReference rootRef = getRootReference();
    if (rootRef == null) {
      return null;
    }
    final Roaring64Bitmap merged = collectViaLeafWalk(rootRef, prefixBuf, prefixLen);
    if (merged == null || merged.isEmpty()) {
      return null;
    }
    return NodeReferences.owning(merged);
  }

  /**
   * Phase 7v fallback: walk every leaf in the trie (left-to-right traversal order, NOT lex order —
   * robust against I8 violations), filter each entry by exact prefix match. Used only when the
   * primary PEXT-routed cursor returns 0 chunks for a key that is in fact stored. O(total trie
   * entries) per call; only triggered on miss.
   */
  private @Nullable Roaring64Bitmap collectViaLeafWalk(PageReference rootRef, byte[] prefixBuf, int prefixLen) {
    final int compositeLen = prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES;
    Roaring64Bitmap merged = null;
    try (HOTTrieReader reader = new HOTTrieReader(getStorageEngineReader())) {
      HOTLeafPage leaf = reader.navigateToLeftmostLeaf(rootRef);
      int tornRounds = 0;
      while (leaf != null) {
        int idx = 0;
        while (true) {
          // One entry's read batch (or the end-of-leaf decision) against the UNPINNED leaf,
          // validated before any effect: the chunk copy reaches the deserializer and the merge
          // only after its bytes are proven stable, so `merged` never absorbs torn state and
          // recovery is a same-position re-read on a refreshed copy.
          boolean leafDone = false;
          byte[] composite = null;
          byte[] chunkBytes = null;
          try {
            if (idx >= leaf.getEntryCount()) {
              leafDone = true;
            } else {
              final byte[] candidate = leaf.getKey(idx);
              if (candidate != null && candidate.length == compositeLen
                  && Arrays.compareUnsigned(candidate, 0, prefixLen, prefixBuf, 0, prefixLen) == 0) {
                composite = candidate;
                chunkBytes = leaf.getValue(idx);
              }
            }
          } catch (RuntimeException e) {
            if (reader.validateCurrentLeaf()) {
              throw e; // stable bytes — genuine corruption, not a torn read
            }
            leaf = refreshTornLeaf(reader, ++tornRounds);
            continue;
          }
          if (!reader.validateCurrentLeaf()) {
            leaf = refreshTornLeaf(reader, ++tornRounds);
            continue;
          }
          tornRounds = 0;
          if (leafDone) {
            break;
          }
          if (chunkBytes != null) {
            merged = mergeChunk(merged, composite, chunkBytes);
          }
          idx++;
        }
        leaf = reader.advanceToNextLeaf();
      }
    }
    return merged;
  }

  /** Bounded torn-read recovery: reload the current leaf in place and re-adopt the fresh copy. */
  private static HOTLeafPage refreshTornLeaf(final HOTTrieReader reader, final int round) {
    if (round > 64) {
      throw new IllegalStateException(
          "HOT leaf walk failed stamp validation on 64 consecutive rounds — sustained allocator thrashing");
    }
    if (!reader.refreshCurrentLeaf()) {
      throw new IllegalStateException("HOT leaf walk: evicted leaf could not be reloaded through its PageReference");
    }
    return reader.currentLeafPage();
  }

  /** Merge one VALIDATED chunk payload copy into the accumulator bitmap. */
  private @Nullable Roaring64Bitmap mergeChunk(@Nullable Roaring64Bitmap merged, byte[] composite, byte[] chunkBytes) {
    // Unsigned, as everywhere else the chunk trailer is expanded.
    final long chunkIdx = HOTKeySerializer.readChunkIdx(composite, 0, composite.length) & 0xFFFFFFFFL;
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
    final long high = chunkIdx << 16;
    final LongIterator bIt = chunkBitmap.getLongIterator();
    while (bIt.hasNext()) {
      merged.add(high | (bIt.next() & 0xFFFFL));
    }
    return merged;
  }

  /**
   * Create a range iterator over logical entries with keys in {@code [fromKey, toKey]}, both
   * inclusive.
   *
   * @param fromKey start key (inclusive)
   * @param toKey end key (inclusive)
   */
  public Iterator<Map.Entry<K, NodeReferences>> range(K fromKey, K toKey) {
    return range(fromKey, toKey, true, true);
  }

  /**
   * Create a range iterator over logical entries between {@code fromKey} and {@code toKey} with
   * explicit bound inclusivity.
   *
   * <p>
   * Inclusivity is enforced by the cursor itself, on each group's logical key bytes — see
   * {@link ChunkAggregatingIterator}. Callers must NOT post-filter positionally: index keys are not
   * prefix-free, so the composite byte window is wider than the logical range.
   *
   * @param fromKey start key
   * @param toKey end key
   * @param fromInclusive whether {@code fromKey} itself is in range
   * @param toInclusive whether {@code toKey} itself is in range
   */
  public Iterator<Map.Entry<K, NodeReferences>> range(K fromKey, K toKey, boolean fromInclusive, boolean toInclusive) {
    requireNonNull(fromKey);
    requireNonNull(toKey);
    final byte[] fromPrefix = serializeKeyToArray(fromKey);
    final byte[] toPrefix = serializeKeyToArray(toKey);
    return new ChunkAggregatingIterator(fromPrefix, fromInclusive, toPrefix, toInclusive);
  }

  /**
   * Iterate every logical entry in the index. Overrides the abstract base's per-slot iterator — with
   * chunked-bitmap storage, "all entries" means one logical {@link Map.Entry} per prefix, not per
   * chunk slot.
   */
  @Override
  public Iterator<Map.Entry<K, NodeReferences>> iterator() {
    return new ChunkAggregatingIterator(null, true, null, true);
  }

  /**
   * Iterate every logical entry above {@code fromKey}, inclusive or not. Used for {@code GREATER} /
   * {@code GREATER_OR_EQUAL} CAS queries.
   */
  public Iterator<Map.Entry<K, NodeReferences>> iteratorFrom(K fromKey, boolean inclusive) {
    requireNonNull(fromKey);
    final byte[] fromPrefix = serializeKeyToArray(fromKey);
    return new ChunkAggregatingIterator(fromPrefix, inclusive, null, true);
  }

  /** Iterate every logical entry below {@code toKey}, inclusive or not. */
  public Iterator<Map.Entry<K, NodeReferences>> iteratorTo(K toKey, boolean inclusive) {
    requireNonNull(toKey);
    final byte[] toPrefix = serializeKeyToArray(toKey);
    return new ChunkAggregatingIterator(null, true, toPrefix, inclusive);
  }

  @Override
  protected int serializeKey(K key, byte[] buffer, int offset) {
    return keySerializer.serialize(key, buffer, offset);
  }

  @Override
  protected int maxSerializedKeyLength(K key) {
    return keySerializer.maxSerializedLength(key);
  }

  @Override
  protected @Nullable K deserializeKey(byte[] buffer, int offset, int length) {
    return keySerializer.deserialize(buffer, offset, length);
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
