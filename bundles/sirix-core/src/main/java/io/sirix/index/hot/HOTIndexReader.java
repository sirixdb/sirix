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
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.Roaring64Bitmap;

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
   * {@code MAX_STRING_VALUE_BYTES = 246}) plus the 4-byte chunkIdx trailer, rounded to 512 for
   * headroom. {@link AbstractHOTIndexReader#collectChunksViaLowerBoundWalk} writes that trailer IN
   * PLACE after the logical key and hands the descent the composite length, so a point lookup routes
   * straight out of this buffer with no per-call copy of the seek key.
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
   * @param mode must be {@link SearchMode#EQUAL}; range modes go via
   *        {@link #range(Comparable, Comparable)} / {@link #iteratorFrom(Comparable, boolean)}
   * @return reassembled NodeReferences, or {@code null} if no chunks exist for {@code key}
   * @throws IllegalArgumentException if {@code mode} is not {@link SearchMode#EQUAL}. This parameter
   *         used to be documented as "reserved" and silently ignored, i.e. every mode got the
   *         {@code EQUAL} answer; memoization makes that assumption load-bearing, because the cache
   *         key does not carry the mode and a mode-sensitive answer would be served across modes.
   */
  public @Nullable NodeReferences get(final K key, final SearchMode mode) {
    requireNonNull(key);
    requireEqualMode(mode);

    // Size the buffer BEFORE serializing: checking the returned length afterwards is too late,
    // the write past the end has already happened.
    byte[] keyBuf = getKeyBuffer();
    final int required = maxSerializedKeyLength(key);
    if (required > keyBuf.length) {
      keyBuf = new byte[required];
      setKeyBuffer(keyBuf);
    }
    final int prefixLen = serializeKey(key, keyBuf, 0);
    // Through the cache: for a committed revision the walk below is deterministic, so a key already
    // asked about under this revision is answered without re-walking. A writer-backed reader gets a
    // null cache and always computes — see AbstractHOTIndexReader#lookupCache.
    return pointLookup(keyBuf, prefixLen);
  }

  /**
   * Reassemble all chunk slots whose composite key starts with {@code keyBuf[0..keyLen)}, without
   * consulting or populating the memoization cache.
   *
   * <p>
   * This is the whole of what {@link #get} used to do inline. It carries the leftmost-fallback retry
   * for tries written before the writer enforced I8/I12, which is why the override exists rather than
   * leaving the base class's chunk-walk-only default.
   * </p>
   */
  @Override
  protected @Nullable NodeReferences computePointLookup(final byte[] keyBuf, final int keyLen) {
    final NodeReferences collected = collectChunksViaLowerBoundWalk(keyBuf, keyLen);
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
    final Roaring64Bitmap merged = collectViaLeafWalk(rootRef, keyBuf, keyLen);
    if (merged == null || merged.isEmpty()) {
      return null;
    }
    return NodeReferences.owning(merged);
  }

  /**
   * Phase 7v fallback: walk every leaf in the trie (left-to-right traversal order, NOT lex order —
   * robust against I8 violations) and merge every chunk whose composite carries the prefix. Used only
   * when the primary PEXT-routed lookup returns 0 chunks for a key that IS stored. O(total trie
   * entries) per call; only triggered on a miss, and only when explicitly enabled.
   *
   * <p>
   * An unbounded cursor visits exactly the same slots in the same order, so this shares the one
   * chunk-merge implementation rather than restating it — including its torn-read discipline, which a
   * second copy would have to keep in lockstep by hand.
   */
  private @Nullable Roaring64Bitmap collectViaLeafWalk(PageReference rootRef, byte[] prefixBuf, int prefixLen) {
    try (HOTTrieReader reader = new HOTTrieReader(getStorageEngineReader());
        HOTRangeCursor cursor = reader.range(rootRef, null, null)) {
      return NodeReferencesSerializer.mergeChunksInPrefixRange(cursor, prefixBuf, prefixLen);
    }
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
