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
import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import static java.util.Objects.requireNonNull;

/**
 * Generic HOT index writer for object keys (CASValue, QNm).
 *
 * <p>
 * Stores secondary-index postings in a height-optimized trie. Uses thread-local buffers for
 * zero-allocation key serialization.
 * </p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 * <li>Thread-local byte buffers for key/value serialization</li>
 * <li>No Optional - uses @Nullable returns</li>
 * <li>Pre-allocated traversal state</li>
 * </ul>
 *
 * @param <K> the key type (must implement Comparable)
 * @author Johannes Lichtenberger
 */
public final class HOTIndexWriter<K extends Comparable<? super K>> extends AbstractHOTIndexWriter<K> {

  /**
   * Thread-local buffer for key serialization. Sized to fit the largest CAS prefix (10-byte header +
   * {@code MAX_STRING_VALUE_BYTES = 246}) PLUS {@link HOTKeySerializer#CHUNK_IDX_BYTES} (= 4)
   * chunkIdx trailer = 260 bytes minimum; rounded to 512 for headroom across future serializer
   * changes. Previously 256, which overflowed by 4 bytes whenever the prefix maxed out (regression
   * caught by {@code JsonIntegrationTest.testCreateAndScanCASIndex3} via long-string CAS values).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[512]);

  /**
   * Thread-local single-bit chunk-payload {@link NodeReferences} reused across writes to avoid
   * per-call {@code Roaring64Bitmap} allocation. {@link #addNodeKeyToChunk(Comparable, long)} clears
   * the bitmap, sets one bit, and serialises into {@link #lastSerializedValueBuf}.
   */
  private static final ThreadLocal<NodeReferences> SINGLE_BIT_REFS = ThreadLocal.withInitial(NodeReferences::new);

  private final HOTKeySerializer<K> keySerializer;

  /** Lazy reader for chunked-bitmap reassembly during {@link #get} / range scans. */
  private @Nullable HOTTrieReader chunkReader;

  /**
   * Private constructor.
   *
   * @param storageEngineWriter the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  private HOTIndexWriter(StorageEngineWriter storageEngineWriter, HOTKeySerializer<K> keySerializer,
      IndexType indexType, int indexNumber) {
    super(storageEngineWriter, indexType, indexNumber);
    this.keySerializer = requireNonNull(keySerializer);

    // Initialize HOT index tree based on type
    initializeHOTIndex();
  }

  /**
   * Initialize the HOT index tree structure.
   */
  private void initializeHOTIndex() {
    switch (indexType) {
      case PATH -> initializePathIndex();
      case CAS -> initializeCASIndex();
      case NAME -> initializeNameIndex();
      case VALIDTIME -> initializeValidTimeIndex();
      default -> throw new IllegalArgumentException("Unsupported index type for HOT: " + indexType);
    }
  }

  /**
   * Creates a new HOTIndexWriter.
   *
   * @param storageEngineWriter the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType the index type
   * @param indexNumber the index number
   * @param <K> the key type
   * @return a new HOTIndexWriter instance
   */
  public static <K extends Comparable<? super K>> HOTIndexWriter<K> create(StorageEngineWriter storageEngineWriter,
      HOTKeySerializer<K> keySerializer, IndexType indexType, int indexNumber) {
    requireNonNull(storageEngineWriter);
    requireNonNull(indexType);
    HOTIndexNumberValidator.validate(storageEngineWriter, indexType, indexNumber);
    return new HOTIndexWriter<>(storageEngineWriter, keySerializer, indexType, indexNumber);
  }

  /**
   * Index a key-value pair using chunked-bitmap storage.
   *
   * <p>
   * The logical {@link NodeReferences} is split across multiple HOT slots, one per <em>chunk</em>. A
   * chunk holds the low-16 bits of all nodeKeys whose {@code (int)(nodeKey >>> 16)} equals its
   * chunkIdx. The HOT key for a chunk is the composite {@code prefix(key) ‖ chunkIdx_be4}; see
   * {@link HOTKeySerializer#serializeWithChunkIdx}.
   * </p>
   *
   * <h3>Why chunk?</h3>
   * <p>
   * Per-revision write cost grows with the size of the slot value rewritten on update. Without
   * chunking, every commit that touches a single nodeKey on a popular logical key rewrites the whole
   * bitmap (potentially MBs). With chunking, only the one Roaring chunk of the modified nodeKey is
   * rewritten — typical chunk size is a few hundred bytes.
   * </p>
   *
   * <p>
   * If the chunk slot already exists, {@link HOTLeafPage#mergeWithNodeRefs} handles the OR-merge of
   * the new bit into the existing chunk's bitmap; failure paths (page split / compact) are inherited
   * unchanged from the per-slot write.
   * </p>
   *
   * @param key the logical index key (e.g. a {@code QNm} for NAME, a {@code CASValue} for CAS)
   * @param value the node references
   * @return the supplied node references after all contained node keys have been indexed
   */
  public NodeReferences index(K key, NodeReferences value) {
    requireNonNull(key);
    requireNonNull(value);

    final Roaring64Bitmap bitmap = value.getNodeKeys();
    if (bitmap.isEmpty()) {
      return value;
    }
    final LongIterator it = bitmap.getLongIterator();
    while (it.hasNext()) {
      addNodeKeyToChunk(key, it.next());
    }
    return value;
  }

  /**
   * Add a single nodeKey to {@code key}'s chunked bitmap.
   *
   * <p>
   * Equivalent to {@link #index(Comparable, NodeReferences)} with a one-element
   * {@link NodeReferences}, minus the {@code Roaring64Bitmap} allocation: the slot write is an
   * OR-merge ({@link HOTLeafPage#mergeWithNodeRefs}), so a caller that only wants to ADD one
   * reference never has to materialise — let alone read back — the references already stored under
   * {@code key}.
   * </p>
   *
   * @param key the logical index key
   * @param nodeKey the node key to add; must be in {@code [0, 2^48)}
   */
  public void indexNodeKey(K key, long nodeKey) {
    requireNonNull(key);
    addNodeKeyToChunk(key, nodeKey);
  }

  /**
   * A loader that collects {@code (key, nodeKey)} pairs and materialises the whole index in one
   * {@link HOTBulkBuilder} pass — the right shape for building an index over an already-shredded
   * revision.
   *
   * <p>
   * Only valid while the index tree is still empty ({@link #isEmptyTree()}): the loader
   * <em>replaces</em> the root rather than merging into it. Callers that may run against a populated
   * tree must check first and fall back to {@link #indexNodeKey(Comparable, long)}.
   * </p>
   *
   * @return a fresh bulk loader bound to this writer
   */
  public HOTBulkIndexLoader<K> createBulkLoader() {
    return new HOTBulkIndexLoader<>(this, keySerializer);
  }

  /**
   * Add one nodeKey to its chunk slot. Chunked-bitmap write hot path.
   *
   * <p>
   * Builds {@code prefix(key) ‖ chunkIdx_be4} where {@code chunkIdx = (int)(nodeKey >>> 16)}, encodes
   * a single-bit {@link NodeReferences} containing {@code nodeKey & 0xFFFF}, and calls the inherited
   * {@link AbstractHOTIndexWriter#doIndex} which delegates to {@link HOTLeafPage#mergeWithNodeRefs}
   * (OR-merge with any pre-existing chunk).
   * </p>
   */
  private void addNodeKeyToChunk(K key, long nodeKey) {
    AbstractHOTIndexWriter.checkNodeKeyRange(nodeKey);

    final int chunkIdx = (int) (nodeKey >>> 16);
    final long bit16 = nodeKey & 0xFFFFL;

    final byte[] keyBuf = chunkedKeyBuffer(key);
    final int compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);

    // Reusable single-bit payload — clear, set, serialize. Avoids per-call bitmap allocation.
    final NodeReferences singleBit = SINGLE_BIT_REFS.get();
    final Roaring64Bitmap singleBitmap = singleBit.getNodeKeys();
    singleBitmap.clear();
    singleBitmap.add(bit16);
    serializeValueInto(singleBit);

    doIndex(keyBuf, compLen, lastSerializedValueBuf, lastSerializedValueLen);
  }

  /**
   * Reassemble all chunks of a logical key into a single {@link NodeReferences}.
   *
   * <p>
   * Range-scans composite keys in {@code [(prefix, 0), (prefix, 0xFFFFFFFF)]} via
   * {@link HOTTrieReader#lowerBound} (Phase 0b — Binna §4.2) so the seek is O(tree-height) even when
   * the smallest existing chunkIdx for {@code key} is {@code > 0}. For every matching chunk slot the
   * value bitmap is decoded and each bit16 is expanded to a full 64-bit nodeKey via
   * {@code (chunkIdx << 16) | bit16}.
   * </p>
   *
   * @param key the logical index key
   * @param mode must be {@link SearchMode#EQUAL}; range modes go through the reader's
   *        {@code range}/{@code iteratorFrom} APIs
   * @return reassembled NodeReferences, or {@code null} if no chunks exist for {@code key}
   * @throws IllegalArgumentException if {@code mode} is not {@link SearchMode#EQUAL}. This parameter
   *         used to be documented as advisory and silently ignored, i.e. every mode got the
   *         {@code EQUAL} answer; the guard is shared with {@link HOTIndexReader#get} so a caller
   *         cannot learn a different contract from whichever of the two twins it happened to pick.
   */
  public @Nullable NodeReferences get(final K key, final SearchMode mode) {
    requireNonNull(key);
    // Same contract as the reader's get, enforced through the reader's helper so there is ONE copy of
    // the rule. A writer-backed lookup is never memoized, so the cache-key argument does not apply
    // here — but this method ignores `mode` exactly as the reader's did, and leaving the twin
    // unguarded is how a caller learns the restriction from whichever of the two it happened to pick.
    AbstractHOTIndexReader.requireEqualMode(mode);

    final byte[] keyBuf = prefixKeyBuffer(key);
    final int prefixLen = keySerializer.serialize(key, keyBuf, 0);
    return reassembleChunksForPrefix(keyBuf, prefixLen);
  }

  /**
   * Visible to {@link AbstractHOTIndexWriter} sub-paths and internal helpers — assemble the
   * NodeReferences for a prefix already in {@code prefixBuf[0..prefixLen)}.
   */
  private @Nullable NodeReferences reassembleChunksForPrefix(byte[] prefixBuf, int prefixLen) {
    final PageReference rootRef = rootReference;
    if (rootRef == null) {
      return null;
    }

    final byte[] fromBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(prefixBuf, 0, fromBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(fromBytes, prefixLen, 0);

    final byte[] toBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(prefixBuf, 0, toBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(toBytes, prefixLen, 0xFFFFFFFF);

    if (chunkReader == null) {
      chunkReader = new HOTTrieReader(storageEngineWriter);
    }
    // The sweep reads UNPINNED leaves under optimistic stamps — the shared helper validates each
    // slot's copies against the cursor's leaf stamp before anything reaches the deserializer.
    final Roaring64Bitmap merged;
    try (HOTRangeCursor cursor = chunkReader.range(rootRef, fromBytes, toBytes)) {
      merged = NodeReferencesSerializer.mergeChunksInPrefixRange(cursor, prefixBuf, prefixLen);
    }
    if (merged == null || merged.isEmpty()) {
      return null;
    }
    return NodeReferences.owning(merged);
  }

  /**
   * Remove a single nodeKey from the chunked bitmap of {@code key}.
   *
   * <p>
   * Locates the chunk slot {@code (prefix, (int)(nodeKey >>> 16))}, deserializes the chunk bitmap,
   * removes {@code nodeKey & 0xFFFF}, re-serializes (or tombstones if the chunk is now empty). Other
   * chunks of the same logical key are untouched — slot-granular CoW ensures the other chunks do not
   * even appear in the new revision's TIL fragment.
   * </p>
   *
   * @return true if a bit was actually cleared, false if absent
   */
  public boolean remove(K key, long nodeKey) {
    requireNonNull(key);
    AbstractHOTIndexWriter.checkNodeKeyRange(nodeKey);

    final int chunkIdx = (int) (nodeKey >>> 16);
    final long bit16 = nodeKey & 0xFFFFL;

    final byte[] keyBuf = chunkedKeyBuffer(key);
    final int compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);

    return doRemovePostingBit(keyBuf, compLen, bit16);
  }

  /**
   * The thread-local key buffer, grown first if {@code key} could need more than it currently holds.
   *
   * <p>
   * Sizing has to happen BEFORE the write. The previous shape serialized into the buffer and only
   * then compared the returned length against {@code buffer.length} — by which point a key larger
   * than the buffer had already been written past its end. Only NAME keys can reach that: a CAS key
   * is bounded by a constant well under the buffer, but a local name is raw UTF-8 of whatever the
   * document called the field.
   * </p>
   *
   * @param key the key about to be serialized
   * @return a buffer with room for {@code key}'s prefix
   */
  private byte[] prefixKeyBuffer(final K key) {
    return keyBufferOfAtLeast(keySerializer.maxSerializedLength(key));
  }

  /** As {@link #prefixKeyBuffer}, with room for the 4-byte chunkIdx trailer as well. */
  private byte[] chunkedKeyBuffer(final K key) {
    return keyBufferOfAtLeast(keySerializer.maxSerializedLength(key) + HOTKeySerializer.CHUNK_IDX_BYTES);
  }

  private static byte[] keyBufferOfAtLeast(final int required) {
    byte[] keyBuf = KEY_BUFFER.get();
    if (required > keyBuf.length) {
      keyBuf = new byte[required];
      KEY_BUFFER.set(keyBuf);
    }
    return keyBuf;
  }

  @Override
  protected byte[] getKeyBuffer() {
    return KEY_BUFFER.get();
  }

  @Override
  protected void setKeyBuffer(byte[] newBuffer) {
    KEY_BUFFER.set(newBuffer);
  }

  @Override
  protected int serializeKey(K key, byte[] buffer, int offset) {
    return keySerializer.serialize(key, buffer, offset);
  }
}
