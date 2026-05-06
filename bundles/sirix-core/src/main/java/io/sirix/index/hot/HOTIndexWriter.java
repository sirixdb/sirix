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
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.jspecify.annotations.Nullable;
import org.roaringbitmap.longlong.LongIterator;
import org.roaringbitmap.longlong.Roaring64Bitmap;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Generic HOT index writer for object keys (CASValue, QNm).
 *
 * <p>
 * Replaces {@link io.sirix.index.redblacktree.RBTreeWriter} for HOT-based secondary indexes. Uses
 * thread-local buffers for zero-allocation key serialization.
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
   * Thread-local buffer for key serialization (256 bytes default).
   * Sized to fit prefix + {@link HOTKeySerializer#CHUNK_IDX_BYTES}.
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[256]);

  /**
   * Thread-local single-bit chunk-payload {@link NodeReferences} reused across writes to avoid
   * per-call {@code Roaring64Bitmap} allocation. {@link #addNodeKeyToChunk(Comparable, long)} clears
   * the bitmap, sets one bit, and serialises into {@link #lastSerializedValueBuf}.
   */
  private static final ThreadLocal<NodeReferences> SINGLE_BIT_REFS = ThreadLocal.withInitial(NodeReferences::new);

  /**
   * Cap on a permitted nodeKey for chunked-bitmap storage. The chunkIdx is stored as a 32-bit
   * big-endian unsigned int trailer; with {@code chunkIdx = (int)(nodeKey >>> 16)} this gives a
   * full 48-bit nodeKey range — well above any practical Sirix dataset.
   */
  private static final long MAX_NODE_KEY = (1L << 48) - 1L;

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
  private HOTIndexWriter(StorageEngineWriter storageEngineWriter, HOTKeySerializer<K> keySerializer, IndexType indexType,
      int indexNumber) {
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
    return new HOTIndexWriter<>(storageEngineWriter, keySerializer, indexType, indexNumber);
  }

  /**
   * Index a key-value pair using chunked-bitmap storage.
   *
   * <p>The logical {@link NodeReferences} is split across multiple HOT slots, one per
   * <em>chunk</em>. A chunk holds the low-16 bits of all nodeKeys whose
   * {@code (int)(nodeKey >>> 16)} equals its chunkIdx. The HOT key for a chunk is the composite
   * {@code prefix(key) ‖ chunkIdx_be4}; see {@link HOTKeySerializer#serializeWithChunkIdx}.</p>
   *
   * <h3>Why chunk?</h3>
   * <p>Per-revision write cost grows with the size of the slot value rewritten on update.
   * Without chunking, every commit that touches a single nodeKey on a popular logical key
   * rewrites the whole bitmap (potentially MBs). With chunking, only the one Roaring chunk
   * of the modified nodeKey is rewritten — typical chunk size is a few hundred bytes.</p>
   *
   * <p>If the chunk slot already exists, {@link HOTLeafPage#mergeWithNodeRefs} handles the
   * OR-merge of the new bit into the existing chunk's bitmap; failure paths (page split /
   * compact) are inherited unchanged from the per-slot write.</p>
   *
   * @param key the logical index key (e.g. a {@code QNm} for NAME, a {@code CASValue} for CAS)
   * @param value the node references
   * @param move cursor movement mode (ignored for HOT)
   * @return the indexed value (returned unchanged for API parity with {@code RBTreeWriter})
   */
  public NodeReferences index(K key, NodeReferences value, RBTreeReader.MoveCursor move) {
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
   * Add one nodeKey to its chunk slot. Chunked-bitmap write hot path.
   *
   * <p>Builds {@code prefix(key) ‖ chunkIdx_be4} where
   * {@code chunkIdx = (int)(nodeKey >>> 16)}, encodes a single-bit
   * {@link NodeReferences} containing {@code nodeKey & 0xFFFF}, and calls the inherited
   * {@link AbstractHOTIndexWriter#doIndex} which delegates to {@link HOTLeafPage#mergeWithNodeRefs}
   * (OR-merge with any pre-existing chunk).</p>
   */
  private void addNodeKeyToChunk(K key, long nodeKey) {
    if (nodeKey < 0L) {
      throw new IllegalArgumentException("nodeKey must be non-negative: " + nodeKey);
    }
    if (nodeKey > MAX_NODE_KEY) {
      throw new IllegalArgumentException("nodeKey " + nodeKey
          + " exceeds chunked-bitmap range (max " + MAX_NODE_KEY + ")");
    }

    final int chunkIdx = (int) (nodeKey >>> 16);
    final long bit16 = nodeKey & 0xFFFFL;

    byte[] keyBuf = KEY_BUFFER.get();
    int compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);
    if (compLen > keyBuf.length) {
      keyBuf = new byte[compLen];
      KEY_BUFFER.set(keyBuf);
      compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);
    }

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
   * <p>Range-scans composite keys in {@code [(prefix, 0), (prefix, 0xFFFFFFFF)]} via
   * {@link HOTTrieReader#lowerBound} (Phase 0b — Binna §4.2) so the seek is O(tree-height)
   * even when the smallest existing chunkIdx for {@code key} is {@code > 0}. For every
   * matching chunk slot the value bitmap is decoded and each bit16 is expanded to a full
   * 64-bit nodeKey via {@code (chunkIdx << 16) | bit16}.</p>
   *
   * @param key  the logical index key
   * @param mode the search mode (only {@code EQUAL} is meaningful here; range modes go through
   *             the reader's {@code range}/{@code iteratorFrom} APIs)
   * @return reassembled NodeReferences, or {@code null} if no chunks exist for {@code key}
   */
  public @Nullable NodeReferences get(K key, SearchMode mode) {
    requireNonNull(key);

    byte[] keyBuf = KEY_BUFFER.get();
    int prefixLen = keySerializer.serialize(key, keyBuf, 0);
    if (prefixLen > keyBuf.length) {
      keyBuf = new byte[prefixLen];
      KEY_BUFFER.set(keyBuf);
      prefixLen = keySerializer.serialize(key, keyBuf, 0);
    }
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
    Roaring64Bitmap merged = null;
    try (HOTRangeCursor cursor = chunkReader.range(rootRef, fromBytes, toBytes)) {
      while (cursor.hasNext()) {
        final HOTLeafPage leaf = cursor.currentLeafPage();
        final int idx = cursor.currentEntryIndex();
        final byte[] composite = leaf.getKey(idx);
        if (composite.length != prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES
            || Arrays.compareUnsigned(composite, 0, prefixLen, prefixBuf, 0, prefixLen) != 0) {
          // Defensive — toKey already bounds the cursor to the prefix range.
          cursor.advance();
          continue;
        }
        final int chunkIdx = HOTKeySerializer.readChunkIdx(composite, 0, composite.length);
        final byte[] chunkBytes = leaf.getValue(idx);
        if (NodeReferencesSerializer.isTombstone(chunkBytes, 0, chunkBytes.length)) {
          cursor.advance();
          continue;
        }
        final NodeReferences chunkRefs = NodeReferencesSerializer.deserialize(chunkBytes);
        final Roaring64Bitmap chunkBitmap = chunkRefs.getNodeKeys();
        if (chunkBitmap.isEmpty()) {
          cursor.advance();
          continue;
        }
        if (merged == null) {
          merged = new Roaring64Bitmap();
        }
        final long high = ((long) chunkIdx) << 16;
        final LongIterator bIt = chunkBitmap.getLongIterator();
        while (bIt.hasNext()) {
          merged.add(high | (bIt.next() & 0xFFFFL));
        }
        cursor.advance();
      }
    }
    if (merged == null || merged.isEmpty()) {
      return null;
    }
    return new NodeReferences(merged);
  }

  /**
   * Remove a single nodeKey from the chunked bitmap of {@code key}.
   *
   * <p>Locates the chunk slot {@code (prefix, (int)(nodeKey >>> 16))}, deserializes the chunk
   * bitmap, removes {@code nodeKey & 0xFFFF}, re-serializes (or tombstones if the chunk is now
   * empty). Other chunks of the same logical key are untouched — slot-granular CoW ensures the
   * other chunks do not even appear in the new revision's TIL fragment.</p>
   *
   * @return true if a bit was actually cleared, false if absent
   */
  public boolean remove(K key, long nodeKey) {
    requireNonNull(key);
    if (nodeKey < 0L) {
      throw new IllegalArgumentException("nodeKey must be non-negative: " + nodeKey);
    }
    if (nodeKey > MAX_NODE_KEY) {
      throw new IllegalArgumentException("nodeKey " + nodeKey
          + " exceeds chunked-bitmap range (max " + MAX_NODE_KEY + ")");
    }

    final int chunkIdx = (int) (nodeKey >>> 16);
    final long bit16 = nodeKey & 0xFFFFL;

    byte[] keyBuf = KEY_BUFFER.get();
    int compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);
    if (compLen > keyBuf.length) {
      keyBuf = new byte[compLen];
      KEY_BUFFER.set(keyBuf);
      compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);
    }

    final byte[] keySlice = compLen == keyBuf.length ? keyBuf : Arrays.copyOf(keyBuf, compLen);
    final LeafNavigationResult navResult = prepareLeafOfTree(rootReference, keySlice, compLen);
    final HOTLeafPage leaf = navResult.leaf();
    if (leaf == null) {
      return false;
    }
    final int index = leaf.findEntry(keySlice);
    if (index < 0) {
      return false;
    }
    final byte[] valueBytes = leaf.getValue(index);
    if (NodeReferencesSerializer.isTombstone(valueBytes, 0, valueBytes.length)) {
      return false;
    }
    final NodeReferences chunkRefs = NodeReferencesSerializer.deserialize(valueBytes);
    final boolean removed = chunkRefs.removeNodeKey(bit16);
    if (!removed) {
      return false;
    }

    byte[] valueBuf = VALUE_BUFFER.get();
    final int requiredSize = NodeReferencesSerializer.computeSerializedSize(chunkRefs);
    if (requiredSize > valueBuf.length) {
      valueBuf = new byte[requiredSize];
      VALUE_BUFFER.set(valueBuf);
    }
    final int valueLen = NodeReferencesSerializer.serialize(chunkRefs, valueBuf, 0);
    leaf.updateValue(index, Arrays.copyOf(valueBuf, valueLen));
    return true;
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
