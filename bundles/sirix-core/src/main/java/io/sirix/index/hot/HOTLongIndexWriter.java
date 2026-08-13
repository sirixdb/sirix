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
 * Primitive-specialized HOT index writer for long keys (PATH index).
 *
 * <p>
 * Uses primitive {@code long} keys to avoid boxing overhead. This is the high-performance variant
 * for PATH index operations.
 * </p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 * <li>Primitive long parameters (no boxing)</li>
 * <li>Thread-local byte buffers for serialization</li>
 * <li>No Optional - uses @Nullable returns</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 * @see HOTIndexWriter
 * @see AbstractHOTIndexWriter
 */
public final class HOTLongIndexWriter extends AbstractHOTIndexWriter<Long> {

  /**
   * Thread-local buffer for composite key serialization (8 bytes long + 4 bytes chunkIdx_be4).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER =
      ThreadLocal.withInitial(() -> new byte[HOTLongKeySerializer.CHUNKED_SERIALIZED_SIZE]);

  /**
   * Thread-local single-bit chunk-payload reused across writes — see
   * {@link HOTIndexWriter#SINGLE_BIT_REFS} for rationale.
   */
  private static final ThreadLocal<NodeReferences> SINGLE_BIT_REFS = ThreadLocal.withInitial(NodeReferences::new);

  private final HOTLongKeySerializer keySerializer;

  /** Lazy reader for chunked reassembly. */
  private @Nullable HOTTrieReader chunkReader;

  /**
   * Private constructor.
   *
   * @param storageEngineWriter the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType the index type (should be PATH)
   * @param indexNumber the index number
   */
  private HOTLongIndexWriter(StorageEngineWriter storageEngineWriter, HOTLongKeySerializer keySerializer,
      IndexType indexType, int indexNumber) {
    super(storageEngineWriter, indexType, indexNumber);
    this.keySerializer = requireNonNull(keySerializer);

    // HOTLongIndexWriter is specialized for PATH indexes only.
    if (indexType != IndexType.PATH) {
      throw new IllegalArgumentException(
          "HOTLongIndexWriter only supports PATH indexes, use HOTIndexWriter for " + indexType);
    }

    // Initialize HOT index tree
    initializePathIndex();
  }

  /**
   * Creates a new HOTLongIndexWriter for PATH index.
   *
   * @param storageEngineWriter the storage engine writer
   * @param indexType the index type (should be PATH)
   * @param indexNumber the index number
   * @return a new HOTLongIndexWriter instance
   */
  public static HOTLongIndexWriter create(StorageEngineWriter storageEngineWriter, IndexType indexType,
      int indexNumber) {
    return new HOTLongIndexWriter(storageEngineWriter, PathKeySerializer.INSTANCE, indexType, indexNumber);
  }

  /**
   * Chunked-bitmap variant of {@link HOTIndexWriter#index} for primitive long keys (PATH). Splits the
   * input bitmap by {@code chunkIdx = (int)(nodeKey >>> 16)} and ORs each bit16 into its
   * {@code (longKeyBE ‖ chunkIdx_be4)} chunk slot.
   */
  public NodeReferences index(long key, NodeReferences value, RBTreeReader.MoveCursor move) {
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
   * Equivalent to {@link #index(long, NodeReferences, RBTreeReader.MoveCursor)} with a one-element
   * {@link NodeReferences}, minus the {@code Roaring64Bitmap} allocation: the slot write is an
   * OR-merge ({@link HOTLeafPage#mergeWithNodeRefs}), so a caller that only wants to ADD one
   * reference never has to materialise — let alone read back — the references already stored under
   * {@code key}.
   * </p>
   *
   * @param key the logical index key (a path-class record)
   * @param nodeKey the node key to add; must be in {@code [0, 2^48)}
   */
  public void indexNodeKey(long key, long nodeKey) {
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
   * tree must check first and fall back to {@link #indexNodeKey(long, long)}.
   * </p>
   *
   * @return a fresh bulk loader bound to this writer
   */
  public HOTLongBulkIndexLoader createBulkLoader() {
    return new HOTLongBulkIndexLoader(this, keySerializer);
  }

  private void addNodeKeyToChunk(long key, long nodeKey) {
    AbstractHOTIndexWriter.checkNodeKeyRange(nodeKey);
    final int chunkIdx = (int) (nodeKey >>> 16);
    final long bit16 = nodeKey & 0xFFFFL;

    final byte[] keyBuf = KEY_BUFFER.get();
    final int compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);

    final NodeReferences singleBit = SINGLE_BIT_REFS.get();
    final Roaring64Bitmap singleBitmap = singleBit.getNodeKeys();
    singleBitmap.clear();
    singleBitmap.add(bit16);
    serializeValueInto(singleBit);

    doIndex(keyBuf, compLen, lastSerializedValueBuf, lastSerializedValueLen);
  }

  /**
   * Reassemble all chunks of {@code key} into a single {@link NodeReferences}. See
   * {@link HOTIndexReader#get} for the algorithm; this is the primitive-long mirror.
   */
  public @Nullable NodeReferences get(long key, SearchMode mode) {
    final byte[] keyBuf = KEY_BUFFER.get();
    final int prefixLen = keySerializer.serialize(key, keyBuf, 0);

    final PageReference rootRef = rootReference;
    if (rootRef == null) {
      return null;
    }

    final byte[] fromBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(keyBuf, 0, fromBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(fromBytes, prefixLen, 0);

    final byte[] toBytes = new byte[prefixLen + HOTKeySerializer.CHUNK_IDX_BYTES];
    System.arraycopy(keyBuf, 0, toBytes, 0, prefixLen);
    HOTKeySerializer.writeChunkIdxBE(toBytes, prefixLen, 0xFFFFFFFF);

    if (chunkReader == null) {
      chunkReader = new HOTTrieReader(storageEngineWriter);
    }
    // The sweep reads UNPINNED leaves under optimistic stamps — the shared helper validates each
    // slot's copies against the cursor's leaf stamp before anything reaches the deserializer.
    final Roaring64Bitmap merged;
    try (HOTRangeCursor cursor = chunkReader.range(rootRef, fromBytes, toBytes)) {
      merged = NodeReferencesSerializer.mergeChunksInPrefixRange(cursor, keyBuf, prefixLen);
    }
    if (merged == null || merged.isEmpty()) {
      return null;
    }
    return NodeReferences.owning(merged);
  }

  /**
   * Remove a single nodeKey from the chunked bitmap of {@code key}. Mirrors
   * {@link HOTIndexWriter#remove}.
   */
  public boolean remove(long key, long nodeKey) {
    AbstractHOTIndexWriter.checkNodeKeyRange(nodeKey);
    final int chunkIdx = (int) (nodeKey >>> 16);
    final long bit16 = nodeKey & 0xFFFFL;

    final byte[] keyBuf = KEY_BUFFER.get();
    final int compLen = keySerializer.serializeWithChunkIdx(key, chunkIdx, keyBuf, 0);
    final byte[] keySlice = compLen == keyBuf.length
        ? keyBuf
        : Arrays.copyOf(keyBuf, compLen);

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

    if (!chunkRefs.hasNodeKeys()) {
      leaf.deleteAt(index);
      return true;
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
  protected int serializeKey(Long key, byte[] buffer, int offset) {
    return keySerializer.serialize(key, buffer, offset);
  }
}
