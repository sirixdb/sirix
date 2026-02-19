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

import io.sirix.api.StorageEngineWriter;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import org.checkerframework.checker.nullness.qual.Nullable;

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
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[256]);

  private final HOTKeySerializer<K> keySerializer;

  /**
   * Private constructor.
   *
   * @param pageTrx the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  private HOTIndexWriter(StorageEngineWriter pageTrx, HOTKeySerializer<K> keySerializer, IndexType indexType,
      int indexNumber) {
    super(pageTrx, indexType, indexNumber);
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
   * @param pageTrx the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType the index type
   * @param indexNumber the index number
   * @param <K> the key type
   * @return a new HOTIndexWriter instance
   */
  public static <K extends Comparable<? super K>> HOTIndexWriter<K> create(StorageEngineWriter pageTrx,
      HOTKeySerializer<K> keySerializer, IndexType indexType, int indexNumber) {
    return new HOTIndexWriter<>(pageTrx, keySerializer, indexType, indexNumber);
  }

  /**
   * Index a key-value pair.
   *
   * <p>
   * If the key already exists, merges the NodeReferences (OR operation).
   * </p>
   *
   * <p>
   * <b>Edge Case Handling:</b> When many identical keys are merged, the value can grow very large. If
   * the value becomes too large for a single page and the page has only 1 entry (so it can't be
   * split), this method handles it by:
   * <ol>
   * <li>Attempting to compact the page to reclaim fragmented space</li>
   * <li>Retrying the insert operation after compaction</li>
   * <li>If still failing after retries, throwing an informative exception</li>
   * </ol>
   * </p>
   *
   * @param key the index key
   * @param value the node references
   * @param move cursor movement mode (ignored for HOT)
   * @return the indexed value
   */
  public NodeReferences index(K key, NodeReferences value, RBTreeReader.MoveCursor move) {
    requireNonNull(key);
    requireNonNull(value);

    // Serialize key to thread-local buffer
    byte[] keyBuf = KEY_BUFFER.get();
    int keyLen = keySerializer.serialize(key, keyBuf, 0);
    if (keyLen > keyBuf.length) {
      // Key too large - expand buffer
      keyBuf = new byte[keyLen];
      KEY_BUFFER.set(keyBuf);
      keyLen = keySerializer.serialize(key, keyBuf, 0);
    }

    // Serialize value (stores result in lastSerializedValueBuf/Len — no Object[] allocation)
    serializeValueInto(value);

    // Perform the index operation
    doIndex(keyBuf, keyLen, lastSerializedValueBuf, lastSerializedValueLen);

    return value;
  }

  /**
   * Get the NodeReferences for a key.
   *
   * @param key the index key
   * @param mode the search mode
   * @return the node references, or null if not found
   */
  public @Nullable NodeReferences get(K key, SearchMode mode) {
    requireNonNull(key);

    // Serialize key
    byte[] keyBuf = KEY_BUFFER.get();
    int keyLen = keySerializer.serialize(key, keyBuf, 0);

    // Get HOT leaf page
    byte[] keySlice = keyLen == keyBuf.length
        ? keyBuf
        : Arrays.copyOf(keyBuf, keyLen);
    HOTLeafPage leaf = getLeafForRead(keySlice);
    if (leaf == null) {
      return null;
    }

    return getFromLeaf(leaf, keySlice);
  }

  /**
   * Remove a node key from the NodeReferences for a key.
   *
   * <p>
   * If the NodeReferences becomes empty, sets a tombstone.
   * </p>
   *
   * @param key the index key
   * @param nodeKey the node key to remove
   * @return true if the node key was removed
   */
  public boolean remove(K key, long nodeKey) {
    requireNonNull(key);

    // Serialize key
    byte[] keyBuf = KEY_BUFFER.get();
    int keyLen = keySerializer.serialize(key, keyBuf, 0);

    // Navigate to leaf with path tracking
    byte[] keySlice = keyLen == keyBuf.length
        ? keyBuf
        : Arrays.copyOf(keyBuf, keyLen);
    LeafNavigationResult navResult = getLeafWithPath(rootReference, keySlice, keyLen);
    HOTLeafPage leaf = navResult.leaf();
    if (leaf == null) {
      return false;
    }

    // Find entry
    int index = leaf.findEntry(keySlice);
    if (index < 0) {
      return false;
    }

    // Get current value
    byte[] valueBytes = leaf.getValue(index);
    if (NodeReferencesSerializer.isTombstone(valueBytes, 0, valueBytes.length)) {
      return false; // Already deleted
    }

    NodeReferences refs = NodeReferencesSerializer.deserialize(valueBytes);
    boolean removed = refs.removeNodeKey(nodeKey);

    if (removed) {
      // Update entry
      byte[] valueBuf = VALUE_BUFFER.get();
      int valueLen = NodeReferencesSerializer.serialize(refs, valueBuf, 0);
      leaf.updateValue(index, Arrays.copyOf(valueBuf, valueLen));
    }

    return removed;
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
