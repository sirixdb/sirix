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
   * Thread-local buffer for key serialization (8 bytes for long).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[8]);

  private final HOTLongKeySerializer keySerializer;

  /**
   * Private constructor.
   *
   * @param pageTrx the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType the index type (should be PATH)
   * @param indexNumber the index number
   */
  private HOTLongIndexWriter(StorageEngineWriter pageTrx, HOTLongKeySerializer keySerializer, IndexType indexType,
      int indexNumber) {
    super(pageTrx, indexType, indexNumber);
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
   * @param pageTrx the storage engine writer
   * @param indexType the index type (should be PATH)
   * @param indexNumber the index number
   * @return a new HOTLongIndexWriter instance
   */
  public static HOTLongIndexWriter create(StorageEngineWriter pageTrx, IndexType indexType, int indexNumber) {
    return new HOTLongIndexWriter(pageTrx, PathKeySerializer.INSTANCE, indexType, indexNumber);
  }

  /**
   * Index a primitive long key with NodeReferences.
   *
   * <p>
   * If the key already exists, merges the NodeReferences (OR operation). Uses primitive long to avoid
   * boxing.
   * </p>
   *
   * <p>
   * <b>Split Handling:</b> When a leaf page is full, the page is split to accommodate new entries.
   * This allows the index to grow beyond 512 entries.
   * </p>
   *
   * @param key the index key (primitive long, no boxing)
   * @param value the node references
   * @param move cursor movement mode (ignored for HOT)
   * @return the indexed value
   */
  public NodeReferences index(long key, NodeReferences value, RBTreeReader.MoveCursor move) {
    requireNonNull(value);

    // Serialize key to thread-local buffer (no boxing!)
    byte[] keyBuf = KEY_BUFFER.get();
    int keyLen = keySerializer.serialize(key, keyBuf, 0);

    // Serialize value
    Object[] valueResult = serializeValue(value);
    byte[] valueBuf = (byte[]) valueResult[0];
    int valueLen = (int) valueResult[1];

    // Perform the index operation
    doIndex(keyBuf, keyLen, valueBuf, valueLen);

    return value;
  }

  /**
   * Get the NodeReferences for a primitive long key.
   *
   * <p>
   * Uses primitive long to avoid boxing.
   * </p>
   *
   * @param key the index key (primitive long)
   * @param mode the search mode
   * @return the node references, or null if not found
   */
  public @Nullable NodeReferences get(long key, SearchMode mode) {
    // Serialize key (no boxing!)
    byte[] keyBuf = KEY_BUFFER.get();
    keySerializer.serialize(key, keyBuf, 0);

    // Get HOT leaf page
    HOTLeafPage leaf = getLeafForRead(keyBuf);
    if (leaf == null) {
      return null;
    }

    return getFromLeaf(leaf, keyBuf);
  }

  /**
   * Remove a node key from the NodeReferences for a primitive long key.
   *
   * @param key the index key (primitive long)
   * @param nodeKey the node key to remove
   * @return true if the node key was removed
   */
  public boolean remove(long key, long nodeKey) {
    // Serialize key (no boxing!)
    byte[] keyBuf = KEY_BUFFER.get();
    keySerializer.serialize(key, keyBuf, 0);

    // Navigate to leaf with path tracking
    LeafNavigationResult navResult = getLeafWithPath(rootReference, keyBuf, 8);
    HOTLeafPage leaf = navResult.leaf();
    if (leaf == null) {
      return false;
    }

    // Find entry
    int index = leaf.findEntry(keyBuf);
    if (index < 0) {
      return false;
    }

    // Get current value
    byte[] valueBytes = leaf.getValue(index);
    if (NodeReferencesSerializer.isTombstone(valueBytes, 0, valueBytes.length)) {
      return false;
    }

    NodeReferences refs = NodeReferencesSerializer.deserialize(valueBytes);
    boolean removed = refs.removeNodeKey(nodeKey);

    if (removed) {
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
  protected int serializeKey(Long key, byte[] buffer, int offset) {
    return keySerializer.serialize(key, buffer, offset);
  }
}
