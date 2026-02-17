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

import io.sirix.api.StorageEngineReader;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

/**
 * Primitive-specialized HOT index reader for long keys (PATH index).
 *
 * <p>
 * Uses primitive {@code long} keys to avoid boxing overhead. This is the high-performance variant
 * for PATH index read operations.
 * </p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 * <li>Primitive long parameters (no boxing)</li>
 * <li>Thread-local byte buffers for serialization</li>
 * <li>No Optional - uses @Nullable returns</li>
 * <li>Lock-free reads with guard management</li>
 * <li>Proper tree traversal via {@link AbstractHOTIndexReader}</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 * @see HOTIndexReader
 * @see AbstractHOTIndexReader
 */
public final class HOTLongIndexReader extends AbstractHOTIndexReader<Long> {

  /**
   * Thread-local buffer for key serialization (8 bytes for long).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[8]);

  private final HOTLongKeySerializer keySerializer;

  /**
   * Private constructor.
   *
   * @param pageReadTrx the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType the index type (should be PATH)
   * @param indexNumber the index number
   */
  private HOTLongIndexReader(StorageEngineReader pageReadTrx, HOTLongKeySerializer keySerializer, IndexType indexType,
      int indexNumber) {
    super(pageReadTrx, indexType, indexNumber);
    this.keySerializer = keySerializer;
  }

  /**
   * Creates a new HOTLongIndexReader for PATH index.
   *
   * @param pageReadTrx the storage engine reader
   * @param indexType the index type (should be PATH)
   * @param indexNumber the index number
   * @return a new HOTLongIndexReader instance
   */
  public static HOTLongIndexReader create(StorageEngineReader pageReadTrx, IndexType indexType, int indexNumber) {
    return new HOTLongIndexReader(pageReadTrx, PathKeySerializer.INSTANCE, indexType, indexNumber);
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

    // Get root reference
    PageReference rootRef = getRootReference();
    if (rootRef == null) {
      // Fallback to simple case for backwards compatibility
      return getFromSimpleLeaf(keyBuf);
    }

    // Navigate to the correct leaf using tree traversal
    HOTLeafPage leaf = navigateToLeaf(rootRef, keyBuf);
    if (leaf == null) {
      return null;
    }

    return getFromLeaf(leaf, keyBuf);
  }

  /**
   * Fallback method for simple (single leaf) case.
   */
  private @Nullable NodeReferences getFromSimpleLeaf(byte[] keyBuf) {
    HOTLeafPage leaf = pageReadTrx.getHOTLeafPage(indexType, indexNumber);
    if (leaf == null) {
      return null;
    }
    return getFromLeaf(leaf, keyBuf);
  }

  /**
   * Get value from a leaf page with guard management.
   */
  private @Nullable NodeReferences getFromLeaf(HOTLeafPage leaf, byte[] keyBuf) {
    try {
      leaf.acquireGuard();

      // Find entry
      int index = leaf.findEntry(keyBuf);
      if (index < 0) {
        return null;
      }

      // Deserialize value
      byte[] valueBytes = leaf.getValue(index);
      if (NodeReferencesSerializer.isTombstone(valueBytes, 0, valueBytes.length)) {
        return null; // Deleted entry
      }
      return NodeReferencesSerializer.deserialize(valueBytes);
    } finally {
      leaf.releaseGuard();
    }
  }

  /**
   * Check if a key exists in the index.
   *
   * @param key the primitive long key
   * @return true if the key exists and is not a tombstone
   */
  public boolean containsKey(long key) {
    return get(key, SearchMode.EQUAL) != null;
  }

  /**
   * Create a range iterator starting from a specific key.
   *
   * @param fromKey the start key (inclusive)
   * @return iterator over entries starting from the key
   */
  public Iterator<Map.Entry<Long, NodeReferences>> iteratorFrom(long fromKey) {
    byte[] keyBuf = KEY_BUFFER.get();
    keySerializer.serialize(fromKey, keyBuf, 0);
    byte[] fromBytes = Arrays.copyOf(keyBuf, 8);
    return new RangeIterator(fromBytes, null);
  }

  /**
   * Create a range iterator over a key range.
   *
   * @param fromKey the start key (inclusive)
   * @param toKey the end key (exclusive)
   * @return iterator over entries in the range
   */
  public Iterator<Map.Entry<Long, NodeReferences>> range(long fromKey, long toKey) {
    byte[] keyBuf = KEY_BUFFER.get();
    keySerializer.serialize(fromKey, keyBuf, 0);
    byte[] fromBytes = Arrays.copyOf(keyBuf, 8);
    keySerializer.serialize(toKey, keyBuf, 0);
    byte[] toBytes = Arrays.copyOf(keyBuf, 8);
    return new RangeIterator(fromBytes, toBytes);
  }

  @Override
  protected int serializeKey(Long key, byte[] buffer, int offset) {
    return keySerializer.serialize(key, buffer, offset);
  }

  @Override
  protected @Nullable Long deserializeKey(byte[] buffer, int offset, int length) {
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
