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
   * Thread-local buffer for key serialization (256 bytes default).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER = ThreadLocal.withInitial(() -> new byte[256]);

  private final HOTKeySerializer<K> keySerializer;

  /**
   * Private constructor.
   *
   * @param pageReadTrx the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  private HOTIndexReader(StorageEngineReader pageReadTrx, HOTKeySerializer<K> keySerializer, IndexType indexType,
      int indexNumber) {
    super(pageReadTrx, indexType, indexNumber);
    this.keySerializer = requireNonNull(keySerializer);
  }

  /**
   * Creates a new HOTIndexReader.
   *
   * @param pageReadTrx the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType the index type
   * @param indexNumber the index number
   * @param <K> the key type
   * @return a new HOTIndexReader instance
   */
  public static <K extends Comparable<? super K>> HOTIndexReader<K> create(StorageEngineReader pageReadTrx,
      HOTKeySerializer<K> keySerializer, IndexType indexType, int indexNumber) {
    return new HOTIndexReader<>(pageReadTrx, keySerializer, indexType, indexNumber);
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

    // Serialize key to thread-local buffer
    byte[] keyBuf = getKeyBuffer();
    int keyLen = serializeKey(key, keyBuf, 0);
    if (keyLen > keyBuf.length) {
      keyBuf = new byte[keyLen];
      setKeyBuffer(keyBuf);
      keyLen = serializeKey(key, keyBuf, 0);
    }
    byte[] keySlice = keyLen == keyBuf.length
        ? keyBuf
        : Arrays.copyOf(keyBuf, keyLen);

    // Get the root reference
    PageReference rootRef = getRootReference();
    if (rootRef == null) {
      return null;
    }

    // Navigate to the correct leaf using tree traversal
    HOTLeafPage leaf = navigateToLeaf(rootRef, keySlice);
    if (leaf == null) {
      return null;
    }

    try {
      leaf.acquireGuard();

      // Find entry
      int index = leaf.findEntry(keySlice);
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
   * Create a range iterator over entries.
   *
   * @param fromKey start key (inclusive)
   * @param toKey end key (exclusive)
   * @return iterator over key-value pairs in range
   */
  public Iterator<Map.Entry<K, NodeReferences>> range(K fromKey, K toKey) {
    requireNonNull(fromKey);
    requireNonNull(toKey);

    // Serialize keys
    byte[] keyBuf = getKeyBuffer();
    int fromLen = serializeKey(fromKey, keyBuf, 0);
    byte[] fromBytes = Arrays.copyOf(keyBuf, fromLen);
    int toLen = serializeKey(toKey, keyBuf, 0);
    byte[] toBytes = Arrays.copyOf(keyBuf, toLen);

    return new RangeIterator(fromBytes, toBytes);
  }

  /**
   * Create an iterator that starts from a specific key. This is used for efficient range queries
   * (GREATER, GREATER_OR_EQUAL).
   *
   * @param fromKey start key (inclusive)
   * @return iterator over key-value pairs starting from the key
   */
  public Iterator<Map.Entry<K, NodeReferences>> iteratorFrom(K fromKey) {
    requireNonNull(fromKey);

    // Serialize key
    byte[] keyBuf = getKeyBuffer();
    int fromLen = serializeKey(fromKey, keyBuf, 0);
    byte[] fromBytes = Arrays.copyOf(keyBuf, fromLen);

    // Use RangeIterator with null toBytes to indicate "no upper bound"
    return new RangeIterator(fromBytes, null);
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
