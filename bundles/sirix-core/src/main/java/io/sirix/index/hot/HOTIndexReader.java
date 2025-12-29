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
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/**
 * Generic HOT index reader for object keys (CASValue, QNm).
 *
 * <p>Replaces {@link io.sirix.index.redblacktree.RBTreeReader} for HOT-based secondary indexes.
 * Provides read-only access with optimistic concurrency for lock-free reads.</p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 *   <li>Thread-local byte buffers for key serialization</li>
 *   <li>No Optional - uses @Nullable returns</li>
 *   <li>Lock-free reads with version validation</li>
 * </ul>
 *
 * @param <K> the key type (must implement Comparable)
 * @author Johannes Lichtenberger
 */
public final class HOTIndexReader<K extends Comparable<? super K>> {

  /**
   * Thread-local buffer for key serialization (256 bytes default).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER =
      ThreadLocal.withInitial(() -> new byte[256]);

  private final StorageEngineReader pageReadTrx;
  private final HOTKeySerializer<K> keySerializer;
  private final IndexType indexType;
  private final int indexNumber;

  /**
   * Private constructor.
   *
   * @param pageReadTrx   the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType     the index type (PATH, CAS, NAME)
   * @param indexNumber   the index number
   */
  private HOTIndexReader(StorageEngineReader pageReadTrx, HOTKeySerializer<K> keySerializer,
                         IndexType indexType, int indexNumber) {
    this.pageReadTrx = requireNonNull(pageReadTrx);
    this.keySerializer = requireNonNull(keySerializer);
    this.indexType = requireNonNull(indexType);
    this.indexNumber = indexNumber;
  }

  /**
   * Creates a new HOTIndexReader.
   *
   * @param pageReadTrx   the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType     the index type
   * @param indexNumber   the index number
   * @param <K>           the key type
   * @return a new HOTIndexReader instance
   */
  public static <K extends Comparable<? super K>> HOTIndexReader<K> create(
      StorageEngineReader pageReadTrx, HOTKeySerializer<K> keySerializer,
      IndexType indexType, int indexNumber) {
    return new HOTIndexReader<>(pageReadTrx, keySerializer, indexType, indexNumber);
  }

  /**
   * Get the NodeReferences for a key.
   *
   * @param key  the index key
   * @param mode the search mode
   * @return the node references, or null if not found
   */
  public @Nullable NodeReferences get(K key, SearchMode mode) {
    requireNonNull(key);

    // Serialize key to thread-local buffer
    byte[] keyBuf = KEY_BUFFER.get();
    int keyLen = keySerializer.serialize(key, keyBuf, 0);
    if (keyLen > keyBuf.length) {
      keyBuf = new byte[keyLen];
      KEY_BUFFER.set(keyBuf);
      keyLen = keySerializer.serialize(key, keyBuf, 0);
    }

    // Get HOT leaf page using the storage engine's proper page loading
    HOTLeafPage leaf = pageReadTrx.getHOTLeafPage(indexType, indexNumber);
    if (leaf == null) {
      return null;
    }

    try {
      leaf.acquireGuard();

      // Find entry
      byte[] keySlice = keyLen == keyBuf.length ? keyBuf : Arrays.copyOf(keyBuf, keyLen);
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
   * Create an iterator over all entries in the HOT index.
   *
   * @return iterator over all key-value pairs
   */
  public Iterator<Map.Entry<K, NodeReferences>> iterator() {
    return new HOTLeafIterator();
  }
  
  /**
   * Create a range iterator over entries.
   *
   * @param fromKey start key (inclusive)
   * @param toKey   end key (exclusive)
   * @return iterator over key-value pairs in range
   */
  public Iterator<Map.Entry<K, NodeReferences>> range(K fromKey, K toKey) {
    requireNonNull(fromKey);
    requireNonNull(toKey);

    // Serialize keys
    byte[] keyBuf = KEY_BUFFER.get();
    int fromLen = keySerializer.serialize(fromKey, keyBuf, 0);
    byte[] fromBytes = Arrays.copyOf(keyBuf, fromLen);
    int toLen = keySerializer.serialize(toKey, keyBuf, 0);
    byte[] toBytes = Arrays.copyOf(keyBuf, toLen);

    return new RangeIterator(fromBytes, toBytes);
  }

  /**
   * Get the storage engine reader.
   *
   * @return the storage engine reader
   */
  public StorageEngineReader getPageReadTrx() {
    return pageReadTrx;
  }

  /**
   * Iterator over all entries in a HOT leaf page.
   */
  private class HOTLeafIterator implements Iterator<Map.Entry<K, NodeReferences>> {
    private @Nullable HOTLeafPage currentLeaf;
    private int currentIndex;
    private Map.@Nullable Entry<K, NodeReferences> nextEntry;
    
    HOTLeafIterator() {
      // Get the leaf using proper storage engine page loading
      this.currentLeaf = pageReadTrx.getHOTLeafPage(indexType, indexNumber);
      this.currentIndex = 0;
      advance();
    }
    
    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }
    
    @Override
    public Map.Entry<K, NodeReferences> next() {
      if (nextEntry == null) {
        throw new NoSuchElementException();
      }
      Map.Entry<K, NodeReferences> result = nextEntry;
      advance();
      return result;
    }
    
    private void advance() {
      nextEntry = null;
      while (currentLeaf != null) {
        if (currentIndex < currentLeaf.getEntryCount()) {
          byte[] keyBytes = currentLeaf.getKey(currentIndex);
          byte[] valueBytes = currentLeaf.getValue(currentIndex);
          currentIndex++;
          
          if (!NodeReferencesSerializer.isTombstone(valueBytes, 0, valueBytes.length)) {
            K key = keySerializer.deserialize(keyBytes, 0, keyBytes.length);
            NodeReferences refs = NodeReferencesSerializer.deserialize(valueBytes);
            if (key != null && refs != null) {
              nextEntry = Map.entry(key, refs);
              return;
            }
          }
        } else {
          // No more entries in current leaf - for single-level HOT, we're done
          currentLeaf = null;
        }
      }
    }
  }

  /**
   * Range iterator over HOT entries.
   */
  private class RangeIterator implements Iterator<Map.Entry<K, NodeReferences>> {
    @SuppressWarnings("unused")
    private final byte[] fromBytes;
    private final byte[] toBytes;
    private @Nullable HOTLeafPage currentLeaf;
    private int currentIndex;
    private Map.@Nullable Entry<K, NodeReferences> nextEntry;

    RangeIterator(byte[] fromBytes, byte[] toBytes) {
      this.fromBytes = fromBytes;
      this.toBytes = toBytes;
      // Get the leaf using proper storage engine page loading
      this.currentLeaf = pageReadTrx.getHOTLeafPage(indexType, indexNumber);
      this.currentIndex = 0;
      advance();
    }

    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }

    @Override
    public Map.Entry<K, NodeReferences> next() {
      if (nextEntry == null) {
        throw new NoSuchElementException();
      }
      Map.Entry<K, NodeReferences> result = nextEntry;
      advance();
      return result;
    }

    private void advance() {
      nextEntry = null;
      while (currentLeaf != null) {
        if (currentIndex < currentLeaf.getEntryCount()) {
          byte[] key = currentLeaf.getKey(currentIndex);
          
          // Check if key is within range
          if (keySerializer.compare(key, 0, key.length, toBytes, 0, toBytes.length) >= 0) {
            // Past end of range
            currentLeaf = null;
            return;
          }

          byte[] value = currentLeaf.getValue(currentIndex);
          currentIndex++;

          if (!NodeReferencesSerializer.isTombstone(value, 0, value.length)) {
            K deserializedKey = keySerializer.deserialize(key, 0, key.length);
            NodeReferences refs = NodeReferencesSerializer.deserialize(value);
            if (deserializedKey != null && refs != null) {
              nextEntry = Map.entry(deserializedKey, refs);
              return;
            }
          }
        } else {
          // Move to next leaf (for single-level HOT, we're done)
          currentLeaf = null;
        }
      }
    }
  }
}
