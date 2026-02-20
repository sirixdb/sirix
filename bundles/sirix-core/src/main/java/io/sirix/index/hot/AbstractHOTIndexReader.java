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
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.CASPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/**
 * Abstract base class for HOT index readers.
 *
 * <p>
 * Provides common functionality for tree navigation, root reference lookup, and iteration.
 * Subclasses implement key serialization/deserialization.
 * </p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 * <li>Thread-local byte buffers for key serialization</li>
 * <li>No Optional - uses @Nullable returns</li>
 * <li>Lock-free reads with guard management</li>
 * <li>Pre-allocated traversal arrays via {@link HOTTrieReader}</li>
 * </ul>
 *
 * @param <K> the key type exposed by the reader
 * @author Johannes Lichtenberger
 */
public abstract class AbstractHOTIndexReader<K> {

  protected final StorageEngineReader storageEngineReader;
  protected final IndexType indexType;
  protected final int indexNumber;

  /**
   * Protected constructor.
   *
   * @param storageEngineReader the storage engine reader
   * @param indexType the index type (PATH, CAS, NAME)
   * @param indexNumber the index number
   */
  protected AbstractHOTIndexReader(StorageEngineReader storageEngineReader, IndexType indexType, int indexNumber) {
    this.storageEngineReader = requireNonNull(storageEngineReader);
    this.indexType = requireNonNull(indexType);
    this.indexNumber = indexNumber;
  }

  /**
   * Get the storage engine reader.
   *
   * @return the storage engine reader
   */
  public StorageEngineReader getStorageEngineReader() {
    return storageEngineReader;
  }

  /**
   * Get the index type.
   *
   * @return the index type
   */
  public IndexType getIndexType() {
    return indexType;
  }

  /**
   * Get the index number.
   *
   * @return the index number
   */
  public int getIndexNumber() {
    return indexNumber;
  }

  /**
   * Get the root reference for the index.
   *
   * @return the root page reference, or null if not found
   */
  protected @Nullable PageReference getRootReference() {
    final RevisionRootPage rootPage = storageEngineReader.getActualRevisionRootPage();
    return switch (indexType) {
      case PATH -> {
        final PathPage pathPage = storageEngineReader.getPathPage(rootPage);
        if (pathPage == null || indexNumber >= pathPage.getReferences().size()) {
          yield null;
        }
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = storageEngineReader.getCASPage(rootPage);
        if (casPage == null || indexNumber >= casPage.getReferences().size()) {
          yield null;
        }
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = storageEngineReader.getNamePage(rootPage);
        if (namePage == null || indexNumber >= namePage.getReferences().size()) {
          yield null;
        }
        yield namePage.getOrCreateReference(indexNumber);
      }
      default -> null;
    };
  }

  /**
   * Navigate to the leaf page containing the key. Uses {@link HOTTrieReader} for proper tree
   * traversal.
   *
   * @param rootRef the root reference
   * @param key the search key bytes
   * @return the leaf page, or null if not found
   */
  protected @Nullable HOTLeafPage navigateToLeaf(PageReference rootRef, byte[] key) {
    try (var trieReader = new HOTTrieReader(storageEngineReader)) {
      return trieReader.navigateToLeaf(rootRef, key);
    }
  }

  /**
   * Serialize a key to bytes.
   *
   * @param key the key to serialize
   * @param buffer the buffer to write to
   * @param offset the offset in the buffer
   * @return the number of bytes written
   */
  protected abstract int serializeKey(K key, byte[] buffer, int offset);

  /**
   * Deserialize a key from bytes.
   *
   * @param buffer the buffer to read from
   * @param offset the offset in the buffer
   * @param length the number of bytes to read
   * @return the deserialized key, or null if invalid
   */
  protected abstract @Nullable K deserializeKey(byte[] buffer, int offset, int length);

  /**
   * Compare two serialized keys.
   *
   * @param key1 first key bytes
   * @param offset1 offset in first key
   * @param length1 length of first key
   * @param key2 second key bytes
   * @param offset2 offset in second key
   * @param length2 length of second key
   * @return negative if key1 < key2, zero if equal, positive if key1 > key2
   */
  protected abstract int compareKeys(byte[] key1, int offset1, int length1, byte[] key2, int offset2, int length2);

  /**
   * Get the thread-local key buffer.
   *
   * @return the key buffer
   */
  protected abstract byte[] getKeyBuffer();

  /**
   * Set a new key buffer if the current one is too small.
   *
   * @param newBuffer the new buffer
   */
  protected abstract void setKeyBuffer(byte[] newBuffer);

  /**
   * Create an iterator over all entries in the HOT index.
   *
   * @return iterator over all key-value pairs
   */
  public Iterator<Map.Entry<K, NodeReferences>> iterator() {
    return new HOTLeafIterator();
  }

  /**
   * Iterator over all entries in a HOT index, handling tree navigation.
   */
  protected class HOTLeafIterator implements Iterator<Map.Entry<K, NodeReferences>> {
    private @Nullable HOTLeafPage currentLeaf;
    private int currentIndex;
    private Map.@Nullable Entry<K, NodeReferences> nextEntry;
    private final @Nullable HOTTrieReader trieReader;
    private final @Nullable PageReference rootRef;

    protected HOTLeafIterator() {
      this.rootRef = getRootReference();
      if (rootRef != null) {
        this.trieReader = new HOTTrieReader(storageEngineReader);
        // Navigate to leftmost leaf
        this.currentLeaf = trieReader.navigateToLeftmostLeaf(rootRef);
      } else {
        this.trieReader = null;
        // Fallback to simple case
        this.currentLeaf = storageEngineReader.getHOTLeafPage(indexType, indexNumber);
      }
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
            K key = deserializeKey(keyBytes, 0, keyBytes.length);
            NodeReferences refs = NodeReferencesSerializer.deserialize(valueBytes);
            if (key != null && refs != null) {
              nextEntry = Map.entry(key, refs);
              return;
            }
          }
        } else {
          // No more entries in current leaf - try to advance to next leaf
          currentIndex = 0;
          if (trieReader != null) {
            currentLeaf = trieReader.advanceToNextLeaf();
          } else {
            currentLeaf = null;
          }
        }
      }

      // Clean up trie reader when done
      if (trieReader != null && currentLeaf == null) {
        trieReader.close();
      }
    }
  }

  /**
   * Range iterator over HOT entries, handling tree navigation.
   *
   * <p>
   * For range queries, we start from the leftmost leaf and skip entries that are before
   * {@code fromBytes}. This is simpler and more correct than trying to navigate directly to a key
   * that may not exist.
   * </p>
   */
  protected class RangeIterator implements Iterator<Map.Entry<K, NodeReferences>> {
    private final byte[] fromBytes;
    private final byte @Nullable [] toBytes; // null means no upper bound
    private @Nullable HOTLeafPage currentLeaf;
    private int currentIndex;
    private Map.@Nullable Entry<K, NodeReferences> nextEntry;
    private final @Nullable HOTTrieReader trieReader;

    protected RangeIterator(byte[] fromBytes, byte @Nullable [] toBytes) {
      this.fromBytes = fromBytes;
      this.toBytes = toBytes;

      PageReference rootRef = getRootReference();
      if (rootRef != null) {
        this.trieReader = new HOTTrieReader(storageEngineReader);
        // Start from the leftmost leaf and skip entries < fromBytes
        this.currentLeaf = trieReader.navigateToLeftmostLeaf(rootRef);
      } else {
        this.trieReader = null;
        // Fallback to simple case
        this.currentLeaf = storageEngineReader.getHOTLeafPage(indexType, indexNumber);
      }

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

          // Check if key is within range (only if toBytes is set)
          if (toBytes != null && compareKeys(key, 0, key.length, toBytes, 0, toBytes.length) >= 0) {
            // Past end of range
            currentLeaf = null;
            break;
          }

          // Skip entries before fromBytes
          if (compareKeys(key, 0, key.length, fromBytes, 0, fromBytes.length) < 0) {
            currentIndex++;
            continue;
          }

          byte[] value = currentLeaf.getValue(currentIndex);
          currentIndex++;

          if (!NodeReferencesSerializer.isTombstone(value, 0, value.length)) {
            K deserializedKey = deserializeKey(key, 0, key.length);
            NodeReferences refs = NodeReferencesSerializer.deserialize(value);
            if (deserializedKey != null && refs != null) {
              nextEntry = Map.entry(deserializedKey, refs);
              return;
            }
          }
        } else {
          // No more entries in current leaf - try to advance to next leaf
          currentIndex = 0;
          if (trieReader != null) {
            currentLeaf = trieReader.advanceToNextLeaf();
          } else {
            currentLeaf = null;
          }
        }
      }

      // Clean up trie reader when done
      if (trieReader != null && currentLeaf == null) {
        trieReader.close();
      }
    }
  }
}

