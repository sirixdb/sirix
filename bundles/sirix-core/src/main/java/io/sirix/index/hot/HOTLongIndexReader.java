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
import io.sirix.page.CASPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/**
 * Primitive-specialized HOT index reader for long keys (PATH index).
 *
 * <p>Uses primitive {@code long} keys to avoid boxing overhead.
 * This is the high-performance variant for PATH index read operations.</p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 *   <li>Primitive long parameters (no boxing)</li>
 *   <li>Thread-local byte buffers for serialization</li>
 *   <li>No Optional - uses @Nullable returns</li>
 *   <li>Lock-free reads with guard management</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 * @see HOTIndexReader
 */
public final class HOTLongIndexReader {

  /**
   * Thread-local buffer for key serialization (8 bytes for long).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER =
      ThreadLocal.withInitial(() -> new byte[8]);

  private final StorageEngineReader pageReadTrx;
  private final HOTLongKeySerializer keySerializer;
  private final IndexType indexType;
  private final int indexNumber;
  
  /** Cached root page reference. */
  private PageReference rootReference;

  /**
   * Private constructor.
   *
   * @param pageReadTrx   the storage engine reader
   * @param keySerializer the key serializer
   * @param indexType     the index type (should be PATH)
   * @param indexNumber   the index number
   */
  private HOTLongIndexReader(StorageEngineReader pageReadTrx, HOTLongKeySerializer keySerializer,
                             IndexType indexType, int indexNumber) {
    this.pageReadTrx = requireNonNull(pageReadTrx);
    this.keySerializer = requireNonNull(keySerializer);
    this.indexType = requireNonNull(indexType);
    this.indexNumber = indexNumber;
    
    initializeRootReference();
  }
  
  /**
   * Initialize the root reference for the HOT index.
   */
  private void initializeRootReference() {
    final RevisionRootPage revisionRootPage = pageReadTrx.getActualRevisionRootPage();
    
    switch (indexType) {
      case PATH -> {
        final PathPage pathPage = pageReadTrx.getPathPage(revisionRootPage);
        if (pathPage != null) {
          rootReference = pathPage.getOrCreateReference(indexNumber);
        }
      }
      case CAS -> {
        final CASPage casPage = pageReadTrx.getCASPage(revisionRootPage);
        if (casPage != null) {
          rootReference = casPage.getOrCreateReference(indexNumber);
        }
      }
      case NAME -> {
        final NamePage namePage = pageReadTrx.getNamePage(revisionRootPage);
        if (namePage != null) {
          rootReference = namePage.getOrCreateReference(indexNumber);
        }
      }
      default -> throw new IllegalArgumentException("Unsupported index type for HOT: " + indexType);
    }
  }

  /**
   * Creates a new HOTLongIndexReader for PATH index.
   *
   * @param pageReadTrx the storage engine reader
   * @param indexType   the index type (should be PATH)
   * @param indexNumber the index number
   * @return a new HOTLongIndexReader instance
   */
  public static HOTLongIndexReader create(StorageEngineReader pageReadTrx,
                                          IndexType indexType, int indexNumber) {
    return new HOTLongIndexReader(pageReadTrx, PathKeySerializer.INSTANCE, indexType, indexNumber);
  }

  /**
   * Get the NodeReferences for a primitive long key.
   *
   * <p>Uses primitive long to avoid boxing.</p>
   *
   * @param key  the index key (primitive long)
   * @param mode the search mode
   * @return the node references, or null if not found
   */
  public @Nullable NodeReferences get(long key, SearchMode mode) {
    // Serialize key (no boxing!)
    byte[] keyBuf = KEY_BUFFER.get();
    keySerializer.serialize(key, keyBuf, 0);

    // Get HOT leaf page with optimistic concurrency
    HOTLeafPage leaf = getLeafForRead(key);
    if (leaf == null) {
      return null;
    }

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
   * Create an iterator over all entries in the HOT index.
   *
   * @return iterator over all key-value pairs
   */
  public Iterator<Map.Entry<Long, NodeReferences>> iterator() {
    return new HOTLeafIterator();
  }

  /**
   * Get the storage engine reader.
   *
   * @return the storage engine reader
   */
  public StorageEngineReader getPageReadTrx() {
    return pageReadTrx;
  }

  // ===== Private methods =====

  /**
   * Get the HOT leaf page for reading.
   *
   * @param key the primitive long key
   * @return the HOT leaf page, or null if not found
   */
  private @Nullable HOTLeafPage getLeafForRead(long key) {
    if (rootReference == null) {
      return null;
    }
    
    // Check if page is directly on the reference
    Page directPage = rootReference.getPage();
    if (directPage instanceof HOTLeafPage hotLeaf) {
      return hotLeaf;
    }
    
    // No page directly on reference - HOT pages are stored on reference after commit
    // For uncommitted transactions, the page should be in the transaction log
    
    return null;
  }
  
  /**
   * Get page from reference.
   *
   * <p>For HOT pages, the page is stored directly on the PageReference
   * after being written through the transaction log. This allows for
   * zero-copy access without additional storage layer calls.</p>
   *
   * @param ref the page reference
   * @return the page, or null if not found
   */
  private @Nullable Page getPageFromReference(PageReference ref) {
    if (ref == null) {
      return null;
    }
    
    // Check if page is directly on the reference
    // HOT pages are stored directly on the reference after commit
    return ref.getPage();
  }
  
  /**
   * Iterator over all entries in a HOT leaf page.
   */
  private class HOTLeafIterator implements Iterator<Map.Entry<Long, NodeReferences>> {
    private @Nullable HOTLeafPage currentLeaf;
    private int currentIndex;
    private Map.@Nullable Entry<Long, NodeReferences> nextEntry;
    
    HOTLeafIterator() {
      // Get the first leaf
      this.currentLeaf = getFirstLeaf();
      this.currentIndex = 0;
      advance();
    }
    
    @Override
    public boolean hasNext() {
      return nextEntry != null;
    }
    
    @Override
    public Map.Entry<Long, NodeReferences> next() {
      if (nextEntry == null) {
        throw new NoSuchElementException();
      }
      Map.Entry<Long, NodeReferences> result = nextEntry;
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
            long key = keySerializer.deserialize(keyBytes, 0, keyBytes.length);
            NodeReferences refs = NodeReferencesSerializer.deserialize(valueBytes);
            if (refs != null) {
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
    
    private @Nullable HOTLeafPage getFirstLeaf() {
      if (rootReference == null) {
        return null;
      }
      
      Page directPage = rootReference.getPage();
      if (directPage instanceof HOTLeafPage hotLeaf) {
        return hotLeaf;
      }
      
      // No page directly on reference
      
      return null;
    }
  }
}
