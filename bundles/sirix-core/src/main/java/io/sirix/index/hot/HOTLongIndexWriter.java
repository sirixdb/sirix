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
import io.sirix.cache.PageContainer;
import io.sirix.exception.SirixIOException;
import io.sirix.index.IndexType;
import io.sirix.index.SearchMode;
import io.sirix.index.redblacktree.RBTreeReader;
import io.sirix.index.redblacktree.keyvalue.NodeReferences;
import io.sirix.page.CASPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.NamePage;
import io.sirix.page.PageReference;
import io.sirix.page.PathPage;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.interfaces.Page;
import io.sirix.settings.Constants;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Primitive-specialized HOT index writer for long keys (PATH index).
 *
 * <p>Uses primitive {@code long} keys to avoid boxing overhead.
 * This is the high-performance variant for PATH index operations.</p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 *   <li>Primitive long parameters (no boxing)</li>
 *   <li>Thread-local byte buffers for serialization</li>
 *   <li>No Optional - uses @Nullable returns</li>
 * </ul>
 *
 * @author Johannes Lichtenberger
 * @see HOTIndexWriter
 */
public final class HOTLongIndexWriter {

  /**
   * Thread-local buffer for key serialization (8 bytes for long).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER =
      ThreadLocal.withInitial(() -> new byte[8]);

  /**
   * Thread-local buffer for value serialization (4KB default).
   */
  private static final ThreadLocal<byte[]> VALUE_BUFFER =
      ThreadLocal.withInitial(() -> new byte[4096]);

  private final StorageEngineWriter pageTrx;
  private final HOTLongKeySerializer keySerializer;
  private final IndexType indexType;
  
  /**
   * Index number for multi-index support.
   * Will be used when full storage engine integration is complete.
   */
  @SuppressWarnings("unused")
  private final int indexNumber;

  /** Cached root page reference (currently unused, kept for potential future caching). */
  @SuppressWarnings("unused")
  private PageReference rootReference;

  /**
   * Private constructor.
   *
   * @param pageTrx       the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType     the index type (should be PATH)
   * @param indexNumber   the index number
   */
  private HOTLongIndexWriter(StorageEngineWriter pageTrx, HOTLongKeySerializer keySerializer,
                             IndexType indexType, int indexNumber) {
    this.pageTrx = requireNonNull(pageTrx);
    this.keySerializer = requireNonNull(keySerializer);
    this.indexType = requireNonNull(indexType);
    this.indexNumber = indexNumber;
    
    // Initialize HOT index tree
    initializeHOTIndex();
  }

  /**
   * Initialize the HOT index tree structure.
   */
  private void initializeHOTIndex() {
    try {
      final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();
      
      // HOTLongIndexWriter is specialized for PATH indexes only.
      // CAS and NAME indexes use HOTIndexWriter<K> instead.
      if (indexType != IndexType.PATH) {
        throw new IllegalArgumentException(
            "HOTLongIndexWriter only supports PATH indexes, use HOTIndexWriter for " + indexType);
      }
      
      final PathPage pathPage = pageTrx.getPathPage(revisionRootPage);
      final PageReference reference = revisionRootPage.getPathPageReference();
      pageTrx.appendLogRecord(reference, PageContainer.getInstance(pathPage, pathPage));
      
      // Get existing reference first to check if index already exists
      PageReference existingRef = pathPage.getOrCreateReference(indexNumber);
      boolean indexExists = existingRef != null && 
          (existingRef.getKey() != Constants.NULL_ID_LONG || 
           existingRef.getLogKey() != Constants.NULL_ID_INT || 
           existingRef.getPage() != null);
      
      if (!indexExists) {
        // Only create new tree if index doesn't exist
        pathPage.createHOTPathIndexTree(pageTrx, indexNumber, pageTrx.getLog());
      }
      rootReference = pathPage.getOrCreateReference(indexNumber);
    } catch (SirixIOException e) {
      throw new IllegalStateException("Failed to initialize HOT index", e);
    }
  }

  /**
   * Creates a new HOTLongIndexWriter for PATH index.
   *
   * @param pageTrx     the storage engine writer
   * @param indexType   the index type (should be PATH)
   * @param indexNumber the index number
   * @return a new HOTLongIndexWriter instance
   */
  public static HOTLongIndexWriter create(StorageEngineWriter pageTrx,
                                          IndexType indexType, int indexNumber) {
    return new HOTLongIndexWriter(pageTrx, PathKeySerializer.INSTANCE, indexType, indexNumber);
  }

  /**
   * Index a primitive long key with NodeReferences.
   *
   * <p>If the key already exists, merges the NodeReferences (OR operation).
   * Uses primitive long to avoid boxing.</p>
   *
   * @param key   the index key (primitive long, no boxing)
   * @param value the node references
   * @param move  cursor movement mode (ignored for HOT)
   * @return the indexed value
   */
  public NodeReferences index(long key, NodeReferences value, RBTreeReader.MoveCursor move) {
    requireNonNull(value);

    // Serialize key to thread-local buffer (no boxing!)
    byte[] keyBuf = KEY_BUFFER.get();
    int keyLen = keySerializer.serialize(key, keyBuf, 0);

    // Serialize value to thread-local buffer
    byte[] valueBuf = VALUE_BUFFER.get();
    int valueLen = NodeReferencesSerializer.serialize(value, valueBuf, 0);
    if (valueLen > valueBuf.length) {
      valueBuf = new byte[valueLen];
      VALUE_BUFFER.set(valueBuf);
      valueLen = NodeReferencesSerializer.serialize(value, valueBuf, 0);
    }

    // Get or create HOT leaf page
    HOTLeafPage leaf = getLeafForWrite(key);

    // Merge entry
    leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen);

    return value;
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

    // Get HOT leaf page
    HOTLeafPage leaf = getLeafForRead(key);
    if (leaf == null) {
      return null;
    }

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
  }

  /**
   * Remove a node key from the NodeReferences for a primitive long key.
   *
   * @param key     the index key (primitive long)
   * @param nodeKey the node key to remove
   * @return true if the node key was removed
   */
  public boolean remove(long key, long nodeKey) {
    // Serialize key (no boxing!)
    byte[] keyBuf = KEY_BUFFER.get();
    keySerializer.serialize(key, keyBuf, 0);

    // Get HOT leaf page
    HOTLeafPage leaf = getLeafForWrite(key);
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
      leaf.updateValue(index, java.util.Arrays.copyOf(valueBuf, valueLen));
    }

    return removed;
  }

  /**
   * Get the storage engine writer.
   *
   * @return the storage engine writer
   */
  public StorageEngineWriter getPageTrx() {
    return pageTrx;
  }

  // ===== Private methods =====

  /**
   * Get the HOT leaf page for writing.
   *
   * <p>Retrieves or creates the HOT leaf page from the transaction log or storage.
   * Uses the storage engine's versioning-aware page loading for committed data.</p>
   *
   * @param key the primitive long key
   * @return the HOT leaf page for writing
   */
  private HOTLeafPage getLeafForWrite(long key) {
    // Get fresh reference from the index page to ensure consistency
    PageReference currentRef = getRootReference();
    if (currentRef == null) {
      throw new IllegalStateException("HOT index not initialized");
    }
    
    // Check transaction log first (uncommitted modifications)
    PageContainer container = pageTrx.getLog().get(currentRef);
    if (container != null) {
      Page page = container.getModified();
      if (page instanceof HOTLeafPage hotLeaf) {
        return hotLeaf;
      }
    }
    
    // Use storage engine's versioning-aware page loading for committed data
    HOTLeafPage existingLeaf = pageTrx.getHOTLeafPage(indexType, indexNumber);
    if (existingLeaf != null) {
      // Create COW copy for modifications
      HOTLeafPage modifiedLeaf = existingLeaf.copy();
      container = PageContainer.getInstance(existingLeaf, modifiedLeaf);
      pageTrx.getLog().put(currentRef, container);
      return modifiedLeaf;
    }
    
    // Create new leaf page if none exists
    HOTLeafPage newLeaf = new HOTLeafPage(
        currentRef.getKey() >= 0 ? currentRef.getKey() : 0,
        pageTrx.getRevisionNumber(),
        indexType
    );
    container = PageContainer.getInstance(newLeaf, newLeaf);
    pageTrx.getLog().put(currentRef, container);
    return newLeaf;
  }
  
  /**
   * Get the root reference for the index from the index page.
   * This ensures we always use the same reference object as the storage engine.
   */
  private PageReference getRootReference() {
    final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();
    return switch (indexType) {
      case PATH -> {
        final PathPage pathPage = pageTrx.getPathPage(revisionRootPage);
        yield pathPage.getOrCreateReference(indexNumber);
      }
      case CAS -> {
        final CASPage casPage = pageTrx.getCASPage(revisionRootPage);
        yield casPage.getOrCreateReference(indexNumber);
      }
      case NAME -> {
        final NamePage namePage = pageTrx.getNamePage(revisionRootPage);
        yield namePage.getOrCreateReference(indexNumber);
      }
      default -> null;
    };
  }

  /**
   * Get the HOT leaf page for reading.
   *
   * <p>Uses the storage engine's versioning-aware page loading.</p>
   *
   * @param key the primitive long key
   * @return the HOT leaf page, or null if not found
   */
  private @Nullable HOTLeafPage getLeafForRead(long key) {
    // Get fresh reference from the index page to ensure consistency
    PageReference currentRef = getRootReference();
    if (currentRef == null) {
      return null;
    }
    
    // Check transaction log first (uncommitted modifications)
    PageContainer container = pageTrx.getLog().get(currentRef);
    if (container != null) {
      Page page = container.getComplete();
      if (page instanceof HOTLeafPage hotLeaf) {
        return hotLeaf;
      }
    }
    
    // Use storage engine's versioning-aware page loading for committed data
    return pageTrx.getHOTLeafPage(indexType, indexNumber);
  }
}

