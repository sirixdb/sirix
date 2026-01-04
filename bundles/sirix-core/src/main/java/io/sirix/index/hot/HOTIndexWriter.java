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
import io.sirix.page.HOTIndirectPage;
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
 * Generic HOT index writer for object keys (CASValue, QNm).
 *
 * <p>Replaces {@link io.sirix.index.redblacktree.RBTreeWriter} for HOT-based secondary indexes.
 * Uses thread-local buffers for zero-allocation key serialization.</p>
 *
 * <h2>Zero Allocation Design</h2>
 * <ul>
 *   <li>Thread-local byte buffers for key/value serialization</li>
 *   <li>No Optional - uses @Nullable returns</li>
 *   <li>Pre-allocated traversal state</li>
 * </ul>
 *
 * @param <K> the key type (must implement Comparable)
 * @author Johannes Lichtenberger
 */
public final class HOTIndexWriter<K extends Comparable<? super K>> {

  /**
   * Thread-local buffer for key serialization (256 bytes default).
   */
  private static final ThreadLocal<byte[]> KEY_BUFFER =
      ThreadLocal.withInitial(() -> new byte[256]);

  /**
   * Thread-local buffer for value serialization (4KB default).
   */
  private static final ThreadLocal<byte[]> VALUE_BUFFER =
      ThreadLocal.withInitial(() -> new byte[4096]);

  /**
   * Pre-allocated NodeReferences for reuse when creating new entries.
   * Will be used when full HOT trie navigation is implemented.
   */
  @SuppressWarnings("unused")
  private final NodeReferences tempRefs = new NodeReferences();

  private final StorageEngineWriter pageTrx;
  private final HOTKeySerializer<K> keySerializer;
  private final IndexType indexType;
  private final int indexNumber;
  
  /** HOT trie writer for handling page splits. */
  private final io.sirix.access.trx.page.HOTTrieWriter trieWriter;
  
  /** Cached root page reference for the index. */
  private PageReference rootReference;

  /**
   * Private constructor.
   *
   * @param pageTrx       the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType     the index type (PATH, CAS, NAME)
   * @param indexNumber   the index number
   */
  private HOTIndexWriter(StorageEngineWriter pageTrx, HOTKeySerializer<K> keySerializer,
                         IndexType indexType, int indexNumber) {
    this.pageTrx = requireNonNull(pageTrx);
    this.keySerializer = requireNonNull(keySerializer);
    this.indexType = requireNonNull(indexType);
    this.indexNumber = indexNumber;
    this.trieWriter = new io.sirix.access.trx.page.HOTTrieWriter();
    
    // Initialize HOT index tree
    initializeHOTIndex();
  }

  /**
   * Initialize the HOT index tree structure.
   */
  private void initializeHOTIndex() {
    try {
      final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();
      
      switch (indexType) {
        case PATH -> {
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
            pathPage.createHOTPathIndexTree(pageTrx, indexNumber, pageTrx.getLog());
          }
          rootReference = pathPage.getOrCreateReference(indexNumber);
        }
        case CAS -> {
          final CASPage casPage = pageTrx.getCASPage(revisionRootPage);
          final PageReference reference = revisionRootPage.getCASPageReference();
          pageTrx.appendLogRecord(reference, PageContainer.getInstance(casPage, casPage));
          
          // Get existing reference first to check if index already exists
          PageReference existingRef = casPage.getOrCreateReference(indexNumber);
          boolean indexExists = existingRef != null && 
              (existingRef.getKey() != Constants.NULL_ID_LONG || 
               existingRef.getLogKey() != Constants.NULL_ID_INT || 
               existingRef.getPage() != null);
          
          if (!indexExists) {
            casPage.createHOTCASIndexTree(pageTrx, indexNumber, pageTrx.getLog());
          }
          rootReference = casPage.getOrCreateReference(indexNumber);
        }
        case NAME -> {
          final NamePage namePage = pageTrx.getNamePage(revisionRootPage);
          final PageReference reference = revisionRootPage.getNamePageReference();
          pageTrx.appendLogRecord(reference, PageContainer.getInstance(namePage, namePage));
          
          // Get existing reference first to check if index already exists
          PageReference existingRef = namePage.getOrCreateReference(indexNumber);
          boolean indexExists = existingRef != null && 
              (existingRef.getKey() != Constants.NULL_ID_LONG || 
               existingRef.getLogKey() != Constants.NULL_ID_INT || 
               existingRef.getPage() != null);
          
          if (!indexExists) {
            namePage.createHOTNameIndexTree(pageTrx, indexNumber, pageTrx.getLog());
          }
          rootReference = namePage.getOrCreateReference(indexNumber);
        }
        default -> throw new IllegalArgumentException("Unsupported index type for HOT: " + indexType);
      }
    } catch (SirixIOException e) {
      throw new IllegalStateException("Failed to initialize HOT index", e);
    }
  }

  /**
   * Creates a new HOTIndexWriter.
   *
   * @param pageTrx       the storage engine writer
   * @param keySerializer the key serializer
   * @param indexType     the index type
   * @param indexNumber   the index number
   * @param <K>           the key type
   * @return a new HOTIndexWriter instance
   */
  public static <K extends Comparable<? super K>> HOTIndexWriter<K> create(
      StorageEngineWriter pageTrx, HOTKeySerializer<K> keySerializer,
      IndexType indexType, int indexNumber) {
    return new HOTIndexWriter<>(pageTrx, keySerializer, indexType, indexNumber);
  }

  /**
   * Index a key-value pair.
   *
   * <p>If the key already exists, merges the NodeReferences (OR operation).</p>
   *
   * @param key   the index key
   * @param value the node references
   * @param move  cursor movement mode (ignored for HOT)
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

    // Serialize value to thread-local buffer
    byte[] valueBuf = VALUE_BUFFER.get();
    int valueLen = NodeReferencesSerializer.serialize(value, valueBuf, 0);
    if (valueLen > valueBuf.length) {
      // Value too large - expand buffer
      valueBuf = new byte[valueLen];
      VALUE_BUFFER.set(valueBuf);
      valueLen = NodeReferencesSerializer.serialize(value, valueBuf, 0);
    }

    // Get or create HOT leaf page
    PageReference currentRef = getRootReference();
    if (currentRef == null) {
      throw new SirixIOException("HOT index not initialized for " + indexType);
    }
    
    HOTLeafPage leaf = getLeafForWrite(keyBuf, keyLen);
    
    // Merge entry
    boolean success = leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen);
    
    if (!success && leaf.needsSplit()) {
      // Handle split using HOTTrieWriter
      byte[] splitKey = trieWriter.handleLeafSplit(pageTrx, pageTrx.getLog(), leaf, currentRef, currentRef);
      
      // Now determine which page should receive the new entry
      // Compare key with splitKey to route to correct page
      byte[] keySlice = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
      int cmp = compareKeys(keySlice, splitKey);
      
      if (cmp < 0) {
        // Key goes to left page (the original, now smaller)
        success = leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen);
      } else {
        // Key goes to right page - need to get it from the log
        PageContainer rightContainer = findRightPageAfterSplit(currentRef);
        if (rightContainer != null && rightContainer.getModified() instanceof HOTLeafPage rightLeaf) {
          success = rightLeaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen);
        } else {
          throw new SirixIOException("Failed to find right page after split");
        }
      }
      
      if (!success) {
        throw new SirixIOException("Failed to insert after split - this should not happen. " +
            "Index: " + indexType + ", entries: " + leaf.getEntryCount());
      }
    }

    return value;
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

    // Serialize key
    byte[] keyBuf = KEY_BUFFER.get();
    int keyLen = keySerializer.serialize(key, keyBuf, 0);

    // Get HOT leaf page
    HOTLeafPage leaf = getLeafForRead(keyBuf, keyLen);
    if (leaf == null) {
      return null;
    }

    // Find entry
    byte[] keySlice = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
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
  }

  /**
   * Remove a node key from the NodeReferences for a key.
   *
   * <p>If the NodeReferences becomes empty, sets a tombstone.</p>
   *
   * @param key     the index key
   * @param nodeKey the node key to remove
   * @return true if the node key was removed
   */
  public boolean remove(K key, long nodeKey) {
    requireNonNull(key);

    // Serialize key
    byte[] keyBuf = KEY_BUFFER.get();
    int keyLen = keySerializer.serialize(key, keyBuf, 0);

    // Get HOT leaf page
    HOTLeafPage leaf = getLeafForWrite(keyBuf, keyLen);
    if (leaf == null) {
      return false;
    }

    // Find entry
    byte[] keySlice = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
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
   * @param keyBuf the key buffer
   * @param keyLen the key length
   * @return the HOT leaf page for writing
   */
  private HOTLeafPage getLeafForWrite(byte[] keyBuf, int keyLen) {
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
      } else if (page instanceof HOTIndirectPage) {
        // Navigate through the indirect page tree to find the right leaf
        HOTLeafPage navigatedLeaf = navigateToLeaf(keyBuf, keyLen);
        if (navigatedLeaf != null) {
          return navigatedLeaf;
        }
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
   * Get the HOT leaf page for reading.
   *
   * <p>Uses the storage engine's versioning-aware page loading.</p>
   *
   * @param keyBuf the key buffer
   * @param keyLen the key length
   * @return the HOT leaf page, or null if not found
   */
  private @Nullable HOTLeafPage getLeafForRead(byte[] keyBuf, int keyLen) {
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
   * Compare two byte array keys lexicographically.
   *
   * @param key1 first key
   * @param key2 second key
   * @return negative if key1 < key2, 0 if equal, positive if key1 > key2
   */
  private static int compareKeys(byte[] key1, byte[] key2) {
    int minLen = Math.min(key1.length, key2.length);
    for (int i = 0; i < minLen; i++) {
      int cmp = (key1[i] & 0xFF) - (key2[i] & 0xFF);
      if (cmp != 0) {
        return cmp;
      }
    }
    return key1.length - key2.length;
  }
  
  /**
   * Find the right page after a split by looking through the transaction log.
   * After a split, the root reference should point to a HOTIndirectPage with two children.
   *
   * @param rootRef the root reference
   * @return the page container for the right child, or null if not found
   */
  private @Nullable PageContainer findRightPageAfterSplit(PageReference rootRef) {
    // After split, the root should be a HOTIndirectPage (BiNode) with two children
    PageContainer rootContainer = pageTrx.getLog().get(rootRef);
    if (rootContainer == null) {
      return null;
    }
    
    Page rootPage = rootContainer.getModified();
    if (rootPage instanceof HOTIndirectPage indirectPage) {
      // Get the right child (index 1)
      if (indirectPage.getNumChildren() >= 2) {
        PageReference rightRef = indirectPage.getChildReference(1);
        if (rightRef != null) {
          return pageTrx.getLog().get(rightRef);
        }
      }
    }
    
    // If root is still a leaf page, something went wrong
    return null;
  }
  
  /**
   * Navigate to the correct leaf page for a given key.
   * Handles both simple (single leaf) and complex (indirect page tree) structures.
   *
   * @param key the search key
   * @param keyLen the key length
   * @return the appropriate HOTLeafPage, or null if not found
   */
  private @Nullable HOTLeafPage navigateToLeaf(byte[] key, int keyLen) {
    PageReference rootRef = getRootReference();
    if (rootRef == null) {
      return null;
    }
    
    // Check transaction log first
    PageContainer container = pageTrx.getLog().get(rootRef);
    if (container != null) {
      Page page = container.getModified();
      if (page instanceof HOTLeafPage hotLeaf) {
        // Simple case - root is a leaf
        return hotLeaf;
      } else if (page instanceof HOTIndirectPage indirectPage) {
        // Complex case - navigate through indirect pages
        return navigateThroughIndirectPage(indirectPage, key, keyLen);
      }
    }
    
    // Fall back to storage engine
    return pageTrx.getHOTLeafPage(indexType, indexNumber);
  }
  
  /**
   * Navigate through a HOTIndirectPage to find the appropriate leaf.
   *
   * @param indirectPage the indirect page to navigate through
   * @param key the search key
   * @param keyLen the key length
   * @return the appropriate HOTLeafPage, or null if not found
   */
  private @Nullable HOTLeafPage navigateThroughIndirectPage(HOTIndirectPage indirectPage, byte[] key, int keyLen) {
    byte[] keySlice = keyLen == key.length ? key : java.util.Arrays.copyOf(key, keyLen);
    
    // Find the appropriate child using the indirect page's lookup
    int childIndex = indirectPage.findChildIndex(keySlice);
    if (childIndex == HOTIndirectPage.NOT_FOUND) {
      // Default to first child if not found
      childIndex = 0;
    }
    
    PageReference childRef = indirectPage.getChildReference(childIndex);
    if (childRef == null) {
      return null;
    }
    
    // Check transaction log for child
    PageContainer childContainer = pageTrx.getLog().get(childRef);
    if (childContainer != null) {
      Page childPage = childContainer.getModified();
      if (childPage instanceof HOTLeafPage hotLeaf) {
        return hotLeaf;
      } else if (childPage instanceof HOTIndirectPage childIndirect) {
        // Recursively navigate
        return navigateThroughIndirectPage(childIndirect, key, keyLen);
      }
    }
    
    // Try to load from storage
    // For now, return null - proper implementation would load from storage
    return null;
  }
}

