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
import io.sirix.cache.TransactionIntentLog;
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

  private final StorageEngineWriter pageTrx;
  private final HOTKeySerializer<K> keySerializer;
  private final IndexType indexType;
  private final int indexNumber;
  
  /** HOT trie writer for handling page splits. */
  private final io.sirix.access.trx.page.HOTTrieWriter trieWriter;
  
  /** Cached root page reference for the index. */
  private PageReference rootReference;

  /**
   * Result of navigating to a leaf page, including the path from root.
   * This is needed for proper split handling.
   */
  private record LeafNavigationResult(
      HOTLeafPage leaf,
      PageReference leafRef,
      HOTIndirectPage[] pathNodes,
      PageReference[] pathRefs,
      int[] pathChildIndices,
      int pathDepth
  ) {}

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
          // CRITICAL: Check if PathPage is already in the transaction log first
          final PageReference pathPageRef = revisionRootPage.getPathPageReference();
          PageContainer pathContainer = pageTrx.getLog().get(pathPageRef);
          final PathPage pathPage;
          if (pathContainer != null && pathContainer.getModified() instanceof PathPage modifiedPath) {
            pathPage = modifiedPath;
          } else {
            pathPage = pageTrx.getPathPage(revisionRootPage);
            pageTrx.appendLogRecord(pathPageRef, PageContainer.getInstance(pathPage, pathPage));
          }
          
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
          // CRITICAL: Check if CASPage is already in the transaction log first
          // This ensures we use the SAME CASPage instance that was modified by previous
          // HOTIndexWriter instances in the same transaction
          final PageReference casPageRef = revisionRootPage.getCASPageReference();
          PageContainer casContainer = pageTrx.getLog().get(casPageRef);
          final CASPage casPage;
          if (casContainer != null && casContainer.getModified() instanceof CASPage modifiedCAS) {
            // Use the CASPage from the log (contains modifications from this transaction)
            casPage = modifiedCAS;
          } else {
            // Load from storage and put in log
            casPage = pageTrx.getCASPage(revisionRootPage);
            pageTrx.appendLogRecord(casPageRef, PageContainer.getInstance(casPage, casPage));
          }
          
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
          // CRITICAL: Check if NamePage is already in the transaction log first
          final PageReference namePageRef = revisionRootPage.getNamePageReference();
          PageContainer nameContainer = pageTrx.getLog().get(namePageRef);
          final NamePage namePage;
          if (nameContainer != null && nameContainer.getModified() instanceof NamePage modifiedName) {
            namePage = modifiedName;
          } else {
            namePage = pageTrx.getNamePage(revisionRootPage);
            pageTrx.appendLogRecord(namePageRef, PageContainer.getInstance(namePage, namePage));
          }
          
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
   * Maximum retry attempts for insert after split/compact.
   */
  private static final int MAX_INSERT_RETRIES = 3;

  /**
   * Index a key-value pair.
   *
   * <p>If the key already exists, merges the NodeReferences (OR operation).</p>
   *
   * <p><b>Edge Case Handling:</b> When many identical keys are merged, the value
   * can grow very large. If the value becomes too large for a single page and
   * the page has only 1 entry (so it can't be split), this method handles it by:
   * <ol>
   *   <li>Attempting to compact the page to reclaim fragmented space</li>
   *   <li>Retrying the insert operation after compaction</li>
   *   <li>If still failing after retries, throwing an informative exception</li>
   * </ol>
   * </p>
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

    // IMPORTANT: Use the cached root reference to ensure consistency
    // The same reference object must be used across all operations so that
    // log.put updates are visible to subsequent log.get calls.
    if (rootReference == null) {
      throw new SirixIOException("HOT index not initialized for " + indexType);
    }
    PageReference rootRef = rootReference;
    
    LeafNavigationResult navResult = getLeafWithPath(rootRef, keyBuf, keyLen);
    HOTLeafPage leaf = navResult.leaf();
    
    // Merge entry
    boolean success = leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen);
    
    // If merge failed, we need to split or compact
    if (!success) {
      success = handleInsertFailure(rootRef, navResult, keyBuf, keyLen, valueBuf, valueLen);
      
      if (!success) {
        // Last resort: check if this is a case of a single huge value
        int entryCount = navResult.leaf().getEntryCount();
        long remainingSpace = navResult.leaf().getRemainingSpace();
        int requiredSpace = 2 + keyLen + 2 + valueLen;
        
        throw new SirixIOException(
            "Failed to insert entry after split/compact attempts. " +
            "This may indicate a single value that exceeds page capacity. " +
            "Index: " + indexType + ", entries: " + entryCount + 
            ", remaining space: " + remainingSpace + ", required: " + requiredSpace +
            ". Consider limiting the number of identical keys or using overflow pages.");
      }
    }

    return value;
  }
  
  /**
   * Handle insert failure by attempting split and/or compact operations.
   *
   * @param rootRef the root reference
   * @param navResult the navigation result with leaf and path
   * @param keyBuf the key buffer
   * @param keyLen the key length
   * @param valueBuf the value buffer
   * @param valueLen the value length
   * @return true if insert eventually succeeded
   */
  private boolean handleInsertFailure(
      PageReference rootRef,
      LeafNavigationResult navResult,
      byte[] keyBuf, int keyLen,
      byte[] valueBuf, int valueLen) {
    
    HOTLeafPage leaf = navResult.leaf();
    
    for (int attempt = 0; attempt < MAX_INSERT_RETRIES; attempt++) {
      // Check if the page can be split
      if (leaf.canSplit()) {
        // Handle split using HOTTrieWriter with proper path information
        byte[] splitKey = trieWriter.handleLeafSplitWithPath(
            pageTrx, 
            pageTrx.getLog(), 
            leaf, 
            navResult.leafRef(),
            rootRef,
            navResult.pathNodes(),
            navResult.pathRefs(),
            navResult.pathChildIndices(),
            navResult.pathDepth()
        );
        
        // CRITICAL: Mark index page dirty so updated root reference gets persisted
        markIndexPageDirty();
        
        if (splitKey != null) {
          // Split succeeded - re-navigate and retry insert
          navResult = getLeafWithPath(rootRef, keyBuf, keyLen);
          leaf = navResult.leaf();
          
          if (leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen)) {
            return true;
          }
        }
        // Split returned null (couldn't split) - fall through to compact
      }
      
      // Try compacting the page to reclaim fragmented space
      int reclaimedSpace = leaf.compact();
      if (reclaimedSpace > 0) {
        // Update the page in the log after compaction
        pageTrx.getLog().put(navResult.leafRef(), 
            PageContainer.getInstance(leaf, leaf));
        
        // Try inserting again after compaction
        if (leaf.mergeWithNodeRefs(keyBuf, keyLen, valueBuf, valueLen)) {
          return true;
        }
      }
      
      // If neither split nor compact helped on this attempt,
      // re-navigate to get fresh state for next attempt
      if (attempt < MAX_INSERT_RETRIES - 1) {
        navResult = getLeafWithPath(rootRef, keyBuf, keyLen);
        leaf = navResult.leaf();
      }
    }
    
    return false;
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
   * Get the HOT leaf page for writing, tracking the navigation path.
   *
   * <p>The path is needed for proper split handling - when a leaf splits,
   * we need to update its parent in the tree.</p>
   *
   * @param keyBuf the key buffer
   * @param keyLen the key length
   * @return navigation result with leaf and path information
   */
  /**
   * Navigate to the correct leaf page for a key, tracking the path from root.
   *
   * @param rootRef the root reference (must be obtained ONCE and reused)
   * @param keyBuf the key buffer
   * @param keyLen the key length
   * @return navigation result with leaf and path
   */
  private LeafNavigationResult getLeafWithPath(PageReference rootRef, byte[] keyBuf, int keyLen) {
    if (rootRef == null) {
      throw new IllegalStateException("HOT index not initialized");
    }
    
    // Use dynamic arrays for deep trees
    java.util.ArrayList<HOTIndirectPage> pathNodesList = new java.util.ArrayList<>();
    java.util.ArrayList<PageReference> pathRefsList = new java.util.ArrayList<>();
    java.util.ArrayList<Integer> pathChildIndicesList = new java.util.ArrayList<>();
    
    PageReference currentRef = rootRef;
    byte[] keySlice = keyLen == keyBuf.length ? keyBuf : java.util.Arrays.copyOf(keyBuf, keyLen);
    
    // Check transaction log first (uncommitted modifications)
    PageContainer container = pageTrx.getLog().get(currentRef);
    if (container != null) {
      Page page = container.getModified();
      
      // Navigate through indirect pages, tracking the path
      // No depth limit - let tree grow as deep as necessary
      while (page instanceof HOTIndirectPage indirectPage) {
        pathNodesList.add(indirectPage);
        pathRefsList.add(currentRef);
        
        int childIndex = indirectPage.findChildIndex(keySlice);
        if (childIndex < 0) {
          childIndex = 0; // Default to first child
        }
        pathChildIndicesList.add(childIndex);
        
        PageReference childRef = indirectPage.getChildReference(childIndex);
        if (childRef == null) {
          throw new IllegalStateException("Null child reference in indirect page");
        }
        currentRef = childRef;
        
        // Get child page from log or storage
        PageContainer childContainer = pageTrx.getLog().get(childRef);
        if (childContainer != null) {
          page = childContainer.getModified();
        } else {
          // Child not in log - check if the child reference has a page swizzled on it
          if (childRef.getPage() != null) {
            page = childRef.getPage();
          } else {
            // Load from storage and prepare for modification
            page = pageTrx.loadHOTPage(childRef);
            if (page instanceof HOTLeafPage loadedLeaf) {
              // Create COW copy for modifications and put in log
              HOTLeafPage modifiedLeaf = loadedLeaf.copy();
              childContainer = PageContainer.getInstance(loadedLeaf, modifiedLeaf);
              pageTrx.getLog().put(childRef, childContainer);
              page = modifiedLeaf;
            }
          }
        }
      }
      
      if (page instanceof HOTLeafPage hotLeaf) {
        // Convert lists to arrays for return
        int pathDepth = pathNodesList.size();
        HOTIndirectPage[] pathNodes = pathNodesList.toArray(new HOTIndirectPage[pathDepth]);
        PageReference[] pathRefs = pathRefsList.toArray(new PageReference[pathDepth]);
        int[] pathChildIndices = pathChildIndicesList.stream().mapToInt(Integer::intValue).toArray();
        return new LeafNavigationResult(hotLeaf, currentRef, pathNodes, pathRefs, pathChildIndices, pathDepth);
      }
    }
    
    // Use storage engine's versioning-aware page loading for committed data
    HOTLeafPage existingLeaf = pageTrx.getHOTLeafPage(indexType, indexNumber);
    if (existingLeaf != null) {
      // Create COW copy for modifications
      HOTLeafPage modifiedLeaf = existingLeaf.copy();
      container = PageContainer.getInstance(existingLeaf, modifiedLeaf);
      pageTrx.getLog().put(rootRef, container);
      return new LeafNavigationResult(modifiedLeaf, rootRef, new HOTIndirectPage[0], new PageReference[0], new int[0], 0);
    }
    
    // Create new leaf page if none exists
    long pageKey = rootRef.getKey() >= 0 ? rootRef.getKey() : 0;
    HOTLeafPage newLeaf = new HOTLeafPage(pageKey, pageTrx.getRevisionNumber(), indexType);
    container = PageContainer.getInstance(newLeaf, newLeaf);
    pageTrx.getLog().put(rootRef, container);
    return new LeafNavigationResult(newLeaf, rootRef, new HOTIndirectPage[0], new PageReference[0], new int[0], 0);
  }
  
  /**
   * Get the HOT leaf page for writing (simple version without path tracking).
   *
   * @param keyBuf the key buffer
   * @param keyLen the key length
   * @return the HOT leaf page for writing
   */
  private HOTLeafPage getLeafForWrite(byte[] keyBuf, int keyLen) {
    PageReference rootRef = getRootReference();
    return getLeafWithPath(rootRef, keyBuf, keyLen).leaf();
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
   * Mark the index page (CASPage/PathPage/NamePage) as dirty so it gets persisted.
   * This must be called after splits that update the root reference.
   */
  private void markIndexPageDirty() {
    final RevisionRootPage revisionRootPage = pageTrx.getActualRevisionRootPage();
    final TransactionIntentLog log = pageTrx.getLog();
    
    switch (indexType) {
      case CAS -> {
        final CASPage casPage = pageTrx.getCASPage(revisionRootPage);
        // Get the reference to the CASPage from the revision root
        final PageReference casPageRef = revisionRootPage.getCASPageReference();
        // Put the updated CASPage into the log
        log.put(casPageRef, PageContainer.getInstance(casPage, casPage));
      }
      case PATH -> {
        final PathPage pathPage = pageTrx.getPathPage(revisionRootPage);
        final PageReference pathPageRef = revisionRootPage.getPathPageReference();
        log.put(pathPageRef, PageContainer.getInstance(pathPage, pathPage));
      }
      case NAME -> {
        final NamePage namePage = pageTrx.getNamePage(revisionRootPage);
        final PageReference namePageRef = revisionRootPage.getNamePageReference();
        log.put(namePageRef, PageContainer.getInstance(namePage, namePage));
      }
      default -> {}
    }
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


