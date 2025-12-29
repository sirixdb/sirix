/*
 * Copyright (c) 2024, Sirix Contributors
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

package io.sirix.access.trx.page;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.index.IndexType;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import io.sirix.utils.OS;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Objects;

/**
 * HOT trie writer for HOT (Height Optimized Trie) navigation.
 * 
 * <p>This class provides an alternative to the bit-decomposition approach of {@code TrieWriter}
 * using semantic key-based navigation with HOT compound nodes.</p>
 * 
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li>Zero allocations on hot path (pre-allocated arrays instead of Deque)</li>
 *   <li>SIMD-optimized child lookup via HOTIndirectPage</li>
 *   <li>COW propagation using parent path tracking</li>
 *   <li>Integration with existing versioning infrastructure</li>
 * </ul>
 * 
 * <p><b>Usage:</b></p>
 * <pre>{@code
 * HOTTrieWriter writer = new HOTTrieWriter();
 * PageContainer container = writer.prepareKeyedLeafForModification(
 *     pageRtx, log, startReference, key, indexType, indexNumber);
 * HOTLeafPage leaf = (HOTLeafPage) container.getModified();
 * leaf.put(key, value);
 * }</pre>
 * 
 * @author Johannes Lichtenberger
 * @see HOTLeafPage
 * @see HOTIndirectPage
 */
public final class HOTTrieWriter {

  /** Maximum tree height (HOT typically has height 2-3). */
  private static final int MAX_TREE_HEIGHT = 8;
  
  /** Memory segment allocator. */
  @SuppressWarnings("unused")
  private final MemorySegmentAllocator allocator;
  
  /** Counter for generating page keys (temporary - will be replaced with proper key allocation). */
  private long nextPageKey = 1000000L; // Start high to avoid conflicts
  
  // ===== Pre-allocated COW path - ZERO allocations on hot path! =====
  private final PageReference[] cowPathRefs = new PageReference[MAX_TREE_HEIGHT];
  private final HOTIndirectPage[] cowPathNodes = new HOTIndirectPage[MAX_TREE_HEIGHT];
  private final int[] cowPathChildIndices = new int[MAX_TREE_HEIGHT];
  private int cowPathDepth = 0;
  
  /**
   * Create a new HOTTrieWriter.
   */
  public HOTTrieWriter() {
    this.allocator = OS.isWindows() 
        ? WindowsMemorySegmentAllocator.getInstance() 
        : LinuxMemorySegmentAllocator.getInstance();
  }
  
  /**
   * Navigate keyed trie (HOT) to find or create leaf page for given key.
   * Analogous to TrieWriter.prepareLeafOfTree() but uses semantic key navigation.
   *
   * @param pageRtx storage engine writer
   * @param log transaction intent log
   * @param startReference root reference (from PathPage/CASPage/NamePage)
   * @param key the search key (semantic bytes, not decomposed bit-by-bit)
   * @param indexType PATH/CAS/NAME
   * @param indexNumber the index number
   * @return PageContainer with complete and modifying HOTLeafPage, or null if not found
   */
  public @Nullable PageContainer prepareKeyedLeafForModification(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull PageReference startReference,
      byte[] key,
      @NonNull IndexType indexType,
      int indexNumber) {
    
    Objects.requireNonNull(pageRtx);
    Objects.requireNonNull(log);
    Objects.requireNonNull(startReference);
    Objects.requireNonNull(key);
    Objects.requireNonNull(indexType);
    
    // Reset COW path (no allocation!)
    cowPathDepth = 0;
    
    // Check if already in log (modified this transaction)
    PageContainer cached = log.get(startReference);
    if (cached != null) {
      return navigateWithinCachedTree(cached, key, pageRtx, log, indexType, indexNumber);
    }
    
    // Navigate HOT trie, COW'ing along the path (uses pre-allocated arrays)
    PageReference leafRef = navigateToLeaf(pageRtx, startReference, key, log);
    
    if (leafRef == null) {
      // Key not found, need to create new leaf
      return createNewLeaf(pageRtx, log, startReference, key, indexType, indexNumber);
    }
    
    // Get leaf page through versioning pipeline
    return dereferenceHOTLeafForModification(pageRtx, leafRef, log);
  }
  
  /**
   * Navigate through keyed trie (HOT) compound nodes to reach leaf.
   * Each node type uses discriminative bits for child-finding (SIMD-optimized).
   * Uses pre-allocated cowPath arrays - ZERO allocations!
   *
   * @param pageRtx the storage engine reader
   * @param startReference the starting reference
   * @param key the search key
   * @param log the transaction intent log
   * @return the leaf page reference, or null if not found
   */
  private @Nullable PageReference navigateToLeaf(
      @NonNull StorageEngineReader pageRtx,
      @NonNull PageReference startReference,
      byte[] key,
      @NonNull TransactionIntentLog log) {
    
    PageReference currentRef = startReference;
    
    while (true) {
      // Check log first
      PageContainer container = log.get(currentRef);
      Page page;
      if (container != null) {
        page = container.getComplete();
      } else {
        page = loadPage(pageRtx, currentRef);
      }
      
      if (page == null) {
        return null; // Empty trie
      }
      
      // Use pageKind check instead of instanceof (faster!)
      if (page instanceof HOTLeafPage) {
        return currentRef;
      }
      
      if (!(page instanceof HOTIndirectPage hotNode)) {
        return null; // Unexpected page type
      }
      
      // Find child reference using HOT node type-specific logic
      int childIndex = hotNode.findChildIndex(key);
      if (childIndex < 0) {
        return null; // Key not found
      }
      
      PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }
      
      // Record path for COW propagation (no allocation!)
      pushCowPath(currentRef, hotNode, childIndex);
      
      currentRef = childRef;
    }
  }
  
  /**
   * Flyweight push for COW path - no allocation!
   */
  private void pushCowPath(PageReference ref, HOTIndirectPage node, int childIdx) {
    if (cowPathDepth >= MAX_TREE_HEIGHT) {
      throw new IllegalStateException("HOT tree exceeds maximum height: " + MAX_TREE_HEIGHT);
    }
    cowPathRefs[cowPathDepth] = ref;
    cowPathNodes[cowPathDepth] = node;
    cowPathChildIndices[cowPathDepth] = childIdx;
    cowPathDepth++;
  }
  
  /**
   * Clear COW path references (allows GC but no allocation).
   */
  private void clearCowPath() {
    for (int i = 0; i < cowPathDepth; i++) {
      cowPathRefs[i] = null;
      cowPathNodes[i] = null;
    }
    cowPathDepth = 0;
  }
  
  /**
   * Dereference HOT leaf page for modification using versioning pipeline.
   * Uses pre-allocated cowPath arrays - ZERO allocations except for page copies!
   */
  private @Nullable PageContainer dereferenceHOTLeafForModification(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull PageReference leafRef,
      @NonNull TransactionIntentLog log) {
    
    // Check if already in log
    PageContainer existing = log.get(leafRef);
    if (existing != null) {
      return existing;
    }
    
    // Load the leaf page
    Page page = loadPage(pageRtx, leafRef);
    if (!(page instanceof HOTLeafPage hotLeaf)) {
      clearCowPath();
      return null;
    }
    
    // Create COW copy for modification
    // For HOTLeafPage, we need to create a new instance with same data
    HOTLeafPage modifiedLeaf = new HOTLeafPage(
        hotLeaf.getPageKey(),
        pageRtx.getRevisionNumber(),
        hotLeaf.getIndexType()
    );
    
    // Copy existing entries (this is the unavoidable COW cost)
    // TODO: Implement proper entry copying when HOTLeafPage has more methods
    
    PageContainer leafContainer = PageContainer.getInstance(hotLeaf, modifiedLeaf);
    log.put(leafRef, leafContainer);
    
    // Propagate COW up the path
    propagateCOW(log, leafRef);
    
    // Clear references to allow GC
    clearCowPath();
    
    return leafContainer;
  }
  
  /**
   * Propagate copy-on-write changes up to ancestors.
   * Each modified HOTIndirectPage gets a new copy in the log.
   * Uses pre-allocated cowPath arrays - ZERO allocations except for page copies!
   */
  private void propagateCOW(@NonNull TransactionIntentLog log, PageReference modifiedChildRef) {
    PageReference childRef = modifiedChildRef;
    
    // Iterate backwards through pre-allocated arrays (no iterator allocation!)
    for (int i = cowPathDepth - 1; i >= 0; i--) {
      PageReference parentRef = cowPathRefs[i];
      HOTIndirectPage parentNode = cowPathNodes[i];
      int childIndex = cowPathChildIndices[i];
      
      // COW the parent node (this allocation is unavoidable - it's the copy!)
      HOTIndirectPage newParent = parentNode.copyWithUpdatedChild(childIndex, childRef);
      
      // Update log
      log.put(parentRef, PageContainer.getInstance(newParent, newParent));
      
      childRef = parentRef;
    }
  }
  
  /**
   * Navigate within a cached tree that's already been modified.
   */
  private @Nullable PageContainer navigateWithinCachedTree(
      @NonNull PageContainer cached,
      byte[] key,
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull IndexType indexType,
      int indexNumber) {
    
    Page page = cached.getModified();
    
    if (page instanceof HOTLeafPage) {
      return cached;
    }
    
    if (page instanceof HOTIndirectPage hotNode) {
      int childIndex = hotNode.findChildIndex(key);
      if (childIndex >= 0) {
        PageReference childRef = hotNode.getChildReference(childIndex);
        if (childRef != null) {
          PageContainer childContainer = log.get(childRef);
          if (childContainer != null) {
            return navigateWithinCachedTree(childContainer, key, pageRtx, log, indexType, indexNumber);
          }
        }
      }
    }
    
    // Not found in cached tree, need to load
    return null;
  }
  
  /**
   * Create a new leaf page for a key that doesn't exist yet.
   */
  private @Nullable PageContainer createNewLeaf(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull PageReference startReference,
      byte[] key,
      @NonNull IndexType indexType,
      int indexNumber) {
    
    // Create new HOTLeafPage
    // TODO: Replace with proper page key allocation from RevisionRootPage
    long newPageKey = nextPageKey++;
    HOTLeafPage newLeaf = new HOTLeafPage(newPageKey, pageRtx.getRevisionNumber(), indexType);
    
    PageReference newRef = new PageReference();
    newRef.setKey(newPageKey);
    
    PageContainer container = PageContainer.getInstance(newLeaf, newLeaf);
    log.put(newRef, container);
    
    // If this is the first page (empty root), update the root reference
    if (cowPathDepth == 0) {
      // Update start reference to point to new leaf
      startReference.setKey(newPageKey);
      log.put(startReference, container);
    } else {
      // Update parent to include new child
      // This would require more complex logic for node splits
      // For now, just propagate the COW
      propagateCOW(log, newRef);
    }
    
    clearCowPath();
    return container;
  }
  
  /**
   * Load a page from storage.
   */
  private @Nullable Page loadPage(@NonNull StorageEngineReader pageRtx, @NonNull PageReference ref) {
    if (ref.getKey() < 0) {
      return null;
    }
    // This would call the actual page loading logic
    // For now, return null - actual implementation would use pageRtx
    return null;
  }
  
  /**
   * Handle leaf split - creates new HOTLeafPage and updates parent.
   * Uses pre-allocated cowPath arrays - no Deque allocation!
   *
   * @param pageRtx the storage engine writer
   * @param log the transaction intent log
   * @param fullPage the full page that needs splitting
   * @param pageRef the reference to the full page
   */
  public void handleLeafSplit(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull HOTLeafPage fullPage,
      @NonNull PageReference pageRef) {
    
    // Allocate new page key
    // TODO: Replace with proper page key allocation from RevisionRootPage
    long newPageKey = nextPageKey++;
    
    // Create new HOTLeafPage with off-heap segment
    HOTLeafPage rightPage = new HOTLeafPage(
        newPageKey,
        pageRtx.getRevisionNumber(),
        fullPage.getIndexType()
    );
    
    // Split entries (right half goes to new page)
    // TODO: Implement splitTo() method in HOTLeafPage
    // byte[] splitKey = fullPage.splitTo(rightPage);
    
    // Add new page to log
    PageReference newRef = new PageReference();
    newRef.setKey(newPageKey);
    log.put(newRef, PageContainer.getInstance(rightPage, rightPage));
    
    // Update parent to have two children instead of one
    if (cowPathDepth > 0) {
      int parentIdx = cowPathDepth - 1;
      updateParentForSplit(log, cowPathRefs[parentIdx], cowPathNodes[parentIdx],
                           cowPathChildIndices[parentIdx], pageRef, newRef);
    } else {
      // Root split - need to create new root
      createNewRootForSplit(pageRtx, log, pageRef, newRef);
    }
  }
  
  /**
   * Update parent node after a leaf split.
   */
  private void updateParentForSplit(
      @NonNull TransactionIntentLog log,
      @NonNull PageReference parentRef,
      @NonNull HOTIndirectPage parent,
      int originalChildIndex,
      @NonNull PageReference leftChild,
      @NonNull PageReference rightChild) {
    
    // This would need to add a new child to the parent node
    // For now, just log the parent with updated reference
    HOTIndirectPage newParent = parent.copyWithUpdatedChild(originalChildIndex, leftChild);
    // TODO: Add rightChild to newParent
    
    log.put(parentRef, PageContainer.getInstance(newParent, newParent));
  }
  
  /**
   * Create a new root node when the root splits.
   */
  private void createNewRootForSplit(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull PageReference leftChild,
      @NonNull PageReference rightChild) {
    
    // TODO: Replace with proper page key allocation from RevisionRootPage
    long newRootKey = nextPageKey++;
    
    // Create a BiNode pointing to the two children
    HOTIndirectPage newRoot = HOTIndirectPage.createBiNode(
        newRootKey,
        pageRtx.getRevisionNumber(),
        0, // discriminative bit position (would be computed from keys)
        leftChild,
        rightChild
    );
    
    PageReference newRootRef = new PageReference();
    newRootRef.setKey(newRootKey);
    log.put(newRootRef, PageContainer.getInstance(newRoot, newRoot));
  }
}

