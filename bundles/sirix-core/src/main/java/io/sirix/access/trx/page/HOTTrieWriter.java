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
import io.sirix.index.hot.DiscriminativeBitComputer;
import io.sirix.index.hot.HeightOptimalSplitter;
import io.sirix.index.hot.NodeUpgradeManager;
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

  /** Maximum tree height - increased to handle unbalanced trees during inserts. */
  private static final int MAX_TREE_HEIGHT = 64;
  
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
    return prepareKeyedLeafForModification(pageRtx, log, startReference, key, indexType, indexNumber, null);
  }

  /**
   * Navigate keyed trie (HOT) with async auto-commit support.
   * Uses layered lookup: active TIL -> pending snapshot -> disk.
   *
   * @param pageRtx storage engine writer
   * @param log transaction intent log
   * @param startReference root reference (from PathPage/CASPage/NamePage)
   * @param key the search key (semantic bytes, not decomposed bit-by-bit)
   * @param indexType PATH/CAS/NAME
   * @param indexNumber the index number
   * @param pendingSnapshot the pending snapshot for layered lookup (may be null)
   * @return PageContainer with complete and modifying HOTLeafPage, or null if not found
   */
  public @Nullable PageContainer prepareKeyedLeafForModification(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull PageReference startReference,
      byte[] key,
      @NonNull IndexType indexType,
      int indexNumber,
      @Nullable CommitSnapshot pendingSnapshot) {
    
    Objects.requireNonNull(pageRtx);
    Objects.requireNonNull(log);
    Objects.requireNonNull(startReference);
    Objects.requireNonNull(key);
    Objects.requireNonNull(indexType);
    
    // Reset COW path (no allocation!)
    cowPathDepth = 0;
    
    // Check if already in log (modified this transaction) - using layered lookup
    PageContainer cached = getFromActiveOrPending(startReference, log, pendingSnapshot);
    if (cached != null) {
      return navigateWithinCachedTree(cached, key, pageRtx, log, indexType, indexNumber, pendingSnapshot);
    }
    
    // Navigate HOT trie, COW'ing along the path (uses pre-allocated arrays)
    PageReference leafRef = navigateToLeaf(pageRtx, startReference, key, log, pendingSnapshot);
    
    if (leafRef == null) {
      // Key not found, need to create new leaf
      return createNewLeaf(pageRtx, log, startReference, key, indexType, indexNumber);
    }
    
    // Get leaf page through versioning pipeline
    return dereferenceHOTLeafForModification(pageRtx, leafRef, log, pendingSnapshot);
  }

  /**
   * 3-step layered lookup for async auto-commit support.
   * Checks: active TIL (via generation) -> pending snapshot -> null (disk)
   *
   * @param ref the page reference
   * @param log the transaction intent log
   * @param pendingSnapshot the pending snapshot (may be null)
   * @return the page container, or null if should load from disk
   */
  private @Nullable PageContainer getFromActiveOrPending(
      @NonNull PageReference ref,
      @NonNull TransactionIntentLog log,
      @Nullable CommitSnapshot pendingSnapshot) {

    // STEP 1: FAST PATH - Check if ref is in active TIL via generation
    final int currentGen = log.getCurrentGeneration();
    if (ref.isInActiveTil(currentGen)) {
      return log.getUnchecked(ref.getLogKey());
    }

    // STEP 2: Check pending snapshot
    if (pendingSnapshot == null) {
      return null;
    }

    return getFromSnapshot(ref, pendingSnapshot);
  }

  /**
   * Look up a page in the pending snapshot.
   */
  private @Nullable PageContainer getFromSnapshot(
      @NonNull PageReference ref,
      @NonNull CommitSnapshot snapshot) {

    // If commit is complete, use disk offset mapping for lazy propagation
    if (snapshot.isCommitComplete()) {
      final int logKey = ref.getLogKey();
      if (logKey >= 0) {
        final long diskOffset = snapshot.getDiskOffset(logKey);
        if (diskOffset >= 0) {
          // Lazy propagation: update ref's key for future disk loads
          ref.setKey(diskOffset);
          return null; // Signal: load from disk using updated key
        }
      }
      return null;
    }

    // Try identity-based lookup
    PageContainer result = snapshot.getByIdentity(ref);
    if (result != null) {
      return result;
    }

    // Fallback for cloned refs
    final int logKey = ref.getLogKey();
    if (logKey >= 0 && logKey < snapshot.size()) {
      return snapshot.getEntry(logKey);
    }

    return null;
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
    return navigateToLeaf(pageRtx, startReference, key, log, null);
  }

  /**
   * Navigate through keyed trie (HOT) with async auto-commit support.
   * Uses layered lookup: active TIL -> pending snapshot -> disk.
   *
   * @param pageRtx the storage engine reader
   * @param startReference the starting reference
   * @param key the search key
   * @param log the transaction intent log
   * @param pendingSnapshot the pending snapshot (may be null)
   * @return the leaf page reference, or null if not found
   */
  private @Nullable PageReference navigateToLeaf(
      @NonNull StorageEngineReader pageRtx,
      @NonNull PageReference startReference,
      byte[] key,
      @NonNull TransactionIntentLog log,
      @Nullable CommitSnapshot pendingSnapshot) {
    
    PageReference currentRef = startReference;
    
    while (true) {
      // Use layered lookup: active TIL -> pending snapshot -> disk
      PageContainer container = getFromActiveOrPending(currentRef, log, pendingSnapshot);
      Page page;
      if (container != null) {
        page = container.getComplete();
        // Check if page is in pending snapshot and perform COW if needed
        if (pendingSnapshot != null && !pendingSnapshot.isCommitComplete()) {
          PageContainer snapshotContainer = pendingSnapshot.getByIdentity(currentRef);
          if (snapshotContainer != null && snapshotContainer == container) {
            // Page is from snapshot - need to COW for this HOTIndirectPage
            if (page instanceof HOTIndirectPage hotNode) {
              HOTIndirectPage copy = new HOTIndirectPage(hotNode);
              PageContainer cowContainer = PageContainer.getInstance(copy, copy);
              log.put(currentRef, cowContainer);
              page = copy;
            }
          }
        }
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
    return dereferenceHOTLeafForModification(pageRtx, leafRef, log, null);
  }

  /**
   * Dereference HOT leaf page with async auto-commit support.
   * Uses layered lookup for correct page resolution.
   */
  private @Nullable PageContainer dereferenceHOTLeafForModification(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull PageReference leafRef,
      @NonNull TransactionIntentLog log,
      @Nullable CommitSnapshot pendingSnapshot) {
    
    // Check if already in active TIL (via generation check)
    if (leafRef.isInActiveTil(log.getCurrentGeneration())) {
      PageContainer existing = log.getUnchecked(leafRef.getLogKey());
      if (existing != null) {
        return existing;
      }
    }

    // Check pending snapshot
    PageContainer snapshotContainer = null;
    if (pendingSnapshot != null && !pendingSnapshot.isCommitComplete()) {
      snapshotContainer = pendingSnapshot.getByIdentity(leafRef);
      if (snapshotContainer == null) {
        int logKey = leafRef.getLogKey();
        if (logKey >= 0 && logKey < pendingSnapshot.size()) {
          snapshotContainer = pendingSnapshot.getEntry(logKey);
        }
      }
    }
    
    // Load the leaf page (from snapshot or disk)
    Page page;
    if (snapshotContainer != null) {
      page = snapshotContainer.getComplete();
    } else {
      page = loadPage(pageRtx, leafRef);
    }

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
    return navigateWithinCachedTree(cached, key, pageRtx, log, indexType, indexNumber, null);
  }

  /**
   * Navigate within a cached tree with async auto-commit support.
   * Uses layered lookup for child page resolution.
   */
  private @Nullable PageContainer navigateWithinCachedTree(
      @NonNull PageContainer cached,
      byte[] key,
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull IndexType indexType,
      int indexNumber,
      @Nullable CommitSnapshot pendingSnapshot) {
    
    Page page = cached.getModified();
    
    if (page instanceof HOTLeafPage) {
      return cached;
    }
    
    if (page instanceof HOTIndirectPage hotNode) {
      int childIndex = hotNode.findChildIndex(key);
      if (childIndex >= 0) {
        PageReference childRef = hotNode.getChildReference(childIndex);
        if (childRef != null) {
          // Use layered lookup for child container
          PageContainer childContainer = getFromActiveOrPending(childRef, log, pendingSnapshot);
          if (childContainer != null) {
            return navigateWithinCachedTree(childContainer, key, pageRtx, log, indexType, indexNumber, pendingSnapshot);
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
   * @param rootReference the root reference for the index (to update if root splits)
   * @return the split key that separates the two pages
   */
  public byte[] handleLeafSplit(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull HOTLeafPage fullPage,
      @NonNull PageReference pageRef,
      @NonNull PageReference rootReference) {
    // Delegate to the path-aware version with empty path (root level split)
    return handleLeafSplitWithPath(pageRtx, log, fullPage, pageRef, rootReference,
        new HOTIndirectPage[MAX_TREE_HEIGHT], new PageReference[MAX_TREE_HEIGHT], new int[MAX_TREE_HEIGHT], 0);
  }
  
  /**
   * Handle leaf page split with explicit path information.
   *
   * <p>This method properly handles splits at any level of the tree by accepting
   * the navigation path from root to the splitting leaf.</p>
   *
   * <p><b>Edge Case:</b> When the page has only 1 entry (common with many identical
   * keys that get merged), splitting won't help. In this case, the method attempts
   * to compact the page to free fragmented space. If that still doesn't help,
   * the method returns {@code null} to signal that the caller needs to handle
   * this differently (e.g., using overflow pages for large values).</p>
   *
   * @param pageRtx       the storage engine writer
   * @param log           the transaction intent log
   * @param fullPage      the full page that needs splitting
   * @param leafRef       the reference to the leaf being split
   * @param rootReference the root reference for the index
   * @param pathNodes     the indirect page nodes on the path from root to leaf
   * @param pathRefs      the page references on the path from root to leaf
   * @param pathChildIndices the child indices taken at each level
   * @param pathDepth     the depth of the path (0 means leaf is at root)
   * @return the split key that separates the two pages, or {@code null} if
   *         the page cannot be split (e.g., only 1 entry with large merged value)
   */
  public @Nullable byte[] handleLeafSplitWithPath(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull HOTLeafPage fullPage,
      @NonNull PageReference leafRef,
      @NonNull PageReference rootReference,
      HOTIndirectPage[] pathNodes,
      PageReference[] pathRefs,
      int[] pathChildIndices,
      int pathDepth) {
    
    Objects.requireNonNull(pageRtx);
    Objects.requireNonNull(log);
    Objects.requireNonNull(fullPage);
    Objects.requireNonNull(leafRef);
    Objects.requireNonNull(rootReference);
    
    // Check if page can be split
    if (!fullPage.canSplit()) {
      // Try compacting the page first to free fragmented space
      int reclaimedSpace = fullPage.compact();
      if (reclaimedSpace > 0) {
        // Update the page in the log after compaction
        log.put(leafRef, PageContainer.getInstance(fullPage, fullPage));
        // Caller should retry the insert after compaction
        return null;
      }
      // Page has only 1 entry with a very large merged value
      // Splitting won't help - caller needs to use overflow pages
      return null;
    }
    
    // Allocate new page key for right sibling
    long newPageKey = nextPageKey++;
    long newRootPageKey = nextPageKey++;
    
    // Create new HOTLeafPage with off-heap segment
    HOTLeafPage rightPage = new HOTLeafPage(
        newPageKey,
        pageRtx.getRevisionNumber(),
        fullPage.getIndexType()
    );
    
    // Use HeightOptimalSplitter for proper height-optimal splitting
    HeightOptimalSplitter.SplitResult splitResult = HeightOptimalSplitter.splitLeafPage(
        fullPage, rightPage, newRootPageKey, pageRtx.getRevisionNumber());
    
    // Handle case where split failed (e.g., single large entry)
    if (splitResult == null) {
      // Close the unused right page to release memory
      rightPage.close();
      // Compact the original page and let caller retry
      fullPage.compact();
      log.put(leafRef, PageContainer.getInstance(fullPage, fullPage));
      return null;
    }
    
    // Get the split key from boundary (first key in right page after split)
    byte[] splitKey = rightPage.getFirstKey();
    
    // Create reference for the new right page
    PageReference rightRef = splitResult.rightChild();
    rightRef.setKey(newPageKey);
    log.put(rightRef, PageContainer.getInstance(rightPage, rightPage));
    
    // Update the original leaf page reference in the log
    log.put(leafRef, PageContainer.getInstance(fullPage, fullPage));
    
    // Now we need to update the parent structure
    if (pathDepth > 0) {
      // Has a parent - update the parent to include the new child
      int parentIdx = pathDepth - 1;
      updateParentForSplitWithPath(pageRtx, log, pathRefs[parentIdx], pathNodes[parentIdx],
          pathChildIndices[parentIdx], leafRef, rightRef, splitKey, rootReference,
          pathNodes, pathRefs, pathChildIndices, parentIdx);
    } else {
      // Root split - create new BiNode as root
      // CRITICAL: When pathDepth=0, leafRef may be the same object as rootReference.
      // We need a SEPARATE reference for the left child, and it MUST be in the log
      // so that HOTIndirectPage.commit() can find and write it to disk.
      PageReference leftChildRef;
      if (leafRef == rootReference) {
        leftChildRef = new PageReference();
        // Put the left child in the log so commit can find it by identity
        log.put(leftChildRef, PageContainer.getInstance(fullPage, fullPage));
      } else {
        leftChildRef = leafRef;
      }

      HOTIndirectPage newRoot = splitResult.newRoot();
      newRoot.setChildReference(0, leftChildRef);
      newRoot.setChildReference(1, rightRef);

      rootReference.setKey(newRootPageKey);
      rootReference.setPage(newRoot);
      log.put(rootReference, PageContainer.getInstance(newRoot, newRoot));
    }
    
    return splitKey;
  }
  
  /**
     * Integrate a BiNode (from a split) into the tree structure.
   * 
   * <p>Reference: HOTSingleThreaded.hpp integrateBiNodeIntoTree() lines 493-547.
   * This implements the height-optimal integration strategy:</p>
   * <ol>
   *   <li>If parent.height > splitEntries.height: create intermediate node</li>
   *   <li>If parent NOT full (< 32 entries): expand parent by adding entry</li>
   *   <li>If parent IS full: split parent and recurse up</li>
   * </ol>
   */
  private void updateParentForSplitWithPath(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull PageReference parentRef,
      @NonNull HOTIndirectPage parent,
      int originalChildIndex,
      @NonNull PageReference leftChild,
      @NonNull PageReference rightChild,
      byte[] splitKey,
      @NonNull PageReference rootReference,
      HOTIndirectPage[] pathNodes,
      PageReference[] pathRefs,
      int[] pathChildIndices,
      int currentPathIdx) {
    
    // Compute discriminative bit between the two split children
    byte[] leftMax = getLastKeyFromChild(leftChild);
    byte[] rightMin = getFirstKeyFromChild(rightChild);
    int discriminativeBit = DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
    
    // Compute the height of the split entries (BiNode containing the split children)
    int leftHeight = getHeightFromChild(leftChild);
    int rightHeight = getHeightFromChild(rightChild);
    int splitEntriesHeight = Math.max(leftHeight, rightHeight) + 1;
    
    // Reference line 502: if parent.height > splitEntries.height, create intermediate node
    if (parent.getHeight() > splitEntriesHeight) {
      // Create intermediate BiNode at current position and update parent's child
      long newBiNodePageKey = nextPageKey++;
      HOTIndirectPage newBiNode = HOTIndirectPage.createBiNode(
          newBiNodePageKey, pageRtx.getRevisionNumber(), discriminativeBit,
          leftChild, rightChild, splitEntriesHeight);
      
      PageReference newBiNodeRef = new PageReference();
      newBiNodeRef.setKey(newBiNodePageKey);
      newBiNodeRef.setPage(newBiNode);
      log.put(newBiNodeRef, PageContainer.getInstance(newBiNode, newBiNode));
      
      HOTIndirectPage updatedParent = parent.withUpdatedChild(originalChildIndex, newBiNodeRef, pageRtx.getRevisionNumber());
      log.put(parentRef, PageContainer.getInstance(updatedParent, updatedParent));
    } else {
      // parent.height == splitEntries.height: integrate into parent
      // Reference lines 504-546
      
      int currentNumChildren = parent.getNumChildren();
      
      // Reference line 516: if parent NOT full, add entry
      // Maximum is 32 entries per node (reference: MAXIMUM_NUMBER_NODE_ENTRIES = 32)
      if (currentNumChildren < NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
        // Parent has space - expand by adding the new entry
        // This replaces one child with two children
        HOTIndirectPage expandedParent = expandParentNode(
            parent, originalChildIndex, leftChild, rightChild,
            discriminativeBit, pageRtx.getRevisionNumber());
        log.put(parentRef, PageContainer.getInstance(expandedParent, expandedParent));
      } else {
        // Reference lines 520-536: parent is FULL - split and recurse
        splitParentAndRecurse(pageRtx, log, parentRef, parent, originalChildIndex,
            leftChild, rightChild, discriminativeBit, rootReference,
            pathNodes, pathRefs, pathChildIndices, currentPathIdx);
      }
    }
  }
  
  /**
   * Get height from a child reference.
   */
  private int getHeightFromChild(PageReference childRef) {
    Page page = childRef.getPage();
    if (page instanceof HOTLeafPage) {
      return 0;
    } else if (page instanceof HOTIndirectPage indirect) {
      return indirect.getHeight();
    }
    return 0;
  }
  
  /**
   * Expand a parent node by replacing one child with two children (from a split).
   * Reference: parentNode.addEntry() in HOTSingleThreaded.hpp
   */
  private HOTIndirectPage expandParentNode(
      HOTIndirectPage parent, int splitChildIndex,
      PageReference leftChild, PageReference rightChild,
      int discriminativeBit, int revision) {
    
    int numChildren = parent.getNumChildren();
    int newNumChildren = numChildren + 1;
    PageReference[] newChildren = new PageReference[newNumChildren];
    
    // Copy children, replacing split child with left+right
    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j++] = leftChild;
        newChildren[j++] = rightChild;
      } else {
        newChildren[j++] = parent.getChildReference(i);
      }
    }
    
    // Compute discriminative bits and partial keys for navigation
    byte initialBytePos = computeInitialBytePos(newChildren);
    long bitMask = computeBitMaskForChildren(newChildren, initialBytePos);
    byte[] partialKeys = computePartialKeysForChildren(newChildren, initialBytePos, bitMask);
    
    // Create appropriate node type based on child count
    if (newNumChildren <= 2) {
      return HOTIndirectPage.createBiNode(parent.getPageKey(), revision, discriminativeBit,
          newChildren[0], newChildren[1], parent.getHeight());
    } else if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNode(parent.getPageKey(), revision,
          initialBytePos, bitMask, partialKeys, newChildren, parent.getHeight());
    } else {
      return HOTIndirectPage.createMultiNode(parent.getPageKey(), revision,
          initialBytePos, bitMask, partialKeys, newChildren, parent.getHeight());
    }
  }
  
  /**
   * Compute initial byte position from children keys.
   */
  private byte computeInitialBytePos(PageReference[] children) {
    if (children.length < 2) return 0;
    
    // Find the first discriminative byte position
    byte[] first = getFirstKeyFromChild(children[0]);
    byte[] second = getFirstKeyFromChild(children[1]);
    int bit = DiscriminativeBitComputer.computeDifferingBit(first, second);
    return (byte) (bit / 8);
  }
  
  /**
   * Compute bit mask that covers all discriminative bits for the children.
   */
  private long computeBitMaskForChildren(PageReference[] children, byte initialBytePos) {
    long mask = 0;
    for (int i = 0; i < children.length - 1; i++) {
      byte[] key1 = getLastKeyFromChild(children[i]);
      byte[] key2 = getFirstKeyFromChild(children[i + 1]);
      if (key1.length == 0 || key2.length == 0) continue;
      
      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      int byteOffset = bit / 8 - (initialBytePos & 0xFF);
      if (byteOffset >= 0 && byteOffset < 8) {
        int bitInByte = 7 - (bit % 8);
        mask |= 1L << (byteOffset * 8 + bitInByte);
      }
    }
    return mask != 0 ? mask : 1L;
  }
  
  /**
   * Compute partial keys for all children based on the bit mask.
   */
  private byte[] computePartialKeysForChildren(PageReference[] children, byte initialBytePos, long bitMask) {
    byte[] partialKeys = new byte[children.length];
    for (int i = 0; i < children.length; i++) {
      byte[] key = getFirstKeyFromChild(children[i]);
      partialKeys[i] = computePartialKey(key, initialBytePos, bitMask);
    }
    return partialKeys;
  }
  
  /**
   * Split a full parent node and recurse up the tree.
   * 
   * <p>Reference: HOTSingleThreadedNode.hpp lines 549-565 (split function).
   * Uses the MOST SIGNIFICANT discriminative bit to split entries into two groups:
   * - "Smaller" entries: those with MSB=0
   * - "Larger" entries: those with MSB=1</p>
   */
  private void splitParentAndRecurse(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull PageReference parentRef,
      @NonNull HOTIndirectPage parent,
      int originalChildIndex,
      @NonNull PageReference leftChild,
      @NonNull PageReference rightChild,
      int discriminativeBit,
      @NonNull PageReference rootReference,
      HOTIndirectPage[] pathNodes,
      PageReference[] pathRefs,
      int[] pathChildIndices,
      int currentPathIdx) {
    
    int numChildren = parent.getNumChildren();
    
    // Reference: getMaskForLargerEntries() finds entries with MSB set
    // The MSB (most significant bit) determines the split boundary
    // Find the split point based on the most significant discriminative bit
    int msbPosition = findMostSignificantDiscriminativeBitPosition(parent);
    
    // Build arrays of children with MSB=0 ("smaller") and MSB=1 ("larger")
    java.util.List<PageReference> smallerChildren = new java.util.ArrayList<>();
    java.util.List<PageReference> largerChildren = new java.util.ArrayList<>();
    
    for (int i = 0; i < numChildren; i++) {
      if (i == originalChildIndex) {
        // Replace the original child with the split children
        // Determine which half each goes to based on their keys
        byte[] leftKey = getFirstKeyFromChild(leftChild);
        byte[] rightKey = getFirstKeyFromChild(rightChild);
        boolean leftHasMsbSet = hasBitSet(leftKey, msbPosition);
        boolean rightHasMsbSet = hasBitSet(rightKey, msbPosition);
        
        if (!leftHasMsbSet) {
          smallerChildren.add(leftChild);
        } else {
          largerChildren.add(leftChild);
        }
        if (!rightHasMsbSet) {
          smallerChildren.add(rightChild);
        } else {
          largerChildren.add(rightChild);
        }
      } else {
        PageReference child = parent.getChildReference(i);
        byte[] childKey = getFirstKeyFromChild(child);
        if (!hasBitSet(childKey, msbPosition)) {
          smallerChildren.add(child);
        } else {
          largerChildren.add(child);
        }
      }
    }
    
    // Ensure we have entries in both halves (fallback to half split if needed)
    if (smallerChildren.isEmpty() || largerChildren.isEmpty()) {
      // Fall back to simple half split
      splitParentHalfAndRecurse(pageRtx, log, parentRef, parent, originalChildIndex,
          leftChild, rightChild, rootReference, pathNodes, pathRefs, pathChildIndices, currentPathIdx);
      return;
    }
    
    PageReference[] leftChildren = smallerChildren.toArray(PageReference[]::new);
    PageReference[] rightChildren = largerChildren.toArray(PageReference[]::new);
    
    // Create left node (entries with MSB=0)
    HOTIndirectPage leftNode = createNodeFromChildren(leftChildren, parent.getPageKey(),
        pageRtx.getRevisionNumber(), parent.getHeight());
    PageReference leftNodeRef = new PageReference();
    leftNodeRef.setKey(parent.getPageKey());
    leftNodeRef.setPage(leftNode);
    log.put(leftNodeRef, PageContainer.getInstance(leftNode, leftNode));
    
    // Create right node (entries with MSB=1) - new page key
    long rightPageKey = nextPageKey++;
    HOTIndirectPage rightNode = createNodeFromChildren(rightChildren, rightPageKey,
        pageRtx.getRevisionNumber(), parent.getHeight());
    PageReference rightNodeRef = new PageReference();
    rightNodeRef.setKey(rightPageKey);
    rightNodeRef.setPage(rightNode);
    log.put(rightNodeRef, PageContainer.getInstance(rightNode, rightNode));
    
    // The split bit becomes the new root's discriminative bit
    // Reference: uses mMostSignificantDiscriminativeBitIndex for new BiNode
    int newRootDiscrimBit = msbPosition;
    
    // Recurse up to integrate [leftNode, rightNode] into grandparent
    if (currentPathIdx > 0) {
      int grandparentIdx = currentPathIdx - 1;
      updateParentForSplitWithPath(pageRtx, log, pathRefs[grandparentIdx], pathNodes[grandparentIdx],
          pathChildIndices[grandparentIdx], leftNodeRef, rightNodeRef,
          getFirstKeyFromChild(rightNodeRef), rootReference,
          pathNodes, pathRefs, pathChildIndices, grandparentIdx);
    } else {
      // At root - create new root BiNode
      long newRootKey = nextPageKey++;
      HOTIndirectPage newRoot = HOTIndirectPage.createBiNode(
          newRootKey, pageRtx.getRevisionNumber(), newRootDiscrimBit,
          leftNodeRef, rightNodeRef, parent.getHeight() + 1);
      
      rootReference.setKey(newRootKey);
      rootReference.setPage(newRoot);
      log.put(rootReference, PageContainer.getInstance(newRoot, newRoot));
    }
  }
  
  /**
   * Find the most significant discriminative bit position for the parent node.
   * Reference: getMaskForHighestBit() in DiscriminativeBitsRepresentation
   */
  private int findMostSignificantDiscriminativeBitPosition(HOTIndirectPage parent) {
    // For BiNode, it's the single discriminative bit
    // For SpanNode/MultiNode, find the MSB from the bitMask
    long bitMask = parent.getBitMask();
    if (bitMask == 0) {
      return 0;
    }
    // Find the highest set bit (most significant)
    int bytePos = parent.getInitialBytePos() & 0xFF;
    int highestBit = 63 - Long.numberOfLeadingZeros(bitMask);
    return bytePos * 8 + (7 - (highestBit % 8));
  }
  
  /**
   * Check if a key has a specific bit set.
   */
  private boolean hasBitSet(byte[] key, int bitPosition) {
    int byteIndex = bitPosition / 8;
    int bitIndex = 7 - (bitPosition % 8);  // MSB-first within byte
    if (byteIndex >= key.length) {
      return false;
    }
    return ((key[byteIndex] >> bitIndex) & 1) == 1;
  }
  
  /**
   * Fallback: split in half if MSB-based split fails.
   */
  private void splitParentHalfAndRecurse(
      @NonNull StorageEngineWriter pageRtx,
      @NonNull TransactionIntentLog log,
      @NonNull PageReference parentRef,
      @NonNull HOTIndirectPage parent,
      int originalChildIndex,
      @NonNull PageReference leftChild,
      @NonNull PageReference rightChild,
      @NonNull PageReference rootReference,
      HOTIndirectPage[] pathNodes,
      PageReference[] pathRefs,
      int[] pathChildIndices,
      int currentPathIdx) {
    
    int numChildren = parent.getNumChildren();
    int splitPoint = numChildren / 2;
    boolean splitInLeftHalf = originalChildIndex < splitPoint;
    
    int leftCount = splitPoint + (splitInLeftHalf ? 1 : 0);
    PageReference[] leftChildren = new PageReference[leftCount];
    int li = 0;
    for (int i = 0; i < splitPoint; i++) {
      if (i == originalChildIndex) {
        leftChildren[li++] = leftChild;
        leftChildren[li++] = rightChild;
      } else {
        leftChildren[li++] = parent.getChildReference(i);
      }
    }
    
    int rightCount = numChildren - splitPoint + (splitInLeftHalf ? 0 : 1);
    PageReference[] rightChildren = new PageReference[rightCount];
    int ri = 0;
    for (int i = splitPoint; i < numChildren; i++) {
      if (i == originalChildIndex) {
        rightChildren[ri++] = leftChild;
        rightChildren[ri++] = rightChild;
      } else {
        rightChildren[ri++] = parent.getChildReference(i);
      }
    }
    
    HOTIndirectPage leftNode = createNodeFromChildren(leftChildren, parent.getPageKey(),
        pageRtx.getRevisionNumber(), parent.getHeight());
    PageReference leftNodeRef = new PageReference();
    leftNodeRef.setKey(parent.getPageKey());
    leftNodeRef.setPage(leftNode);
    log.put(leftNodeRef, PageContainer.getInstance(leftNode, leftNode));
    
    long rightPageKey = nextPageKey++;
    HOTIndirectPage rightNode = createNodeFromChildren(rightChildren, rightPageKey,
        pageRtx.getRevisionNumber(), parent.getHeight());
    PageReference rightNodeRef = new PageReference();
    rightNodeRef.setKey(rightPageKey);
    rightNodeRef.setPage(rightNode);
    log.put(rightNodeRef, PageContainer.getInstance(rightNode, rightNode));
    
    if (currentPathIdx > 0) {
      int grandparentIdx = currentPathIdx - 1;
      updateParentForSplitWithPath(pageRtx, log, pathRefs[grandparentIdx], pathNodes[grandparentIdx],
          pathChildIndices[grandparentIdx], leftNodeRef, rightNodeRef,
          getFirstKeyFromChild(rightNodeRef), rootReference,
          pathNodes, pathRefs, pathChildIndices, grandparentIdx);
    } else {
      byte[] lMax = getLastKeyFromChild(leftNodeRef);
      byte[] rMin = getFirstKeyFromChild(rightNodeRef);
      int rootDiscrimBit = DiscriminativeBitComputer.computeDifferingBit(lMax, rMin);
      
      long newRootKey = nextPageKey++;
      HOTIndirectPage newRoot = HOTIndirectPage.createBiNode(
          newRootKey, pageRtx.getRevisionNumber(), rootDiscrimBit,
          leftNodeRef, rightNodeRef, parent.getHeight() + 1);
      
      rootReference.setKey(newRootKey);
      rootReference.setPage(newRoot);
      log.put(rootReference, PageContainer.getInstance(newRoot, newRoot));
    }
  }
  
  /**
   * Create appropriate node type for given children array.
   */
  private HOTIndirectPage createNodeFromChildren(PageReference[] children, long pageKey, int revision, int height) {
    if (children.length <= 2) {
      byte[] leftMax = getLastKeyFromChild(children[0]);
      byte[] rightMin = children.length > 1 ? getFirstKeyFromChild(children[1]) : leftMax;
      int discriminativeBit = DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
      return HOTIndirectPage.createBiNode(pageKey, revision, discriminativeBit,
          children[0], children.length > 1 ? children[1] : children[0], height);
    } else {
      byte initialBytePos = computeInitialBytePos(children);
      long bitMask = computeBitMaskForChildren(children, initialBytePos);
      byte[] partialKeys = computePartialKeysForChildren(children, initialBytePos, bitMask);
      
      if (children.length <= 16) {
        return HOTIndirectPage.createSpanNode(pageKey, revision, initialBytePos, bitMask,
            partialKeys, children, height);
      } else {
        return HOTIndirectPage.createMultiNode(pageKey, revision, initialBytePos, bitMask,
            partialKeys, children, height);
      }
    }
  }
  
  /**
   * Compute partial key by extracting discriminative bits from a key.
   */
  private byte computePartialKey(byte[] key, byte initialBytePos, long bitMask) {
    if (key == null || key.length == 0) {
      return 0;
    }
    
    // Extract 8 bytes from key starting at initialBytePos
    long keyWord = 0;
    for (int i = 0; i < 8 && (initialBytePos + i) < key.length; i++) {
      keyWord |= ((long) (key[initialBytePos + i] & 0xFF)) << (i * 8);
    }
    
    // Apply PEXT-style extraction: compress bits selected by mask
    return (byte) Long.compress(keyWord, bitMask);
  }
  
  /**
   * Get the last key from a child reference (leaf or indirect page).
   */
  private byte[] getLastKeyFromChild(PageReference childRef) {
    Page page = childRef.getPage();
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getLastKey();
    } else if (page instanceof HOTIndirectPage indirect) {
      // Descend to rightmost leaf
      return getLastKeyFromIndirectPage(indirect);
    }
    return new byte[0];
  }
  
  /**
   * Descend through indirect pages to find the last key.
   */
  private byte[] getLastKeyFromIndirectPage(HOTIndirectPage indirect) {
    int numChildren = indirect.getNumChildren();
    if (numChildren == 0) {
      return new byte[0];
    }
    PageReference lastChild = indirect.getChildReference(numChildren - 1);
    Page page = lastChild.getPage();
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getLastKey();
    } else if (page instanceof HOTIndirectPage child) {
      return getLastKeyFromIndirectPage(child);
    }
    return new byte[0];
  }
  
  /**
   * Get the first key from a child reference (leaf or indirect page).
   */
  private byte[] getFirstKeyFromChild(PageReference childRef) {
    Page page = childRef.getPage();
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getFirstKey();
    } else if (page instanceof HOTIndirectPage indirect) {
      // Descend to leftmost leaf
      return getFirstKeyFromIndirectPage(indirect);
    }
    return new byte[0];
  }
  
  /**
   * Descend through indirect pages to find the first key.
   */
  private byte[] getFirstKeyFromIndirectPage(HOTIndirectPage indirect) {
    int numChildren = indirect.getNumChildren();
    if (numChildren == 0) {
      return new byte[0];
    }
    PageReference firstChild = indirect.getChildReference(0);
    Page page = firstChild.getPage();
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getFirstKey();
    } else if (page instanceof HOTIndirectPage child) {
      return getFirstKeyFromIndirectPage(child);
    }
    return new byte[0];
  }
}

