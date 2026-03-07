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
import io.sirix.index.hot.NodeUpgradeManager;
import java.util.Arrays;
import java.util.function.LongSupplier;
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
 * <p>
 * This class provides an alternative to the bit-decomposition approach of {@code KeyedTrieWriter} using
 * semantic key-based navigation with HOT compound nodes.
 * </p>
 * 
 * <p>
 * <b>Key Features:</b>
 * </p>
 * <ul>
 * <li>Zero allocations on hot path (pre-allocated arrays instead of Deque)</li>
 * <li>SIMD-optimized child lookup via HOTIndirectPage</li>
 * <li>COW propagation using parent path tracking</li>
 * <li>Integration with existing versioning infrastructure</li>
 * </ul>
 * 
 * <p>
 * <b>Usage:</b>
 * </p>
 * 
 * <pre>{@code
 * HOTTrieWriter writer = new HOTTrieWriter(pageKeyAllocator);
 * PageContainer container =
 *     writer.prepareKeyedLeafForModification(storageEngineReader, log, startReference, key, indexType, indexNumber);
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

  /** Persistent page key allocator — replaces the old hardcoded nextPageKey counter. */
  private final LongSupplier pageKeyAllocator;

  // ===== Pre-allocated COW path - ZERO allocations on hot path! =====
  private final PageReference[] cowPathRefs = new PageReference[MAX_TREE_HEIGHT];
  private final HOTIndirectPage[] cowPathNodes = new HOTIndirectPage[MAX_TREE_HEIGHT];
  private final int[] cowPathChildIndices = new int[MAX_TREE_HEIGHT];
  private int cowPathDepth = 0;

  // ===== Pre-allocated buffers for splitParentAndRecurse - avoids ArrayList =====
  private static final int MAX_SPLIT_CHILDREN = NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN + 2;
  private final PageReference[] splitSmallerBuf = new PageReference[MAX_SPLIT_CHILDREN];
  private final PageReference[] splitLargerBuf = new PageReference[MAX_SPLIT_CHILDREN];

  /**
   * Create a new HOTTrieWriter with persistent page key allocation.
   *
   * <p>The supplier is called each time a new page key is needed (during splits and
   * new page creation). It must return a unique, monotonically increasing key that
   * is persisted across transactions (typically backed by an index page counter).</p>
   *
   * @param pageKeyAllocator supplier of persistent page keys
   */
  public HOTTrieWriter(final LongSupplier pageKeyAllocator) {
    this.allocator = OS.isWindows()
        ? WindowsMemorySegmentAllocator.getInstance()
        : LinuxMemorySegmentAllocator.getInstance();
    this.pageKeyAllocator = Objects.requireNonNull(pageKeyAllocator);
  }

  /**
   * Create a new HOTTrieWriter with a temporary in-memory counter (for tests only).
   *
   * @deprecated Use {@link #HOTTrieWriter(LongSupplier)} with a persistent allocator.
   */
  @Deprecated
  public HOTTrieWriter() {
    this.allocator = OS.isWindows()
        ? WindowsMemorySegmentAllocator.getInstance()
        : LinuxMemorySegmentAllocator.getInstance();
    final long[] counter = {1_000_000L};
    this.pageKeyAllocator = () -> counter[0]++;
  }

  /**
   * Navigate keyed trie (HOT) to find or create leaf page for given key. Analogous to
   * KeyedTrieWriter.prepareLeafOfTree() but uses semantic key navigation.
   *
   * @param storageEngineReader storage engine writer
   * @param log transaction intent log
   * @param startReference root reference (from PathPage/CASPage/NamePage)
   * @param key the search key (semantic bytes, not decomposed bit-by-bit)
   * @param indexType PATH/CAS/NAME
   * @param indexNumber the index number
   * @return PageContainer with complete and modifying HOTLeafPage, or null if not found
   */
  public @Nullable PageContainer prepareKeyedLeafForModification(@NonNull StorageEngineWriter storageEngineReader,
      @NonNull TransactionIntentLog log, @NonNull PageReference startReference, byte[] key,
      @NonNull IndexType indexType, int indexNumber) {

    Objects.requireNonNull(storageEngineReader);
    Objects.requireNonNull(log);
    Objects.requireNonNull(startReference);
    Objects.requireNonNull(key);
    Objects.requireNonNull(indexType);

    // Reset COW path (no allocation!)
    cowPathDepth = 0;

    // Check if already in log (modified this transaction)
    PageContainer cached = log.get(startReference);
    if (cached != null) {
      return navigateWithinCachedTree(cached, key, storageEngineReader, log, indexType, indexNumber);
    }

    // Navigate HOT trie, COW'ing along the path (uses pre-allocated arrays)
    PageReference leafRef = navigateToLeaf(storageEngineReader, startReference, key, log);

    if (leafRef == null) {
      // Key not found, need to create new leaf
      return createNewLeaf(storageEngineReader, log, startReference, key, indexType, indexNumber);
    }

    // Get leaf page through versioning pipeline
    return dereferenceHOTLeafForModification(storageEngineReader, leafRef, log);
  }

  /**
   * Navigate through keyed trie (HOT) compound nodes to reach leaf. Each node type uses
   * discriminative bits for child-finding (SIMD-optimized). Uses pre-allocated cowPath arrays - ZERO
   * allocations!
   *
   * @param storageEngineReader the storage engine reader
   * @param startReference the starting reference
   * @param key the search key
   * @param log the transaction intent log
   * @return the leaf page reference, or null if not found
   */
  private @Nullable PageReference navigateToLeaf(@NonNull StorageEngineReader storageEngineReader,
      @NonNull PageReference startReference, byte[] key, @NonNull TransactionIntentLog log) {

    PageReference currentRef = startReference;

    while (true) {
      // Check log first
      PageContainer container = log.get(currentRef);
      Page page;
      if (container != null) {
        page = container.getComplete();
      } else {
        page = loadPage(storageEngineReader, currentRef);
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
   * Dereference HOT leaf page for modification using versioning pipeline. Uses pre-allocated cowPath
   * arrays - ZERO allocations except for page copies!
   */
  private @Nullable PageContainer dereferenceHOTLeafForModification(@NonNull StorageEngineWriter storageEngineReader,
      @NonNull PageReference leafRef, @NonNull TransactionIntentLog log) {

    // Check if already in log
    PageContainer existing = log.get(leafRef);
    if (existing != null) {
      return existing;
    }

    // Load the leaf page
    Page page = loadPage(storageEngineReader, leafRef);
    if (!(page instanceof HOTLeafPage hotLeaf)) {
      clearCowPath();
      return null;
    }

    // Create COW copy for modification using deep copy (copies off-heap data + slot offsets)
    HOTLeafPage modifiedLeaf = hotLeaf.copy();

    PageContainer leafContainer = PageContainer.getInstance(hotLeaf, modifiedLeaf);
    log.put(leafRef, leafContainer);

    // Propagate COW up the path
    propagateCOW(log, leafRef);

    // Clear references to allow GC
    clearCowPath();

    return leafContainer;
  }

  /**
   * Propagate copy-on-write changes up to ancestors. Each modified HOTIndirectPage gets a new copy in
   * the log. Uses pre-allocated cowPath arrays - ZERO allocations except for page copies!
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
   * Navigate within a cached tree that's already been modified (iterative, no stack overflow risk).
   */
  private @Nullable PageContainer navigateWithinCachedTree(@NonNull PageContainer cached, byte[] key,
      @NonNull StorageEngineWriter storageEngineReader, @NonNull TransactionIntentLog log, @NonNull IndexType indexType,
      int indexNumber) {

    PageContainer current = cached;
    while (true) {
      final Page page = current.getModified();

      if (page instanceof HOTLeafPage) {
        return current;
      }

      if (page instanceof HOTIndirectPage hotNode) {
        final int childIndex = hotNode.findChildIndex(key);
        if (childIndex >= 0) {
          final PageReference childRef = hotNode.getChildReference(childIndex);
          if (childRef != null) {
            final PageContainer childContainer = log.get(childRef);
            if (childContainer != null) {
              current = childContainer; // iterate, not recurse
              continue;
            }
          }
        }
      }

      // Not found in cached tree
      return null;
    }
  }

  /**
   * Create a new leaf page for a key that doesn't exist yet.
   */
  private @Nullable PageContainer createNewLeaf(@NonNull StorageEngineWriter storageEngineReader, @NonNull TransactionIntentLog log,
      @NonNull PageReference startReference, byte[] key, @NonNull IndexType indexType, int indexNumber) {

    // Create new HOTLeafPage
    long newPageKey = pageKeyAllocator.getAsLong();
    HOTLeafPage newLeaf = new HOTLeafPage(newPageKey, storageEngineReader.getRevisionNumber(), indexType);

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
   * Load a page from storage or return swizzled in-memory page.
   *
   * <p>Checks {@link PageReference#getPage()} first (zero I/O for in-memory swizzled pages),
   * then falls through to {@link StorageEngineReader#loadHOTPage(PageReference)} which handles
   * both the transaction log (via logKey) and persistent storage (via key).</p>
   */
  private @Nullable Page loadPage(@NonNull StorageEngineReader storageEngineReader, @NonNull PageReference ref) {
    // Check swizzled in-memory page first — avoids I/O for pages already loaded
    final Page swizzled = ref.getPage();
    if (swizzled != null) {
      return swizzled;
    }
    // Need both key < 0 AND logKey < 0 to truly mean "not yet persisted"
    if (ref.getKey() < 0 && ref.getLogKey() < 0) {
      return null;
    }
    return storageEngineReader.loadHOTPage(ref);
  }

  /**
   * Handle leaf page split AND insert atomically using MSDB-aware splitting.
   *
   * <p>
   * Matches the C++ reference implementation's approach (Binna's thesis Listing 3.1,
   * {@code insertNewValue()} + {@code integrateBiNodeIntoTree()}), adapted for COW
   * semantics and multi-entry leaf pages:
   * </p>
   * <ol>
   * <li>Split the full leaf by MSDB that includes the new key's disc bits</li>
   * <li>Insert the new key into the correct half (based on disc bit, not key order)</li>
   * <li>Compute BiNode disc bit from boundary keys (equals MSDB by proof)</li>
   * <li>Walk up the path with COW to integrate the BiNode</li>
   * </ol>
   *
   * <p>
   * No re-navigation needed: the MSDB guarantees all left keys have bit=0 and all right
   * keys have bit=1, so the BiNode's disc-bit routing is correct for all keys including
   * the newly inserted one. The previous "split → re-navigate → insert" approach was needed
   * because the split didn't include the new key in the MSDB computation.
   * </p>
   *
   * @param storageEngineReader the storage engine writer
   * @param log the transaction intent log
   * @param fullPage the full leaf page
   * @param leafRef the reference to the leaf
   * @param rootReference the root reference for the index
   * @param pathNodes indirect page nodes on the path from root to leaf
   * @param pathRefs page references on the path
   * @param pathChildIndices child indices taken at each level
   * @param pathDepth the depth of the path
   * @param keyBuf the key to insert
   * @param keyLen the key length
   * @param valueBuf the value to insert
   * @param valueLen the value length
   * @return {@code true} if the split+insert succeeded, {@code false} if it failed
   */
  public boolean handleLeafSplitAndInsert(@NonNull StorageEngineWriter storageEngineReader,
      @NonNull TransactionIntentLog log, @NonNull HOTLeafPage fullPage, @NonNull PageReference leafRef,
      @NonNull PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int pathDepth, byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen) {

    Objects.requireNonNull(storageEngineReader);
    Objects.requireNonNull(log);
    Objects.requireNonNull(fullPage);
    Objects.requireNonNull(leafRef);
    Objects.requireNonNull(rootReference);

    // If page has < 1 entry, can't split
    if (fullPage.getEntryCount() < 1) {
      return false;
    }

    // Allocate page key for right half
    final long newPageKey = pageKeyAllocator.getAsLong();
    final int revision = storageEngineReader.getRevisionNumber();

    // Create target page for right half
    final HOTLeafPage rightPage = new HOTLeafPage(newPageKey, revision, fullPage.getIndexType());
    boolean rightPageOwnershipTransferred = false;

    try {
      // MSDB-aware split+insert: splits entries by the most significant discriminative
      // bit (including the new key), then inserts the new key into the correct half.
      // This matches the C++ split(insertInformation, valueToInsert) approach.
      if (!fullPage.splitToWithInsert(rightPage, keyBuf, keyLen, valueBuf, valueLen)) {
        rightPage.close();
        return false;
      }

      // Boundary keys now include the new key — BiNode disc bit is correct
      final byte[] leftMax = fullPage.getLastKey();
      final byte[] rightMin = rightPage.getFirstKey();

      if (leftMax.length == 0 || rightMin.length == 0) {
        rightPage.close();
        return false;
      }

      final int discriminativeBit = DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
      if (discriminativeBit < 0) {
        rightPage.close();
        return false;
      }

      // Store right page in TIL
      final PageReference rightRef = new PageReference();
      rightRef.setKey(newPageKey);
      rightRef.setPage(rightPage);
      log.put(rightRef, PageContainer.getInstance(rightPage, rightPage));
      rightPageOwnershipTransferred = true;

      // Swizzle post-split left page onto leafRef
      leafRef.setPage(fullPage);
      log.put(leafRef, PageContainer.getInstance(fullPage, fullPage));

      // Integrate split into tree (walk up path with COW)
      // Matches C++ integrateBiNodeIntoTree() — walk up the insert stack,
      // creating COW copies at each level instead of in-place pointer mutation.
      if (pathDepth > 0) {
        final int parentIdx = pathDepth - 1;
        updateParentForSplitWithPath(storageEngineReader, log, pathRefs[parentIdx],
            pathNodes[parentIdx], pathChildIndices[parentIdx], leafRef, rightRef,
            rightMin, rootReference, pathNodes, pathRefs, pathChildIndices, parentIdx);
      } else {
        // Root split — create BiNode as new root (only allocate here, not above)
        final long newRootPageKey = pageKeyAllocator.getAsLong();
        final HOTIndirectPage biNode = HOTIndirectPage.createBiNode(
            newRootPageKey, revision, discriminativeBit, leafRef, rightRef);

        PageReference leftChildRef;
        if (leafRef == rootReference) {
          leftChildRef = new PageReference();
          log.put(leftChildRef, PageContainer.getInstance(fullPage, fullPage));
        } else {
          leftChildRef = leafRef;
        }

        biNode.setChildReference(0, leftChildRef);
        biNode.setChildReference(1, rightRef);
        rootReference.setKey(newRootPageKey);
        rootReference.setPage(biNode);
        log.put(rootReference, PageContainer.getInstance(biNode, biNode));
      }

      return true;
    } finally {
      if (!rightPageOwnershipTransferred) {
        rightPage.close();
      }
    }
  }

  /**
   * Integrate a BiNode (from a split) into the tree structure.
   * 
   * <p>
   * Reference: HOTSingleThreaded.hpp integrateBiNodeIntoTree() lines 493-547. This implements the
   * height-optimal integration strategy:
   * </p>
   * <ol>
   * <li>If parent.height > splitEntries.height: create intermediate node</li>
   * <li>If parent NOT full (< 32 entries): expand parent by adding entry</li>
   * <li>If parent IS full: split parent and recurse up</li>
   * </ol>
   */
  private void updateParentForSplitWithPath(@NonNull StorageEngineWriter storageEngineReader, @NonNull TransactionIntentLog log,
      @NonNull PageReference parentRef, @NonNull HOTIndirectPage parent, int originalChildIndex,
      @NonNull PageReference leftChild, @NonNull PageReference rightChild, byte[] splitKey,
      @NonNull PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int currentPathIdx) {

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
      long newBiNodePageKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage newBiNode = HOTIndirectPage.createBiNode(newBiNodePageKey, storageEngineReader.getRevisionNumber(),
          discriminativeBit, leftChild, rightChild, splitEntriesHeight);

      PageReference newBiNodeRef = new PageReference();
      newBiNodeRef.setKey(newBiNodePageKey);
      newBiNodeRef.setPage(newBiNode);
      log.put(newBiNodeRef, PageContainer.getInstance(newBiNode, newBiNode));

      HOTIndirectPage updatedParent =
          parent.withUpdatedChild(originalChildIndex, newBiNodeRef, storageEngineReader.getRevisionNumber());
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
        HOTIndirectPage expandedParent = expandParentNode(parent, originalChildIndex, leftChild, rightChild,
            discriminativeBit, storageEngineReader.getRevisionNumber());
        log.put(parentRef, PageContainer.getInstance(expandedParent, expandedParent));
      } else {
        // Reference lines 520-536: parent is FULL - split and recurse
        splitParentAndRecurse(storageEngineReader, log, parentRef, parent, originalChildIndex, leftChild, rightChild,
            discriminativeBit, rootReference, pathNodes, pathRefs, pathChildIndices, currentPathIdx);
      }
    }
  }

  /**
   * Get height from a child reference.
   */
  private int getHeightFromChild(PageReference childRef) {
    final Page page = childRef.getPage();
    if (page instanceof HOTLeafPage) {
      return 0;
    } else if (page instanceof HOTIndirectPage indirect) {
      return indirect.getHeight();
    }
    return 0;
  }

  /**
   * Expand a parent node by replacing one child with two children (from a split). Reference:
   * parentNode.addEntry() in HOTSingleThreaded.hpp
   */
  private HOTIndirectPage expandParentNode(HOTIndirectPage parent, int splitChildIndex, PageReference leftChild,
      PageReference rightChild, int discriminativeBit, int revision) {

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

    // Sort children by first key to ensure correct boundary computation.
    // HOT trie routing by disc bits can place keys out of key-order (e.g., key 1024
    // routes to child[0..511] because its disc bit matches). Without sorting,
    // boundary computation misses discriminative bits between non-adjacent children.
    sortChildrenByFirstKey(newChildren);

    // Compute discriminative bits and partial keys for navigation
    byte initialBytePos = computeInitialBytePos(newChildren);
    long bitMask = computeBitMaskForChildren(newChildren, initialBytePos);
    byte[] partialKeys = computePartialKeysForChildren(newChildren, initialBytePos, bitMask);

    // Create appropriate node type based on child count
    if (newNumChildren <= 2) {
      return HOTIndirectPage.createBiNode(parent.getPageKey(), revision, discriminativeBit, newChildren[0],
          newChildren[1], parent.getHeight());
    } else if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNode(parent.getPageKey(), revision, initialBytePos, bitMask, partialKeys,
          newChildren, parent.getHeight());
    } else {
      return HOTIndirectPage.createMultiNode(parent.getPageKey(), revision, initialBytePos, bitMask, partialKeys,
          newChildren, parent.getHeight());
    }
  }

  /**
   * Compute initial byte position from children keys.
   */
  private byte computeInitialBytePos(PageReference[] children) {
    if (children.length < 2)
      return 0;

    int minBytePos = Integer.MAX_VALUE;
    for (int i = 0; i < children.length - 1; i++) {
      byte[] last = getLastKeyFromChild(children[i]);
      byte[] first = getFirstKeyFromChild(children[i + 1]);
      int bit = DiscriminativeBitComputer.computeDifferingBit(last, first);
      minBytePos = Math.min(minBytePos, bit / 8);
    }
    return (byte) minBytePos;
  }

  /**
   * Compute bit mask that covers all discriminative bits for the children.
   * Uses big-endian layout: byte 0 of window → bits 56-63, matching getKeyWordAt().
   */
  private long computeBitMaskForChildren(PageReference[] children, byte initialBytePos) {
    long mask = 0;
    for (int i = 0; i < children.length - 1; i++) {
      byte[] key1 = getLastKeyFromChild(children[i]);
      byte[] key2 = getFirstKeyFromChild(children[i + 1]);
      if (key1.length == 0 || key2.length == 0)
        continue;

      int bit = DiscriminativeBitComputer.computeDifferingBit(key1, key2);
      int byteOffset = bit / 8 - (initialBytePos & 0xFF);
      if (byteOffset >= 0 && byteOffset < 8) {
        int bitInByte = 7 - (bit % 8);
        mask |= 1L << ((7 - byteOffset) * 8 + bitInByte);
      }
    }
    return mask != 0
        ? mask
        : 1L;
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
   * <p>
   * Reference: HOTSingleThreadedNode.hpp lines 549-565 (split function). Uses the MOST SIGNIFICANT
   * discriminative bit to split entries into two groups: - "Smaller" entries: those with MSB=0 -
   * "Larger" entries: those with MSB=1
   * </p>
   */
  private void splitParentAndRecurse(@NonNull StorageEngineWriter storageEngineReader, @NonNull TransactionIntentLog log,
      @NonNull PageReference parentRef, @NonNull HOTIndirectPage parent, int originalChildIndex,
      @NonNull PageReference leftChild, @NonNull PageReference rightChild, int discriminativeBit,
      @NonNull PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int currentPathIdx) {

    final int numChildren = parent.getNumChildren();

    // Reference: getMaskForLargerEntries() finds entries with MSB set
    // The MSB (most significant bit) determines the split boundary
    // Find the split point based on the most significant discriminative bit
    final int msbPosition = findMostSignificantDiscriminativeBitPosition(parent);

    // Partition children into "smaller" (MSB=0) and "larger" (MSB=1) using pre-allocated buffers
    int smallerCount = 0;
    int largerCount = 0;

    for (int i = 0; i < numChildren; i++) {
      if (i == originalChildIndex) {
        // Replace the original child with the two split children
        final byte[] leftKey = getFirstKeyFromChild(leftChild);
        final byte[] rightKey = getFirstKeyFromChild(rightChild);
        if (!hasBitSet(leftKey, msbPosition)) {
          splitSmallerBuf[smallerCount++] = leftChild;
        } else {
          splitLargerBuf[largerCount++] = leftChild;
        }
        if (!hasBitSet(rightKey, msbPosition)) {
          splitSmallerBuf[smallerCount++] = rightChild;
        } else {
          splitLargerBuf[largerCount++] = rightChild;
        }
      } else {
        final PageReference child = parent.getChildReference(i);
        final byte[] childKey = getFirstKeyFromChild(child);
        if (!hasBitSet(childKey, msbPosition)) {
          splitSmallerBuf[smallerCount++] = child;
        } else {
          splitLargerBuf[largerCount++] = child;
        }
      }
    }

    // Ensure we have entries in both halves (fallback to half split if needed)
    if (smallerCount == 0 || largerCount == 0) {
      splitParentHalfAndRecurse(storageEngineReader, log, parentRef, parent, originalChildIndex, leftChild, rightChild,
          rootReference, pathNodes, pathRefs, pathChildIndices, currentPathIdx);
      return;
    }

    // Trim to exact size — small allocation but avoids API contract issues with oversized arrays
    final PageReference[] leftChildren = Arrays.copyOf(splitSmallerBuf, smallerCount);
    final PageReference[] rightChildren = Arrays.copyOf(splitLargerBuf, largerCount);

    // Create left node (entries with MSB=0)
    HOTIndirectPage leftNode =
        createNodeFromChildren(leftChildren, parent.getPageKey(), storageEngineReader.getRevisionNumber(), parent.getHeight());
    PageReference leftNodeRef = new PageReference();
    leftNodeRef.setKey(parent.getPageKey());
    leftNodeRef.setPage(leftNode);
    log.put(leftNodeRef, PageContainer.getInstance(leftNode, leftNode));

    // Create right node (entries with MSB=1) - new page key
    long rightPageKey = pageKeyAllocator.getAsLong();
    HOTIndirectPage rightNode =
        createNodeFromChildren(rightChildren, rightPageKey, storageEngineReader.getRevisionNumber(), parent.getHeight());
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
      updateParentForSplitWithPath(storageEngineReader, log, pathRefs[grandparentIdx], pathNodes[grandparentIdx],
          pathChildIndices[grandparentIdx], leftNodeRef, rightNodeRef, getFirstKeyFromChild(rightNodeRef),
          rootReference, pathNodes, pathRefs, pathChildIndices, grandparentIdx);
    } else {
      // At root - create new root BiNode
      long newRootKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage newRoot = HOTIndirectPage.createBiNode(newRootKey, storageEngineReader.getRevisionNumber(), newRootDiscrimBit,
          leftNodeRef, rightNodeRef, parent.getHeight() + 1);

      rootReference.setKey(newRootKey);
      rootReference.setPage(newRoot);
      log.put(rootReference, PageContainer.getInstance(newRoot, newRoot));
    }
  }

  /**
   * Find the most significant discriminative bit position for the parent node. With big-endian
   * layout, the highest set bit in the mask directly maps: absolute_bit = initialBytePos * 8 + CLZ.
   * This is because bit 63 = byte 0 MSB, bit 62 = byte 0 bit 1, etc.
   */
  private int findMostSignificantDiscriminativeBitPosition(HOTIndirectPage parent) {
    long bitMask = parent.getBitMask();
    if (bitMask == 0) {
      return 0;
    }
    int bytePos = parent.getInitialBytePos();
    // With BE: CLZ gives the offset from MSB of the 8-byte window, which IS the absolute
    // bit offset within the window. Add bytePos * 8 for the absolute position.
    return bytePos * 8 + Long.numberOfLeadingZeros(bitMask);
  }

  /**
   * Check if a key has a specific bit set.
   */
  private boolean hasBitSet(byte[] key, int bitPosition) {
    int byteIndex = bitPosition / 8;
    int bitIndex = 7 - (bitPosition % 8); // MSB-first within byte
    if (byteIndex >= key.length) {
      return false;
    }
    return ((key[byteIndex] >> bitIndex) & 1) == 1;
  }

  /**
   * Fallback: split in half if MSB-based split fails.
   */
  private void splitParentHalfAndRecurse(@NonNull StorageEngineWriter storageEngineReader, @NonNull TransactionIntentLog log,
      @NonNull PageReference parentRef, @NonNull HOTIndirectPage parent, int originalChildIndex,
      @NonNull PageReference leftChild, @NonNull PageReference rightChild, @NonNull PageReference rootReference,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices, int currentPathIdx) {

    int numChildren = parent.getNumChildren();
    int splitPoint = numChildren / 2;
    boolean splitInLeftHalf = originalChildIndex < splitPoint;

    int leftCount = splitPoint + (splitInLeftHalf
        ? 1
        : 0);
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

    int rightCount = numChildren - splitPoint + (splitInLeftHalf
        ? 0
        : 1);
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

    HOTIndirectPage leftNode =
        createNodeFromChildren(leftChildren, parent.getPageKey(), storageEngineReader.getRevisionNumber(), parent.getHeight());
    PageReference leftNodeRef = new PageReference();
    leftNodeRef.setKey(parent.getPageKey());
    leftNodeRef.setPage(leftNode);
    log.put(leftNodeRef, PageContainer.getInstance(leftNode, leftNode));

    long rightPageKey = pageKeyAllocator.getAsLong();
    HOTIndirectPage rightNode =
        createNodeFromChildren(rightChildren, rightPageKey, storageEngineReader.getRevisionNumber(), parent.getHeight());
    PageReference rightNodeRef = new PageReference();
    rightNodeRef.setKey(rightPageKey);
    rightNodeRef.setPage(rightNode);
    log.put(rightNodeRef, PageContainer.getInstance(rightNode, rightNode));

    if (currentPathIdx > 0) {
      int grandparentIdx = currentPathIdx - 1;
      updateParentForSplitWithPath(storageEngineReader, log, pathRefs[grandparentIdx], pathNodes[grandparentIdx],
          pathChildIndices[grandparentIdx], leftNodeRef, rightNodeRef, getFirstKeyFromChild(rightNodeRef),
          rootReference, pathNodes, pathRefs, pathChildIndices, grandparentIdx);
    } else {
      byte[] lMax = getLastKeyFromChild(leftNodeRef);
      byte[] rMin = getFirstKeyFromChild(rightNodeRef);
      int rootDiscrimBit = DiscriminativeBitComputer.computeDifferingBit(lMax, rMin);

      long newRootKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage newRoot = HOTIndirectPage.createBiNode(newRootKey, storageEngineReader.getRevisionNumber(), rootDiscrimBit,
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
    // Sort children by first key for correct boundary computation
    sortChildrenByFirstKey(children);

    if (children.length <= 2) {
      byte[] leftMax = getLastKeyFromChild(children[0]);
      byte[] rightMin = children.length > 1
          ? getFirstKeyFromChild(children[1])
          : leftMax;
      int discriminativeBit = DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
      return HOTIndirectPage.createBiNode(pageKey, revision, discriminativeBit, children[0], children.length > 1
          ? children[1]
          : children[0], height);
    } else {
      byte initialBytePos = computeInitialBytePos(children);
      long bitMask = computeBitMaskForChildren(children, initialBytePos);
      byte[] partialKeys = computePartialKeysForChildren(children, initialBytePos, bitMask);

      if (children.length <= 16) {
        return HOTIndirectPage.createSpanNode(pageKey, revision, initialBytePos, bitMask, partialKeys, children,
            height);
      } else {
        return HOTIndirectPage.createMultiNode(pageKey, revision, initialBytePos, bitMask, partialKeys, children,
            height);
      }
    }
  }

  /**
   * Compute partial key by extracting discriminative bits from a key.
   * Uses big-endian layout: byte 0 of window → bits 56-63, matching getKeyWordAt().
   */
  private byte computePartialKey(byte[] key, byte initialBytePos, long bitMask) {
    if (key == null || key.length == 0) {
      return 0;
    }

    // Extract 8 bytes from key starting at initialBytePos (big-endian)
    long keyWord = 0;
    for (int i = 0; i < 8 && (initialBytePos + i) < key.length; i++) {
      keyWord |= ((long) (key[initialBytePos + i] & 0xFF)) << ((7 - i) * 8);
    }

    // Apply PEXT-style extraction: compress bits selected by mask
    return (byte) Long.compress(keyWord, bitMask);
  }

  /**
   * Get the last key from a child reference (leaf or indirect page).
   */
  private byte[] getLastKeyFromChild(PageReference childRef) {
    final Page page = childRef.getPage();
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getLastKey();
    } else if (page instanceof HOTIndirectPage indirect) {
      return getLastKeyFromIndirectPage(indirect);
    }
    return new byte[0];
  }

  /**
   * Descend through indirect pages to find the last key.
   */
  private byte[] getLastKeyFromIndirectPage(HOTIndirectPage indirect) {
    final int numChildren = indirect.getNumChildren();
    if (numChildren == 0) {
      return new byte[0];
    }
    final PageReference lastChild = indirect.getChildReference(numChildren - 1);
    final Page page = lastChild.getPage();
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getLastKey();
    } else if (page instanceof HOTIndirectPage child) {
      return getLastKeyFromIndirectPage(child);
    }
    return new byte[0];
  }

  /**
   * Sort children by their first key to ensure correct boundary computation.
   * HOT trie routing can place keys out of key-order (a key may route to a child
   * covering a different key range based on disc bit values). Without sorting,
   * the boundary computation between adjacent children may miss discriminative
   * bits, leading to duplicate partial keys and unreachable children.
   */
  private void sortChildrenByFirstKey(PageReference[] children) {
    if (children.length <= 1) {
      return;
    }
    Arrays.sort(children, (a, b) -> Arrays.compareUnsigned(getFirstKeyFromChild(a), getFirstKeyFromChild(b)));
  }

  /**
   * Get the first key from a child reference (leaf or indirect page).
   */
  private byte[] getFirstKeyFromChild(PageReference childRef) {
    final Page page = childRef.getPage();
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getFirstKey();
    } else if (page instanceof HOTIndirectPage indirect) {
      return getFirstKeyFromIndirectPage(indirect);
    }
    return new byte[0];
  }

  /**
   * Descend through indirect pages to find the first key.
   */
  private byte[] getFirstKeyFromIndirectPage(HOTIndirectPage indirect) {
    final int numChildren = indirect.getNumChildren();
    if (numChildren == 0) {
      return new byte[0];
    }
    final PageReference firstChild = indirect.getChildReference(0);
    final Page page = firstChild.getPage();
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getFirstKey();
    } else if (page instanceof HOTIndirectPage child) {
      return getFirstKeyFromIndirectPage(child);
    }
    return new byte[0];
  }
}

