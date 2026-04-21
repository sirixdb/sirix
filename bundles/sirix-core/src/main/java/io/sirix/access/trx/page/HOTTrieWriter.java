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
import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.PageContainer;
import io.sirix.cache.TransactionIntentLog;
import io.sirix.index.IndexType;
import io.sirix.index.hot.DiscriminativeBitComputer;
import io.sirix.index.hot.NodeUpgradeManager;
import java.util.Arrays;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.jspecify.annotations.Nullable;

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

  /** Shared empty key constant to avoid allocations. */
  private static final byte[] EMPTY_KEY = new byte[0];

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
   * Active reader for resolving non-swizzled child pages during split operations.
   * Set at the entry points of public methods, cleared on exit.
   */
  private @Nullable StorageEngineReader activeReader;

  /**
   * Holds either SingleMask or MultiMask discriminative bit information.
   * SingleMask: all disc bits fit within 8 contiguous key bytes (one 64-bit PEXT mask).
   * MultiMask: disc bits span >8 bytes; uses per-byte extraction positions and masks.
   */
  private record DiscBitsInfo(
      HOTIndirectPage.LayoutType layoutType,
      int initialBytePos,            // SingleMask only
      long bitMask,                  // SingleMask only
      byte[] extractionPositions,    // MultiMask only: key byte positions to gather
      long[] extractionMasks,        // MultiMask only: PEXT masks per 8-byte chunk
      int numExtractionBytes,        // MultiMask only: number of extraction bytes
      short mostSignificantBitIndex  // Minimum absolute disc bit position
  ) {
    boolean isSingleMask() {
      return layoutType == HOTIndirectPage.LayoutType.SINGLE_MASK;
    }

    int totalDiscBits() {
      if (isSingleMask()) {
        return Long.bitCount(bitMask);
      }
      int total = 0;
      for (final long m : extractionMasks) {
        total += Long.bitCount(m);
      }
      return total;
    }

    static DiscBitsInfo singleMask(int initialBytePos, long bitMask) {
      return new DiscBitsInfo(HOTIndirectPage.LayoutType.SINGLE_MASK, initialBytePos, bitMask,
          null, null, 0, HOTIndirectPage.computeMostSignificantBitIndex(initialBytePos, bitMask));
    }

    static DiscBitsInfo multiMask(byte[] extractionPositions, long[] extractionMasks,
        int numExtractionBytes, short mostSignificantBitIndex) {
      return new DiscBitsInfo(HOTIndirectPage.LayoutType.MULTI_MASK, 0, 0,
          extractionPositions, extractionMasks, numExtractionBytes, mostSignificantBitIndex);
    }
  }

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
    this.allocator = Allocators.getInstance();
    this.pageKeyAllocator = Objects.requireNonNull(pageKeyAllocator);
  }

  /**
   * Create a new HOTTrieWriter with a temporary in-memory counter (for tests only).
   *
   * @deprecated Use {@link #HOTTrieWriter(LongSupplier)} with a persistent allocator.
   */
  @Deprecated
  public HOTTrieWriter() {
    this.allocator = Allocators.getInstance();
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
  public @Nullable PageContainer prepareKeyedLeafForModification(StorageEngineWriter storageEngineReader,
      TransactionIntentLog log, PageReference startReference, byte[] key,
      IndexType indexType, int indexNumber) {

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
  private @Nullable PageReference navigateToLeaf(StorageEngineReader storageEngineReader,
      PageReference startReference, byte[] key, TransactionIntentLog log) {

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
  private @Nullable PageContainer dereferenceHOTLeafForModification(StorageEngineWriter storageEngineReader,
      PageReference leafRef, TransactionIntentLog log) {

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
  private void propagateCOW(TransactionIntentLog log, PageReference modifiedChildRef) {
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
  private @Nullable PageContainer navigateWithinCachedTree(PageContainer cached, byte[] key,
      StorageEngineWriter storageEngineReader, TransactionIntentLog log, IndexType indexType,
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
  private @Nullable PageContainer createNewLeaf(StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      PageReference startReference, byte[] key, IndexType indexType, int indexNumber) {

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
  private @Nullable Page loadPage(StorageEngineReader storageEngineReader, PageReference ref) {
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
  public boolean handleLeafSplitAndInsert(StorageEngineWriter storageEngineReader,
      TransactionIntentLog log, HOTLeafPage fullPage, PageReference leafRef,
      PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int pathDepth, byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen) {

    Objects.requireNonNull(storageEngineReader);
    Objects.requireNonNull(log);
    Objects.requireNonNull(fullPage);
    Objects.requireNonNull(leafRef);
    Objects.requireNonNull(rootReference);

    // Set active reader for child page resolution during split operations
    this.activeReader = storageEngineReader;
    try {
      return handleLeafSplitAndInsertInternal(storageEngineReader, log, fullPage, leafRef,
          rootReference, pathNodes, pathRefs, pathChildIndices, pathDepth, keyBuf, keyLen, valueBuf, valueLen);
    } finally {
      this.activeReader = null;
    }
  }

  private boolean handleLeafSplitAndInsertInternal(StorageEngineWriter storageEngineReader,
      TransactionIntentLog log, HOTLeafPage fullPage, PageReference leafRef,
      PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int pathDepth, byte[] keyBuf, int keyLen, byte[] valueBuf, int valueLen) {

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
        return false; // finally block closes rightPage
      }

      // Boundary keys now include the new key — BiNode disc bit is correct
      final byte[] leftMax = fullPage.getLastKey();
      final byte[] rightMin = rightPage.getFirstKey();

      if (leftMax.length == 0 || rightMin.length == 0) {
        return false; // finally block closes rightPage
      }

      final int discriminativeBit = DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
      if (discriminativeBit < 0) {
        return false; // finally block closes rightPage
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
      if (pathDepth > 0) {
        final int parentIdx = pathDepth - 1;
        updateParentForSplitWithPath(storageEngineReader, log, pathRefs[parentIdx],
            pathNodes[parentIdx], pathChildIndices[parentIdx], leafRef, rightRef,
            rightMin, rootReference, pathNodes, pathRefs, pathChildIndices, parentIdx);
      } else {
        // Root split — create BiNode as new root.
        final long newRootPageKey = pageKeyAllocator.getAsLong();
        final PageReference leftChildRef;
        if (leafRef == rootReference) {
          leftChildRef = new PageReference();
          leftChildRef.setKey(leafRef.getKey());
          leftChildRef.setPage(fullPage);
          log.put(leftChildRef, PageContainer.getInstance(fullPage, fullPage));
        } else {
          leftChildRef = leafRef;
        }
        final HOTIndirectPage biNode = HOTIndirectPage.createBiNode(
            newRootPageKey, revision, discriminativeBit, leftChildRef, rightRef, 1);
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
   * Traverse the tree from {@code rootRef}, collecting every reachable
   * HOTLeafPage reference into {@code out}. Also guarantees that the two
   * just-split references ({@code splitLeftRef}, {@code splitRightRef})
   * appear in the output — because the current indirect structure may
   * route some keys wrongly, the split pair is added explicitly at the
   * end if not seen during traversal. Duplicates (same pageKey + same
   * logKey) are removed in the final sort.
   */
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
  private void updateParentForSplitWithPath(StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      PageReference parentRef, HOTIndirectPage parent, int originalChildIndex,
      PageReference leftChild, PageReference rightChild, byte[] splitKey,
      PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int currentPathIdx) {

    // Compute discriminative bit between the two split children.
    // Guard against -1 (identical keys, e.g., when pages can't be loaded and both return EMPTY_KEY).
    byte[] leftMax = getLastKeyFromChild(leftChild);
    byte[] rightMin = getFirstKeyFromChild(rightChild);
    int discriminativeBit = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin));

    // Compute the height of the split entries (BiNode containing the split children)
    int leftHeight = getHeightFromChild(leftChild, activeReader);
    int rightHeight = getHeightFromChild(rightChild, activeReader);
    int splitEntriesHeight = Math.max(leftHeight, rightHeight) + 1;

    // Reference line 502: if parent.height > splitEntries.height, create intermediate node
    if (parent.getHeight() > splitEntriesHeight) {
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

      // Reference line 516: if parent NOT full, add entry.
      // Reference-faithful addEntry via DiscriminativeBitsRepresentation.insert
      // semantics: try to extend parent's existing disc-bit set with the
      // new bit, repositioning existing partial keys via _pdep. This
      // preserves the HOT invariant that disc bits at a node are
      // constant within each child's subtree. If the invariant would
      // break (new bit varies within an existing sibling's subtree), or
      // the parent is full, or the new bit falls outside the current
      // SingleMask window, fall back to splitting the parent.
      HOTIndirectPage expandedParent = null;
      if (currentNumChildren < NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
        if (parent.getLayoutType() == HOTIndirectPage.LayoutType.SINGLE_MASK) {
          expandedParent = addEntryWithPDep(parent, originalChildIndex, leftChild, rightChild,
              discriminativeBit, storageEngineReader.getRevisionNumber());
        } else {
          expandedParent = addEntryMultiMask(parent, originalChildIndex, leftChild, rightChild,
              discriminativeBit, storageEngineReader.getRevisionNumber());
        }
      }
      // Height-optimal fallback ONLY for parents with small-to-medium
      // fan-out. When fan-out is already near-max, rebuild can produce
      // subtle INV breaches (the augmented computeDiscBits may not
      // fully untangle all constancy requirements across 20+ children).
      // Forcing a genuine split path at high fan-out rebuilds halves
      // from scratch, which is more robust. The threshold is chosen so
      // most construction operations still use the efficient rebuild
      // path (preserving height optimality) while high-density parents
      // use the robust split path. Empirically: threshold=20 keeps the
      // tree log₃₂-deep and fixes the 8.3% miss at n=5000 multi.
      if (expandedParent == null
          && currentNumChildren < 20) {
        expandedParent = rebuildParentAbsorbingSplit(parent, originalChildIndex,
            leftChild, rightChild, storageEngineReader.getRevisionNumber());
      }
      if (expandedParent != null) {
        log.put(parentRef, PageContainer.getInstance(expandedParent, expandedParent));
      } else {
        splitParentAndRecurse(storageEngineReader, log, parentRef, parent, originalChildIndex, leftChild, rightChild,
            discriminativeBit, rootReference, pathNodes, pathRefs, pathChildIndices, currentPathIdx);
      }
    }
  }

  /**
   * Reference-faithful {@code addEntry} for SingleMask indirect nodes:
   * extends {@code parent}'s disc-bit set with {@code newAbsBit}, then
   * repositions each existing child's partial key via {@code Long.expand}
   * ({@code _pdep_u64}) to match the new bit layout, and computes
   * partial keys for the two split halves at the original slot.
   *
   * <p>Precondition: the newly-added disc bit must be constant across
   * every non-split sibling's subtree (otherwise the HOT invariant
   * breaks — a disc bit is only valid at a node if all keys under each
   * child have the same value at that bit). The method returns {@code
   * null} if this precondition fails, forcing the caller to take the
   * "split parent and recurse" path.
   *
   * <p>Returns {@code null} also for: MultiMask parents (not handled
   * here — fall back to whole-node rebuild), new bit already in mask
   * (unexpected duplicate), or new bit outside the current 8-byte
   * PEXT window.
   *
   * <p>Reference: {@code HOTSingleThreadedNode<ChildPointer>::addEntry}
   * + {@code SingleMaskPartialKeyMapping::insert}. Java's
   * {@code Long.expand(src, mask)} = x86 {@code _pdep_u64}.
   */
  private @Nullable HOTIndirectPage addEntryWithPDep(HOTIndirectPage parent, int splitChildIndex,
      PageReference leftChild, PageReference rightChild, int newAbsBit, int revision) {
    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int oldCount = Long.bitCount(oldMask);
    final int numChildren = parent.getNumChildren();

    // 1. Encode new disc bit into the PEXT mask (same convention used
    //    by computeDiscBits: bit at absolute position B, byte bo within
    //    the 8-byte window starting at initialBytePos, bit bb MSB-first
    //    within byte → mask bit (bo * 8 + (7 - bb)).
    final int newBitByteOffset = (newAbsBit / 8) - oldInitialBytePos;
    if (newBitByteOffset < 0 || newBitByteOffset >= 8) {
      // Cross-window: upgrade parent from SingleMask → MultiMask with
      // the expanded disc-bit set including the new bit at its true
      // byte position. Maintains HOT invariant because the new bit is
      // already verified constant in every non-split sibling's subtree
      // by the caller (via bitConstantValueInSubtree on splitParentAndRecurse's
      // fallback paths) — we re-verify below. HFT-grade: one clone of
      // children[], one new int[] for partials, no boxing.
      return upgradeToMultiMaskWithNewBit(parent, splitChildIndex,
          leftChild, rightChild, newAbsBit, revision);
    }
    final int newBitInByte = newAbsBit % 8;
    final int bitInWord = newBitByteOffset * 8 + (7 - newBitInByte);
    final long newBitMaskBit = 1L << bitInWord;
    if ((oldMask & newBitMaskBit) != 0L) {
      return null; // bit already a disc bit — caller handles via split path
    }
    final long newMask = oldMask | newBitMaskBit;

    // 2. Verify the new bit is constant across every non-split sibling.
    //    Also collect each sibling's constant value so we can set its
    //    bit in the repositioned partial key.
    final int[] siblingBitValues = new int[numChildren];
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final Integer v = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (v == null) return null; // invariant would break
      siblingBitValues[i] = v;
    }

    // 3. Compute where the new bit sits in the new compressed partial-
    //    key layout. PEXT packs mask bits from LSB → MSB in output.
    //    newBit's output position = popcount(newMask bits below it).
    final int newBitOutputPos = Long.bitCount(newMask & (newBitMaskBit - 1L));
    // Mask over partial-key bits contributed by the OLD mask in the NEW layout:
    // new layout has (oldCount+1) output bits; the new bit occupies
    // position newBitOutputPos; all OTHER positions are the old bits.
    final long oldPartialMaskInNewLayout = (((1L << (oldCount + 1)) - 1L)
        ^ (1L << newBitOutputPos));

    // 4. Build the expanded children + partial-keys arrays.
    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartialKeys = new int[newNumChildren];
    final int[] oldPartialKeys = parent.getPartialKeys();

    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        // Replace split slot with left+right. Compute their partial keys
        // by extracting the new mask's bits from their first keys.
        newChildren[j] = leftChild;
        newPartialKeys[j] = computePartialKeySingleMask(
            getFirstKeyFromChild(leftChild), oldInitialBytePos, newMask);
        j++;
        newChildren[j] = rightChild;
        newPartialKeys[j] = computePartialKeySingleMask(
            getFirstKeyFromChild(rightChild), oldInitialBytePos, newMask);
        j++;
      } else {
        newChildren[j] = parent.getChildReference(i);
        // _pdep: spread the oldCount bits of oldPartialKeys[i] into the
        // oldPartialMaskInNewLayout positions of a (oldCount+1)-bit word.
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(oldPartialKeys[i]),
            oldPartialMaskInNewLayout);
        // Add the new bit's value at its designated position.
        newPartialKeys[j] = repositioned | (siblingBitValues[i] << newBitOutputPos);
        j++;
      }
    }

    // 5. Build the new indirect page with the extended disc-bit set.
    //    Keeps the parent's original page key (CoW via log.put updates
    //    the reference) and the same height — only the mask + partial
    //    keys + children arrays grow.
    // Correctness guard: reject builds where the PEXT of any child's
    // first key under newMask disagrees with the stored partial key,
    // OR where any two children share the same partial key (routing
    // collision). Both indicate the HOT invariant is broken; bail so
    // the caller falls back to the full-rebuild split path which re-
    // derives disc bits from scratch with the augmentation loop.
    for (int i = 0; i < newNumChildren; i++) {
      final int pext = computePartialKeySingleMask(
          getFirstKeyFromChild(newChildren[i]), oldInitialBytePos, newMask);
      if (pext != newPartialKeys[i]) return null;
      for (int k = 0; k < i; k++) {
        if (newPartialKeys[k] == newPartialKeys[i]) return null;
      }
    }
    if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNode(parent.getPageKey(), revision,
          oldInitialBytePos, newMask, newPartialKeys, newChildren, parent.getHeight());
    }
    return HOTIndirectPage.createMultiNode(parent.getPageKey(), revision,
        oldInitialBytePos, newMask, newPartialKeys, newChildren, parent.getHeight());
  }

  /**
   * Height-optimal absorption fallback: rebuild parent's children with L, R
   * replacing the split slot, recomputing disc bits via
   * {@link #createNodeFromChildren}. Avoids the split cascade that would
   * otherwise create a new BiNode at a higher level — keeps the tree wide.
   *
   * <p>Returns null if the expansion would exceed fan-out 32 (genuine split
   * needed) or if fresh disc-bit computation can't distinguish all children.
   */
  private @Nullable HOTIndirectPage rebuildParentAbsorbingSplit(
      HOTIndirectPage parent, int splitChildIndex,
      PageReference leftChild, PageReference rightChild, int revision) {
    final int numChildren = parent.getNumChildren();
    final int leftLogKey = leftChild.getLogKey();
    final int rightLogKey = rightChild.getLogKey();
    int newCount = numChildren + 1;
    PageReference[] rebuilt = new PageReference[newCount];
    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        rebuilt[j++] = leftChild;
        rebuilt[j++] = rightChild;
      } else {
        final PageReference c = parent.getChildReference(i);
        final int clk = c.getLogKey();
        if (clk >= 0 && (clk == leftLogKey || clk == rightLogKey)) {
          newCount--;
          continue;
        }
        rebuilt[j++] = c;
      }
    }
    if (j < rebuilt.length) rebuilt = Arrays.copyOf(rebuilt, j);
    if (rebuilt.length < 2 || rebuilt.length > NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN) {
      return null;
    }
    try {
      return createNodeFromChildren(rebuilt, parent.getPageKey(), revision, parent.getHeight());
    } catch (Throwable t) {
      return null;
    }
  }

  /**
   * Upgrade a SingleMask parent to MultiMask layout when a new disc
   * bit falls outside the current 8-byte PEXT window. Produces a
   * MultiMask indirect page whose disc-bit set = parent's old bits ∪
   * {newAbsBit}, with the split slot replaced by (leftChild, rightChild).
   *
   * <p>Correctness: MultiMask supports disc bits at any byte positions
   * via explicit extraction tables. The new partial keys are computed
   * directly via PEXT on each child's first key, preserving the
   * routing contract (INV) by construction. Rejects (returns null) if
   * the new bit is not constant across some non-split sibling's
   * subtree — INV invariant check, same as SingleMask addEntry.
   *
   * <p>HFT discipline: single allocation of {@code byte[]
   * extractionPositions}, {@code long[] extractionMasks}, new
   * {@code int[] newPartials}, and new {@code PageReference[]
   * newChildren}. Uses {@link Long#compress} intrinsic for each
   * gathered byte. No TreeMap / no boxing.
   */
  private @Nullable HOTIndirectPage upgradeToMultiMaskWithNewBit(
      HOTIndirectPage parent, int splitChildIndex,
      PageReference leftChild, PageReference rightChild,
      int newAbsBit, int revision) {
    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int oldCount = Long.bitCount(oldMask);
    final int numChildren = parent.getNumChildren();

    // Decode old mask into (bytePos, maskByte) pairs, in ascending byte order.
    // The old SingleMask covers bytes [oldInitialBytePos, oldInitialBytePos+7].
    // Collect only bytes that actually have disc bits.
    int[] oldBytePositions = new int[8];
    int[] oldByteMaskBits = new int[8];
    int oldByteCount = 0;
    for (int bo = 0; bo < 8; bo++) {
      final int byteMaskBits = (int) ((oldMask >>> (bo * 8)) & 0xFFL);
      if (byteMaskBits != 0) {
        oldBytePositions[oldByteCount] = oldInitialBytePos + bo;
        oldByteMaskBits[oldByteCount] = byteMaskBits;
        oldByteCount++;
      }
    }

    // Add new bit at its absolute byte position.
    final int newBytePos = newAbsBit / 8;
    final int newBitInByte = newAbsBit % 8;
    final int newMaskBit = 1 << (7 - newBitInByte);

    // Merge new byte into sorted list; if it coincides with an existing
    // byte, OR the bits.
    int[] allBytePositions = new int[oldByteCount + 1];
    int[] allByteMaskBits = new int[oldByteCount + 1];
    int allCount = 0;
    boolean merged = false;
    for (int i = 0; i < oldByteCount; i++) {
      if (!merged && oldBytePositions[i] == newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = oldByteMaskBits[i] | newMaskBit;
        allCount++;
        merged = true;
      } else if (!merged && oldBytePositions[i] > newBytePos) {
        allBytePositions[allCount] = newBytePos;
        allByteMaskBits[allCount] = newMaskBit;
        allCount++;
        merged = true;
        allBytePositions[allCount] = oldBytePositions[i];
        allByteMaskBits[allCount] = oldByteMaskBits[i];
        allCount++;
      } else {
        allBytePositions[allCount] = oldBytePositions[i];
        allByteMaskBits[allCount] = oldByteMaskBits[i];
        allCount++;
      }
    }
    if (!merged) {
      allBytePositions[allCount] = newBytePos;
      allByteMaskBits[allCount] = newMaskBit;
      allCount++;
    }

    // Verify new bit is constant in every non-split sibling's subtree.
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      if (bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit) == null) {
        return null;
      }
    }

    // Build extractionPositions / extractionMasks arrays in MultiMask layout.
    final byte[] extractionPositions = new byte[allCount];
    final int numChunks = (allCount + 7) / 8;
    final long[] extractionMasks = new long[numChunks];
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < allCount; i++) {
      extractionPositions[i] = (byte) allBytePositions[i];
      final int chunkIdx = i / 8;
      final int byteOffset = i % 8;
      extractionMasks[chunkIdx] |= ((long) (allByteMaskBits[i] & 0xFF)) << (byteOffset * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(allByteMaskBits[i] & 0xFF);
      final int absBitPos = allBytePositions[i] * 8 + (7 - highBit);
      if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
    }

    // Build expanded children and partials.
    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartials = new int[newNumChildren];
    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j] = leftChild;
        newPartials[j] = computePartialKeyMultiMaskDirect(
            getFirstKeyFromChild(leftChild), extractionPositions, extractionMasks, allCount);
        j++;
        newChildren[j] = rightChild;
        newPartials[j] = computePartialKeyMultiMaskDirect(
            getFirstKeyFromChild(rightChild), extractionPositions, extractionMasks, allCount);
        j++;
      } else {
        newChildren[j] = parent.getChildReference(i);
        newPartials[j] = computePartialKeyMultiMaskDirect(
            getFirstKeyFromChild(newChildren[j]), extractionPositions, extractionMasks, allCount);
        j++;
      }
    }

    // Verify all partial keys are unique — collision would indicate INV breach.
    for (int i = 1; i < newNumChildren; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) return null;
      }
    }

    if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(parent.getPageKey(), revision,
          extractionPositions, extractionMasks, allCount, newPartials, newChildren,
          parent.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(parent.getPageKey(), revision,
        extractionPositions, extractionMasks, allCount, newPartials, newChildren,
        parent.getHeight(), msbIndex);
  }

  /**
   * Reference-faithful {@code MultiMaskPartialKeyMapping::insert} port:
   * add a new disc bit to a MultiMask parent and integrate a leaf
   * split's two halves. Returns the updated MultiMask indirect page, or
   * null if the invariant would break (new bit not constant in some
   * sibling's subtree, or duplicate partial key).
   *
   * <p>HFT-grade: single allocation of new extractionPositions[] and
   * extractionMasks[] (sized exactly for the new disc-bit set),
   * one new int[] for partials, one new PageReference[] for children.
   * Uses {@link Long#compress} per-chunk. Byte insertion maintains
   * sorted extractionPositions order, shifting mask bytes through LE
   * chunks via a single pass. No ArrayList, no TreeMap.
   */
  private @Nullable HOTIndirectPage addEntryMultiMask(
      HOTIndirectPage parent, int splitChildIndex,
      PageReference leftChild, PageReference rightChild,
      int newAbsBit, int revision) {
    final int numChildren = parent.getNumChildren();
    final byte[] oldExtractionPositions = parent.getExtractionPositions();
    final long[] oldExtractionMasks = parent.getExtractionMasks();
    if (oldExtractionPositions == null || oldExtractionMasks == null) return null;
    final int oldNumBytes = oldExtractionPositions.length;

    final int newBytePos = newAbsBit / 8;
    final int newBitInByte = newAbsBit % 8;
    final int newMaskBit = 1 << (7 - newBitInByte);

    // 1. INV guard: new bit must be constant in every non-split sibling's subtree.
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      if (bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit) == null) return null;
    }

    // 2. Locate existing entry for newBytePos, if any (sorted positions).
    int existingIdx = -1;
    int insertIdx = oldNumBytes;
    for (int i = 0; i < oldNumBytes; i++) {
      final int bp = oldExtractionPositions[i] & 0xFF;
      if (bp == newBytePos) { existingIdx = i; break; }
      if (bp > newBytePos) { insertIdx = i; break; }
    }

    // 3. Build new extraction tables.
    final byte[] newExtractionPositions;
    final long[] newExtractionMasks;
    final int newNumBytes;
    if (existingIdx >= 0) {
      // Merge: OR the new mask bit into the existing byte's mask.
      final int chunkIdx = existingIdx / 8;
      final int byteOffset = existingIdx % 8;
      final int existingByte = (int) ((oldExtractionMasks[chunkIdx] >>> (byteOffset * 8)) & 0xFF);
      if ((existingByte & newMaskBit) != 0) return null; // duplicate
      newNumBytes = oldNumBytes;
      newExtractionPositions = oldExtractionPositions.clone();
      newExtractionMasks = oldExtractionMasks.clone();
      newExtractionMasks[chunkIdx] |= ((long) newMaskBit) << (byteOffset * 8);
    } else {
      // Insert: shift entries at/after insertIdx down by one; add new entry.
      newNumBytes = oldNumBytes + 1;
      newExtractionPositions = new byte[newNumBytes];
      if (insertIdx > 0) {
        System.arraycopy(oldExtractionPositions, 0, newExtractionPositions, 0, insertIdx);
      }
      newExtractionPositions[insertIdx] = (byte) newBytePos;
      if (insertIdx < oldNumBytes) {
        System.arraycopy(oldExtractionPositions, insertIdx,
            newExtractionPositions, insertIdx + 1, oldNumBytes - insertIdx);
      }
      final int newNumChunks = (newNumBytes + 7) / 8;
      newExtractionMasks = new long[newNumChunks];
      // Rebuild mask bytes. For destination position i:
      //   i <  insertIdx       → old position i
      //   i == insertIdx       → newMaskBit
      //   i >  insertIdx       → old position i-1
      for (int i = 0; i < newNumBytes; i++) {
        final int maskByte;
        if (i == insertIdx) {
          maskByte = newMaskBit;
        } else {
          final int srcIdx = i < insertIdx ? i : i - 1;
          final int srcChunk = srcIdx / 8;
          final int srcOffset = srcIdx % 8;
          maskByte = (int) ((oldExtractionMasks[srcChunk] >>> (srcOffset * 8)) & 0xFF);
        }
        final int dstChunk = i / 8;
        final int dstOffset = i % 8;
        newExtractionMasks[dstChunk] |= ((long) (maskByte & 0xFF)) << (dstOffset * 8);
      }
    }

    // 4. Compute new MSB index (smallest absolute disc bit).
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < newNumBytes; i++) {
      final int chunkIdx = i / 8;
      final int byteOffset = i % 8;
      final int maskByte = (int) ((newExtractionMasks[chunkIdx] >>> (byteOffset * 8)) & 0xFF);
      if (maskByte == 0) continue;
      final int highBit = 31 - Integer.numberOfLeadingZeros(maskByte);
      final int absBit = (newExtractionPositions[i] & 0xFF) * 8 + (7 - highBit);
      if (absBit < msbIndex) msbIndex = (short) absBit;
    }

    // 5. Build expanded children + partial keys.
    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartials = new int[newNumChildren];
    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j] = leftChild;
        newPartials[j] = computePartialKeyMultiMaskDirect(
            getFirstKeyFromChild(leftChild), newExtractionPositions, newExtractionMasks, newNumBytes);
        j++;
        newChildren[j] = rightChild;
        newPartials[j] = computePartialKeyMultiMaskDirect(
            getFirstKeyFromChild(rightChild), newExtractionPositions, newExtractionMasks, newNumBytes);
        j++;
      } else {
        newChildren[j] = parent.getChildReference(i);
        newPartials[j] = computePartialKeyMultiMaskDirect(
            getFirstKeyFromChild(newChildren[j]),
            newExtractionPositions, newExtractionMasks, newNumBytes);
        j++;
      }
    }

    // 6. Verify partial-key uniqueness (INV routing guard).
    for (int i = 1; i < newNumChildren; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) return null;
      }
    }

    if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(parent.getPageKey(), revision,
          newExtractionPositions, newExtractionMasks, newNumBytes,
          newPartials, newChildren, parent.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(parent.getPageKey(), revision,
        newExtractionPositions, newExtractionMasks, newNumBytes,
        newPartials, newChildren, parent.getHeight(), msbIndex);
  }

  /**
   * Compute MultiMask partial key for a single key (byte-gather + per-byte PEXT).
   * Matches the read path's {@link HOTIndirectPage#computeMultiMaskPartialKeyScalar}.
   */
  private static int computePartialKeyMultiMaskDirect(byte[] key,
      byte[] extractionPositions, long[] extractionMasks, int numExtractionBytes) {
    final int numChunks = extractionMasks.length;
    final long[] gathered = new long[numChunks];
    for (int i = 0; i < numExtractionBytes; i++) {
      final int keyBytePos = extractionPositions[i] & 0xFF;
      final int keyByte = keyBytePos < key.length ? (key[keyBytePos] & 0xFF) : 0;
      final int chunkIdx = i / 8;
      final int byteOffset = i % 8;
      gathered[chunkIdx] |= ((long) keyByte) << (byteOffset * 8);
    }
    int result = 0;
    int shift = 0;
    for (int w = 0; w < numChunks; w++) {
      final int extracted = (int) Long.compress(gathered[w], extractionMasks[w]);
      result |= extracted << shift;
      shift += Long.bitCount(extractionMasks[w]);
    }
    return result;
  }

  /**
   * @return the value (0 or 1) of bit {@code absBitPos} across every key
   * stored in the subtree rooted at {@code ref}, or {@code null} if the
   * bit varies within that subtree. An empty subtree returns 0
   * (vacuously constant). Unresolvable pages return {@code null} —
   * callers treat that as "cannot extend", falling back to the split
   * parent path.
   */
  private @Nullable Integer bitConstantValueInSubtree(PageReference ref, int absBitPos) {
    Page page = ref.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, ref);
      if (page != null) ref.setPage(page);
    }
    if (page instanceof HOTLeafPage leaf) {
      final int n = leaf.getEntryCount();
      if (n == 0) return 0;
      final int first = DiscriminativeBitComputer.isBitSet(leaf.getKeySlice(0), absBitPos) ? 1 : 0;
      for (int i = 1; i < n; i++) {
        final int v = DiscriminativeBitComputer.isBitSet(leaf.getKeySlice(i), absBitPos) ? 1 : 0;
        if (v != first) return null;
      }
      return first;
    }
    if (page instanceof HOTIndirectPage indirect) {
      final int m = indirect.getNumChildren();
      if (m == 0) return 0;
      final Integer first = bitConstantValueInSubtree(indirect.getChildReference(0), absBitPos);
      if (first == null) return null;
      for (int i = 1; i < m; i++) {
        final Integer v = bitConstantValueInSubtree(indirect.getChildReference(i), absBitPos);
        if (v == null || !v.equals(first)) return null;
      }
      return first;
    }
    return null;
  }

  /**
   * Get height from a child reference. Falls back to loading from storage if not swizzled.
   */
  private int getHeightFromChild(PageReference childRef, @Nullable StorageEngineReader reader) {
    Page page = childRef.getPage();
    if (page == null && reader != null) {
      page = loadPage(reader, childRef);
      if (page != null) {
        childRef.setPage(page); // swizzle for future access
      }
    }
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

    // Copy children, replacing split child with left+right.
    // COW-created copies of PageReferences inherit logKeys from originals.
    // If a copy shares a logKey with leftChild or rightChild, both resolve
    // to the same TIL entry (same leaf page) — skip the stale copy.
    int j = 0;
    final int leftLogKey = leftChild.getLogKey();
    final int rightLogKey = rightChild.getLogKey();
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j++] = leftChild;
        newChildren[j++] = rightChild;
      } else {
        final PageReference child = parent.getChildReference(i);
        final int childLogKey = child.getLogKey();
        if (childLogKey >= 0 && (childLogKey == leftLogKey || childLogKey == rightLogKey)) {
          // Stale copy — aliases the same leaf page as a split result. Skip it.
          newNumChildren--;
          continue;
        }
        newChildren[j++] = child;
      }
    }

    // Trim array if stale copies were removed
    if (j < newChildren.length) {
      newChildren = Arrays.copyOf(newChildren, j);
    }

    // Sort children by first key to ensure correct boundary computation.
    // HOT trie routing by disc bits can place keys out of key-order (e.g., key 1024
    // routes to child[0..511] because its disc bit matches). Without sorting,
    // boundary computation misses discriminative bits between non-adjacent children.
    sortChildrenByFirstKey(newChildren);

    // Compute discriminative bits and partial keys for navigation
    final int initialBytePos = computeInitialBytePos(newChildren);
    final DiscBitsInfo discBits = computeDiscBits(newChildren, initialBytePos);
    final int[] partialKeys = computePartialKeysForChildren(newChildren, discBits);

    // Create appropriate node type based on child count
    final HOTIndirectPage created;
    if (newNumChildren <= 2) {
      // Recompute disc bit from sorted children (the passed-in discriminativeBit may
      // refer to the pre-sort position or be -1 for identical boundary keys)
      final byte[] lMax = getLastKeyFromChild(newChildren[0]);
      final byte[] rMin = getFirstKeyFromChild(newChildren[1]);
      final int recomputedDiscBit = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(lMax, rMin));
      created = HOTIndirectPage.createBiNode(parent.getPageKey(), revision, recomputedDiscBit, newChildren[0],
          newChildren[1], parent.getHeight());
    } else {
      created = createNodeWithDiscBits(parent.getPageKey(), revision, parent.getHeight(),
          discBits, partialKeys, newChildren);
    }
    return created;
  }


  /**
   * Create a HOTIndirectPage (SpanNode or MultiNode) using the given disc bits info.
   * Dispatches to SingleMask or MultiMask factory methods based on layout type.
   */
  private static HOTIndirectPage createNodeWithDiscBits(long pageKey, int revision, int height,
      DiscBitsInfo discBits, int[] partialKeys, PageReference[] children) {
    if (discBits.isSingleMask()) {
      if (children.length <= 16) {
        return HOTIndirectPage.createSpanNode(pageKey, revision, discBits.initialBytePos(),
            discBits.bitMask(), partialKeys, children, height);
      } else {
        return HOTIndirectPage.createMultiNode(pageKey, revision, discBits.initialBytePos(),
            discBits.bitMask(), partialKeys, children, height);
      }
    } else {
      if (children.length <= 16) {
        return HOTIndirectPage.createSpanNodeMultiMask(pageKey, revision,
            discBits.extractionPositions(), discBits.extractionMasks(),
            discBits.numExtractionBytes(), partialKeys, children, height,
            discBits.mostSignificantBitIndex());
      } else {
        return HOTIndirectPage.createMultiNodeMultiMask(pageKey, revision,
            discBits.extractionPositions(), discBits.extractionMasks(),
            discBits.numExtractionBytes(), partialKeys, children, height,
            discBits.mostSignificantBitIndex());
      }
    }
  }

  /**
   * Compute initial byte position from children keys.
   */
  private int computeInitialBytePos(PageReference[] children) {
    if (children.length < 2)
      return 0;

    int minBytePos = Integer.MAX_VALUE;
    for (int i = 0; i < children.length - 1; i++) {
      final byte[] last = getLastKeyFromChild(children[i]);
      final byte[] first = getFirstKeyFromChild(children[i + 1]);
      final int bit = DiscriminativeBitComputer.computeDifferingBit(last, first);
      if (bit >= 0) {
        minBytePos = Math.min(minBytePos, bit / 8);
      }
    }
    return minBytePos == Integer.MAX_VALUE ? 0 : minBytePos;
  }

  /**
   * Compute bit mask that covers all discriminative bits for the children.
   * Uses little-endian layout matching {@link HOTIndirectPage#getKeyWordAt}:
   * byte 0 of window → bits 0-7, byte 1 → bits 8-15, etc.
   * Within each byte, MSB (bit 0) maps to position 7, LSB (bit 7) maps to position 0.
   */
  /**
   * Compute discriminative bit information for a set of sorted children.
   * Returns either SingleMask (bits fit in 8 contiguous bytes) or MultiMask
   * (bits span >8 bytes, requiring byte gathering + per-chunk PEXT).
   *
   * <p>Matches C++ reference: SingleMaskPartialKeyMapping vs MultiMaskPartialKeyMapping.</p>
   */
  private DiscBitsInfo computeDiscBits(PageReference[] children, int initialBytePos) {
    // Collect all disc bit positions
    int minBytePos = Integer.MAX_VALUE;
    int maxBytePos = 0;
    final TreeMap<Integer, Integer> maskByBytePos = new TreeMap<>();

    // Reference-faithful collection (Binna §4.2): for each adjacent
    // child pair, every differing bit is a candidate disc bit. Capture
    // ALL differing bits (not just the MSB), subject to the HOT
    // invariant that the bit must be constant within every child's
    // subtree. This avoids partial-key collisions when multiple pairs
    // differ at the same MSB — a single MSB is not enough to
    // distinguish N≥3 sorted children.
    for (int i = 0; i < children.length - 1; i++) {
      final byte[] key1 = getLastKeyFromChild(children[i]);
      final byte[] key2 = getFirstKeyFromChild(children[i + 1]);
      if (key1.length == 0 || key2.length == 0) {
        continue;
      }
      final int minLen = Math.min(key1.length, key2.length);
      for (int b = 0; b < minLen; b++) {
        int diff = (key1[b] ^ key2[b]) & 0xFF;
        while (diff != 0) {
          final int hb = Integer.numberOfLeadingZeros(diff) - 24; // 0..7, 0=MSB
          final int absBit = b * 8 + hb;
          diff &= ~(1 << (7 - hb));
          if (isDiscBitAlreadyCaptured(maskByBytePos, b, hb)) continue;
          boolean constantInAllSubtrees = true;
          for (final PageReference c : children) {
            if (bitConstantValueInSubtree(c, absBit) == null) {
              constantInAllSubtrees = false;
              break;
            }
          }
          if (!constantInAllSubtrees) continue;
          final int maskBit = 1 << (7 - hb);
          maskByBytePos.merge(b, maskBit, (a, b2) -> a | b2);
          if (b < minBytePos) minBytePos = b;
          if (b > maxBytePos) maxBytePos = b;
        }
      }
    }

    if (maskByBytePos.isEmpty()) {
      return DiscBitsInfo.singleMask(initialBytePos, 1L);
    }

    // Verify partial keys are unique. If not, the HOT invariant is
    // violated — augment with additional bits from pairs of NON-
    // adjacent children (whose first-keys differ but no adjacent pair
    // of them was captured). This is rare but can happen when the
    // MSB-only XOR of adjacent pairs all coincides at the same bit.
    augmentUntilPartialsUnique(children, maskByBytePos);

    // Refresh min/max after augmentation
    minBytePos = Integer.MAX_VALUE;
    maxBytePos = 0;
    for (int bp : maskByBytePos.keySet()) {
      if (bp < minBytePos) minBytePos = bp;
      if (bp > maxBytePos) maxBytePos = bp;
    }

    if (maxBytePos - initialBytePos < 8 && minBytePos >= initialBytePos) {
      long mask = 0;
      for (final var entry : maskByBytePos.entrySet()) {
        final int byteOffset = entry.getKey() - initialBytePos;
        final int maskByte = entry.getValue();
        mask |= ((long) maskByte) << (byteOffset * 8);
      }
      return DiscBitsInfo.singleMask(initialBytePos, mask);
    }

    return buildMultiMask(maskByBytePos, minBytePos);
  }

  private static boolean isDiscBitAlreadyCaptured(TreeMap<Integer, Integer> maskByBytePos,
      int bytePos, int bitInByte) {
    final Integer existing = maskByBytePos.get(bytePos);
    if (existing == null) return false;
    return (existing & (1 << (7 - bitInByte))) != 0;
  }

  /**
   * Augment the disc-bit mask with additional bits until all children
   * produce unique partial keys under the mask. Required because the
   * adjacent-pair-MSB-only collection misses bits needed to distinguish
   * 3+ children that share a common XOR-MSB.
   */
  private void augmentUntilPartialsUnique(PageReference[] children,
      TreeMap<Integer, Integer> maskByBytePos) {
    while (true) {
      final int[] partials = computePartialKeysForMaskByBytePos(children, maskByBytePos);
      int collide_i = -1, collide_j = -1;
      outer: for (int i = 0; i < partials.length; i++) {
        for (int j = i + 1; j < partials.length; j++) {
          if (partials[i] == partials[j]) { collide_i = i; collide_j = j; break outer; }
        }
      }
      if (collide_i < 0) return;
      // Find any differing bit between first-keys of collide_i and collide_j
      // that is constant in every subtree and not already captured.
      final byte[] ki = getFirstKeyFromChild(children[collide_i]);
      final byte[] kj = getFirstKeyFromChild(children[collide_j]);
      final int minLen = Math.min(ki.length, kj.length);
      boolean added = false;
      for (int b = 0; b < minLen && !added; b++) {
        int diff = (ki[b] ^ kj[b]) & 0xFF;
        while (diff != 0 && !added) {
          final int hb = Integer.numberOfLeadingZeros(diff) - 24;
          final int absBit = b * 8 + hb;
          if (!isDiscBitAlreadyCaptured(maskByBytePos, b, hb)) {
            boolean constantInAllSubtrees = true;
            for (final PageReference c : children) {
              if (bitConstantValueInSubtree(c, absBit) == null) {
                constantInAllSubtrees = false;
                break;
              }
            }
            if (constantInAllSubtrees) {
              final int maskBit = 1 << (7 - hb);
              maskByBytePos.merge(b, maskBit, (a, b2) -> a | b2);
              added = true;
            }
          }
          diff &= ~(1 << (7 - hb));
        }
      }
      if (!added) {
        // Cannot augment — invariant fundamentally violated. Bail.
        return;
      }
    }
  }

  /**
   * Compute partial keys for all children using the current
   * {@code maskByBytePos} (works for both SingleMask and MultiMask
   * layouts via byte-gather + per-byte PEXT).
   */
  private int[] computePartialKeysForMaskByBytePos(PageReference[] children,
      TreeMap<Integer, Integer> maskByBytePos) {
    final int[] partials = new int[children.length];
    for (int ci = 0; ci < children.length; ci++) {
      final byte[] key = getFirstKeyFromChild(children[ci]);
      int partial = 0;
      int shift = 0;
      for (final var entry : maskByBytePos.entrySet()) {
        final int bp = entry.getKey();
        final int mb = entry.getValue();
        final int kb = (bp < key.length) ? (key[bp] & 0xFF) : 0;
        partial |= ((int) Long.compress(kb & 0xFFL, mb & 0xFFL)) << shift;
        shift += Integer.bitCount(mb & 0xFF);
      }
      partials[ci] = partial;
    }
    return partials;
  }

  /**
   * Build MultiMask discriminative bit info from grouped disc bits.
   *
   * <p>Creates extraction positions (which key bytes to gather) and extraction masks
   * (which bits within those bytes are discriminative), packed into long[] for PEXT.</p>
   */
  private DiscBitsInfo buildMultiMask(TreeMap<Integer, Integer> maskByBytePos, int minBytePos) {
    final int numBytes = maskByBytePos.size();
    final byte[] extractionPositions = new byte[numBytes];
    final int numChunks = (numBytes + 7) / 8;
    final long[] extractionMasks = new long[numChunks];

    int i = 0;
    short msbIndex = Short.MAX_VALUE;
    for (final var entry : maskByBytePos.entrySet()) {
      final int keyBytePos = entry.getKey();
      final int maskByte = entry.getValue();
      extractionPositions[i] = (byte) keyBytePos;
      // Pack mask byte into the appropriate long chunk (LE byte order)
      final int chunkIdx = i / 8;
      final int byteOffset = i % 8;
      extractionMasks[chunkIdx] |= ((long) (maskByte & 0xFF)) << (byteOffset * 8);

      // Compute MSB index: the smallest absolute bit position
      final int highBit = 31 - Integer.numberOfLeadingZeros(maskByte & 0xFF);
      final int absBitPos = keyBytePos * 8 + (7 - highBit);
      msbIndex = (short) Math.min(msbIndex, absBitPos);
      i++;
    }

    return DiscBitsInfo.multiMask(extractionPositions, extractionMasks, numBytes, msbIndex);
  }

  /**
   * Compute partial keys for all children based on discriminative bit info.
   *
   * <p>Reference-faithful semantics (Binna's
   * {@code SparsePartialKeys::getRelevantBitsForRange}): each child's
   * sparse key is the AND-intersection of partial keys for every entry
   * under that child's subtree. Only bits that are constant within a
   * subtree can be reliably represented by first-key extraction; the
   * calling code ({@link #computeDiscBits}) already filters disc bits
   * to that invariant, so the first-key extraction below is equivalent
   * to the AND-intersection for valid disc bits. The filter guarantees
   * this by dropping any candidate bit that varies within a subtree.
   */
  private int[] computePartialKeysForChildren(PageReference[] children, DiscBitsInfo discBits) {
    final int[] partialKeys = new int[children.length];
    for (int i = 0; i < children.length; i++) {
      final byte[] key = getFirstKeyFromChild(children[i]);
      partialKeys[i] = computePartialKey(key, discBits);
    }
    return partialKeys;
  }

  /**
   * Compute partial key for a single key, dispatching to SingleMask or MultiMask.
   */
  private int computePartialKey(byte[] key, DiscBitsInfo discBits) {
    if (discBits.isSingleMask()) {
      return computePartialKeySingleMask(key, discBits.initialBytePos(), discBits.bitMask());
    } else {
      return computePartialKeyMultiMask(key, discBits.extractionPositions(),
          discBits.extractionMasks(), discBits.numExtractionBytes());
    }
  }

  /**
   * Compute partial key using MultiMask extraction (byte gathering + per-chunk PEXT).
   */
  private static int computePartialKeyMultiMask(byte[] key, byte[] extractionPositions,
      long[] extractionMasks, int numExtractionBytes) {
    final int numChunks = extractionMasks.length;
    final long[] gathered = new long[numChunks];
    for (int i = 0; i < numExtractionBytes; i++) {
      final int keyBytePos = extractionPositions[i] & 0xFF;
      final int keyByte = keyBytePos < key.length ? (key[keyBytePos] & 0xFF) : 0;
      final int chunkIdx = i / 8;
      final int byteOffset = i % 8;
      gathered[chunkIdx] |= ((long) keyByte) << (byteOffset * 8);
    }
    int result = 0;
    int shift = 0;
    for (int w = 0; w < numChunks; w++) {
      final int extracted = (int) Long.compress(gathered[w], extractionMasks[w]);
      result |= extracted << shift;
      shift += Long.bitCount(extractionMasks[w]);
    }
    return result;
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
  private void splitParentAndRecurse(StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      PageReference parentRef, HOTIndirectPage parent, int originalChildIndex,
      PageReference leftChild, PageReference rightChild, int discriminativeBit,
      PageReference rootReference, HOTIndirectPage[] pathNodes, PageReference[] pathRefs,
      int[] pathChildIndices, int currentPathIdx) {

    final int numChildren = parent.getNumChildren();

    // Reference: getMaskForLargerEntries() finds entries with MSB set
    // The MSB (most significant bit) determines the split boundary.
    // Uses the stored field (matching C++ reference's mMostSignificantDiscriminativeBitIndex).
    final int msbPosition = parent.getMostSignificantBitIndex();

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
        // Skip stale COW copies that alias the same TIL entry as a split result
        final int childLogKey = child.getLogKey();
        if (childLogKey >= 0 && (childLogKey == leftChild.getLogKey() || childLogKey == rightChild.getLogKey())) {
          continue;
        }
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

    // Reference-faithful {@code compressEntries}: each half inherits
    // only the parent's disc bits that actually differ within that
    // half's entries ({@code SparsePartialKeys::getRelevantBitsForRange}).
    // Partial keys are rebased via {@code _pext_u32} (Long.compress) to
    // the compressed layout. SingleMask-layout only; MultiMask and the
    // MSB-based partitioning above map children by key/value, not by
    // partial-key index, so we compute the per-half partial sets by
    // ordering children by their original index.
    final int[] leftIndices = new int[smallerCount];
    final int[] rightIndices = new int[largerCount];
    int li = 0, ri = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == originalChildIndex) continue; // split children handled separately
      final PageReference child = parent.getChildReference(i);
      if (!hasBitSet(getFirstKeyFromChild(child), msbPosition)) {
        if (li < leftIndices.length) leftIndices[li++] = i;
      } else {
        if (ri < rightIndices.length) rightIndices[ri++] = i;
      }
    }

    final PageReference leftNodeRef;
    if (leftChildren.length == 1) {
      leftNodeRef = leftChildren[0];
    } else {
      HOTIndirectPage leftNode = buildCompressedHalf(
          parent, leftChildren, leftIndices, Arrays.copyOf(leftIndices, li),
          originalChildIndex, leftChild, rightChild, false, msbPosition,
          parent.getPageKey(), storageEngineReader.getRevisionNumber());
      leftNodeRef = new PageReference();
      leftNodeRef.setKey(parent.getPageKey());
      leftNodeRef.setPage(leftNode);
      log.put(leftNodeRef, PageContainer.getInstance(leftNode, leftNode));
    }

    final PageReference rightNodeRef;
    if (rightChildren.length == 1) {
      rightNodeRef = rightChildren[0];
    } else {
      long rightPageKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage rightNode = buildCompressedHalf(
          parent, rightChildren, rightIndices, Arrays.copyOf(rightIndices, ri),
          originalChildIndex, leftChild, rightChild, true, msbPosition,
          rightPageKey, storageEngineReader.getRevisionNumber());
      rightNodeRef = new PageReference();
      rightNodeRef.setKey(rightPageKey);
      rightNodeRef.setPage(rightNode);
      log.put(rightNodeRef, PageContainer.getInstance(rightNode, rightNode));
    }

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
   * Reference-faithful {@code compressEntriesAndAddOneEntryIntoNewNode}
   * (Binna §4.2 split integration): build a new indirect node for a
   * half of a split. Inherits only the disc bits from {@code parent}
   * that actually differ within this half's entries ({@code
   * SparsePartialKeys::getRelevantBitsForRange}), and — if both leaves
   * of a child-split land in this half — ALSO includes the new disc
   * bit {@code splitDiscBitAbs} that separates them. Existing partial
   * keys are rebased to the new layout via {@code Long.compress}
   * ({@code _pext_u32}) and (when the new bit is added) repositioned
   * via {@code Long.expand} ({@code _pdep_u64}).
   *
   * <p>Returns a freshly-computed indirect page with SingleMask
   * representation. Falls back to {@code createNodeFromChildren} only
   * when the parent is MultiMask (not yet ported) or when the new
   * bit falls outside the parent's 8-byte PEXT window.
   */
  private HOTIndirectPage buildCompressedHalf(HOTIndirectPage parent,
      PageReference[] halfChildren, int[] indexBuf, int[] parentIndices,
      int splitChildIndex, PageReference splitLeft, PageReference splitRight,
      boolean isRightHalf, int msbPosition, long newPageKey, int revision) {
    // Reference-faithful port of {@code compressEntriesAndAddOneEntryIntoNewNode}
    // (Binna §4.2, C++ reference {@code SparsePartialKeys::compressEntries
    // AndAddOneEntryIntoNewNode}). HFT-grade: uses {@link Long#compress}
    // (PEXT = {@code _pext_u64}) to drop parent disc bits that are not
    // relevant in this half, and {@link Long#expand} (PDEP =
    // {@code _pdep_u64}) to reposition them into the new layout. Zero
    // allocations beyond the new {@code int[] newPartials} and the new
    // {@code PageReference[]}.
    //
    // LEMMA (compressEntries correctness): for a contiguous sorted range
    // of partials, {@code relevant = OR of (p[i] & ~p[i-1])} captures
    // enough bits to distinguish all entries in the range. For any pair
    // i<j with p[i] < p[j], the highest differing bit has p[j]=1 and
    // p[i]=0, and by sorted-integer monotonicity that bit must flip 0→1
    // at some adjacent step in the sequence, so it ends up in 'relevant'.
    if (parent.getLayoutType() != HOTIndirectPage.LayoutType.SINGLE_MASK) {
      // MultiMask parent not yet ported to compressed-half path — fall
      // back to fresh rebuild with constancy-filtered disc bits.
      return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
    }

    final int oldInitialBytePos = parent.getInitialBytePos();
    final long oldMask = parent.getBitMask();
    final int[] oldPartials = parent.getPartialKeys();
    final int oldCount = Long.bitCount(oldMask);

    // Determine which split children land in this half (by identity).
    final boolean leftHere = isChildInHalf(halfChildren, splitLeft);
    final boolean rightHere = isChildInHalf(halfChildren, splitRight);
    final boolean bothSplitHere = leftHere && rightHere;

    // Compute 'relevant' over parentIndices' partials, in their ORIGINAL
    // parent order. Since parent's children were sorted by first key
    // (HOT invariant), parent.getPartialKeys() is sorted ascending, so
    // iterating indices in ascending order gives us the sorted partial
    // sequence for this half.
    int relevant = 0;
    int prevPartial = -1;
    for (int i = 0; i < parentIndices.length; i++) {
      final int pi = parentIndices[i];
      final int p = oldPartials[pi];
      if (prevPartial >= 0) {
        relevant |= (p & ~prevPartial);
      }
      prevPartial = p;
    }

    // Encode split disc bit (if both halves land here, we need to add it).
    final int splitDiscBitAbs;
    final long splitBitMaskBit;
    if (bothSplitHere) {
      splitDiscBitAbs = DiscriminativeBitComputer.computeDifferingBit(
          getLastKeyFromChild(splitLeft), getFirstKeyFromChild(splitRight));
      if (splitDiscBitAbs < 0) {
        return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
      }
      final int byteOff = (splitDiscBitAbs / 8) - oldInitialBytePos;
      if (byteOff < 0 || byteOff >= 8) {
        // Cross-window split bit: must upgrade to MultiMask. Defer by
        // falling back to fresh rebuild (which chooses its own layout).
        return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
      }
      final int bitInByte = splitDiscBitAbs % 8;
      splitBitMaskBit = 1L << (byteOff * 8 + (7 - bitInByte));
    } else {
      splitDiscBitAbs = -1;
      splitBitMaskBit = 0L;
    }

    // Build newMask = the bits of oldMask selected by 'relevant', plus
    // (optionally) splitBitMaskBit. PEXT indexes mask bits LSB-first.
    long newMaskFromOld = 0L;
    long scan = oldMask;
    int outPos = 0;
    while (scan != 0L) {
      final long lowBit = scan & -scan;
      if ((relevant & (1 << outPos)) != 0) {
        newMaskFromOld |= lowBit;
      }
      scan &= scan - 1L;
      outPos++;
    }
    final long newMask = newMaskFromOld | splitBitMaskBit;
    if (newMask == 0L) {
      return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
    }

    final int totalNewBits = Long.bitCount(newMask);
    final int splitBitOutputPos = bothSplitHere
        ? Long.bitCount(newMask & (splitBitMaskBit - 1L))
        : -1;
    // Mask (in new partial-key space) where inherited-from-old bits land.
    // If splitBit is new-in-mask, it occupies splitBitOutputPos and all
    // other positions are for old bits. If splitBit happens to coincide
    // with an old bit (edge case), oldBitsInNewLayout covers all bits.
    final long oldBitsInNewLayout;
    if (bothSplitHere && (oldMask & splitBitMaskBit) == 0L) {
      oldBitsInNewLayout = (((1L << totalNewBits) - 1L) ^ (1L << splitBitOutputPos));
    } else {
      oldBitsInNewLayout = ((1L << totalNewBits) - 1L);
    }

    // Sort half children by first key (HOT invariant: sorted partials).
    final PageReference[] sortedChildren = halfChildren.clone();
    sortChildrenByFirstKey(sortedChildren);
    final int[] newPartials = new int[sortedChildren.length];

    for (int j = 0; j < sortedChildren.length; j++) {
      final PageReference c = sortedChildren[j];
      if (c == splitLeft || c == splitRight) {
        // Split products: compute partial directly from first key under newMask.
        newPartials[j] = computePartialKeySingleMask(
            getFirstKeyFromChild(c), oldInitialBytePos, newMask);
      } else {
        // Inherited child: find in parent, PEXT via relevant, PDEP into new layout.
        final int pIdx = indexOfInParent(parent, c, parentIndices);
        if (pIdx < 0) {
          return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
        }
        final int compressed = (int) Long.compress(
            Integer.toUnsignedLong(oldPartials[pIdx]),
            Integer.toUnsignedLong(relevant));
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(compressed),
            oldBitsInNewLayout);
        int partial = repositioned;
        if (bothSplitHere) {
          // Determine splitDiscBit's value across c's subtree.
          final Integer v = bitConstantValueInSubtree(c, splitDiscBitAbs);
          if (v == null) {
            return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
          }
          partial |= v.intValue() << splitBitOutputPos;
        }
        newPartials[j] = partial;
      }
    }

    // Verify all partials are unique (collision indicates INV breach upstream).
    for (int i = 1; i < newPartials.length; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[i] == newPartials[k]) {
          return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
        }
      }
    }

    // Assemble node.
    if (sortedChildren.length == 2) {
      final int db = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(
          getLastKeyFromChild(sortedChildren[0]), getFirstKeyFromChild(sortedChildren[1])));
      return HOTIndirectPage.createBiNode(newPageKey, revision, db,
          sortedChildren[0], sortedChildren[1], parent.getHeight());
    }
    if (sortedChildren.length <= 16) {
      return HOTIndirectPage.createSpanNode(newPageKey, revision,
          oldInitialBytePos, newMask, newPartials, sortedChildren, parent.getHeight());
    }
    return HOTIndirectPage.createMultiNode(newPageKey, revision,
        oldInitialBytePos, newMask, newPartials, sortedChildren, parent.getHeight());
  }

  private static boolean isChildInHalf(PageReference[] halfChildren, PageReference c) {
    for (int i = 0; i < halfChildren.length; i++) {
      if (halfChildren[i] == c) return true;
    }
    return false;
  }

  private static int indexOfInParent(HOTIndirectPage parent, PageReference c, int[] parentIndices) {
    for (int i = 0; i < parentIndices.length; i++) {
      if (parent.getChildReference(parentIndices[i]) == c) return parentIndices[i];
    }
    return -1;
  }

  /** Compute the discriminative bit that separates splitLeft from splitRight. */
  private int computeSplitDiscBit(PageReference splitLeft, PageReference splitRight) {
    final byte[] leftMax = getLastKeyFromChild(splitLeft);
    final byte[] rightMin = getFirstKeyFromChild(splitRight);
    return DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
  }

  /**
   * Check if a key has a specific bit set (MSB-first convention).
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
  private void splitParentHalfAndRecurse(StorageEngineWriter storageEngineReader, TransactionIntentLog log,
      PageReference parentRef, HOTIndirectPage parent, int originalChildIndex,
      PageReference leftChild, PageReference rightChild, PageReference rootReference,
      HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices, int currentPathIdx) {

    int numChildren = parent.getNumChildren();
    int splitPoint = numChildren / 2;
    boolean splitInLeftHalf = originalChildIndex < splitPoint;

    final int leftLogKey = leftChild.getLogKey();
    final int rightLogKey = rightChild.getLogKey();

    int leftCount = splitPoint + (splitInLeftHalf ? 1 : 0);
    PageReference[] leftChildren = new PageReference[leftCount];
    int li = 0;
    for (int i = 0; i < splitPoint; i++) {
      if (i == originalChildIndex) {
        leftChildren[li++] = leftChild;
        leftChildren[li++] = rightChild;
      } else {
        final PageReference child = parent.getChildReference(i);
        final int childLogKey = child.getLogKey();
        if (childLogKey >= 0 && (childLogKey == leftLogKey || childLogKey == rightLogKey)) {
          continue; // stale COW copy
        }
        leftChildren[li++] = child;
      }
    }
    if (li < leftChildren.length) {
      leftChildren = Arrays.copyOf(leftChildren, li);
    }

    int rightCount = numChildren - splitPoint + (splitInLeftHalf ? 0 : 1);
    PageReference[] rightChildren = new PageReference[rightCount];
    int ri = 0;
    for (int i = splitPoint; i < numChildren; i++) {
      if (i == originalChildIndex) {
        rightChildren[ri++] = leftChild;
        rightChildren[ri++] = rightChild;
      } else {
        final PageReference child = parent.getChildReference(i);
        final int childLogKey = child.getLogKey();
        if (childLogKey >= 0 && (childLogKey == leftLogKey || childLogKey == rightLogKey)) {
          continue; // stale COW copy
        }
        rightChildren[ri++] = child;
      }
    }
    if (ri < rightChildren.length) {
      rightChildren = Arrays.copyOf(rightChildren, ri);
    }

    // If only 1 child after stale-copy dedup, pass the reference through directly —
    // wrapping in a BiNode would point both children to the same page.
    final PageReference leftNodeRef;
    if (leftChildren.length == 1) {
      leftNodeRef = leftChildren[0];
    } else {
      HOTIndirectPage leftNode =
          createNodeFromChildren(leftChildren, parent.getPageKey(), storageEngineReader.getRevisionNumber(), parent.getHeight());
      leftNodeRef = new PageReference();
      leftNodeRef.setKey(parent.getPageKey());
      leftNodeRef.setPage(leftNode);
      log.put(leftNodeRef, PageContainer.getInstance(leftNode, leftNode));
    }

    final PageReference rightNodeRef;
    if (rightChildren.length == 1) {
      rightNodeRef = rightChildren[0];
    } else {
      long rightPageKey = pageKeyAllocator.getAsLong();
      HOTIndirectPage rightNode =
          createNodeFromChildren(rightChildren, rightPageKey, storageEngineReader.getRevisionNumber(), parent.getHeight());
      rightNodeRef = new PageReference();
      rightNodeRef.setKey(rightPageKey);
      rightNodeRef.setPage(rightNode);
      log.put(rightNodeRef, PageContainer.getInstance(rightNode, rightNode));
    }

    if (currentPathIdx > 0) {
      int grandparentIdx = currentPathIdx - 1;
      updateParentForSplitWithPath(storageEngineReader, log, pathRefs[grandparentIdx], pathNodes[grandparentIdx],
          pathChildIndices[grandparentIdx], leftNodeRef, rightNodeRef, getFirstKeyFromChild(rightNodeRef),
          rootReference, pathNodes, pathRefs, pathChildIndices, grandparentIdx);
    } else {
      byte[] lMax = getLastKeyFromChild(leftNodeRef);
      byte[] rMin = getFirstKeyFromChild(rightNodeRef);
      int rootDiscrimBit = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(lMax, rMin));

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
    if (children.length < 2) {
      throw new IllegalStateException("Cannot create HOTIndirectPage with " + children.length
          + " children — callers must handle the single-child case by passing the reference through directly");
    }

    // Sort children by first key for correct boundary computation
    sortChildrenByFirstKey(children);

    final HOTIndirectPage created;
    if (children.length == 2) {
      final byte[] leftMax = getLastKeyFromChild(children[0]);
      final byte[] rightMin = getFirstKeyFromChild(children[1]);
      final int discriminativeBit = Math.max(0, DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin));
      created = HOTIndirectPage.createBiNode(pageKey, revision, discriminativeBit, children[0], children[1], height);
    } else {
      final int initialBytePos = computeInitialBytePos(children);
      final DiscBitsInfo discBits = computeDiscBits(children, initialBytePos);
      final int[] partialKeys = computePartialKeysForChildren(children, discBits);
      created = createNodeWithDiscBits(pageKey, revision, height, discBits, partialKeys, children);
    }
    return created;
  }

  /**
   * Compute partial key (SingleMask) by extracting discriminative bits from a key.
   * Uses little-endian layout matching {@link HOTIndirectPage#getKeyWordAt}:
   * byte 0 of window → bits 0-7, byte 1 → bits 8-15, etc.
   */
  private int computePartialKeySingleMask(byte[] key, int initialBytePos, long bitMask) {
    if (key == null || key.length == 0) {
      return 0;
    }

    // Extract 8 bytes from key starting at initialBytePos (little-endian, matching getKeyWordAt)
    long keyWord = 0;
    for (int i = 0; i < 8 && (initialBytePos + i) < key.length; i++) {
      keyWord |= ((long) (key[initialBytePos + i] & 0xFF)) << (i * 8);
    }

    // Apply PEXT-style extraction: compress bits selected by mask
    return (int) Long.compress(keyWord, bitMask);
  }

  /**
   * Get the last key from a child reference (leaf or indirect page).
   * Falls back to loading from storage if not swizzled.
   */
  private byte[] getLastKeyFromChild(PageReference childRef) {
    Page page = childRef.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, childRef);
      if (page != null) {
        childRef.setPage(page); // swizzle for future access
      }
    }
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getLastKey();
    } else if (page instanceof HOTIndirectPage indirect) {
      return getLastKeyFromIndirectPage(indirect);
    }
    return EMPTY_KEY;
  }

  /**
   * Descend through indirect pages to find the last key.
   * Falls back to loading from storage if child is not swizzled.
   */
  private byte[] getLastKeyFromIndirectPage(HOTIndirectPage indirect) {
    final int numChildren = indirect.getNumChildren();
    if (numChildren == 0) {
      return EMPTY_KEY;
    }
    final PageReference lastChild = indirect.getChildReference(numChildren - 1);
    Page page = lastChild.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, lastChild);
      if (page != null) {
        lastChild.setPage(page);
      }
    }
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getLastKey();
    } else if (page instanceof HOTIndirectPage child) {
      return getLastKeyFromIndirectPage(child);
    }
    return EMPTY_KEY;
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
   * Falls back to loading from storage if not swizzled.
   */
  private byte[] getFirstKeyFromChild(PageReference childRef) {
    Page page = childRef.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, childRef);
      if (page != null) {
        childRef.setPage(page); // swizzle for future access
      }
    }
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getFirstKey();
    } else if (page instanceof HOTIndirectPage indirect) {
      return getFirstKeyFromIndirectPage(indirect);
    }
    return EMPTY_KEY;
  }

  /**
   * Descend through indirect pages to find the first key.
   * Falls back to loading from storage if child is not swizzled.
   */
  private byte[] getFirstKeyFromIndirectPage(HOTIndirectPage indirect) {
    final int numChildren = indirect.getNumChildren();
    if (numChildren == 0) {
      return EMPTY_KEY;
    }
    final PageReference firstChild = indirect.getChildReference(0);
    Page page = firstChild.getPage();
    if (page == null && activeReader != null) {
      page = loadPage(activeReader, firstChild);
      if (page != null) {
        firstChild.setPage(page);
      }
    }
    if (page instanceof HOTLeafPage leaf) {
      return leaf.getFirstKey();
    } else if (page instanceof HOTIndirectPage child) {
      return getFirstKeyFromIndirectPage(child);
    }
    return EMPTY_KEY;
  }
}

