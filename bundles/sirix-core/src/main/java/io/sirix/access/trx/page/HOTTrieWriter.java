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
   * TIL bound for the current split / augment scope. Set/reset symmetrically with
   * {@link #activeReader} so {@link #splitSubtreeOnBit} can install the freshly-built halves
   * in the CoW log without explicit threading.
   */
  private @Nullable TransactionIntentLog activeLog;

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

    // Set active reader+log for child page resolution and CoW page registration during split
    // / augment operations. The log binding lets {@link #splitSubtreeOnBit} install
    // freshly-built half-subtrees in the TIL without explicit threading through helpers.
    this.activeReader = storageEngineReader;
    this.activeLog = log;
    try {
      return handleLeafSplitAndInsertInternal(storageEngineReader, log, fullPage, leafRef,
          rootReference, pathNodes, pathRefs, pathChildIndices, pathDepth, keyBuf, keyLen, valueBuf, valueLen);
    } finally {
      this.activeReader = null;
      this.activeLog = null;
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
    // BE word layout: byte at window-offset {@code bo} occupies long bits ((7-bo)*8)..((7-bo)*8+7).
    // Within the byte, MSB-first bit {@code bb} is at long-bit ((7-bo)*8 + (7-bb)) = 63-(bo*8+bb).
    final int bitInWord = (7 - newBitByteOffset) * 8 + (7 - newBitInByte);
    final long newBitMaskBit = 1L << bitInWord;
    if ((oldMask & newBitMaskBit) != 0L) {
      return null; // bit already a disc bit — caller handles via split path
    }
    final long newMask = oldMask | newBitMaskBit;

    // 2. Compute where the new bit sits in the new compressed partial-key layout. PEXT packs
    //    mask bits from LSB → MSB in output, so the new bit's output position equals the
    //    population count of new-mask bits below it.
    final int newBitOutputPos = Long.bitCount(newMask & (newBitMaskBit - 1L));
    // Mask over partial-key bits contributed by the OLD mask in the NEW layout: new layout
    // has (oldCount+1) output bits; the new bit occupies position newBitOutputPos; all OTHER
    // positions hold the existing bits.
    final long oldPartialMaskInNewLayout =
        (((1L << (oldCount + 1)) - 1L) ^ (1L << newBitOutputPos));

    // 3. Verify the new disc bit is constant across every existing non-split sibling's
    //    subtree, and capture each sibling's constant value to set its repositioned partial
    //    bit. Required because Sirix's multi-entry leaves can hold keys differing at any
    //    bit; under sparse-path encoding subset-match routing misroutes keys destined for
    //    an off-path sibling that spans both bit values (the leaf-split's products beat the
    //    off-path sibling on most-specific match). Constancy ensures no off-path span exists.
    //    Binna's reference doesn't need this gate because his single-TID leaves trivially
    //    have constancy.
    final int[] siblingBitValues = new int[numChildren];
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int v = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (v < 0) return null; // non-constant — caller takes splitParentAndRecurse
      siblingBitValues[i] = v;
    }

    // 4. Build the expanded children + partial-keys arrays. With constancy verified above,
    //    each sibling's value at the new disc bit is determined; we set its partial bit at
    //    the new output position to that value. Equivalent to dense-PEXT-of-first-key under
    //    the constancy invariant — first key's bit value matches every other key's bit
    //    value in the subtree.
    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartialKeys = new int[newNumChildren];
    final int[] oldPartialKeys = parent.getPartialKeys();

    // leftChild has the new disc bit = 0, rightChild has it = 1 — by construction of the
    // leaf split that produced them. The split products inherit the original split-child's
    // path (under the old mask) and extend it with the new bit value on each side.
    final int splitChildOldPartial = oldPartialKeys[splitChildIndex];
    final int splitChildRepositioned =
        (int) Long.expand(Integer.toUnsignedLong(splitChildOldPartial), oldPartialMaskInNewLayout);

    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j] = leftChild;
        newPartialKeys[j] = splitChildRepositioned; // new bit = 0 (LEFT)
        j++;
        newChildren[j] = rightChild;
        newPartialKeys[j] = splitChildRepositioned | (1 << newBitOutputPos); // new bit = 1 (RIGHT)
        j++;
      } else {
        newChildren[j] = parent.getChildReference(i);
        // Reposition the existing partial into the new layout, then OR in the sibling's
        // verified-constant value at the new disc bit's output position.
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(oldPartialKeys[i]), oldPartialMaskInNewLayout);
        newPartialKeys[j] = repositioned | (siblingBitValues[i] << newBitOutputPos);
        j++;
      }
    }

    // 4. Sanity guard: under the sparse-path encoding the new partial keys must be unique
    //    (HOT I3). The repositioning is a bijection on the old partials, the split-child
    //    halves' partials differ at the new bit, and they differ from non-split siblings'
    //    partials because their old-bit positions were already unique. A duplicate here
    //    indicates an upstream invariant break — bail so the caller takes the split path.
    for (int i = 1; i < newNumChildren; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartialKeys[k] == newPartialKeys[i]) return null;
      }
    }

    // 5. Sort children + partials by partial-key (HOT I7 / Binna §4.2). Under sparse-path
    //    encoding the canonical slot order is sparse-partial-key order, NOT first-key order.
    //    The leaf-split halves' partials are repositionedSplitX and repositionedSplitX |
    //    (1 << newBitOutputPos); their relative position vs. other siblings' repositioned
    //    partials depends on newBitOutputPos. If newBitOutputPos is a high bit (= the new
    //    disc bit is more significant than some old disc bits), the rightChild's partial may
    //    sort AFTER an existing sibling — so co-sorting is required, not adjacent placement.
    sortChildrenAndPartialsByPartial(newChildren, newPartialKeys);

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
      // BE: byte at window-position {@code bo} occupies long bits ((7-bo)*8)..((7-bo)*8+7).
      final int byteMaskBits = (int) ((oldMask >>> ((7 - bo) * 8)) & 0xFFL);
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

    // Build extractionPositions / extractionMasks arrays in MultiMask layout.
    final byte[] extractionPositions = new byte[allCount];
    final int numChunks = (allCount + 7) / 8;
    final long[] extractionMasks = new long[numChunks];
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < allCount; i++) {
      extractionPositions[i] = (byte) allBytePositions[i];
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      // BE chunk packing: extraction-byte at chunk-offset {@code o} → long bits
      // ((7-o)*8)..((7-o)*8+7).
      extractionMasks[chunkIdx] |= ((long) (allByteMaskBits[i] & 0xFF)) << ((7 - byteOffsetInChunk) * 8);
      final int highBit = 31 - Integer.numberOfLeadingZeros(allByteMaskBits[i] & 0xFF);
      final int absBitPos = allBytePositions[i] * 8 + (7 - highBit);
      if (absBitPos < msbIndex) msbIndex = (short) absBitPos;
    }

    // Verify constancy of the new disc bit across every existing non-split sibling — see
    // {@link #addEntryWithPDep} for the multi-entry-leaf rationale. On non-constant return,
    // caller falls back to splitParentAndRecurse.
    final int[] siblingBitValues = new int[numChildren];
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int v = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (v < 0) return null;
      siblingBitValues[i] = v;
    }

    // Build expanded children and partials. With constancy verified, each existing sibling
    // contributes its constant bit value at the new disc bit's output position.
    final int newBitOutputPos = multiMaskNewBitOutputPos(extractionPositions, extractionMasks,
        allCount, newBytePos, newBitInByte);
    final long oldPartialMaskInNewLayout = (((1L << (oldCount + 1)) - 1L) ^ (1L << newBitOutputPos));
    final int[] oldPartialKeys = parent.getPartialKeys();
    final int splitChildOldPartial = oldPartialKeys[splitChildIndex];
    final int splitChildRepositioned =
        (int) Long.expand(Integer.toUnsignedLong(splitChildOldPartial), oldPartialMaskInNewLayout);

    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartials = new int[newNumChildren];
    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j] = leftChild;
        newPartials[j] = splitChildRepositioned; // new bit = 0 (LEFT)
        j++;
        newChildren[j] = rightChild;
        newPartials[j] = splitChildRepositioned | (1 << newBitOutputPos); // new bit = 1 (RIGHT)
        j++;
      } else {
        newChildren[j] = parent.getChildReference(i);
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(oldPartialKeys[i]), oldPartialMaskInNewLayout);
        newPartials[j] = repositioned | (siblingBitValues[i] << newBitOutputPos);
        j++;
      }
    }

    // Sanity guard — partial keys must be unique under sparse-path encoding (HOT I3).
    for (int i = 1; i < newNumChildren; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) return null;
      }
    }

    // Sort children + partials by partial-key (HOT I7 / Binna §4.2). See addEntryWithPDep.
    sortChildrenAndPartialsByPartial(newChildren, newPartials);

    if (newNumChildren <= 16) {
      return HOTIndirectPage.createSpanNodeMultiMask(parent.getPageKey(), revision,
          extractionPositions, extractionMasks, allCount, newPartials, newChildren,
          parent.getHeight(), msbIndex);
    }
    return HOTIndirectPage.createMultiNodeMultiMask(parent.getPageKey(), revision,
        extractionPositions, extractionMasks, allCount, newPartials, newChildren,
        parent.getHeight(), msbIndex);
  }

  /** Sum of bits set across every chunk of a MultiMask extraction-mask array. */
  private static int oldExtractionMaskTotalBits(long[] extractionMasks) {
    int total = 0;
    for (final long m : extractionMasks) total += Long.bitCount(m);
    return total;
  }

  /**
   * Compute the new bit's output position (LSB=0) in a MultiMask partial-key layout. The
   * BE-concat ordering places extractionPositions[0]'s mask bits at the highest result bits
   * (MSB-first within each byte). We walk that ordering and count bits until we reach the
   * new bit.
   */
  private static int multiMaskNewBitOutputPos(byte[] newPositions, long[] newMasks,
      int newNumBytes, int newBytePos, int newBitInByte) {
    int totalBits = 0;
    for (final long m : newMasks) totalBits += Long.bitCount(m);
    int bitsAccumulated = 0;
    for (int i = 0; i < newNumBytes; i++) {
      final int bytePos = newPositions[i] & 0xFF;
      final int byteMask = (int) ((newMasks[i / 8] >>> ((7 - (i % 8)) * 8)) & 0xFF);
      // Within each byte: iterate MSB-first (mask bit 7 = MSB-first index 0).
      for (int mfBit = 0; mfBit < 8; mfBit++) {
        final int byteBit = 7 - mfBit;
        if ((byteMask & (1 << byteBit)) == 0) continue;
        if (bytePos == newBytePos && mfBit == newBitInByte) {
          return totalBits - 1 - bitsAccumulated;
        }
        bitsAccumulated++;
      }
    }
    throw new IllegalStateException("multiMaskNewBitOutputPos: new bit not found in new layout");
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

    // 1. Verify constancy of the new disc bit across every existing non-split sibling.
    //    Required because Sirix's multi-entry leaves can hold keys differing at any bit;
    //    sparse-path subset-match routing misroutes keys destined for a sibling that spans
    //    both bit values. See addEntryWithPDep for the full rationale.
    final int[] siblingBitValues = new int[numChildren];
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) continue;
      final int v = bitConstantValueInSubtree(parent.getChildReference(i), newAbsBit);
      if (v < 0) return null;
      siblingBitValues[i] = v;
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
    // BE chunk packing: extraction-byte at chunk-offset {@code o} → long bits
    // ((7-o)*8)..((7-o)*8+7); reads use the same shift to extract a byte from a chunk.
    if (existingIdx >= 0) {
      // Merge: OR the new mask bit into the existing byte's mask.
      final int chunkIdx = existingIdx / 8;
      final int byteOffsetInChunk = existingIdx % 8;
      final int existingByte =
          (int) ((oldExtractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
      if ((existingByte & newMaskBit) != 0) return null; // duplicate
      newNumBytes = oldNumBytes;
      newExtractionPositions = oldExtractionPositions.clone();
      newExtractionMasks = oldExtractionMasks.clone();
      newExtractionMasks[chunkIdx] |= ((long) newMaskBit) << ((7 - byteOffsetInChunk) * 8);
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
          final int srcOffsetInChunk = srcIdx % 8;
          maskByte =
              (int) ((oldExtractionMasks[srcChunk] >>> ((7 - srcOffsetInChunk) * 8)) & 0xFF);
        }
        final int dstChunk = i / 8;
        final int dstOffsetInChunk = i % 8;
        newExtractionMasks[dstChunk] |= ((long) (maskByte & 0xFF)) << ((7 - dstOffsetInChunk) * 8);
      }
    }

    // 4. Compute new MSB index (smallest absolute disc bit).
    short msbIndex = Short.MAX_VALUE;
    for (int i = 0; i < newNumBytes; i++) {
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      final int maskByte =
          (int) ((newExtractionMasks[chunkIdx] >>> ((7 - byteOffsetInChunk) * 8)) & 0xFF);
      if (maskByte == 0) continue;
      final int highBit = 31 - Integer.numberOfLeadingZeros(maskByte);
      final int absBit = (newExtractionPositions[i] & 0xFF) * 8 + (7 - highBit);
      if (absBit < msbIndex) msbIndex = (short) absBit;
    }

    // 5. Build expanded children + partial keys with Binna §4.2 sparse-path encoding: split
    //    products inherit the original split-child's path (under the OLD layout) and extend
    //    it with new-bit = 0 (left) / 1 (right). Non-split siblings inherit their existing
    //    paths; their new-bit position stays 0 (the new BiNode is not on their path).
    final int oldTotalBits = oldExtractionMaskTotalBits(oldExtractionMasks);
    final int newBitOutputPos = multiMaskNewBitOutputPos(newExtractionPositions, newExtractionMasks,
        newNumBytes, newBytePos, newBitInByte);
    final long oldPartialMaskInNewLayout =
        (((1L << (oldTotalBits + 1)) - 1L) ^ (1L << newBitOutputPos));
    final int[] oldPartialKeys = parent.getPartialKeys();
    final int splitChildOldPartial = oldPartialKeys[splitChildIndex];
    final int splitChildRepositioned =
        (int) Long.expand(Integer.toUnsignedLong(splitChildOldPartial), oldPartialMaskInNewLayout);

    final int newNumChildren = numChildren + 1;
    final PageReference[] newChildren = new PageReference[newNumChildren];
    final int[] newPartials = new int[newNumChildren];
    int j = 0;
    for (int i = 0; i < numChildren; i++) {
      if (i == splitChildIndex) {
        newChildren[j] = leftChild;
        newPartials[j] = splitChildRepositioned;
        j++;
        newChildren[j] = rightChild;
        newPartials[j] = splitChildRepositioned | (1 << newBitOutputPos);
        j++;
      } else {
        newChildren[j] = parent.getChildReference(i);
        final int repositioned = (int) Long.expand(
            Integer.toUnsignedLong(oldPartialKeys[i]), oldPartialMaskInNewLayout);
        newPartials[j] = repositioned | (siblingBitValues[i] << newBitOutputPos);
        j++;
      }
    }

    // 6. Verify partial-key uniqueness (INV routing guard).
    for (int i = 1; i < newNumChildren; i++) {
      for (int k = 0; k < i; k++) {
        if (newPartials[k] == newPartials[i]) return null;
      }
    }

    // 7. Sort children + partials by partial-key (HOT I7 / Binna §4.2). See addEntryWithPDep.
    sortChildrenAndPartialsByPartial(newChildren, newPartials);

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
    // BE chunk packing: extraction-byte at chunk-offset {@code o} → long bits ((7-o)*8)..((7-o)*8+7).
    final long[] gathered = new long[numChunks];
    for (int i = 0; i < numExtractionBytes; i++) {
      final int keyBytePos = extractionPositions[i] & 0xFF;
      final int keyByte = keyBytePos < key.length ? (key[keyBytePos] & 0xFF) : 0;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      gathered[chunkIdx] |= ((long) keyByte) << ((7 - byteOffsetInChunk) * 8);
    }
    // BE concatenate across chunks: chunk 0 occupies the HIGH bits of the result.
    int totalBits = 0;
    for (final long m : extractionMasks) totalBits += Long.bitCount(m);
    int result = 0;
    int shift = totalBits;
    for (int w = 0; w < numChunks; w++) {
      final int extracted = (int) Long.compress(gathered[w], extractionMasks[w]);
      shift -= Long.bitCount(extractionMasks[w]);
      result |= extracted << shift;
    }
    return result;
  }

  /**
   * Verify that bit {@code absBitPos} has the same value across every key in the subtree
   * rooted at {@code ref}.
   *
   * <p>Returns {@code 0} or {@code 1} for the constant value, or {@code -1} if the bit
   * varies within the subtree (or the page can't be loaded). Empty subtrees return {@code 0}
   * (vacuously constant).
   *
   * <p><b>Why this exists.</b> Binna's HOT thesis uses single-TID leaves: every leaf is a
   * single key, so any disc bit is trivially constant in any leaf-rooted subtree, and
   * Binna's sparse-path encoding "off-path siblings span both bit values" case never occurs.
   * Sirix's multi-entry leaves break that property — a single leaf can hold keys differing
   * at any bit. Under multi-entry leaves, sparse-path subset-match routing misroutes keys
   * destined for an "off-path" sibling whose subtree has both values at a new disc bit
   * (the leaf-split's products, with {@code sparseStored} bit set, win the most-specific
   * subset match over the off-path sibling). The fix is to gate addEntryWithPDep /
   * addEntryMultiMask / upgradeToMultiMaskWithNewBit on this check: extension is allowed
   * only when the new bit is constant in every existing non-split sibling. Otherwise the
   * caller falls back to splitParentAndRecurse, which rebuilds the parent.
   *
   * <p><b>HFT grade.</b> Primitive {@code int} return (sentinel {@code -1} for "non-constant"
   * — no boxing, no allocation, no exception path. Recursion depth bounded by tree height
   * (≤ 7 in practice). Per-call cost: one byte fetch + bit extract per leaf entry; bounded
   * by total stored keys in the subtree.
   */
  private int bitConstantValueInSubtree(PageReference ref, int absBitPos) {
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
        if (v != first) return -1;
      }
      return first;
    }
    if (page instanceof HOTIndirectPage indirect) {
      final int m = indirect.getNumChildren();
      if (m == 0) return 0;
      final int first = bitConstantValueInSubtree(indirect.getChildReference(0), absBitPos);
      if (first < 0) return -1;
      for (int i = 1; i < m; i++) {
        final int v = bitConstantValueInSubtree(indirect.getChildReference(i), absBitPos);
        if (v < 0 || v != first) return -1;
      }
      return first;
    }
    return -1;
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

    // Sort by partial-key (HOT I7) — children are sorted by first-key above to drive
    // adjacent-pair disc-bit collection in computeDiscBits, but the canonical storage
    // order is partial-key.
    sortChildrenAndPartialsByPartial(newChildren, partialKeys);

    // Create appropriate node type based on child count
    final HOTIndirectPage created;
    if (newNumChildren <= 2) {
      // Recompute disc bit from partial-sorted children (the passed-in discriminativeBit may
      // refer to the pre-sort position or be -1 for identical boundary keys).
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
    // Capture EVERY bit where adjacent children's first-keys differ (no constancy filter on the
    // rebuild path — see the residual-I6-violation note in the file header). Adjacent-pair MSB
    // collection is sufficient for sorted children: any pair with different sparse partials must
    // differ at some adjacent-pair-captured bit (transitivity).
    //
    // <p>Some configurations of multi-entry-leaf children produce stored partials that don't
    // round-trip through PEXT-descent for every contained key (validator I6 violations). Those
    // residual irregularities are absorbed by {@link HOTTrieReader#lowerBound}'s Phase 2b
    // insertion-point fast path + Phase 3-5 walk-up, so end-to-end reads remain correct
    // (microbench full-scan misses=0). Tightening this to a true constancy filter requires
    // either dropping multi-entry leaves (giving up Sirix's storage density) or implementing
    // structural rebalancing during restructuring (significant complexity); both are out of
    // scope for this branch.
    int minBytePos = Integer.MAX_VALUE;
    int maxBytePos = 0;
    final TreeMap<Integer, Integer> maskByBytePos = new TreeMap<>();

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
          diff &= ~(1 << (7 - hb));
          if (isDiscBitAlreadyCaptured(maskByBytePos, b, hb)) continue;
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

    if (maxBytePos - initialBytePos < 8 && minBytePos >= initialBytePos) {
      long mask = 0;
      for (final var entry : maskByBytePos.entrySet()) {
        // BE word layout: byte at window-position p occupies long-bits ((7-p)*8 .. (7-p)*8+7).
        final int byteOffset = entry.getKey() - initialBytePos;
        final int maskByte = entry.getValue();
        mask |= ((long) (maskByte & 0xFF)) << ((7 - byteOffset) * 8);
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

  // Non-adjacent-pair augment was investigated for I6-violation reduction on multi-entry-leaf
  // workloads (microbench at 500K reports 159 residual violations, all absorbed by
  // HOTTrieReader's lower_bound walk-up). The augment+filter combination needs constancy-
  // respecting bits between every colliding pair to terminate cleanly; adversarial workloads
  // hit cases where no such bit exists, and the augment bails leaving partial-key duplicates
  // that are MUCH worse (475K I3 violations and 87% read miss in the same microbench). The
  // current adjacent-pair-no-filter approach minimises violations end-to-end.

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
      // BE chunk layout: extraction-byte offset {@code o} within a chunk lands at long-bit
      // {@code (7-o)*8 .. (7-o)*8+7}. Within {@code maskByte}, bit 7 = byte's MSB.
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      extractionMasks[chunkIdx] |= ((long) (maskByte & 0xFF)) << ((7 - byteOffsetInChunk) * 8);

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
  /**
   * Compute partial keys for {@code children} using Binna §4.2 sparse-path encoding.
   *
   * <p>For each disc bit (in absolute-bit-position order, MSB-first), we recursively partition
   * the sorted children at this bit into LEFT (bit=0) and RIGHT (bit=1) halves. A child's
   * partial-key bit at position {@code p} (the partial-key bit corresponding to this disc bit)
   * is set IFF (a) the partition at this bit is non-trivial within the child's current
   * sub-range — i.e., the BiNode at this disc bit is on the child's path — AND (b) the child
   * lies in the RIGHT half. Off-path bits stay 0.</p>
   *
   * <p>This matches Binna's reference {@code SparsePartialKeys.hpp} where stored partial
   * keys carry "1" only for path BiNodes and 0 elsewhere. Routing (Sirix's
   * {@link io.sirix.page.HOTIndirectPage#findChildIndex} subset-match) returns the highest-
   * specificity match, exactly as Binna's {@code SparsePartialKeys::search}.</p>
   */
  private int[] computePartialKeysForChildren(PageReference[] children, DiscBitsInfo discBits) {
    final int n = children.length;
    final int[] partialKeys = new int[n];
    if (n <= 1) return partialKeys;

    // Collect the disc bit positions in MSB-first absolute-bit order. Result-bit position
    // (totalDiscBits-1) corresponds to the most significant disc bit (smallest absolute
    // bit index).
    final int[] discAbsPositions = collectDiscBitsMsbFirst(discBits);
    final int totalDiscBits = discAbsPositions.length;
    if (totalDiscBits == 0) return partialKeys;

    // Cache children's first-keys so we don't re-load on every disc-bit step.
    final byte[][] firstKeys = new byte[n][];
    for (int i = 0; i < n; i++) firstKeys[i] = getFirstKeyFromChild(children[i]);

    computeSparsePathRecursive(firstKeys, partialKeys, /*rangeStart=*/ 0, /*rangeEnd=*/ n,
        /*discBitIdx=*/ 0, discAbsPositions, totalDiscBits);
    return partialKeys;
  }

  /**
   * Recursive partition for sparse-path encoding. At {@code discBitIdx}, find the split point
   * within {@code [rangeStart, rangeEnd)}. If the partition is non-trivial, set the
   * corresponding partial-key bit on every RIGHT-half child and recurse into both halves; the
   * LEFT half implicitly leaves the bit at 0. If the partition is trivial (all children in
   * range agree at this bit), the BiNode is NOT on this range's children's paths — leave the
   * bit at 0 for everyone in range and recurse with the next disc bit.
   */
  private void computeSparsePathRecursive(byte[][] firstKeys, int[] partials,
      int rangeStart, int rangeEnd, int discBitIdx, int[] discAbsPositions, int totalDiscBits) {
    if (rangeEnd - rangeStart <= 1 || discBitIdx >= totalDiscBits) return;
    final int absBit = discAbsPositions[discBitIdx];
    // BE: result bit (totalDiscBits-1) = MSB of partial = first (most-significant) disc bit.
    final int partialBitPos = totalDiscBits - 1 - discBitIdx;

    int splitIdx = rangeEnd;
    for (int j = rangeStart; j < rangeEnd; j++) {
      if (DiscriminativeBitComputer.isBitSet(firstKeys[j], absBit)) {
        splitIdx = j;
        break;
      }
    }

    if (splitIdx > rangeStart && splitIdx < rangeEnd) {
      // Non-trivial split: BiNode is on every range-child's path.
      for (int j = splitIdx; j < rangeEnd; j++) partials[j] |= (1 << partialBitPos);
      computeSparsePathRecursive(firstKeys, partials, rangeStart, splitIdx,
          discBitIdx + 1, discAbsPositions, totalDiscBits);
      computeSparsePathRecursive(firstKeys, partials, splitIdx, rangeEnd,
          discBitIdx + 1, discAbsPositions, totalDiscBits);
    } else {
      // Trivial split (all agree): BiNode is NOT on this range's children's paths. Skip.
      computeSparsePathRecursive(firstKeys, partials, rangeStart, rangeEnd,
          discBitIdx + 1, discAbsPositions, totalDiscBits);
    }
  }

  /**
   * Maximum disc bits per node — bounded by partial-key bit width (uint32_t = 32 bits in
   * Sirix's SpanNode/MultiNode partial). Used to size the stack-cap'd disc-bit position
   * buffer in {@link #collectDiscBitsMsbFirst}.
   */
  private static final int MAX_DISC_BITS = 32;

  /**
   * Collect disc bit positions in MSB-first absolute-bit order: smallest absolute bit
   * position first (= most significant). Walks SingleMask or MultiMask layout.
   *
   * <p>HFT-grade: writes into a primitive {@code int[]} sized to MultiMask theoretical max
   * (8 extraction bytes × 8 bits = 64). MAX_DISC_BITS = 32 is the partial-key invariant
   * (HOT I3 — partials must be unique 32-bit values), but collection-time tooling can
   * temporarily see >32 bits in transient states (e.g., before {@link #augmentUntilPartialsUnique}
   * trims). The writer downstream treats >32 bits as an INV violation and bails to the
   * split path; this method must not throw before that bail can trigger. Returns a trimmed
   * copy. No {@code List<Integer>}, no autoboxing, no growth, single bounded allocation.
   */
  private static int[] collectDiscBitsMsbFirst(DiscBitsInfo discBits) {
    // Sized to the theoretical max (8 extraction bytes × 8 bits/byte) so transient
    // out-of-spec disc-bit counts don't AIOOBE before the downstream INV check fires.
    final int[] buf = new int[64];
    int n = 0;
    if (discBits.isSingleMask()) {
      final int initialBytePos = discBits.initialBytePos();
      long mask = discBits.bitMask();
      while (mask != 0L && n < buf.length) {
        final int highBit = 63 - Long.numberOfLeadingZeros(mask);
        // BE: byte at window-offset bo occupies long bits ((7-bo)*8)..((7-bo)*8+7).
        // Within byte, MSB-first bit bb is at long-bit ((7-bo)*8 + (7-bb)).
        // Solve: highBit = (7-bo)*8 + (7-bb) → bo = 7 - highBit/8, bb = 7 - highBit%8.
        final int bo = 7 - (highBit / 8);
        final int bb = 7 - (highBit % 8);
        buf[n++] = (initialBytePos + bo) * 8 + bb;
        mask &= ~(1L << highBit);
      }
    } else {
      // MultiMask: iterate extractionPositions in order; within each byte, MSB-first.
      final byte[] extractionPositions = discBits.extractionPositions();
      final long[] extractionMasks = discBits.extractionMasks();
      final int numExtractionBytes = discBits.numExtractionBytes();
      for (int i = 0; i < numExtractionBytes && n < buf.length; i++) {
        final int bytePos = extractionPositions[i] & 0xFF;
        final int byteMask = (int) ((extractionMasks[i / 8] >>> ((7 - (i % 8)) * 8)) & 0xFF);
        for (int bb = 0; bb < 8 && n < buf.length; bb++) {
          final int byteBit = 7 - bb;
          if ((byteMask & (1 << byteBit)) == 0) continue;
          buf[n++] = bytePos * 8 + bb;
        }
      }
    }
    return n == buf.length ? buf : Arrays.copyOf(buf, n);
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
    // BE chunk packing: extraction-byte at chunk-offset {@code o} → long bits ((7-o)*8)..((7-o)*8+7).
    final long[] gathered = new long[numChunks];
    for (int i = 0; i < numExtractionBytes; i++) {
      final int keyBytePos = extractionPositions[i] & 0xFF;
      final int keyByte = keyBytePos < key.length ? (key[keyBytePos] & 0xFF) : 0;
      final int chunkIdx = i / 8;
      final int byteOffsetInChunk = i % 8;
      gathered[chunkIdx] |= ((long) keyByte) << ((7 - byteOffsetInChunk) * 8);
    }
    // BE concatenate across chunks: chunk 0 occupies the HIGH bits of the result.
    int totalBits = 0;
    for (final long m : extractionMasks) totalBits += Long.bitCount(m);
    int result = 0;
    int shift = totalBits;
    for (int w = 0; w < numChunks; w++) {
      final int extracted = (int) Long.compress(gathered[w], extractionMasks[w]);
      shift -= Long.bitCount(extractionMasks[w]);
      result |= extracted << shift;
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
      // BE: byte at window-offset {@code byteOff} occupies long bits ((7-byteOff)*8)..((7-byteOff)*8+7);
      // MSB-first bit-in-byte {@code bitInByte} is at long-bit ((7-byteOff)*8 + (7-bitInByte)).
      splitBitMaskBit = 1L << ((7 - byteOff) * 8 + (7 - bitInByte));
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
        // Sparse-path encoding (Binna §4.2): non-split sibling's path does NOT include the
        // split-disc-bit's BiNode, so its new-bit position stays 0 regardless of how the
        // bit varies within its subtree. Constancy check removed.
        newPartials[j] = repositioned;
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

    // Sort by partial-key (HOT I7 / Binna §4.2). The sortedChildren array was sorted by
    // first-key earlier to drive the relevant-bits derivation; the canonical storage order
    // under sparse-path encoding is partial-key.
    sortChildrenAndPartialsByPartial(sortedChildren, newPartials);

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
   * Create an indirect node from a list of children built from scratch (no parent context).
   *
   * <p><b>Encoding</b>: this path computes partial keys via dense PEXT of each child's first
   * key under the derived disc-bit mask, NOT Binna's sparse-path encoding. The two encodings
   * coincide whenever the constancy invariant holds (every captured disc bit has the same
   * value across all leaves in each child's subtree). For workloads where constancy holds —
   * which empirically covers all currently-tested non-adversarial paths through this method
   * — the dense-PEXT result is valid HOT and indistinguishable from sparse-path.</p>
   *
   * <p><b>Limitation</b>: under truly adversarial workloads where constancy can't be achieved
   * AND the rebuild path enters this method (rare; reached only by buildCompressedHalf
   * fallbacks for MultiMask layouts and split-cascade halves), the produced node may carry
   * dense-PEXT partials at positions where sparse-path encoding would store 0. This is
   * detected by {@link io.sirix.index.hot.HOTInvariantValidator}'s I-Binna check; on tested
   * workloads no violation surfaces. A full sparse-path rewrite would require deriving each
   * child's path through the would-be virtual binary patricia trie and zeroing non-path bits
   * — left as follow-up work.</p>
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
      // HOT I7: store children in sparse-partial-key order (children were sorted by first-key
      // above to drive adjacent-pair disc-bit collection; partial-key order is the canonical
      // slot order under sparse-path encoding per Binna §4.2).
      sortChildrenAndPartialsByPartial(children, partialKeys);
      created = createNodeWithDiscBits(pageKey, revision, height, discBits, partialKeys, children);
    }
    return created;
  }

  /**
   * Compute partial key (SingleMask) by extracting discriminative bits from a key.
   * Uses BE layout matching {@link HOTIndirectPage#getKeyWordAt}: byte at {@code initialBytePos}
   * → long bits 56-63, {@code initialBytePos+1} → 48-55, ..., {@code initialBytePos+7} → 0-7.
   */
  private int computePartialKeySingleMask(byte[] key, int initialBytePos, long bitMask) {
    if (key == null || key.length == 0) {
      return 0;
    }

    // BE word: byte at offset i in window lands in long bits ((7-i)*8)..((7-i)*8+7).
    long keyWord = 0;
    for (int i = 0; i < 8 && (initialBytePos + i) < key.length; i++) {
      keyWord |= ((long) (key[initialBytePos + i] & 0xFF)) << ((7 - i) * 8);
    }

    // Apply PEXT-style extraction: compress bits selected by mask.
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
   * Co-sort {@code children} and {@code partials} by ascending partial-key value (HOT I7
   * invariant: children are stored in sparse-partial-key order, per Binna §4.2 and the
   * reference {@code SparsePartialKeys}). Since under sparse-path encoding the partial-key
   * order is the canonical ordering — first-key order can diverge for sibling subtrees with
   * off-path range overlap — every indirect-node construction site uses this helper.
   *
   * <p>HFT-grade: in-place insertion sort over the parallel arrays. Children's count is
   * bounded by {@link HOTIndirectPage#MAX_NODE_ENTRIES} = 32, so insertion sort is faster
   * than {@link Arrays#sort} (no comparator allocation, better cache behavior).
   */
  private static void sortChildrenAndPartialsByPartial(PageReference[] children, int[] partials) {
    final int n = children.length;
    if (n <= 1) return;
    if (partials.length < n) {
      throw new IllegalArgumentException(
          "partials.length=" + partials.length + " < children.length=" + n);
    }
    // Insertion sort — partials are unique (HOT I3) so equal pivots can't occur.
    for (int i = 1; i < n; i++) {
      final int curPartial = partials[i];
      final PageReference curChild = children[i];
      int j = i - 1;
      while (j >= 0 && Integer.compareUnsigned(partials[j], curPartial) > 0) {
        partials[j + 1] = partials[j];
        children[j + 1] = children[j];
        j--;
      }
      partials[j + 1] = curPartial;
      children[j + 1] = curChild;
    }
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

