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
import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Objects;

/**
 * HOT trie reader for HOT (Height Optimized Trie) navigation.
 * 
 * <p>
 * This class provides read-only access to HOT indexes with proper guard management to prevent page
 * eviction during active use.
 * </p>
 * 
 * <p>
 * <b>Key Features:</b>
 * </p>
 * <ul>
 * <li>Guard acquisition for page lifetime management</li>
 * <li>Zero-copy value access via MemorySegment slices</li>
 * <li>SIMD-optimized child lookup via HOTIndirectPage</li>
 * <li>Pre-allocated traversal arrays for zero allocations</li>
 * </ul>
 * 
 * <p>
 * <b>Usage:</b>
 * </p>
 * 
 * <pre>{@code
 * try (HOTTrieReader reader = new HOTTrieReader(storageEngineReader)) {
 *   MemorySegment value = reader.get(rootRef, key);
 *   if (value != null) {
 *     // Use value...
 *   }
 * }
 * }</pre>
 * 
 * @author Johannes Lichtenberger
 * @see HOTLeafPage
 * @see HOTIndirectPage
 * @see HOTTrieWriter
 */
public final class HOTTrieReader implements AutoCloseable {

  /**
   * Maximum tree height for pre-allocated path traversal arrays.
   *
   * <p>With minimum fanout of 2 (BiNode): max height = log2(2^63) ~ 63.
   * With typical fanout of 16+ (SpanNode/MultiNode): height ~ 13.
   * We use 64 as a generous safety margin. Exceeding this limit indicates
   * a bug in split/merge logic or index corruption.</p>
   */
  private static final int MAX_TREE_HEIGHT = 64;

  /** The storage engine reader. */
  private final StorageEngineReader storageEngineReader;

  // ===== Pre-allocated traversal path - ZERO allocations on hot path! =====
  private final PageReference[] pathRefs = new PageReference[MAX_TREE_HEIGHT];
  private final HOTIndirectPage[] pathNodes = new HOTIndirectPage[MAX_TREE_HEIGHT];
  private final int[] pathChildIndices = new int[MAX_TREE_HEIGHT];
  private int pathDepth = 0;

  // ===== Currently guarded leaf page =====
  private HOTLeafPage guardedLeaf = null;

  /**
   * Create a new HOTTrieReader.
   *
   * @param storageEngineReader the storage engine reader
   */
  public HOTTrieReader(@NonNull StorageEngineReader storageEngineReader) {
    this.storageEngineReader = Objects.requireNonNull(storageEngineReader);
  }

  /**
   * Find value for exact key match. Returns null if not found - no Optional allocation!
   *
   * @param rootRef the root page reference
   * @param key the search key
   * @return the value as a MemorySegment slice, or null if not found
   */
  public @Nullable MemorySegment get(@NonNull PageReference rootRef, byte[] key) {
    Objects.requireNonNull(rootRef);
    Objects.requireNonNull(key);

    // Release any previously guarded leaf
    releaseGuardedLeaf();

    // Navigate to leaf
    HOTLeafPage leaf = navigateToLeaf(rootRef, key);
    if (leaf == null) {
      return null;
    }

    // Acquire guard to prevent eviction
    leaf.acquireGuard();
    guardedLeaf = leaf;

    try {
      // Find entry in leaf
      int index = leaf.findEntry(key);
      if (index < 0) {
        return null; // Not found (guard intentionally held for next call)
      }

      return leaf.getValueSlice(index);
    } catch (Exception e) {
      // On exception, release the guard to avoid leaking it
      releaseGuardedLeaf();
      throw e;
    }
  }

  /**
   * Check if a key exists in the trie.
   *
   * @param rootRef the root page reference
   * @param key the search key
   * @return true if key exists
   */
  public boolean containsKey(@NonNull PageReference rootRef, byte[] key) {
    Objects.requireNonNull(rootRef);
    Objects.requireNonNull(key);

    // Release any previously guarded leaf
    releaseGuardedLeaf();

    HOTLeafPage leaf = navigateToLeaf(rootRef, key);
    if (leaf == null) {
      return false;
    }

    // Acquire guard temporarily
    leaf.acquireGuard();
    try {
      int index = leaf.findEntry(key);
      return index >= 0;
    } finally {
      leaf.releaseGuard();
    }
  }

  /**
   * Create a range cursor for iterating over a key range.
   *
   * @param rootRef the root page reference
   * @param fromKey the start key (inclusive)
   * @param toKey the end key (inclusive)
   * @return the range cursor
   */
  public HOTRangeCursor range(@NonNull PageReference rootRef, byte[] fromKey, byte[] toKey) {
    return new HOTRangeCursor(this, rootRef, fromKey, toKey);
  }

  /**
   * Navigate through HOT trie to reach the leaf containing the key. Uses pre-allocated path arrays -
   * ZERO allocations!
   * 
   * <p>
   * <b>Prefetching Strategy (Reference: thesis section 4.3.4):</b>
   * </p>
   * <p>
   * For optimal performance on modern CPUs with deep memory hierarchies:
   * </p>
   * <ul>
   * <li>Prefetch child's first cache line before navigating (hide memory latency)</li>
   * <li>HOT's compound nodes reduce tree height → fewer prefetch opportunities needed</li>
   * <li>Java's MemorySegment.prefetch() can be used with off-heap pages (JDK 21+)</li>
   * </ul>
   *
   * @param rootRef the root reference
   * @param key the search key
   * @return the leaf page, or null if not found
   */
  public @Nullable HOTLeafPage navigateToLeaf(@NonNull PageReference rootRef, byte[] key) {
    pathDepth = 0;
    PageReference currentRef = rootRef;

    while (true) {
      Page page = loadPage(currentRef);
      if (page == null) {
        return null; // Empty trie
      }

      if (page instanceof HOTLeafPage leaf) {
        return leaf;
      }

      if (!(page instanceof HOTIndirectPage hotNode)) {
        return null; // Unexpected page type
      }

      // Find child reference using HOT node type-specific logic (uses PEXT/Long.compress)
      final int childIndex = hotNode.findChildIndex(key);
      if (childIndex < 0) {
        return null; // Key not found
      }

      final PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }

      // Async SSD prefetch: fire-and-forget load of the next sibling's page on a virtual thread.
      // Overlaps SSD I/O with the CPU work of descending into the current subtree.
      final int nextSibling = childIndex + 1;
      if (nextSibling < hotNode.getNumChildren()) {
        final PageReference siblingRef = hotNode.getChildReference(nextSibling);
        if (siblingRef != null && siblingRef.getPage() == null && siblingRef.getKey() >= 0) {
          prefetchPage(siblingRef);
        }
      }

      // Record path for parent-based range traversal
      pushPath(currentRef, hotNode, childIndex);

      currentRef = childRef;
    }
  }

  /**
   * Navigate to the leftmost leaf in the subtree. Used for range scan initialization.
   *
   * @param rootRef the root reference
   * @return the leftmost leaf, or null if empty
   */
  public @Nullable HOTLeafPage navigateToLeftmostLeaf(@NonNull PageReference rootRef) {
    pathDepth = 0;
    PageReference currentRef = rootRef;

    while (true) {
      Page page = loadPage(currentRef);
      if (page == null) {
        return null;
      }

      if (page instanceof HOTLeafPage leaf) {
        return leaf;
      }

      if (!(page instanceof HOTIndirectPage hotNode)) {
        return null;
      }

      // Take the first (leftmost) child
      final int childIndex = 0;
      final PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }

      // Async SSD prefetch: fire-and-forget load of second child on a virtual thread.
      if (hotNode.getNumChildren() > 1) {
        final PageReference siblingRef = hotNode.getChildReference(1);
        if (siblingRef != null && siblingRef.getPage() == null && siblingRef.getKey() >= 0) {
          prefetchPage(siblingRef);
        }
      }

      pushPath(currentRef, hotNode, childIndex);
      currentRef = childRef;
    }
  }

  /**
   * Advance to the next leaf in sorted order using parent-based traversal. This is the COW-compatible
   * alternative to sibling pointers.
   *
   * @return the next leaf, or null if no more leaves
   */
  public @Nullable HOTLeafPage advanceToNextLeaf() {
    // Pop back up the tree until we find an unvisited sibling
    while (pathDepth > 0) {
      int parentIdx = pathDepth - 1;
      HOTIndirectPage parent = pathNodes[parentIdx];
      int currentChildIdx = pathChildIndices[parentIdx];
      int numChildren = parent.getNumChildren();

      // Check if there's a next sibling
      if (currentChildIdx + 1 < numChildren) {
        // Found next sibling - descend to its leftmost leaf
        final int nextChildIdx = currentChildIdx + 1;
        pathChildIndices[parentIdx] = nextChildIdx;

        final PageReference nextChildRef = parent.getChildReference(nextChildIdx);
        if (nextChildRef != null) {
          // Async SSD prefetch: fire-and-forget load of next-next sibling on a virtual thread.
          final int prefetchIdx = nextChildIdx + 1;
          if (prefetchIdx < numChildren) {
            final PageReference prefetchRef = parent.getChildReference(prefetchIdx);
            if (prefetchRef != null && prefetchRef.getPage() == null && prefetchRef.getKey() >= 0) {
              prefetchPage(prefetchRef);
            }
          }

          final HOTLeafPage result = descendToLeftmostLeaf(nextChildRef);
          if (result != null) {
            return result;
          }
          // If descend failed, continue to next sibling or pop up
        }
      }

      // No more siblings at this level, pop up
      pathDepth--;
    }

    // Exhausted the tree
    return null;
  }

  /**
   * Descend to the leftmost leaf from a given reference. Prefetches the next sibling
   * at each level for range scan readahead.
   */
  private @Nullable HOTLeafPage descendToLeftmostLeaf(@NonNull PageReference ref) {
    PageReference currentRef = ref;

    while (true) {
      final Page page = loadPage(currentRef);
      if (page == null) {
        return null;
      }

      if (page instanceof HOTLeafPage leaf) {
        return leaf;
      }

      if (!(page instanceof HOTIndirectPage hotNode)) {
        return null;
      }

      final int childIndex = 0;
      final PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }

      // Async SSD prefetch: fire-and-forget load of next sibling on a virtual thread
      if (hotNode.getNumChildren() > 1) {
        final PageReference siblingRef = hotNode.getChildReference(1);
        if (siblingRef != null && siblingRef.getPage() == null && siblingRef.getKey() >= 0) {
          prefetchPage(siblingRef);
        }
      }

      pushPath(currentRef, hotNode, childIndex);
      currentRef = childRef;
    }
  }

  /**
   * Flyweight push for traversal path - no allocation!
   */
  private void pushPath(PageReference ref, HOTIndirectPage node, int childIdx) {
    if (pathDepth >= MAX_TREE_HEIGHT) {
      throw new IllegalStateException("HOT tree exceeds maximum height: " + MAX_TREE_HEIGHT);
    }
    pathRefs[pathDepth] = ref;
    pathNodes[pathDepth] = node;
    pathChildIndices[pathDepth] = childIdx;
    pathDepth++;
  }

  /**
   * Clear traversal path (allows GC but no allocation).
   */
  void clearPath() {
    for (int i = 0; i < pathDepth; i++) {
      pathRefs[i] = null;
      pathNodes[i] = null;
    }
    pathDepth = 0;
  }

  /**
   * Get the current traversal path depth.
   */
  public int getPathDepth() {
    return pathDepth;
  }

  /**
   * Load a page from storage. Checks the page reference's in-memory page first (swizzle check),
   * then falls back to storage. After loading from storage, the page is swizzled onto the
   * reference so subsequent accesses avoid SSD I/O entirely.
   *
   * <p><b>SSD optimization:</b> Swizzling loaded pages onto their PageReference eliminates
   * redundant I/O for repeated traversals through the same internal nodes. Since HOT trees
   * have low height (typically 3-5 levels with compound nodes), keeping all internal nodes
   * swizzled is memory-efficient and avoids the dominant cost of random SSD reads.</p>
   */
  private @Nullable Page loadPage(@NonNull PageReference ref) {
    // First check if page is already swizzled (in-memory from transaction log or prior load)
    final Page inMemory = ref.getPage();
    if (inMemory != null) {
      return inMemory;
    }

    // CRITICAL: Check BOTH storage key AND log key before giving up.
    // When a page is in the transaction log, key is set to NULL_ID_LONG (-1)
    // but logKey is set to the index in the log. We must call loadHOTPage
    // which checks the transaction log using the logKey.
    if (ref.getKey() < 0 && ref.getLogKey() < 0) {
      return null; // Page not in storage AND not in transaction log
    }

    // Load from storage via the storage engine reader
    // The storage engine will handle versioning/fragment combining AND transaction log lookup
    final Page loaded = storageEngineReader.loadHOTPage(ref);

    // Swizzle: pin the loaded page on the reference so future accesses skip I/O.
    // This is safe because PageReference.setPage is idempotent and HOT pages are
    // immutable once loaded (COW creates new pages for modifications).
    if (loaded != null) {
      ref.setPage(loaded);
    }

    return loaded;
  }

  /**
   * Asynchronously prefetch a page into swizzled state.
   *
   * <p><b>SSD optimization:</b> Fires the I/O on a virtual thread so the calling traversal
   * can continue descending without blocking on sibling I/O. When the virtual thread
   * completes, the result is swizzled onto the PageReference (volatile field). If the
   * traversal later reaches this reference, {@link #loadPage} finds it already swizzled
   * and avoids a redundant read.</p>
   *
   * <p>Race safety: if {@code loadPage} is called before the async task finishes,
   * it will not find a swizzled page and will load synchronously. The duplicate load
   * is benign — both paths produce the same immutable page and {@code setPage} on
   * the volatile field is idempotent.</p>
   */
  private void prefetchPage(@NonNull PageReference ref) {
    Thread.startVirtualThread(() -> {
      final Page loaded = storageEngineReader.loadHOTPage(ref);
      if (loaded != null) {
        ref.setPage(loaded);
      }
    });
  }

  /**
   * Release the currently guarded leaf page.
   */
  private void releaseGuardedLeaf() {
    if (guardedLeaf != null) {
      guardedLeaf.releaseGuard();
      guardedLeaf = null;
    }
  }

  /**
   * Get the storage engine reader.
   */
  StorageEngineReader getStorageEngineReader() {
    return storageEngineReader;
  }

  @Override
  public void close() {
    releaseGuardedLeaf();
    clearPath();
  }
}

