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
 * <p>This class provides read-only access to HOT indexes with proper guard management
 * to prevent page eviction during active use.</p>
 * 
 * <p><b>Key Features:</b></p>
 * <ul>
 *   <li>Guard acquisition for page lifetime management</li>
 *   <li>Zero-copy value access via MemorySegment slices</li>
 *   <li>SIMD-optimized child lookup via HOTIndirectPage</li>
 *   <li>Pre-allocated traversal arrays for zero allocations</li>
 * </ul>
 * 
 * <p><b>Usage:</b></p>
 * <pre>{@code
 * try (HOTTrieReader reader = new HOTTrieReader(pageRtx)) {
 *     MemorySegment value = reader.get(rootRef, key);
 *     if (value != null) {
 *         // Use value...
 *     }
 * }
 * }</pre>
 * 
 * @author Johannes Lichtenberger
 * @see HOTLeafPage
 * @see HOTIndirectPage
 * @see HOTTrieWriter
 */
public final class HOTTrieReader implements AutoCloseable {

  /** Maximum tree height. */
  private static final int MAX_TREE_HEIGHT = 8;
  
  /** The storage engine reader. */
  private final StorageEngineReader pageRtx;
  
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
   * @param pageRtx the storage engine reader
   */
  public HOTTrieReader(@NonNull StorageEngineReader pageRtx) {
    this.pageRtx = Objects.requireNonNull(pageRtx);
  }
  
  /**
   * Find value for exact key match.
   * Returns null if not found - no Optional allocation!
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
    
    // Find entry in leaf
    int index = leaf.findEntry(key);
    if (index < 0) {
      return null; // Not found
    }
    
    return leaf.getValueSlice(index);
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
   * Navigate through HOT trie to reach the leaf containing the key.
   * Uses pre-allocated path arrays - ZERO allocations!
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
      
      // Find child reference using HOT node type-specific logic
      int childIndex = hotNode.findChildIndex(key);
      if (childIndex < 0) {
        return null; // Key not found
      }
      
      PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }
      
      // Record path for parent-based range traversal
      pushPath(currentRef, hotNode, childIndex);
      
      currentRef = childRef;
    }
  }
  
  /**
   * Navigate to the leftmost leaf in the subtree.
   * Used for range scan initialization.
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
      int childIndex = 0;
      PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
      }
      
      pushPath(currentRef, hotNode, childIndex);
      currentRef = childRef;
    }
  }
  
  /**
   * Advance to the next leaf in sorted order using parent-based traversal.
   * This is the COW-compatible alternative to sibling pointers.
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
        int nextChildIdx = currentChildIdx + 1;
        pathChildIndices[parentIdx] = nextChildIdx;
        
        PageReference nextChildRef = parent.getChildReference(nextChildIdx);
        if (nextChildRef != null) {
          return descendToLeftmostLeaf(nextChildRef);
        }
      }
      
      // No more siblings at this level, pop up
      pathDepth--;
    }
    
    // Exhausted the tree
    return null;
  }
  
  /**
   * Descend to the leftmost leaf from a given reference.
   */
  private @Nullable HOTLeafPage descendToLeftmostLeaf(@NonNull PageReference ref) {
    PageReference currentRef = ref;
    
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
      
      int childIndex = 0;
      PageReference childRef = hotNode.getChildReference(childIndex);
      if (childRef == null) {
        return null;
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
  int getPathDepth() {
    return pathDepth;
  }
  
  /**
   * Load a page from storage.
   * Checks the page reference's in-memory page first, then falls back to storage.
   */
  private @Nullable Page loadPage(@NonNull PageReference ref) {
    // First check if page is already in memory (from transaction log or cache)
    Page inMemory = ref.getPage();
    if (inMemory != null) {
      return inMemory;
    }
    
    if (ref.getKey() < 0) {
      return null;
    }
    
    // Load from storage via the storage engine reader
    // The storage engine will handle versioning/fragment combining
    return pageRtx.loadHOTPage(ref);
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
  StorageEngineReader getPageRtx() {
    return pageRtx;
  }
  
  @Override
  public void close() {
    releaseGuardedLeaf();
    clearPath();
  }
}

