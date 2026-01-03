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

package io.sirix.page;

import io.sirix.index.hot.SparsePartialKeys;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * HOT (Height Optimized Trie) indirect page for cache-friendly secondary indexes.
 * 
 * <p>Implements compound nodes that span multiple logical trie levels within a single page.
 * Uses discriminative bits and SIMD-optimized child lookup.</p>
 * 
 * <p><b>Node Layout Types (from HOT dissertation):</b></p>
 * <ul>
 *   <li>SINGLE_MASK: 9 bytes (initialBytePos + 64-bit mask), uses PEXT instruction</li>
 *   <li>MULTI_MASK: Variable size, for bits spread across multiple bytes</li>
 *   <li>POSITION_SEQUENCE: Variable size, explicit bit positions</li>
 * </ul>
 * 
 * <p><b>Node Types:</b></p>
 * <ul>
 *   <li>BiNode: 2 children, 1 discriminative bit</li>
 *   <li>SpanNode: 2-16 children, SIMD-searchable partial keys</li>
 *   <li>MultiNode: 17-32 children, direct byte indexing</li>
 * </ul>
 * 
 * <p><b>Cache-Line Alignment (Reference: thesis section 4.3.2):</b></p>
 * <p>For optimal cache performance on modern CPUs:</p>
 * <ul>
 *   <li>Hot data (bitMask, partialKeys) should be aligned to 64-byte cache lines</li>
 *   <li>BiNode fits in a single cache line (2 children + mask = ~24 bytes)</li>
 *   <li>SpanNode uses up to 2 cache lines (16 children + mask + keys)</li>
 *   <li>Child references are accessed only after lookup, can be in separate lines</li>
 * </ul>
 * 
 * <p><b>SIMD Optimization (Reference: thesis section 4.3.3):</b></p>
 * <p>SpanNode uses Long.compress() which maps to PEXT (Parallel Bit Extract)
 * instruction on x86-64 with BMI2. Linear search of 2-16 partial keys is typically
 * faster than binary search due to better branch prediction and cache locality.</p>
 * 
 * @author Johannes Lichtenberger
 * @see HOTLeafPage
 * @see Page
 * @see <a href="https://github.com/speedskater/hot">Reference Implementation</a>
 */
public final class HOTIndirectPage implements Page {

  /** Sentinel for "not found" in child lookup. */
  public static final int NOT_FOUND = -1;

  /** Cache line size on modern x86-64 CPUs (64 bytes). */
  public static final int CACHE_LINE_SIZE = 64;

  /** Maximum children for a node (from reference: MAXIMUM_NUMBER_NODE_ENTRIES = 32). */
  public static final int MAX_NODE_ENTRIES = 32;

  /**
   * Node type enumeration.
   */
  public enum NodeType {
    /** 2 children, 1 discriminative bit. */
    BI_NODE,
    /** 2-16 children, SIMD-searchable partial keys. */
    SPAN_NODE,
    /** 17-256 children, direct byte indexing. */
    MULTI_NODE
  }

  /**
   * Layout type for discriminative bit storage.
   */
  public enum LayoutType {
    /** Single 64-bit mask with initial byte position. */
    SINGLE_MASK,
    /** Multiple 8-bit masks per byte. */
    MULTI_MASK,
    /** Explicit list of bit positions. */
    POSITION_SEQUENCE
  }

  // ===== Page identity =====
  private final long pageKey;
  private final int revision;
  private final int height; // Distance from leaves
  
  // ===== Node structure =====
  private final NodeType nodeType;
  private LayoutType layoutType; // Set in factory methods
  private int numChildren;
  
  // ===== Discriminative bits (layout-dependent) =====
  // SINGLE_MASK layout
  private byte initialBytePos;
  private long bitMask;
  
  // MULTI_MASK layout
  private byte[] maskBytePosArray;
  private byte[] maskArray;
  
  // POSITION_SEQUENCE layout
  private short[] bitPositions;
  
  // ===== Child data =====
  private byte[] partialKeys; // Up to 256 partial keys (raw storage)
  private @Nullable SparsePartialKeys<Byte> sparsePartialKeys; // SIMD-accelerated search
  private final PageReference[] childReferences; // References to child pages
  
  // ===== MultiNode direct index (256 bytes) =====
  private byte[] childIndex; // Maps byte value -> child slot
  
  /**
   * Create a new BiNode with 2 children.
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param discriminativeBitPos the bit position that distinguishes the two children
   * @param leftChild reference to left child (bit=0)
   * @param rightChild reference to right child (bit=1)
   * @return new BiNode
   */
  public static HOTIndirectPage createBiNode(long pageKey, int revision,
                                             int discriminativeBitPos,
                                             @NonNull PageReference leftChild,
                                             @NonNull PageReference rightChild) {
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, 0, NodeType.BI_NODE, 2);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = (byte) (discriminativeBitPos / 8);
    
    // Compute bit mask for PEXT extraction
    // getKeyWordAt uses little-endian: byte 0 goes to bits 0-7, byte 1 to bits 8-15, etc.
    // Within each byte, bit 0 (MSB) is at position 7, bit 7 (LSB) is at position 0
    int byteWithinWindow = (discriminativeBitPos / 8) - page.initialBytePos;
    int bitWithinByte = discriminativeBitPos % 8; // 0=MSB, 7=LSB
    int bitInWord = byteWithinWindow * 8 + (7 - bitWithinByte);
    page.bitMask = 1L << bitInWord;
    
    page.partialKeys = new byte[] { 0, 1 };
    page.childReferences[0] = leftChild;
    page.childReferences[1] = rightChild;
    return page;
  }

  /**
   * Create a new SpanNode with 2-16 children.
   *
   * <p><b>Reference:</b> SpanNode uses SparsePartialKeys for SIMD-accelerated
   * child lookup. The search pattern is: {@code (denseKey & sparseKey) == sparseKey}</p>
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param initialBytePos initial byte position for SINGLE_MASK
   * @param bitMask the 64-bit mask for extracting discriminative bits
   * @param partialKeys array of partial keys (extracted bits for each child)
   * @param children array of child references
   * @return new SpanNode
   */
  public static HOTIndirectPage createSpanNode(long pageKey, int revision,
                                               byte initialBytePos, long bitMask,
                                               byte[] partialKeys,
                                               PageReference[] children) {
    if (children.length < 2 || children.length > 16) {
      throw new IllegalArgumentException("SpanNode must have 2-16 children");
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, 0, NodeType.SPAN_NODE, children.length);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = initialBytePos;
    page.bitMask = bitMask;
    page.partialKeys = partialKeys.clone();
    
    // Create SIMD-accelerated SparsePartialKeys for fast search
    page.sparsePartialKeys = SparsePartialKeys.forBytes(children.length);
    for (int i = 0; i < children.length; i++) {
      page.sparsePartialKeys.setEntry(i, partialKeys[i]);
    }
    
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  /**
   * Create a new MultiNode with 17-256 children.
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param discriminativeByte the byte position used for direct indexing
   * @param childIndex 256-byte array mapping byte values to child slots
   * @param children array of child references
   * @return new MultiNode
   */
  public static HOTIndirectPage createMultiNode(long pageKey, int revision,
                                                byte discriminativeByte,
                                                byte[] childIndex,
                                                PageReference[] children) {
    if (children.length < 17 || children.length > 256) {
      throw new IllegalArgumentException("MultiNode must have 17-256 children");
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, 0, NodeType.MULTI_NODE, children.length);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = discriminativeByte;
    page.childIndex = childIndex.clone();
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  /**
   * Private constructor for factory methods.
   */
  private HOTIndirectPage(long pageKey, int revision, int height, NodeType nodeType, int numChildren) {
    this.pageKey = pageKey;
    this.revision = revision;
    this.height = height;
    this.nodeType = nodeType;
    this.numChildren = numChildren;
    this.childReferences = new PageReference[numChildren];
    this.layoutType = LayoutType.SINGLE_MASK; // Default
  }

  /**
   * Copy constructor for COW operations.
   *
   * @param other the page to copy
   */
  public HOTIndirectPage(HOTIndirectPage other) {
    this.pageKey = other.pageKey;
    this.revision = other.revision;
    this.height = other.height;
    this.nodeType = other.nodeType;
    this.layoutType = other.layoutType;
    this.numChildren = other.numChildren;
    this.initialBytePos = other.initialBytePos;
    this.bitMask = other.bitMask;
    
    if (other.maskBytePosArray != null) {
      this.maskBytePosArray = other.maskBytePosArray.clone();
    }
    if (other.maskArray != null) {
      this.maskArray = other.maskArray.clone();
    }
    if (other.bitPositions != null) {
      this.bitPositions = other.bitPositions.clone();
    }
    if (other.partialKeys != null) {
      this.partialKeys = other.partialKeys.clone();
      // Recreate SparsePartialKeys for SIMD search
      if (other.nodeType == NodeType.SPAN_NODE) {
        this.sparsePartialKeys = SparsePartialKeys.forBytes(other.numChildren);
        for (int i = 0; i < other.numChildren; i++) {
          this.sparsePartialKeys.setEntry(i, other.partialKeys[i]);
        }
      }
    }
    if (other.childIndex != null) {
      this.childIndex = other.childIndex.clone();
    }
    
    this.childReferences = new PageReference[other.childReferences.length];
    for (int i = 0; i < other.numChildren; i++) {
      if (other.childReferences[i] != null) {
        this.childReferences[i] = new PageReference(other.childReferences[i]);
      }
    }
  }

  /**
   * Create a copy with an updated child reference (for COW propagation).
   *
   * @param childIndex the child index to update
   * @param newChildRef the new child reference
   * @return new page with updated child
   */
  public HOTIndirectPage copyWithUpdatedChild(int childIndex, PageReference newChildRef) {
    HOTIndirectPage copy = new HOTIndirectPage(this);
    copy.childReferences[childIndex] = newChildRef;
    return copy;
  }

  // ===== Child lookup methods =====

  /**
   * Find child index for the given key.
   * Uses SIMD-optimized lookup based on node type.
   *
   * @param key the search key
   * @return child index, or NOT_FOUND (-1) if not found
   */
  public int findChildIndex(byte[] key) {
    return switch (nodeType) {
      case BI_NODE -> findChildBiNode(key);
      case SPAN_NODE -> findChildSpanNode(key);
      case MULTI_NODE -> findChildMultiNode(key);
    };
  }

  /**
   * BiNode lookup: Extract single bit and return 0 or 1.
   */
  private int findChildBiNode(byte[] key) {
    if (key.length <= initialBytePos) {
      return 0; // Key too short, go left
    }
    // Extract the discriminative bit
    int bytePos = initialBytePos & 0xFF;
    if (bytePos >= key.length) {
      return 0;
    }
    long keyWord = getKeyWordAt(key, bytePos);
    long extracted = Long.compress(keyWord, bitMask); // PEXT intrinsic
    return (int) (extracted & 1);
  }

  /**
   * SpanNode lookup: Extract partial key and search in partial keys array.
   * 
   * <p><b>Reference:</b> SparsePartialKeys.hpp search() method</p>
   * 
   * <p>Uses SIMD-accelerated search when SparsePartialKeys is available.
   * The search pattern is: {@code (densePartialKey & sparsePartialKey) == sparsePartialKey}</p>
   * 
   * <p>This finds ALL entries that could match the search key based on the
   * discriminative bits. For an exact match, we take the first (lowest index) match.</p>
   */
  private int findChildSpanNode(byte[] key) {
    int bytePos = initialBytePos & 0xFF;
    if (bytePos >= key.length) {
      return 0;
    }
    long keyWord = getKeyWordAt(key, bytePos);
    int densePartialKey = (int) Long.compress(keyWord, bitMask); // PEXT intrinsic
    
    // Use SIMD-accelerated search if available
    if (sparsePartialKeys != null) {
      // SIMD search returns bitmask of all matching entries
      int matchMask = sparsePartialKeys.search(densePartialKey);
      if (matchMask == 0) {
        return NOT_FOUND;
      }
      // Return lowest matching index (trailing zeros count)
      return Integer.numberOfTrailingZeros(matchMask);
    }
    
    // Fallback: Linear search (for deserialized pages without SparsePartialKeys)
    for (int i = 0; i < numChildren; i++) {
      // Check: (denseKey & sparseKey[i]) == sparseKey[i]
      int sparseKey = partialKeys[i] & 0xFF;
      if ((densePartialKey & sparseKey) == sparseKey) {
        return i;
      }
    }
    return NOT_FOUND;
  }

  /**
   * MultiNode lookup: Direct byte indexing.
   */
  private int findChildMultiNode(byte[] key) {
    int bytePos = initialBytePos & 0xFF;
    if (bytePos >= key.length) {
      return childIndex[0] & 0xFF; // Default to first entry
    }
    int keyByte = key[bytePos] & 0xFF;
    int index = childIndex[keyByte] & 0xFF;
    return index < numChildren ? index : NOT_FOUND;
  }

  /**
   * Extract up to 8 bytes from key starting at given position.
   * Uses little-endian byte order for PEXT compatibility.
   */
  private static long getKeyWordAt(byte[] key, int pos) {
    long result = 0;
    int end = Math.min(pos + 8, key.length);
    for (int i = pos; i < end; i++) {
      result |= ((long) (key[i] & 0xFF)) << ((i - pos) * 8);
    }
    return result;
  }

  /**
   * Get child reference at index.
   *
   * @param index the child index
   * @return the page reference
   */
  public PageReference getChildReference(int index) {
    Objects.checkIndex(index, numChildren);
    return childReferences[index];
  }

  /**
   * Set child reference at index.
   *
   * @param index the child index
   * @param ref the page reference
   */
  public void setChildReference(int index, PageReference ref) {
    Objects.checkIndex(index, numChildren);
    childReferences[index] = ref;
  }

  /**
   * Get number of children.
   *
   * @return number of children
   */
  public int getNumChildren() {
    return numChildren;
  }

  /**
   * Get node type.
   *
   * @return the node type
   */
  public NodeType getNodeType() {
    return nodeType;
  }

  /**
   * Get layout type.
   *
   * @return the layout type
   */
  public LayoutType getLayoutType() {
    return layoutType;
  }

  /**
   * Get page key.
   *
   * @return the page key
   */
  public long getPageKey() {
    return pageKey;
  }

  /**
   * Get revision.
   *
   * @return the revision
   */
  public int getRevision() {
    return revision;
  }

  /**
   * Get height (distance from leaves).
   *
   * @return the height
   */
  public int getHeight() {
    return height;
  }

  /**
   * Get initial byte position for discriminative bit extraction.
   *
   * @return the initial byte position
   */
  public int getInitialBytePos() {
    return initialBytePos & 0xFF;
  }

  /**
   * Get the 64-bit discriminative bit mask.
   *
   * @return the bit mask
   */
  public long getBitMask() {
    return bitMask;
  }

  /**
   * Get partial key at index.
   *
   * @param index the index
   * @return the partial key byte
   */
  public byte getPartialKey(int index) {
    if (partialKeys == null || index < 0 || index >= partialKeys.length) {
      return 0;
    }
    return partialKeys[index];
  }

  /**
   * Create a copy with a new page key.
   *
   * @param newPageKey the new page key
   * @param newRevision the new revision
   * @return the copy with new page key
   */
  public HOTIndirectPage copyWithNewPageKey(long newPageKey, int newRevision) {
    HOTIndirectPage copy = new HOTIndirectPage(newPageKey, newRevision, this.height, this.nodeType, this.numChildren);
    copy.layoutType = this.layoutType;
    copy.initialBytePos = this.initialBytePos;
    copy.bitMask = this.bitMask;
    
    if (this.maskBytePosArray != null) {
      copy.maskBytePosArray = this.maskBytePosArray.clone();
    }
    if (this.maskArray != null) {
      copy.maskArray = this.maskArray.clone();
    }
    if (this.bitPositions != null) {
      copy.bitPositions = this.bitPositions.clone();
    }
    if (this.partialKeys != null) {
      copy.partialKeys = this.partialKeys.clone();
    }
    if (this.childIndex != null) {
      copy.childIndex = this.childIndex.clone();
    }
    
    for (int i = 0; i < this.numChildren; i++) {
      if (this.childReferences[i] != null) {
        copy.childReferences[i] = new PageReference(this.childReferences[i]);
      }
    }
    
    return copy;
  }

  /**
   * Create a new SpanNode with explicit height.
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param initialBytePos initial byte position for SINGLE_MASK
   * @param bitMask the 64-bit mask for extracting discriminative bits
   * @param partialKeys array of partial keys (extracted bits for each child)
   * @param children array of child references
   * @param height the height of this node
   * @return new SpanNode
   */
  public static HOTIndirectPage createSpanNode(long pageKey, int revision,
                                               int initialBytePos, long bitMask,
                                               byte[] partialKeys,
                                               PageReference[] children,
                                               int height) {
    if (children.length < 2 || children.length > 16) {
      throw new IllegalArgumentException("SpanNode must have 2-16 children");
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, height, NodeType.SPAN_NODE, children.length);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = (byte) initialBytePos;
    page.bitMask = bitMask;
    page.partialKeys = partialKeys.clone();
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  /**
   * Create a new MultiNode with explicit height.
   *
   * @param pageKey the page key
   * @param revision the revision
   * @param initialBytePos initial byte position
   * @param bitMask the 64-bit mask for extracting discriminative bits
   * @param partialKeys array of partial keys
   * @param children array of child references
   * @param height the height of this node
   * @return new MultiNode
   */
  public static HOTIndirectPage createMultiNode(long pageKey, int revision,
                                                int initialBytePos, long bitMask,
                                                byte[] partialKeys,
                                                PageReference[] children,
                                                int height) {
    if (children.length < 1 || children.length > 32) {
      throw new IllegalArgumentException("MultiNode must have 1-32 children, got: " + children.length);
    }
    HOTIndirectPage page = new HOTIndirectPage(pageKey, revision, height, NodeType.MULTI_NODE, children.length);
    page.layoutType = LayoutType.SINGLE_MASK;
    page.initialBytePos = (byte) initialBytePos;
    page.bitMask = bitMask;
    page.partialKeys = partialKeys.clone();
    System.arraycopy(children, 0, page.childReferences, 0, children.length);
    return page;
  }

  // ===== Page interface implementation =====

  @Override
  public List<PageReference> getReferences() {
    List<PageReference> refs = new ArrayList<>(numChildren);
    for (int i = 0; i < numChildren; i++) {
      if (childReferences[i] != null) {
        refs.add(childReferences[i]);
      }
    }
    return refs;
  }

  @Override
  public PageReference getOrCreateReference(int offset) {
    if (offset < 0 || offset >= numChildren) {
      return null;
    }
    if (childReferences[offset] == null) {
      childReferences[offset] = new PageReference();
    }
    return childReferences[offset];
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    if (offset < 0 || offset >= childReferences.length) {
      return true; // Page full
    }
    childReferences[offset] = pageReference;
    return false;
  }

  @Override
  public String toString() {
    return "HOTIndirectPage{" +
        "pageKey=" + pageKey +
        ", revision=" + revision +
        ", height=" + height +
        ", nodeType=" + nodeType +
        ", layoutType=" + layoutType +
        ", numChildren=" + numChildren +
        '}';
  }
}

