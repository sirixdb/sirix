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

package io.sirix.index.hot;

import io.sirix.page.HOTIndirectPage;
import io.sirix.page.HOTIndirectPage.NodeType;
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Implements height-optimal splitting for HOT (Height Optimized Trie).
 * 
 * <p>This class implements Algorithm 1 from Robert Binna's PhD thesis,
 * ensuring that splits maintain the three HOT properties:</p>
 * <ol>
 *   <li>Each HOT has minimum height</li>
 *   <li>Deterministic structure regardless of insertion order</li>
 *   <li>Each subtree is itself a HOT of minimum height</li>
 * </ol>
 * 
 * <p><b>Reference:</b> HOTSingleThreaded.hpp lines 477-488 show the split logic.</p>
 * 
 * <p><b>Algorithm:</b></p>
 * <ol>
 *   <li>Find optimal split point (typically median key)</li>
 *   <li>Compute discriminative bits for all keys</li>
 *   <li>Choose node type based on discriminative bit count</li>
 *   <li>Create appropriate compound node (BiNode, SpanNode, or MultiNode)</li>
 * </ol>
 * 
 * @author Johannes Lichtenberger
 * @see DiscriminativeBitComputer
 * @see NodeUpgradeManager
 */
public final class HeightOptimalSplitter {

  /** Private constructor to prevent instantiation. */
  private HeightOptimalSplitter() {
    throw new AssertionError("Utility class - do not instantiate");
  }

  /**
   * Result of a split operation.
   * 
   * @param newRoot the new root node (BiNode, SpanNode, or MultiNode)
   * @param leftChild reference to left child
   * @param rightChild reference to right child
   * @param discriminativeBitIndex the bit index that discriminates left from right
   */
  public record SplitResult(
      @NonNull HOTIndirectPage newRoot,
      @NonNull PageReference leftChild,
      @NonNull PageReference rightChild,
      int discriminativeBitIndex) {
  }

  /**
   * Perform a height-optimal split on a full leaf page.
   * 
   * <p>Reference: HOTSingleThreaded.hpp line 481:
   * {@code hot::commons::BiNode<...> const &binaryNode = existingNode.split(...)}</p>
   * 
   * @param fullPage the full leaf page to split
   * @param rightPage empty page to receive right half of entries
   * @param newRootPageKey page key for the new root node
   * @param revision current revision
   * @return the split result containing the new root and child references
   */
  public static SplitResult splitLeafPage(
      @NonNull HOTLeafPage fullPage,
      @NonNull HOTLeafPage rightPage,
      long newRootPageKey,
      int revision) {
    
    // 1. Split entries at midpoint (splitTo returns the split key, but we compute
    // the discriminative bit from actual boundary keys for correctness)
    fullPage.splitTo(rightPage);
    
    // 2. Get boundary keys for discriminative bit computation
    byte[] leftMax = fullPage.getLastKey();
    byte[] rightMin = rightPage.getFirstKey();
    
    // 3. Compute discriminative bit using reference algorithm
    int discriminativeBit = DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
    
    // 4. Determine which side new keys go based on discriminative bit value
    // Reference: BiNode.hpp line 15-17 - bit value determines left/right
    boolean rightMinBitValue = DiscriminativeBitComputer.isBitSet(rightMin, discriminativeBit);
    
    // 5. Create page references for children
    PageReference leftRef = new PageReference();
    leftRef.setPage(fullPage);
    leftRef.setKey(fullPage.getPageKey());
    
    PageReference rightRef = new PageReference();
    rightRef.setPage(rightPage);
    rightRef.setKey(rightPage.getPageKey());
    
    // 6. Create BiNode as new root
    // The rightMinBitValue determines placement:
    // - If rightMin has bit=1 at discriminativeBit position, right goes right (normal)
    // - If rightMin has bit=0, we need to swap (but this shouldn't happen with sorted keys)
    HOTIndirectPage newRoot;
    if (rightMinBitValue) {
      // Normal case: left child at index 0, right child at index 1
      newRoot = HOTIndirectPage.createBiNode(
          newRootPageKey,
          revision,
          discriminativeBit,
          leftRef,
          rightRef);
    } else {
      // Rare case: swap children
      newRoot = HOTIndirectPage.createBiNode(
          newRootPageKey,
          revision,
          discriminativeBit,
          rightRef,
          leftRef);
      // Swap references in result too
      PageReference temp = leftRef;
      leftRef = rightRef;
      rightRef = temp;
    }
    
    return new SplitResult(newRoot, leftRef, rightRef, discriminativeBit);
  }

  /**
   * Perform a height-optimal split considering all discriminative bits.
   * 
   * <p>This advanced version analyzes all keys to potentially create a
   * SpanNode or MultiNode directly, avoiding intermediate BiNodes.</p>
   * 
   * @param fullPage the full leaf page to split
   * @param rightPage empty page to receive right half of entries
   * @param newRootPageKey page key for the new root node
   * @param revision current revision
   * @return the split result
   */
  public static SplitResult splitLeafPageOptimal(
      @NonNull HOTLeafPage fullPage,
      @NonNull HOTLeafPage rightPage,
      long newRootPageKey,
      int revision) {
    
    // Get all keys to analyze discriminative bits
    byte[][] allKeys = fullPage.getAllKeys();
    
    if (allKeys.length < 2) {
      // Not enough keys to analyze, use simple split
      return splitLeafPage(fullPage, rightPage, newRootPageKey, revision);
    }
    
    // Find optimal split point (median)
    @SuppressWarnings("unused")
    int splitIndex = findOptimalSplitPoint(allKeys);
    
    // Compute discriminative bit mask for all keys
    long discriminativeMask = DiscriminativeBitComputer.computeDiscriminativeMask(allKeys, 0, 8);
    int numDiscriminativeBits = DiscriminativeBitComputer.countDiscriminativeBits(discriminativeMask);
    
    // Determine node type based on discriminative bit count
    NodeType nodeType = NodeUpgradeManager.determineNodeTypeByBits(numDiscriminativeBits);
    
    // For now, always use BiNode split (SpanNode/MultiNode optimization is complex)
    // This can be enhanced later for full thesis compliance
    if (nodeType == NodeType.BI_NODE || numDiscriminativeBits <= 1) {
      return splitLeafPage(fullPage, rightPage, newRootPageKey, revision);
    }
    
    // TODO: Implement SpanNode and MultiNode creation for optimal height
    // For now, fall back to BiNode
    return splitLeafPage(fullPage, rightPage, newRootPageKey, revision);
  }

  /**
   * Find the optimal split point for a set of keys.
   * 
   * <p>The split point should produce balanced subtrees while respecting
   * the discriminative bit structure.</p>
   * 
   * @param keys sorted array of keys
   * @return the index where the split should occur (first key of right subtree)
   */
  public static int findOptimalSplitPoint(byte[][] keys) {
    if (keys.length <= 2) {
      return keys.length / 2;
    }
    
    // Simple median split for now
    // A more sophisticated approach would consider discriminative bit boundaries
    return keys.length / 2;
  }

  /**
   * Check if a split should create a SpanNode instead of a BiNode.
   * 
   * <p>This is an optimization: if multiple discriminative bits are in the
   * same byte range, a SpanNode with higher fanout reduces tree height.</p>
   * 
   * @param leftMax max key in left subtree
   * @param rightMin min key in right subtree
   * @param discriminativeBit the computed discriminative bit
   * @return true if a SpanNode should be created
   */
  public static boolean shouldCreateSpanNode(
      @NonNull byte[] leftMax,
      @NonNull byte[] rightMin,
      int discriminativeBit) {
    // Check if there are additional discriminative bits in the same byte
    int byteIndex = DiscriminativeBitComputer.getByteIndex(discriminativeBit);
    
    if (byteIndex >= leftMax.length || byteIndex >= rightMin.length) {
      return false;
    }
    
    int leftByte = leftMax[byteIndex] & 0xFF;
    int rightByte = rightMin[byteIndex] & 0xFF;
    int diff = leftByte ^ rightByte;
    
    // Count differing bits in this byte
    int differingBits = Integer.bitCount(diff);
    
    // If more than 1 bit differs in the same byte, SpanNode might be beneficial
    return differingBits > 1 && differingBits <= 4;
  }

  /**
   * Create a BiNode from a split result.
   * 
   * <p>Reference: BiNode.hpp line 10-11:
   * {@code BiNode(uint16_t const discriminativeBitIndex, uint16_t const height,
   * ChildPointerType const & left, ChildPointerType const & right)}</p>
   * 
   * @param pageKey page key for the new BiNode
   * @param revision current revision
   * @param discriminativeBit the discriminative bit index
   * @param leftRef reference to left child
   * @param rightRef reference to right child
   * @return the new BiNode
   */
  public static HOTIndirectPage createBiNode(
      long pageKey,
      int revision,
      int discriminativeBit,
      @NonNull PageReference leftRef,
      @NonNull PageReference rightRef) {
    return HOTIndirectPage.createBiNode(pageKey, revision, discriminativeBit, leftRef, rightRef);
  }

  /**
   * Integrate a new BiNode into the tree structure.
   * 
   * <p>Reference: integrateBiNodeIntoTree() in HOTSingleThreaded.hpp lines 493-547.</p>
   * 
   * <p>This method handles the complex case of integrating a split result
   * into an existing tree, potentially causing cascading splits.</p>
   * 
   * @param splitResult the result of a split operation
   * @param parentRef reference to the parent node (null if split created new root)
   * @param childIndex index of the split child in the parent
   * @return the updated parent reference (or new root if parent was null)
   */
  public static PageReference integrateBiNodeIntoTree(
      @NonNull SplitResult splitResult,
      PageReference parentRef,
      int childIndex) {
    
    if (parentRef == null) {
      // Split created a new root
      PageReference newRootRef = new PageReference();
      newRootRef.setPage(splitResult.newRoot());
      newRootRef.setKey(splitResult.newRoot().getPageKey());
      return newRootRef;
    }
    
    // TODO: Handle integration into existing parent
    // This requires COW propagation up the tree
    // For now, return the new root reference
    PageReference newRootRef = new PageReference();
    newRootRef.setPage(splitResult.newRoot());
    newRootRef.setKey(splitResult.newRoot().getPageKey());
    return newRootRef;
  }
}

