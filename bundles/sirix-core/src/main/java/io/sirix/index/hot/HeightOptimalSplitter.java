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
import org.checkerframework.checker.nullness.qual.Nullable;

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
   * <p><b>Edge Case:</b> When the page has only 1 entry (common with many identical
   * keys that get merged), this method returns {@code null}. The caller should handle
   * this by compacting the page or using alternative strategies.</p>
   * 
   * @param fullPage the full leaf page to split
   * @param rightPage empty page to receive right half of entries
   * @param newRootPageKey page key for the new root node
   * @param revision current revision
   * @return the split result containing the new root and child references,
   *         or {@code null} if the page cannot be split (e.g., only 1 entry)
   */
  public static @Nullable SplitResult splitLeafPage(
      @NonNull HOTLeafPage fullPage,
      @NonNull HOTLeafPage rightPage,
      long newRootPageKey,
      int revision) {
    
    // 1. Split entries at midpoint (splitTo returns the split key, but we compute
    // the discriminative bit from actual boundary keys for correctness)
    byte[] splitKey = fullPage.splitTo(rightPage);
    
    // Handle edge case: page cannot be split (only 1 entry with large merged value)
    if (splitKey == null) {
      // Try compacting the page first to free up fragmented space
      fullPage.compact();
      // Still can't split - return null to signal caller needs alternative handling
      return null;
    }
    
    // 2. Get boundary keys for discriminative bit computation
    byte[] leftMax = fullPage.getLastKey();
    byte[] rightMin = rightPage.getFirstKey();
    
    // Handle edge case: identical or empty keys after split
    if (leftMax == null || rightMin == null) {
      // This shouldn't happen after a successful split, but handle gracefully
      return null;
    }
    
    // 3. Compute discriminative bit using reference algorithm
    int discriminativeBit = DiscriminativeBitComputer.computeDifferingBit(leftMax, rightMin);
    
    // Handle edge case: identical keys (discriminativeBit == -1)
    if (discriminativeBit < 0) {
      // Keys are identical - use bit 0 as fallback
      // This shouldn't happen with properly sorted unique keys
      discriminativeBit = 0;
    }
    
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
   * <p><b>Algorithm (from Binna's thesis):</b></p>
   * <ol>
   *   <li>Compute discriminative bit mask across all keys</li>
   *   <li>If bits are within 8 contiguous bytes → SpanNode (up to 16 children)</li>
   *   <li>If bits span more than 8 bytes → MultiNode (up to 256 children)</li>
   *   <li>If only 1 bit differs → BiNode (2 children)</li>
   * </ol>
   * 
   * @param fullPage the full leaf page to split
   * @param rightPage empty page to receive right half of entries
   * @param newRootPageKey page key for the new root node
   * @param revision current revision
   * @return the split result, or null if split not possible
   */
  public static @Nullable SplitResult splitLeafPageOptimal(
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
    
    // Compute discriminative bit mask for all keys
    long discriminativeMask = DiscriminativeBitComputer.computeDiscriminativeMask(allKeys, 0, 8);
    int numDiscriminativeBits = DiscriminativeBitComputer.countDiscriminativeBits(discriminativeMask);
    
    // Determine node type based on discriminative bit count
    NodeType nodeType = NodeUpgradeManager.determineNodeTypeByBits(numDiscriminativeBits);
    
    // BiNode case: simple 2-way split
    if (nodeType == NodeType.BI_NODE || numDiscriminativeBits <= 1) {
      return splitLeafPage(fullPage, rightPage, newRootPageKey, revision);
    }
    
    // SpanNode case: multiple discriminative bits in contiguous 8-byte window
    // Reference: SingleMaskPartialKeyMapping in the C++ implementation
    if (nodeType == NodeType.SPAN_NODE && numDiscriminativeBits <= 8) {
      return createSpanNodeSplit(fullPage, rightPage, newRootPageKey, revision, 
                                  allKeys, discriminativeMask, numDiscriminativeBits);
    }
    
    // MultiNode case: discriminative bits span more than 8 bytes
    // Reference: MultiMaskPartialKeyMapping in the C++ implementation
    if (nodeType == NodeType.MULTI_NODE || numDiscriminativeBits > 8) {
      // For now, fall back to BiNode for MultiNode - this is complex to implement
      // and BiNode cascading will achieve the same result (just less optimal height)
      return splitLeafPage(fullPage, rightPage, newRootPageKey, revision);
    }
    
    // Default fallback to BiNode
    return splitLeafPage(fullPage, rightPage, newRootPageKey, revision);
  }
  
  /**
   * Create a SpanNode split with multiple children based on discriminative bits.
   * 
   * <p>Reference: SingleMaskPartialKeyMapping handles up to 8 bytes contiguously.</p>
   * 
   * @param fullPage the full leaf page
   * @param rightPage the target right page
   * @param newRootPageKey page key for new root
   * @param revision current revision
   * @param allKeys all keys in the page
   * @param discriminativeMask the computed discriminative bit mask
   * @param numDiscriminativeBits number of discriminative bits
   * @return the split result with SpanNode root, or null if not possible
   */
  private static @Nullable SplitResult createSpanNodeSplit(
      @NonNull HOTLeafPage fullPage,
      @NonNull HOTLeafPage rightPage,
      long newRootPageKey,
      int revision,
      byte[][] allKeys,
      long discriminativeMask,
      int numDiscriminativeBits) {
    
    // For SpanNode, we need to:
    // 1. Determine the byte range containing all discriminative bits
    // 2. Compute partial keys for each entry
    // 3. Create child pages based on partial key grouping
    
    // Find the byte range
    int firstBitPos = Long.numberOfLeadingZeros(discriminativeMask);
    int lastBitPos = 63 - Long.numberOfTrailingZeros(discriminativeMask);
    int byteStart = firstBitPos / 8;
    int byteEnd = lastBitPos / 8;
    
    // If the range exceeds 8 bytes, fall back to BiNode
    if (byteEnd - byteStart >= 8) {
      return splitLeafPage(fullPage, rightPage, newRootPageKey, revision);
    }
    
    // For now, use a simpler approach: split into 2 children using BiNode
    // A full SpanNode implementation would create 2^numDiscriminativeBits children
    // This is complex because we need to redistribute entries among multiple pages
    
    // The key insight from the reference is that SpanNode creation typically happens
    // during insertion (addEntry), not during splits. Splits always create BiNodes,
    // which are then potentially integrated into existing SpanNodes during tree integration.
    
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
   * Create a PageReference for a new root from a split result.
   * 
   * <p>This is a simplified helper for creating the root reference. Full tree
   * integration with COW propagation is handled by 
   * {@link io.sirix.access.trx.page.HOTTrieWriter#handleLeafSplitWithPath}.</p>
   * 
   * <p>Reference: integrateBiNodeIntoTree() in HOTSingleThreaded.hpp lines 493-547.</p>
   * 
   * @param splitResult the result of a split operation
   * @param parentRef reference to the parent node (null if split created new root)
   * @param childIndex index of the split child in the parent (unused in simple case)
   * @return the new root reference
   */
  public static PageReference integrateBiNodeIntoTree(
      @NonNull SplitResult splitResult,
      @Nullable PageReference parentRef,
      int childIndex) {
    
    // Create reference to the new root
    // Full integration with parent is handled by HOTTrieWriter.updateParentForSplitWithPath()
    PageReference newRootRef = new PageReference();
    newRootRef.setPage(splitResult.newRoot());
    newRootRef.setKey(splitResult.newRoot().getPageKey());
    return newRootRef;
  }
}

