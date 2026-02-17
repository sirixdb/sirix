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
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.List;

/**
 * Manages node type transitions in HOT (Height Optimized Trie).
 * 
 * <p>
 * Handles upgrades and downgrades between node types:
 * </p>
 * <ul>
 * <li>BiNode (2 children, 1 discriminative bit)</li>
 * <li>SpanNode (2-16 children, 2-4 discriminative bits)</li>
 * <li>MultiNode (17-256 children, 5-8 discriminative bits)</li>
 * </ul>
 * 
 * <p>
 * <b>Reference:</b> NodeAllocationInformations.hpp defines max 32 entries per node.
 * </p>
 * 
 * <p>
 * <b>Upgrade Triggers:</b>
 * </p>
 * <ul>
 * <li>BiNode → SpanNode: When sibling BiNodes share discriminative bits in same byte</li>
 * <li>SpanNode → MultiNode: When children exceed 16</li>
 * </ul>
 * 
 * <p>
 * <b>Downgrade Triggers:</b>
 * </p>
 * <ul>
 * <li>SpanNode → BiNode: After merge, only 2 children remain</li>
 * <li>MultiNode → SpanNode: After delete, children drop to ≤16</li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 * @see HOTIndirectPage
 */
public final class NodeUpgradeManager {

  /** Maximum children for BiNode. */
  public static final int BI_NODE_MAX_CHILDREN = 2;

  /** Maximum children for SpanNode. */
  public static final int SPAN_NODE_MAX_CHILDREN = 16;

  /** Maximum children for MultiNode (and maximum per compound node). */
  public static final int MULTI_NODE_MAX_CHILDREN = 32;

  /** Maximum discriminative bits for SpanNode (2^4 = 16 children). */
  public static final int SPAN_NODE_MAX_BITS = 4;

  /** Private constructor to prevent instantiation. */
  private NodeUpgradeManager() {
    throw new AssertionError("Utility class - do not instantiate");
  }

  /**
   * Determine the appropriate node type for a given number of children.
   * 
   * @param numChildren the number of children
   * @return the appropriate node type
   * @throws IllegalArgumentException if numChildren is invalid
   */
  public static NodeType determineNodeType(int numChildren) {
    if (numChildren < 1) {
      throw new IllegalArgumentException("Node must have at least 1 child: " + numChildren);
    }
    if (numChildren <= BI_NODE_MAX_CHILDREN) {
      return NodeType.BI_NODE;
    }
    if (numChildren <= SPAN_NODE_MAX_CHILDREN) {
      return NodeType.SPAN_NODE;
    }
    if (numChildren <= MULTI_NODE_MAX_CHILDREN) {
      return NodeType.MULTI_NODE;
    }
    throw new IllegalArgumentException("Too many children for single node: " + numChildren);
  }

  /**
   * Determine the appropriate node type based on discriminative bit count.
   * 
   * @param numDiscriminativeBits the number of discriminative bits
   * @return the appropriate node type
   */
  public static NodeType determineNodeTypeByBits(int numDiscriminativeBits) {
    if (numDiscriminativeBits <= 1) {
      return NodeType.BI_NODE;
    }
    if (numDiscriminativeBits <= SPAN_NODE_MAX_BITS) {
      return NodeType.SPAN_NODE;
    }
    return NodeType.MULTI_NODE;
  }

  /**
   * Check if two BiNodes should be merged into a SpanNode.
   * 
   * <p>
   * Thesis criteria: If sibling BiNodes share discriminative bits in the same byte, merge into a
   * SpanNode with higher fanout (lower height).
   * </p>
   * 
   * @param biNode1 first BiNode
   * @param biNode2 second BiNode
   * @return true if nodes should be merged into a SpanNode
   */
  public static boolean shouldMergeToSpanNode(@NonNull HOTIndirectPage biNode1, @NonNull HOTIndirectPage biNode2) {
    if (biNode1.getNodeType() != NodeType.BI_NODE || biNode2.getNodeType() != NodeType.BI_NODE) {
      return false;
    }
    // Same initial byte position means discriminative bits are in same region
    return biNode1.getInitialBytePos() == biNode2.getInitialBytePos();
  }

  /**
   * Check if a node needs to be upgraded after an insertion.
   * 
   * @param node the current node
   * @param newChildCount the child count after insertion
   * @return true if the node needs upgrading
   */
  public static boolean needsUpgrade(@NonNull HOTIndirectPage node, int newChildCount) {
    NodeType currentType = node.getNodeType();
    NodeType requiredType = determineNodeType(newChildCount);
    return requiredType.ordinal() > currentType.ordinal();
  }

  /**
   * Check if a node should be downgraded after a deletion.
   * 
   * @param node the current node
   * @param newChildCount the child count after deletion
   * @return true if the node should be downgraded
   */
  public static boolean shouldDowngrade(@NonNull HOTIndirectPage node, int newChildCount) {
    if (newChildCount < 1) {
      return true; // Node should be removed entirely
    }
    NodeType currentType = node.getNodeType();
    NodeType optimalType = determineNodeType(newChildCount);
    return optimalType.ordinal() < currentType.ordinal();
  }

  /**
   * Create a SpanNode by merging multiple BiNodes.
   * 
   * <p>
   * Reference: NodeMergeInformation.hpp shows how discriminative bits are combined.
   * </p>
   * 
   * @param biNodes list of BiNodes to merge
   * @param newPageKey page key for the new SpanNode
   * @param revision current revision
   * @return the new SpanNode
   */
  public static HOTIndirectPage mergeToSpanNode(@NonNull List<HOTIndirectPage> biNodes, long newPageKey, int revision) {
    if (biNodes.isEmpty()) {
      throw new IllegalArgumentException("Cannot merge empty list of BiNodes");
    }
    if (biNodes.size() == 1) {
      // Single BiNode, just return a copy
      return biNodes.get(0).copyWithNewPageKey(newPageKey, revision);
    }

    // Compute combined discriminative mask
    long combinedMask = 0;
    int initialBytePos = biNodes.get(0).getInitialBytePos();

    for (HOTIndirectPage biNode : biNodes) {
      combinedMask |= biNode.getBitMask();
      // Verify all nodes share the same initial byte position
      if (biNode.getInitialBytePos() != initialBytePos) {
        throw new IllegalArgumentException("Cannot merge BiNodes with different initial byte positions");
      }
    }

    // Collect all children with their partial keys
    int totalChildren = 0;
    for (HOTIndirectPage biNode : biNodes) {
      totalChildren += biNode.getNumChildren();
    }

    if (totalChildren > SPAN_NODE_MAX_CHILDREN) {
      throw new IllegalArgumentException(
          "Total children (" + totalChildren + ") exceeds SpanNode max (" + SPAN_NODE_MAX_CHILDREN + ")");
    }

    // Create new SpanNode with combined mask and all children
    PageReference[] childRefs = new PageReference[totalChildren];
    byte[] partialKeys = new byte[totalChildren];
    int childIdx = 0;

    for (HOTIndirectPage biNode : biNodes) {
      for (int i = 0; i < biNode.getNumChildren(); i++) {
        childRefs[childIdx] = biNode.getChildReference(i);
        partialKeys[childIdx] = biNode.getPartialKey(i);
        childIdx++;
      }
    }

    // Calculate height (max of children heights + 1)
    int maxChildHeight = 0;
    for (HOTIndirectPage biNode : biNodes) {
      maxChildHeight = Math.max(maxChildHeight, biNode.getHeight() - 1);
    }

    return HOTIndirectPage.createSpanNode(newPageKey, revision, initialBytePos, combinedMask, partialKeys, childRefs,
        maxChildHeight + 1);
  }

  /**
   * Upgrade a SpanNode to a MultiNode.
   * 
   * @param spanNode the SpanNode to upgrade
   * @param newPageKey page key for the new MultiNode
   * @param revision current revision
   * @param additionalChild additional child to add (can be null)
   * @param additionalPartialKey partial key for the additional child
   * @return the new MultiNode
   */
  public static HOTIndirectPage upgradeToMultiNode(@NonNull HOTIndirectPage spanNode, long newPageKey, int revision,
      PageReference additionalChild, byte additionalPartialKey) {
    if (spanNode.getNodeType() != NodeType.SPAN_NODE) {
      throw new IllegalArgumentException("Can only upgrade SpanNode to MultiNode");
    }

    int newChildCount = spanNode.getNumChildren() + (additionalChild != null
        ? 1
        : 0);
    PageReference[] childRefs = new PageReference[newChildCount];
    byte[] partialKeys = new byte[newChildCount];

    // Copy existing children
    for (int i = 0; i < spanNode.getNumChildren(); i++) {
      childRefs[i] = spanNode.getChildReference(i);
      partialKeys[i] = spanNode.getPartialKey(i);
    }

    // Add new child if provided
    if (additionalChild != null) {
      childRefs[newChildCount - 1] = additionalChild;
      partialKeys[newChildCount - 1] = additionalPartialKey;
    }

    return HOTIndirectPage.createMultiNode(newPageKey, revision, spanNode.getInitialBytePos(), spanNode.getBitMask(),
        partialKeys, childRefs, spanNode.getHeight());
  }

  /**
   * Downgrade a SpanNode to a BiNode after deletion.
   * 
   * @param spanNode the SpanNode to downgrade
   * @param newPageKey page key for the new BiNode
   * @param revision current revision
   * @return the new BiNode
   */
  public static HOTIndirectPage downgradeToNode(@NonNull HOTIndirectPage spanNode, long newPageKey, int revision) {
    int numChildren = spanNode.getNumChildren();

    if (numChildren > BI_NODE_MAX_CHILDREN) {
      // Still too many children for BiNode, keep as SpanNode but with new key
      return spanNode.copyWithNewPageKey(newPageKey, revision);
    }

    if (numChildren == 2) {
      // Convert to BiNode
      PageReference leftRef = spanNode.getChildReference(0);
      PageReference rightRef = spanNode.getChildReference(1);

      // Find the single discriminative bit position
      long mask = spanNode.getBitMask();
      int bitPos = Long.numberOfLeadingZeros(mask);
      int absoluteBitPos = spanNode.getInitialBytePos() * 8 + bitPos;

      return HOTIndirectPage.createBiNode(newPageKey, revision, absoluteBitPos, leftRef, rightRef);
    }

    if (numChildren == 1) {
      // Single child - this node should be collapsed by the caller
      throw new IllegalStateException("Single-child node should be collapsed, not downgraded");
    }

    throw new IllegalStateException("Cannot downgrade node with " + numChildren + " children");
  }

  /**
   * Check if a node is full (cannot accept more children without splitting).
   * 
   * @param node the node to check
   * @return true if the node is full
   */
  public static boolean isFull(@NonNull HOTIndirectPage node) {
    return node.getNumChildren() >= MULTI_NODE_MAX_CHILDREN;
  }

  /**
   * Check if a node is underfilled (candidate for merge with sibling).
   * 
   * <p>
   * Reference: HOTSingleThreaded.hpp line 170 checks if total entries after merge would fit in one
   * node.
   * </p>
   * 
   * @param node the node to check
   * @param minFillFactor minimum fill factor (0.0-1.0)
   * @return true if node is underfilled
   */
  public static boolean isUnderfilled(@NonNull HOTIndirectPage node, double minFillFactor) {
    int maxChildren = getMaxChildrenForType(node.getNodeType());
    return node.getNumChildren() < maxChildren * minFillFactor;
  }

  /**
   * Get the maximum number of children for a node type.
   * 
   * @param nodeType the node type
   * @return maximum number of children
   */
  public static int getMaxChildrenForType(NodeType nodeType) {
    return switch (nodeType) {
      case BI_NODE -> BI_NODE_MAX_CHILDREN;
      case SPAN_NODE -> SPAN_NODE_MAX_CHILDREN;
      case MULTI_NODE -> MULTI_NODE_MAX_CHILDREN;
    };
  }
}

