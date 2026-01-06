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
import io.sirix.page.HOTLeafPage;
import io.sirix.page.PageReference;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Handles merging of sibling nodes in HOT (Height Optimized Trie) after deletions.
 * 
 * <p>This class implements the merge-on-delete optimization from Robert Binna's
 * PhD thesis. After a deletion, if a node becomes underfilled, it may be merged
 * with its sibling to maintain optimal tree height.</p>
 * 
 * <p><b>Reference:</b> HOTSingleThreaded.hpp lines 166-209 show the merge logic.</p>
 * 
 * <p><b>Merge Criteria:</b></p>
 * <ul>
 *   <li>Nodes must be siblings (share same parent)</li>
 *   <li>Combined entry count must fit in a single node (≤32)</li>
 *   <li>Nodes should be at the same height</li>
 * </ul>
 * 
 * <p><b>Benefits:</b></p>
 * <ul>
 *   <li>Maintains minimum tree height</li>
 *   <li>Reduces memory overhead</li>
 *   <li>Improves cache efficiency</li>
 * </ul>
 * 
 * @author Johannes Lichtenberger
 * @see NodeUpgradeManager
 */
public final class SiblingMerger {

  /** Minimum fill factor before considering merge (25%). */
  public static final double MIN_FILL_FACTOR = 0.25;
  
  /** Maximum entries per node (from reference implementation). */
  public static final int MAX_ENTRIES_PER_NODE = 32;

  /** Private constructor to prevent instantiation. */
  private SiblingMerger() {
    throw new AssertionError("Utility class - do not instantiate");
  }

  /**
   * Result of a merge operation.
   * 
   * @param mergedNode the merged node (or null if merge not possible)
   * @param success true if merge was successful
   * @param replacesLeft true if merged node should replace left, false for right
   */
  public record MergeResult(
      @Nullable HOTIndirectPage mergedNode,
      boolean success,
      boolean replacesLeft) {
    
    public static MergeResult failure() {
      return new MergeResult(null, false, false);
    }
    
    public static MergeResult success(HOTIndirectPage mergedNode, boolean replacesLeft) {
      return new MergeResult(mergedNode, true, replacesLeft);
    }
  }

  /**
   * Check if a node should be merged with its sibling after deletion.
   * 
   * <p>Reference: HOTSingleThreaded.hpp line 170 checks:
   * {@code totalNumberEntries = potentialDirectNeighbour->getNumberEntries() +
   * current->getNumberEntries() - 1}</p>
   * 
   * @param node the node to check
   * @return true if the node is a candidate for merging
   */
  public static boolean shouldMerge(@NonNull HOTIndirectPage node) {
    int maxChildren = NodeUpgradeManager.getMaxChildrenForType(node.getNodeType());
    return node.getNumChildren() < maxChildren * MIN_FILL_FACTOR;
  }

  /**
   * Check if two sibling nodes can be merged.
   * 
   * @param left the left sibling
   * @param right the right sibling
   * @return true if the nodes can be merged
   */
  public static boolean canMerge(@NonNull HOTIndirectPage left, @NonNull HOTIndirectPage right) {
    // Must be at the same height
    if (left.getHeight() != right.getHeight()) {
      return false;
    }
    
    // Combined count must fit in a single node
    int totalChildren = left.getNumChildren() + right.getNumChildren();
    return totalChildren <= MAX_ENTRIES_PER_NODE;
  }

  /**
   * Merge two sibling indirect pages into one.
   * 
   * <p>Reference: NodeMergeInformation.hpp shows the merge algorithm.</p>
   * 
   * @param left the left sibling
   * @param right the right sibling
   * @param newPageKey page key for the merged node
   * @param revision current revision
   * @return the merge result
   */
  public static MergeResult mergeSiblings(
      @NonNull HOTIndirectPage left,
      @NonNull HOTIndirectPage right,
      long newPageKey,
      int revision) {
    
    if (!canMerge(left, right)) {
      return MergeResult.failure();
    }
    
    int totalChildren = left.getNumChildren() + right.getNumChildren();
    
    // Collect all children from both nodes
    PageReference[] childRefs = new PageReference[totalChildren];
    byte[] partialKeys = new byte[totalChildren];
    
    int idx = 0;
    for (int i = 0; i < left.getNumChildren(); i++) {
      childRefs[idx] = left.getChildReference(i);
      partialKeys[idx] = left.getPartialKey(i);
      idx++;
    }
    for (int i = 0; i < right.getNumChildren(); i++) {
      childRefs[idx] = right.getChildReference(i);
      partialKeys[idx] = right.getPartialKey(i);
      idx++;
    }
    
    // Combine discriminative bit masks
    long combinedMask = left.getBitMask() | right.getBitMask();
    int initialBytePos = Math.min(left.getInitialBytePos(), right.getInitialBytePos());
    
    // Create appropriate node type based on child count
    HOTIndirectPage mergedNode;
    if (totalChildren <= 2) {
      // Create BiNode
      mergedNode = HOTIndirectPage.createBiNode(
          newPageKey,
          revision,
          initialBytePos * 8 + Long.numberOfLeadingZeros(combinedMask),
          childRefs[0],
          childRefs[1]);
    } else if (totalChildren <= 16) {
      // Create SpanNode
      mergedNode = HOTIndirectPage.createSpanNode(
          newPageKey,
          revision,
          initialBytePos,
          combinedMask,
          partialKeys,
          childRefs,
          left.getHeight());
    } else {
      // Create MultiNode
      mergedNode = HOTIndirectPage.createMultiNode(
          newPageKey,
          revision,
          initialBytePos,
          combinedMask,
          partialKeys,
          childRefs,
          left.getHeight());
    }
    
    return MergeResult.success(mergedNode, true);
  }

  /**
   * Merge two sibling leaf pages into one.
   * 
   * @param left the left sibling
   * @param right the right sibling
   * @param targetPage the page to receive merged entries
   * @return true if merge was successful
   */
  public static boolean mergeLeafPages(
      @NonNull HOTLeafPage left,
      @NonNull HOTLeafPage right,
      @NonNull HOTLeafPage targetPage) {
    
    // Check if combined entries fit
    int totalEntries = left.getEntryCount() + right.getEntryCount();
    if (totalEntries > HOTLeafPage.MAX_ENTRIES) {
      return false;
    }
    
    // Copy entries from left
    for (int i = 0; i < left.getEntryCount(); i++) {
      byte[] key = left.getKey(i);
      byte[] value = left.getValue(i);
      if (key != null && value != null) {
        targetPage.put(key, value);
      }
    }
    
    // Copy entries from right
    for (int i = 0; i < right.getEntryCount(); i++) {
      byte[] key = right.getKey(i);
      byte[] value = right.getValue(i);
      if (key != null && value != null) {
        targetPage.put(key, value);
      }
    }
    
    return true;
  }

  /**
   * Handle deletion with potential merge.
   * 
   * <p>Reference: removeWithStack() in HOTSingleThreaded.hpp lines 133-139.</p>
   * 
   * @param node the node from which an entry was deleted
   * @param sibling the sibling node (if available)
   * @param newPageKey page key for merged node (if merge occurs)
   * @param revision current revision
   * @return the merge result, or failure if no merge occurred
   */
  public static MergeResult handleDeletionWithMerge(
      @NonNull HOTIndirectPage node,
      @Nullable HOTIndirectPage sibling,
      long newPageKey,
      int revision) {
    
    // Check if merge is warranted
    if (!shouldMerge(node)) {
      return MergeResult.failure();
    }
    
    // Check if sibling is available and merge is possible
    if (sibling == null || !canMerge(node, sibling)) {
      return MergeResult.failure();
    }
    
    // Determine order (left/right) based on discriminative bit
    // For simplicity, assume node is left if its first partial key is smaller
    boolean nodeIsLeft = node.getPartialKey(0) < sibling.getPartialKey(0);
    
    if (nodeIsLeft) {
      return mergeSiblings(node, sibling, newPageKey, revision);
    } else {
      MergeResult result = mergeSiblings(sibling, node, newPageKey, revision);
      // Swap the replacesLeft flag since we swapped the order
      return new MergeResult(result.mergedNode(), result.success(), !result.replacesLeft());
    }
  }

  /**
   * Check if a BiNode can be collapsed (replaced by its single remaining child).
   * 
   * <p>After deletion, if a BiNode has only one child, the BiNode should be
   * removed and replaced by its child directly.</p>
   * 
   * @param biNode the BiNode to check
   * @return true if the BiNode can be collapsed
   */
  public static boolean canCollapseBiNode(@NonNull HOTIndirectPage biNode) {
    return biNode.getNodeType() == HOTIndirectPage.NodeType.BI_NODE
        && biNode.getNumChildren() == 1;
  }

  /**
   * Get the remaining child of a collapsible BiNode.
   * 
   * @param biNode the BiNode to collapse
   * @return reference to the remaining child
   * @throws IllegalStateException if the BiNode cannot be collapsed
   */
  public static PageReference getCollapsedChild(@NonNull HOTIndirectPage biNode) {
    if (!canCollapseBiNode(biNode)) {
      throw new IllegalStateException("BiNode cannot be collapsed: has " + biNode.getNumChildren() + " children");
    }
    return biNode.getChildReference(0);
  }

  /**
   * Calculate the fill factor of a node.
   * 
   * @param node the node to check
   * @return fill factor (0.0 - 1.0)
   */
  public static double getFillFactor(@NonNull HOTIndirectPage node) {
    int maxChildren = NodeUpgradeManager.getMaxChildrenForType(node.getNodeType());
    return (double) node.getNumChildren() / maxChildren;
  }
}

