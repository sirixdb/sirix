/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.axis;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.checkerframework.checker.index.qual.NonNegative;
import io.sirix.api.NodeCursor;

/**
 * <p>
 * Iterate over all structural descendants starting at a given node (in preorder). Self might or
 * might not be included.
 * </p>
 */
public final class DescendantAxis extends AbstractAxis {

  /** Stack for remembering next nodeKey in document order. */
  private LongArrayList rightSiblingKeyStack;

  /** Determines if it's the first call to hasNext(). */
  private boolean first;
  
  /** 
   * The right sibling key of the start node.
   * Used to detect when traversal should stop (when we would move to start's right sibling).
   * This avoids the costly moveTo+check+moveBack pattern in hasNextNode.
   */
  private long startNodeRightSiblingKey;

  /**
   * Constructor initializing internal state.
   *
   * @param cursor cursor to iterate with
   */
  public DescendantAxis(final NodeCursor cursor) {
    super(cursor);
  }

  /**
   * Constructor initializing internal state.
   *
   * @param cursor cursor to iterate with
   * @param includeSelf determines if current node is included or not
   */
  public DescendantAxis(final NodeCursor cursor, final IncludeSelf includeSelf) {
    super(cursor, includeSelf);
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    first = true;
    rightSiblingKeyStack = new LongArrayList();
    // Cache the right sibling key of the start node to avoid moveTo in hasNextNode
    final NodeCursor cursor = getCursor();
    final long currentKey = cursor.getNodeKey();
    cursor.moveTo(nodeKey);
    startNodeRightSiblingKey = cursor.getRightSiblingKey();
    cursor.moveTo(currentKey);
  }

  @Override
  protected long nextKey() {
    long key;

    final NodeCursor cursor = getCursor();

    // Determines if first call to hasNext().
    if (first) {
      first = false;

      if (includeSelf() == IncludeSelf.YES) {
        key = cursor.getNodeKey();
      } else {
        key = cursor.getFirstChildKey();
      }

      return key;
    }

    // Always follow first child if there is one.
    if (cursor.hasFirstChild()) {
      key = cursor.getFirstChildKey();
      if (cursor.hasRightSibling()) {
        rightSiblingKeyStack.add(cursor.getRightSiblingKey());
      }
      return key;
    }

    // Then follow right sibling if there is one.
    if (cursor.hasRightSibling()) {
      final long currKey = cursor.getNodeKey();
      key = cursor.getRightSiblingKey();
      return hasNextNode(key, currKey);
    }

    // Then follow right sibling on stack.
    if (!rightSiblingKeyStack.isEmpty()) {
      final long currKey = cursor.getNodeKey();
      key = rightSiblingKeyStack.popLong();
      return hasNextNode(key, currKey);
    }

    return done();
  }

  /**
   * Determines if the subtree-traversal is finished.
   * 
   * OPTIMIZATION: Instead of moving to the candidate node to check if its left sibling
   * is the start node (then moving back), we simply check if the candidate key equals
   * the cached right sibling key of the start node. This is equivalent logic:
   * - Old: candidate.leftSiblingKey == startKey
   * - New: candidateKey == startNode.rightSiblingKey
   * 
   * This eliminates 2 moveTo() calls per check, which is significant during traversal.
   *
   * @param key next key (candidate to visit)
   * @param currKey current node key (unused after optimization, kept for API compatibility)
   * @return the key if traversal should continue, or done() if finished
   */
  private long hasNextNode(@NonNegative final long key, final @NonNegative long currKey) {
    // Check if the candidate is the right sibling of the start node
    // If so, we've finished traversing all descendants of the start subtree
    if (key == startNodeRightSiblingKey) {
      return done();
    }
    return key;
  }
}
