/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
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

package org.sirix.axis;

import java.util.ArrayDeque;
import java.util.Deque;

import javax.annotation.Nonnull;

import org.sirix.api.INodeCursor;

/**
 * <h1>FollowingAxis</h1>
 * 
 * <p>
 * Iterate over all following nodes of kind ELEMENT or TEXT starting at a given node. Self is not included.
 * </p>
 */
public final class FollowingAxis extends AbsAxis {

  /** Determines if it's the first node. */
  private boolean mIsFirst;

  /** {@link Deque} reference to save right sibling keys. */
  private Deque<Long> mRightSiblingStack;

  /**
   * Constructor initializing internal state.
   * 
   * @param paramRtx
   *          exclusive (immutable) trx to iterate with
   */
  public FollowingAxis(@Nonnull final INodeCursor pRtx) {
    super(pRtx);
    mIsFirst = true;
    mRightSiblingStack = new ArrayDeque<>();
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mIsFirst = true;
    mRightSiblingStack = new ArrayDeque<>();
  }

  @Override
  public boolean hasNext() {
    if (!isHasNext()) {
      return false;
    }
    if (isNext()) {
      return true;
    }
    // Assure, that preceding is not evaluated on an attribute or a
    // namespace.
    if (mIsFirst) {
      switch (getTransaction().getNode().getKind()) {
      case ATTRIBUTE:
      case NAMESPACE:
        resetToStartKey();
        return false;
      default:
      }
    }

    resetToLastKey();
    final long currKey = getTransaction().getNode().getNodeKey();

    if (mIsFirst) {
      mIsFirst = false;

      /*
       * The first following is either a right sibling, or the right
       * sibling of the first ancestor that has a right sibling. Note:
       * ancestors and descendants are no following node!
       */
      if (getTransaction().getStructuralNode().hasRightSibling()) {
        getTransaction().moveToRightSibling();
        mKey = getTransaction().getNode().getNodeKey();

        if (getTransaction().getStructuralNode().hasRightSibling()) {
          // Push right sibling on a stack to reduce path traversal.
          mRightSiblingStack.push(getTransaction().getStructuralNode()
            .getRightSiblingKey());
        }

        getTransaction().moveTo(currKey);
        return true;
      }
      // Try to find the right sibling of one of the ancestors.
      while (getTransaction().getNode().hasParent()) {
        getTransaction().moveToParent();
        if (getTransaction().getStructuralNode().hasRightSibling()) {
          getTransaction().moveToRightSibling();
          mKey = getTransaction().getNode().getNodeKey();

          if (getTransaction().getStructuralNode().hasRightSibling()) {
            mRightSiblingStack.push(getTransaction().getStructuralNode()
              .getRightSiblingKey());
          }
          getTransaction().moveTo(currKey);
          return true;
        }
      }
      // CurrentNode is last key in the document order.
      resetToStartKey();
      return false;

    }
    // Step down the tree in document order.
    if (getTransaction().getStructuralNode().hasFirstChild()) {
      getTransaction().moveToFirstChild();
      mKey = getTransaction().getNode().getNodeKey();

      if (getTransaction().getStructuralNode().hasRightSibling()) {
        // Push right sibling on a stack to reduce path traversal.
        mRightSiblingStack.push(getTransaction().getStructuralNode()
          .getRightSiblingKey());
      }

      getTransaction().moveTo(currKey);
      return true;
    }
    if (mRightSiblingStack.isEmpty()) {
      // Try to find the right sibling of one of the ancestors.
      while (getTransaction().getNode().hasParent()) {
        getTransaction().moveToParent();
        if (getTransaction().getStructuralNode().hasRightSibling()) {
          getTransaction().moveToRightSibling();
          mKey = getTransaction().getNode().getNodeKey();

          if (getTransaction().getStructuralNode().hasRightSibling()) {
            // Push right sibling on a stack to reduce path
            // traversal.
            mRightSiblingStack.push(getTransaction().getStructuralNode()
              .getRightSiblingKey());
          }

          getTransaction().moveTo(currKey);
          return true;
        }
      }

    } else {
      // Get root key of sibling subtree.
      getTransaction().moveTo(mRightSiblingStack.pop());
      mKey = getTransaction().getNode().getNodeKey();

      if (getTransaction().getStructuralNode().hasRightSibling()) {
        // Push right sibling on a stack to reduce path traversal.
        mRightSiblingStack.push(getTransaction().getStructuralNode()
          .getRightSiblingKey());
      }

      getTransaction().moveTo(currKey);
      return true;

    }
    resetToStartKey();
    return false;
  }
}
