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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.INodeCursor;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.settings.EFixed;

/**
 * <h1>DescendantAxis</h1>
 * 
 * <p>
 * Iterate over all descendants of kind ELEMENT or TEXT starting at a given node. Self is not included.
 * </p>
 */
public final class DescendantAxis extends AbsAxis {

  /** Stack for remembering next nodeKey in document order. */
  private Deque<Long> mRightSiblingKeyStack;

  /** Determines if it's the first call to hasNext(). */
  private boolean mFirst;

  /**
   * Constructor initializing internal state.
   * 
   * @param pRtx
   *          exclusive (immutable) trx to iterate with
   */
  public DescendantAxis(@Nonnull final INodeCursor pRtx) {
    super(pRtx);
  }

  /**
   * Constructor initializing internal state.
   * 
   * @param pRtx
   *          Exclusive (immutable) trx to iterate with.
   * @param pIncludeSelf
   *          Is self included?
   */
  public DescendantAxis(@Nonnull final INodeCursor pRtx,
    @Nonnull final EIncludeSelf pIncludeSelf) {
    super(pRtx, pIncludeSelf);
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mFirst = true;
    mRightSiblingKeyStack = new ArrayDeque<>();
  }

  @Override
  public boolean hasNext() {
    if (!isHasNext()) {
      return false;
    }
    if (isNext()) {
      return true;
    }

    resetToLastKey();

    // Determines if first call to hasNext().
    if (mFirst) {
      mFirst = false;

      if (isSelfIncluded() == EIncludeSelf.YES) {
        mKey = getTransaction().getNodeKey();
      } else {
        mKey = getTransaction().getFirstChildKey();
      }

      if (mKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
        resetToStartKey();
        return false;
      }
      return true;
    }

    // Always follow first child if there is one.
    if (getTransaction().hasFirstChild()) {
      mKey = getTransaction().getFirstChildKey();
      if (getTransaction().hasRightSibling()) {
        mRightSiblingKeyStack.push(getTransaction().getRightSiblingKey());
      }
      return true;
    }

    // Then follow right sibling if there is one.
    if (getTransaction().hasRightSibling()) {
      final long currKey = getTransaction().getNodeKey();
      mKey = getTransaction().getRightSiblingKey();
      return hasNextNode(currKey);
    }

    // Then follow right sibling on stack.
    if (mRightSiblingKeyStack.size() > 0) {
      final long currKey = getTransaction().getNodeKey();
      mKey = mRightSiblingKeyStack.pop();
      return hasNextNode(currKey);
    }

    // Then end.
    resetToStartKey();
    return false;
  }

  /**
   * Determines if the subtree-traversal is finished.
   * 
   * @param pCurrKey
   *          current node key
   * @return {@code false} if finished, {@code true} if not
   */
  private boolean hasNextNode(@Nonnegative final long pCurrKey) {
    getTransaction().moveTo(mKey);
    if (getTransaction().getLeftSiblingKey() == getStartKey()) {
      resetToStartKey();
      return false;
    } else {
      getTransaction().moveTo(pCurrKey);
      return true;
    }
  }
}
