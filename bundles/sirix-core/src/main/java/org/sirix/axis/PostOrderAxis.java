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

import javax.annotation.Nonnull;

import org.sirix.api.INodeTraversal;
import org.sirix.settings.EFixed;

/**
 * <h1>PostOrder</h1>
 * 
 * <p>
 * Iterate over the whole tree starting with the last node.
 * </p>
 */
public final class PostOrderAxis extends AbsAxis {

  private boolean mMovedToParent;

  private boolean mIsStartKey;

  /**
   * Constructor initializing internal state.
   * 
   * @param pRtx
   *          exclusive (immutable) trx to iterate with
   */
  public PostOrderAxis(@Nonnull final INodeTraversal pRtx) {
    super(pRtx);
  }

  /**
   * Constructor initializing internal state.
   * 
   * @param pRtx
   *          exclusive (immutable) trx to iterate with
   */
  public PostOrderAxis(@Nonnull final INodeTraversal pRtx,
    @Nonnull final EIncludeSelf pIncludeSelf) {
    super(pRtx, pIncludeSelf);
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mKey = pNodeKey;
    mMovedToParent = false;
    mIsStartKey = false;
  }

  @Override
  public boolean hasNext() {
    if (isNext()) {
      return true;
    }

    resetToLastKey();

    // No subtree.
    if (!getTransaction().getStructuralNode().hasFirstChild()
      && getTransaction().getNode().getNodeKey() == getStartKey()
      || mIsStartKey) {
      if (!mIsStartKey && isSelfIncluded() == EIncludeSelf.YES) {
        mIsStartKey = true;
        return true;
      } else {
        resetToStartKey();
        return false;
      }
    }

    final long currKey = mKey;

    // Move down in the tree if it hasn't moved down before.
    if ((!mMovedToParent && getTransaction().getStructuralNode()
      .hasFirstChild())
      || (getTransaction().getStructuralNode().hasRightSibling() && getTransaction()
        .moveToRightSibling())) {
      while (getTransaction().getStructuralNode().hasFirstChild()) {
        getTransaction().moveTo(
          getTransaction().getStructuralNode().getFirstChildKey());
      }

      mKey = getTransaction().getNode().getNodeKey();
      getTransaction().moveTo(currKey);
      return true;
    }

    // Move to the right sibling or parent node after walking down.
    if (getTransaction().getStructuralNode().hasRightSibling()) {
      mKey = getTransaction().getStructuralNode().getRightSiblingKey();
    } else {
      mKey = getTransaction().getNode().getParentKey();
      mMovedToParent = true;
    }

    // Stop traversal if needed.
    if (mKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
      resetToStartKey();
      return false;
    }

    // Traversal is at start key.
    if (mKey == getStartKey()) {
      if (isSelfIncluded() == EIncludeSelf.YES) {
        mIsStartKey = true;
        return true;
      } else {
        resetToStartKey();
        return false;
      }
    }

    // Move back to current node.
    getTransaction().moveTo(currKey);
    return true;
  }

}
