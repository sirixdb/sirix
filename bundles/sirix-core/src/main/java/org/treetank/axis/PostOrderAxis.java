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

package org.treetank.axis;

import org.treetank.api.INodeReadTrx;
import org.treetank.settings.EFixed;

/**
 * <h1>PostOrder</h1>
 * 
 * <p>
 * Iterate over the whole tree starting with the last node.
 * </p>
 */
public class PostOrderAxis extends AbsAxis {

  private boolean mMovedToParent;

  private boolean mDone;

  /**
   * Constructor initializing internal state.
   * 
   * @param pRtx
   *          exclusive (immutable) trx to iterate with
   */
  public PostOrderAxis(final INodeReadTrx pRtx) {
    super(pRtx);
  }

  @Override
  public final void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mKey = pNodeKey;
    mMovedToParent = false;
    mDone = false;
  }

  @Override
  public boolean hasNext() {
    if (isNext()) {
      return true;
    } else {
      resetToLastKey();

      if (mDone) {
        resetToStartKey();
        return false;
      }

      final long currKey = mKey;
      if ((!mMovedToParent && getTransaction().getStructuralNode().hasFirstChild())
        || (getTransaction().getStructuralNode().hasRightSibling() && getTransaction().moveToRightSibling())) {
        while (getTransaction().getStructuralNode().hasFirstChild()) {
          getTransaction().moveTo(getTransaction().getStructuralNode().getFirstChildKey());
        }

        mKey = getTransaction().getNode().getNodeKey();
        getTransaction().moveTo(currKey);
        return true;
      }

      if (getTransaction().getStructuralNode().hasRightSibling()) {
        mKey = getTransaction().getStructuralNode().getRightSiblingKey();
      } else {
        mKey = getTransaction().getNode().getParentKey();
        mMovedToParent = true;
      }

      if (mKey == EFixed.NULL_NODE_KEY.getStandardProperty()) {
        resetToStartKey();
        return false;
      }

      if (mKey == getStartKey()) {
        mDone = true;
      }

      getTransaction().moveTo(currKey);
      return true;
    }
  }

}
