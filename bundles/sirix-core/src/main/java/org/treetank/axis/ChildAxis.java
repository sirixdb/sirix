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
import org.treetank.node.interfaces.IStructNode;

/**
 * <h1>ChildAxis</h1>
 * 
 * <p>
 * Iterate over all children of kind ELEMENT or TEXT starting at a given node. Self is not included.
 * </p>
 */
public class ChildAxis extends AbsAxis {

  /** Has another child node. */
  private boolean mFirst;

  /**
   * Constructor initializing internal state.
   * 
   * @param paramRtx
   *          exclusive (immutable) trx to iterate with.
   */
  public ChildAxis(final INodeReadTrx paramRtx) {
    super(paramRtx);
  }

  @Override
  public final void reset(final long paramNodeKey) {
    super.reset(paramNodeKey);
    mFirst = true;
  }

  @Override
  public final boolean hasNext() {
    if (isNext()) {
      return true;
    } else {
      resetToLastKey();
      final IStructNode node = getTransaction().getStructuralNode();
      if (!mFirst && node.hasRightSibling()) {
        mKey = node.getRightSiblingKey();
        return true;
      } else if (mFirst && getTransaction().getStructuralNode().hasFirstChild()) {
        mFirst = false;
        mKey = node.getFirstChildKey();
        return true;
      } else {
        resetToStartKey();
        return false;
      }
    }
  }

}
