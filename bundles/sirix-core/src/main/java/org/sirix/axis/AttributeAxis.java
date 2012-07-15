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

import org.sirix.api.INodeReadTrx;
import org.sirix.node.EKind;
import org.sirix.node.ElementNode;

/**
 * <h1>AttributeAxis</h1>
 * 
 * <p>
 * Iterate over all attibutes of a given node.
 * </p>
 */
public final class AttributeAxis extends AbsAxis {

  /** Remember next key to visit. */
  private int mNextIndex;

  /**
   * Constructor initializing internal state.
   * 
   * @param paramRtx
   *          exclusive (immutable) mTrx to iterate with
   */
  public AttributeAxis(@Nonnull final INodeReadTrx pRtx) {
    super(pRtx);
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mNextIndex = 0;
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
    // move back to element, if there was already an attribute found. In
    // this
    // case the current node was set to an attribute by resetToLastKey()
    if (mNextIndex > 0) {
      assert getTransaction().getNode().getKind() == EKind.ATTRIBUTE;
      getTransaction().moveToParent();
    }

    if (getTransaction().getNode().getKind() == EKind.ELEMENT) {
      final ElementNode element = (ElementNode)getTransaction().getNode();
      if (mNextIndex < ((ElementNode)getTransaction().getNode())
        .getAttributeCount()) {
        mKey = element.getAttributeKey(mNextIndex);
        mNextIndex += 1;
        return true;
      }
    }
    resetToStartKey();
    return false;
  }

}
