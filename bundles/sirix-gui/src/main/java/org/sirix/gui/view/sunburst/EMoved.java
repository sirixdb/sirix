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

package org.sirix.gui.view.sunburst;

import java.util.Deque;

import org.sirix.api.INodeReadTrx;

/**
 * Determines movement of transaction and updates {@link Deque}s accordingly.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public enum EMoved {

  /** Start of traversal or cursor moved to a right sibling of a node. */
  STARTRIGHTSIBL {
    @Override
    public void processMove(final INodeReadTrx pRtx, final Item pItem, final Deque<Float> pAngleStack,
      final Deque<Float> pExtensionStack, final Deque<Integer> pParentStack,
      final Deque<Integer> pDescendantsStack) {
      // Do nothing.
    }

    @Override
    public void processCompareMove(final INodeReadTrx pRtx, final Item pItem, final Deque<Float> pAngleStack,
      final Deque<Float> pExtensionStack, final Deque<Integer> pDescendants,
      final Deque<Integer> pParentStack, final Deque<Integer> pModificationStack) {
      // Do nothing.
    }
  },

  /** Next node is a child of the current node. */
  CHILD {
    @Override
    public void processMove(final INodeReadTrx pRtx, final Item pItem, final Deque<Float> pAngleStack,
      final Deque<Float> pExtensionStack, final Deque<Integer> pParentStack,
      final Deque<Integer> pDescendantsStack) {
      assert !pAngleStack.isEmpty();
      pItem.mAngle = pAngleStack.peek();
      assert !pExtensionStack.isEmpty();
      pItem.mExtension = pExtensionStack.peek();
      assert !pParentStack.isEmpty();
      pItem.mIndexToParent = pParentStack.peek();
      assert !pDescendantsStack.isEmpty();
      pItem.mParentDescendantCount = pDescendantsStack.peek();
    }

    @Override
    public void processCompareMove(final INodeReadTrx pRtx, final Item pItem, final Deque<Float> pAngleStack,
      final Deque<Float> pExtensionStack, final Deque<Integer> pDescendantsStack,
      final Deque<Integer> pParentStack, final Deque<Integer> pModificationStack) {
      assert !pAngleStack.isEmpty();
      pItem.mAngle = pAngleStack.peek();
      assert !pExtensionStack.isEmpty();
      pItem.mExtension = pExtensionStack.peek();
      assert !pDescendantsStack.isEmpty();
      pItem.mParentDescendantCount = pDescendantsStack.peek();
      assert !pParentStack.isEmpty();
      pItem.mIndexToParent = pParentStack.peek();
      assert !pModificationStack.isEmpty();
      pItem.mParentModificationCount = pModificationStack.peek();
    }
  },

  /** Next node is the rightsibling of the first anchestor node which has one. */
  ANCHESTSIBL {
    @Override
    public void processMove(final INodeReadTrx pRtx, final Item pItem, final Deque<Float> pAngleStack,
      final Deque<Float> pExtensionStack, final Deque<Integer> pParentStack,
      final Deque<Integer> pDescendantsStack) {
      assert !pAngleStack.isEmpty();
      pItem.mAngle = pAngleStack.pop();
      assert !pExtensionStack.isEmpty();
      pItem.mAngle += pExtensionStack.pop();
      assert !pExtensionStack.isEmpty();
      pItem.mExtension = pExtensionStack.peek();
      assert !pParentStack.isEmpty();
      pParentStack.pop();
      assert !pParentStack.isEmpty();
      pItem.mIndexToParent = pParentStack.peek();
      assert !pDescendantsStack.isEmpty();
      pDescendantsStack.pop();
      assert !pDescendantsStack.isEmpty();
      pItem.mParentDescendantCount = pDescendantsStack.peek();
    }

    @Override
    public void processCompareMove(final INodeReadTrx pRtx, final Item pItem, final Deque<Float> pAngleStack,
      final Deque<Float> pExtensionStack, final Deque<Integer> pDescendantsStack,
      final Deque<Integer> pParentStack, final Deque<Integer> pModificationStack) {
      assert !pAngleStack.isEmpty();
      pItem.mAngle = pAngleStack.pop();
      assert !pExtensionStack.isEmpty();
      pItem.mAngle += pExtensionStack.pop();
      assert !pExtensionStack.isEmpty();
      pItem.mExtension = pExtensionStack.peek();
      assert !pParentStack.isEmpty();
      pParentStack.pop();
      assert !pParentStack.isEmpty();
      pItem.mIndexToParent = pParentStack.peek();
      assert !pDescendantsStack.isEmpty();
      pDescendantsStack.pop();
      assert !pDescendantsStack.isEmpty();
      pItem.mParentDescendantCount = pDescendantsStack.peek();
      assert !pModificationStack.isEmpty();
      pModificationStack.pop();
      assert !pModificationStack.isEmpty();
      pItem.mParentModificationCount = pModificationStack.peek();
    }
  };

  /**
   * Process movement of sirix {@link INodeReadTrx}.
   * 
   * @param pRtx
   *          sirix {@link INodeReadTrx}
   * @param pItem
   *          Item which does hold sunburst item angles and extensions
   * @param pAngleStack
   *          Deque for angles
   * @param pExtensionStack
   *          Deque for extensions
   * @param pParentStack
   *          Deque for parent indexes
   * @param pDescendantsStack
   *          Deque for descendants
   */
  public abstract void processMove(final INodeReadTrx pRtx, final Item pItem, final Deque<Float> pAngleStack,
    final Deque<Float> pExtensionStack, final Deque<Integer> pParentStack,
    final Deque<Integer> pDescendantsStack);

  /**
   * Process movement of sirix {@link INodeReadTrx}, while comparing revisions.
   * 
   * @param pRtx
   *          sirix {@link INodeReadTrx}
   * @param pItem
   *          Item which does hold sunburst item angles and extensions
   * @param pAngleStack
   *          Deque for angles
   * @param pExtensionStack
   *          Deque for extensions
   * @param pDescendantsStack
   *          Deque for descendants
   * @param pParentStack
   *          Deque for parent indexes
   * @param pModificationStack
   *          Deque for modifications
   */
  public abstract void processCompareMove(final INodeReadTrx pRtx, final Item pItem,
    final Deque<Float> pAngleStack, final Deque<Float> pExtensionStack,
    final Deque<Integer> pDescendantsStack, final Deque<Integer> pParentStack,
    final Deque<Integer> pModificationStack);
}
