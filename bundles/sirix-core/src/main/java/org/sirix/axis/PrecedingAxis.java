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
import org.sirix.api.INodeReadTrx;
import org.sirix.node.EKind;
import org.sirix.settings.EFixed;

/**
 * <h1>PrecedingAxis</h1>
 * 
 * <p>
 * Iterate over all preceding nodes of kind ELEMENT or TEXT starting at a given node. Self is not included.
 * Note that the nodes are retrieved in reverse document order.
 * </p>
 */
public final class PrecedingAxis extends AbsAxis {

  /** Determines if it's the first call or not. */
  private boolean mIsFirst;

  /** Stack to save nodeKeys. */
  private Deque<Long> mStack;

  /**
   * Constructor initializing internal state.
   * 
   * @param pRtx
   *          exclusive (immutable) trx to iterate with
   */
  public PrecedingAxis(@Nonnull final INodeCursor pRtx) {
    super(pRtx);
    mIsFirst = true;
    mStack = new ArrayDeque<>();
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mIsFirst = true;
    mStack = new ArrayDeque<>();
  }
  
  @Override
  protected long nextKey() {
		final INodeReadTrx rtx = getTrx();
		
    // Assure, that preceding is not evaluated on an attribute or a namespace.
    if (mIsFirst) {
      mIsFirst = false;
      if (rtx.getKind() == EKind.ATTRIBUTE
        || rtx.getKind() == EKind.NAMESPACE) {
        return done();
      }
    }

    // Current node key.
    final long key = rtx.getNodeKey();

    if (!mStack.isEmpty()) {
      // Return all nodes of the current subtree in reverse document order.
      return mStack.pop();
    }

    if (rtx.hasLeftSibling()) {
      getTrx().moveToLeftSibling();
      /*
       * Because this axis return the precedings in reverse document
       * order, we need to iterate to the node in the subtree, that comes last in
       * document order.
       */
      getLastChild();
      final long nodeKey = rtx.getNodeKey();
      getTrx().moveTo(key);
      return nodeKey;
    }

    while (rtx.hasParent()) {
      // Ancestors are not part of the preceding set.
      getTrx().moveToParent();
      if (rtx.hasLeftSibling()) {
        getTrx().moveToLeftSibling();
        // Move to last node in the subtree.
        getLastChild();
        final long nodeKey = rtx.getNodeKey();
        getTrx().moveTo(key);
        return nodeKey;
      }
    }
    
    return done();
  }

  /**
   * Moves the transaction to the node in the current subtree, that is last in
   * document order and pushes all other node key on a stack. At the end the
   * stack contains all node keys except for the last one in reverse document
   * order.
   */
  private void getLastChild() {
  	final INodeReadTrx rtx = getTrx();
  	
    // Nodekey of the root of the current subtree.
    final long parent = rtx.getNodeKey();

    /*
     * Traverse tree in pre order to the leftmost leaf of the subtree and
     * push all nodes to the stack
     */
    if (rtx.hasFirstChild()) {
      while (rtx.hasFirstChild()) {
        mStack.push(rtx.getNodeKey());
        getTrx().moveToFirstChild();
      }

      /*
       * Traverse all the siblings of the leftmost leave and all their
       * descendants and push all of them to the stack
       */
      while (rtx.hasRightSibling()) {
        mStack.push(rtx.getNodeKey());
        getTrx().moveToRightSibling();
        getLastChild();
      }

      /*
       * Step up the path till the root of the current subtree and process
       * all right siblings and their descendants on each step.
       */
      if (rtx.hasParent()
        && (rtx.getParentKey() != parent)) {

        mStack.push(rtx.getNodeKey());
        while (rtx.hasParent()
          && (rtx.getParentKey() != parent)) {

          getTrx().moveToParent();

          /*
           * Traverse all the siblings of the leftmost leave and all
           * their descendants and push all of them to the stack
           */
          while (rtx.hasRightSibling()) {
            getTrx().moveToRightSibling();
            getLastChild();
            mStack.push(rtx.getNodeKey());
          }
        }

        /*
         * Set transaction to the node in the subtree that is last in
         * document order.
         */
        getTrx().moveTo(mStack.pop());
      }
    }
  }
}
