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

import org.sirix.api.INodeTraversal;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.IStructNode;

/**
 * <h1>PrecedingAxis</h1>
 * 
 * <p>
 * Iterate over all preceding nodes of kind ELEMENT or TEXT starting at a given node. Self is not included.
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
  public PrecedingAxis(@Nonnull final INodeTraversal pRtx) {
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
  public boolean hasNext() {
    if (!isHasNext()) {
      return false;
    }
    if (isNext()) {
      return true;
    }
    // Assure, that preceding is not evaluated on an attribute or a namespace.
    if (mIsFirst) {
      mIsFirst = false;
      if (getTransaction().getNode().getKind() == EKind.ATTRIBUTE
        || getTransaction().getNode().getKind() == EKind.NAMESPACE) {
        resetToStartKey();
        return false;
      }
    }

    resetToLastKey();

    // Current node key.
    final long key = mKey;

    if (!mStack.isEmpty()) {
      // Return all nodes of the current subtree in reverse document order.
      mKey = mStack.pop();
      return true;
    }

    if (getTransaction().getStructuralNode().hasLeftSibling()) {
      getTransaction().moveToLeftSibling();
      /*
       * Because this axis return the precedings in reverse document
       * order, we need to iterate to the node in the subtree, that comes last in
       * document order.
       */
      getLastChild();
      mKey = getTransaction().getNode().getNodeKey();
      getTransaction().moveTo(key);
      return true;
    }

    while (getTransaction().getNode().hasParent()) {
      // Ancestors are not part of the preceding set.
      getTransaction().moveToParent();
      if (getTransaction().getStructuralNode().hasLeftSibling()) {
        getTransaction().moveToLeftSibling();
        // Move to last node in the subtree.
        getLastChild();
        mKey = getTransaction().getNode().getNodeKey();
        getTransaction().moveTo(key);
        return true;
      }
    }

    resetToStartKey();
    return false;
  }

  /**
   * Moves the transaction to the node in the current subtree, that is last in
   * document order and pushes all other node key on a stack. At the end the
   * stack contains all node keys except for the last one in reverse document
   * order.
   */
  private void getLastChild() {
    // Nodekey of the root of the current subtree.
    final long parent = getTransaction().getNode().getNodeKey();

    /*
     * Traverse tree in pre order to the leftmost leaf of the subtree and
     * push all nodes to the stack
     */
    if (((IStructNode)getTransaction().getNode()).hasFirstChild()) {
      while (((IStructNode)getTransaction().getNode()).hasFirstChild()) {
        mStack.push(getTransaction().getNode().getNodeKey());
        getTransaction().moveToFirstChild();
      }

      /*
       * Traverse all the siblings of the leftmost leave and all their
       * descendants and push all of them to the stack
       */
      while (getTransaction().getStructuralNode().hasRightSibling()) {
        mStack.push(getTransaction().getNode().getNodeKey());
        getTransaction().moveToRightSibling();
        getLastChild();
      }

      /*
       * Step up the path till the root of the current subtree and process
       * all right siblings and their descendants on each step.
       */
      if (getTransaction().getNode().hasParent()
        && (getTransaction().getNode().getParentKey() != parent)) {

        mStack.push(getTransaction().getNode().getNodeKey());
        while (getTransaction().getNode().hasParent()
          && (getTransaction().getNode().getParentKey() != parent)) {

          getTransaction().moveToParent();

          /*
           * Traverse all the siblings of the leftmost leave and all
           * their descendants and push all of them to the stack
           */
          while (getTransaction().getStructuralNode().hasRightSibling()) {
            getTransaction().moveToRightSibling();
            getLastChild();
            mStack.push(getTransaction().getNode().getNodeKey());
          }
        }

        /*
         * Set transaction to the node in the subtree that is last in
         * document order.
         */
        getTransaction().moveTo(mStack.pop());
      }
    }
  }
}
