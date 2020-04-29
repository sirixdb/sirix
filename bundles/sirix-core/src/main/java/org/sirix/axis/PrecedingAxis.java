/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.axis;

import java.util.ArrayDeque;
import java.util.Deque;
import org.sirix.api.NodeCursor;
import org.sirix.node.NodeKind;

/**
 * <p>
 * Iterate over all preceding nodes of kind ELEMENT or TEXT starting at a given node. Self is not
 * included. Note that the nodes are retrieved in reverse document order.
 * </p>
 */
public final class PrecedingAxis extends AbstractAxis {

  /** Determines if it's the first call or not. */
  private boolean mIsFirst;

  /** Stack to save nodeKeys. */
  private Deque<Long> mStack;

  /**
   * Constructor initializing internal state.
   *
   * @param cursor cursor to iterate with
   */
  public PrecedingAxis(final NodeCursor cursor) {
    super(cursor);
    mIsFirst = true;
    mStack = new ArrayDeque<>();
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    mIsFirst = true;
    mStack = new ArrayDeque<>();
  }

  @Override
  protected long nextKey() {
    final NodeCursor cursor = getCursor();

    // Assure, that preceding is not evaluated on an attribute or a namespace.
    if (mIsFirst) {
      mIsFirst = false;
      if (cursor.getKind() == NodeKind.ATTRIBUTE || cursor.getKind() == NodeKind.NAMESPACE) {
        return done();
      }
    }

    // Current node key.
    final long key = cursor.getNodeKey();

    if (!mStack.isEmpty()) {
      // Return all nodes of the current subtree in reverse document order.
      return mStack.pop();
    }

    if (cursor.hasLeftSibling()) {
      cursor.moveToLeftSibling();
      /*
       * Because this axis return the precedings in reverse document order, we need to iterate to
       * the node in the subtree, that comes last in document order.
       */
      getLastChild();
      final long nodeKey = cursor.getNodeKey();
      cursor.moveTo(key);
      return nodeKey;
    }

    while (cursor.hasParent()) {
      // Ancestors are not part of the preceding set.
      cursor.moveToParent();
      if (cursor.hasLeftSibling()) {
        cursor.moveToLeftSibling();
        // Move to last node in the subtree.
        getLastChild();
        final long nodeKey = cursor.getNodeKey();
        cursor.moveTo(key);
        return nodeKey;
      }
    }

    return done();
  }

  /**
   * Moves the transaction to the node in the current subtree, that is last in document order and
   * pushes all other node key on a stack. At the end the stack contains all node keys except for
   * the last one in reverse document order.
   */
  private void getLastChild() {
    final NodeCursor cursor = getCursor();

    // Nodekey of the root of the current subtree.
    final long parent = cursor.getNodeKey();

    /*
     * Traverse tree in pre order to the leftmost leaf of the subtree and push all nodes to the
     * stack
     */
    if (cursor.hasFirstChild()) {
      while (cursor.hasFirstChild()) {
        mStack.push(cursor.getNodeKey());
        cursor.moveToFirstChild();
      }

      /*
       * Traverse all the siblings of the leftmost leave and all their descendants and push all of
       * them to the stack
       */
      while (cursor.hasRightSibling()) {
        mStack.push(cursor.getNodeKey());
        cursor.moveToRightSibling();
        getLastChild();
      }

      /*
       * Step up the path till the root of the current subtree and process all right siblings and
       * their descendants on each step.
       */
      if (cursor.hasParent() && (cursor.getParentKey() != parent)) {
        mStack.push(cursor.getNodeKey());
        while (cursor.hasParent() && (cursor.getParentKey() != parent)) {
          cursor.moveToParent();

          /*
           * Traverse all the siblings of the leftmost leave and all their descendants and push all
           * of them to the stack
           */
          while (cursor.hasRightSibling()) {
            cursor.moveToRightSibling();
            getLastChild();
            mStack.push(cursor.getNodeKey());
          }
        }

        /*
         * Set cursor to the node in the subtree that is last in document order.
         */
        cursor.moveTo(mStack.pop());
      }
    }
  }
}
