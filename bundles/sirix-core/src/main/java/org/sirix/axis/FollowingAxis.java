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

import org.sirix.api.NodeCursor;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * <h1>FollowingAxis</h1>
 *
 * <p>
 * Iterate over all following nodes of kind ELEMENT or TEXT starting at a given node. Self is not
 * included.
 * </p>
 */
public final class FollowingAxis extends AbstractAxis {

  /** Determines if it's the first node. */
  private boolean mIsFirst;

  /** {@link Deque} reference to save right sibling keys. */
  private Deque<Long> mRightSiblingStack;

  /**
   * Constructor initializing internal state.
   *
   * @param cursor cursor to iterate with
   */
  public FollowingAxis(final NodeCursor cursor) {
    super(cursor);
    mIsFirst = true;
    mRightSiblingStack = new ArrayDeque<>();
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    mIsFirst = true;
    mRightSiblingStack = new ArrayDeque<>();
  }

  @Override
  protected long nextKey() {
    // Assure, that following is not evaluated on an attribute or a namespace.
    if (mIsFirst) {
      switch (getCursor().getKind()) {
        case ATTRIBUTE:
        case NAMESPACE:
          return done();
        // $CASES-OMITTED$
        default:
      }
    }

    final NodeCursor cursor = getCursor();
    final long currKey = cursor.getNodeKey();

    if (mIsFirst) {
      mIsFirst = false;

      /*
       * The first following is either a right sibling, or the right sibling of the first ancestor
       * that has a right sibling. Note: ancestors and descendants are no following node!
       */
      if (cursor.hasRightSibling()) {
        cursor.moveToRightSibling();
        final long key = cursor.getNodeKey();

        if (cursor.hasRightSibling()) {
          // Push right sibling on a stack to reduce path traversal.
          mRightSiblingStack.push(cursor.getRightSiblingKey());
        }

        cursor.moveTo(currKey);
        return key;
      }
      // Try to find the right sibling of one of the ancestors.
      while (cursor.hasParent()) {
        cursor.moveToParent();
        if (cursor.hasRightSibling()) {
          cursor.moveToRightSibling();
          final long key = cursor.getNodeKey();

          if (cursor.hasRightSibling()) {
            mRightSiblingStack.push(cursor.getRightSiblingKey());
          }
          cursor.moveTo(currKey);
          return key;
        }
      }
      // CurrentNode is last key in the document order.
      return done();
    }

    // Step down the tree in document order.
    if (cursor.hasFirstChild()) {
      cursor.moveToFirstChild();
      final long key = cursor.getNodeKey();

      if (cursor.hasRightSibling()) {
        // Push right sibling on a stack to reduce path traversal.
        mRightSiblingStack.push(cursor.getRightSiblingKey());
      }

      cursor.moveTo(currKey);
      return key;
    }

    if (mRightSiblingStack.isEmpty()) {
      // Try to find the right sibling of one of the ancestors.
      while (cursor.hasParent()) {
        cursor.moveToParent();
        if (cursor.hasRightSibling()) {
          cursor.moveToRightSibling();
          final long key = cursor.getNodeKey();

          if (cursor.hasRightSibling()) {
            // Push right sibling on a stack to reduce path
            // traversal.
            mRightSiblingStack.push(cursor.getRightSiblingKey());
          }

          cursor.moveTo(currKey);
          return key;
        }
      }
    } else {
      // Get root key of sibling subtree.
      cursor.moveTo(mRightSiblingStack.pop());
      final long key = cursor.getNodeKey();

      if (cursor.hasRightSibling()) {
        // Push right sibling on a stack to reduce path traversal.
        mRightSiblingStack.push(cursor.getRightSiblingKey());
      }

      cursor.moveTo(currKey);
      return key;
    }

    return done();
  }
}
