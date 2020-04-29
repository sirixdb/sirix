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
import org.sirix.settings.Fixed;

/**
 * <p>
 * Iterate over the whole tree starting with the last node.
 * </p>
 */
public final class PostOrderAxis extends AbstractAxis {

  /** Determines if transaction moved to the parent before. */
  private boolean mMovedToParent;

  /** Determines if current key is the start node key before the traversal. */
  private boolean mIsStartKey;

  /**
   * Constructor initializing internal state.
   *
   * @param cursor cursor to iterate with
   */
  public PostOrderAxis(final NodeCursor cursor) {
    super(cursor);
  }

  /**
   * Constructor initializing internal state.
   *
   * @param cursor cursor to iterate with
   */
  public PostOrderAxis(final NodeCursor cursor, final IncludeSelf includeSelf) {
    super(cursor, includeSelf);
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    mMovedToParent = false;
    mIsStartKey = false;
  }

  @Override
  protected long nextKey() {
    final NodeCursor cursor = getCursor();

    // No subtree.
    if (!cursor.hasFirstChild() && cursor.getNodeKey() == getStartKey() || mIsStartKey) {
      if (!mIsStartKey && isSelfIncluded() == IncludeSelf.YES) {
        mIsStartKey = true;
        return cursor.getNodeKey();
      } else {
        return done();
      }
    }

    final long currKey = cursor.getNodeKey();

    // Move down in the tree if it hasn't moved down before.
    if ((!mMovedToParent && cursor.hasFirstChild())
        || (cursor.hasRightSibling() && (cursor.moveToRightSibling().hasMoved()))) {
      while (cursor.hasFirstChild()) {
        cursor.moveToFirstChild();
      }

      final long key = cursor.getNodeKey();
      cursor.moveTo(currKey);
      return key;
    }

    // Move to the right sibling or parent node after walking down.
    long key = 0;
    if (cursor.hasRightSibling()) {
      key = cursor.getRightSiblingKey();
    } else {
      key = cursor.getParentKey();
      mMovedToParent = true;
    }

    // Stop traversal if needed.
    if (key == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      return key;
    }

    // Traversal is at start key.
    if (key == getStartKey()) {
      if (isSelfIncluded() == IncludeSelf.YES) {
        mIsStartKey = true;
        return key;
      } else {
        return done();
      }
    }

    // Move back to current node.
    cursor.moveTo(currKey);
    return key;
  }
}
