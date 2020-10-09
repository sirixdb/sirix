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
import org.sirix.settings.Fixed;
import org.sirix.utils.Pair;

/**
 * <p>
 * Iterate over all structural descendants starting at a given node (in preorder). Self might or
 * might not be included.
 * </p>
 */
public final class JsonDescendantAxis extends AbstractAxis {

  /** Stack for remembering next nodeKey in document order. */
  private Deque<Pair<Long, Integer>> mRightSiblingKeyStack;

  /** Determines if it's the first call to hasNext(). */
  private boolean mFirst;

  /** The current depth, starts with zero. */
  private int mDepth;

  /**
   * Constructor initializing internal state.
   *
   * @param cursor cursor to iterate with
   */
  public JsonDescendantAxis(final NodeCursor cursor) {
    super(cursor);
  }

  /**
   * Constructor initializing internal state.
   *
   * @param cursor cursor to iterate with
   * @param includeSelf determines if current node is included or not
   */
  public JsonDescendantAxis(final NodeCursor cursor, final IncludeSelf includeSelf) {
    super(cursor, includeSelf);
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    mFirst = true;
    mRightSiblingKeyStack = new ArrayDeque<>();
  }

  @Override
  protected long nextKey() {
    long key = Fixed.NULL_NODE_KEY.getStandardProperty();

    final NodeCursor cursor = getCursor();

    // Determines if first call to hasNext().
    if (mFirst) {
      mFirst = false;

      if (includeSelf() == IncludeSelf.YES) {
        key = cursor.getNodeKey();
      } else {
        key = cursor.getFirstChildKey();
        mDepth++;
      }

      return key;
    }

    // Always follow first child if there is one.
    if (cursor.hasFirstChild()) {
      key = cursor.getFirstChildKey();
      if (cursor.hasRightSibling()) {
        mRightSiblingKeyStack.push(new Pair<>(cursor.getRightSiblingKey(), mDepth));
      }
      mDepth++;
      return key;
    }

    // Then follow right sibling if there is one.
    if (cursor.hasRightSibling()) {
      key = cursor.getRightSiblingKey();

      if (mDepth == 0)
        return done();
      return key;
    }

    // Then follow right sibling on stack.
    if (mRightSiblingKeyStack.size() > 0) {
      final var pair = mRightSiblingKeyStack.pop();
      key = pair.getFirst();
      mDepth = pair.getSecond();

      if (mDepth == 0)
        return done();
      return key;
    }

    return done();
  }
}
