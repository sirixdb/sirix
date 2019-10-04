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

package org.sirix.axis.visitor;

import org.sirix.api.NodeCursor;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.*;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.AbstractAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.settings.Fixed;

import javax.annotation.Nonnegative;
import java.util.ArrayDeque;
import java.util.Deque;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <h1>VisitorDescendantAxis</h1>
 *
 * <p>
 * Iterate over all descendants of any structural kind starting at a given node by it's unique node
 * key. The currently located node is optionally included. Furthermore a {@link NodeVisitor} is
 * usable to guide the traversal and do whatever you like with the node kind, which is selected by
 * the given {@link NodeCursor} transaction.
 * </p>
 * <p>
 * Note that it is faster to use the standard {@link DescendantAxis} if no visitor is specified.
 * </p>
 *
 * @author Johannes Lichtenberger, University of Konstanz
 */
public final class VisitorDescendantAxis extends AbstractAxis {

  /** Stack for remembering next nodeKey in document order. */
  private Deque<Long> mRightSiblingKeyStack;

  /** Optional visitor. */
  private NodeVisitor mVisitor;

  /** Determines if it is the first call. */
  private boolean mFirst;

  /**
   * Get a new builder instance.
   *
   * @param cursor the cursor to iterate with
   * @return {@link Builder} instance
   */
  public static Builder newBuilder(final NodeCursor cursor) {
    return new Builder(cursor);
  }

  /** The builder. */
  public static class Builder {

    /** Optional visitor. */
    private NodeVisitor mVisitor;

    /** Sirix {@link XmlNodeReadOnlyTrx}. */
    private final NodeCursor mRtx;

    /** Determines if current node should be included or not. */
    private IncludeSelf mIncludeSelf = IncludeSelf.NO;

    /**
     * Constructor.
     *
     * @param rtx Sirix {@link NodeCursor}
     */
    public Builder(final NodeCursor rtx) {
      mRtx = checkNotNull(rtx);
    }

    /**
     * Set include self option.
     *
     * @param pIncludeSelf include self
     * @return this builder instance
     */
    public Builder includeSelf() {
      mIncludeSelf = IncludeSelf.YES;
      return this;
    }

    /**
     * Set visitor.
     *
     * @param visitor the visitor
     * @return this builder instance
     */
    public Builder visitor(final NodeVisitor visitor) {
      mVisitor = checkNotNull(visitor);
      return this;
    }

    /**
     * Build a new instance.
     *
     * @return new {@link DescendantAxis} instance
     */
    public VisitorDescendantAxis build() {
      return new VisitorDescendantAxis(this);
    }
  }

  /**
   * Private constructor.
   *
   * @param builder the builder to construct a new instance
   */
  private VisitorDescendantAxis(final Builder builder) {
    super(builder.mRtx, builder.mIncludeSelf);
    mVisitor = builder.mVisitor;
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    mFirst = true;
    mRightSiblingKeyStack = new ArrayDeque<>();
  }

  @Override
  protected long nextKey() {
    // Visitor.
    VisitResult result = null;

    if (mVisitor != null) {
      if (getTrx() instanceof XmlNodeReadOnlyTrx)
        result = asXdmNodeReadTrx().acceptVisitor((XmlNodeVisitor) mVisitor);
      else if (getTrx() instanceof JsonNodeReadOnlyTrx)
        result = asJsonNodeReadTrx().acceptVisitor((JsonNodeVisitor) mVisitor);
      else
        throw new AssertionError();
    }

    // If visitor is present and the return value is EVisitResult.TERMINATE than
    // return false.
    if (VisitResultType.TERMINATE == result) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }

    final NodeCursor cursor = getCursor();

    // Determines if first call to hasNext().
    if (mFirst) {
      mFirst = false;
      return isSelfIncluded() == IncludeSelf.YES
          ? cursor.getNodeKey()
          : cursor.getFirstChildKey();
    }

    // If visitor is present and the the righ sibling stack must be adapted.
    if (LocalVisitResult.SKIPSUBTREEPOPSTACK == result) {
      mRightSiblingKeyStack.pop();
    }

    // If visitor is present and result is not
    // EVisitResult.SKIPSUBTREE/EVisitResult.SKIPSUBTREEPOPSTACK or visitor is
    // not present.
    if ((result != null && result != VisitResultType.SKIPSUBTREE && result != LocalVisitResult.SKIPSUBTREEPOPSTACK)
        || result == null) {
      // Always follow first child if there is one.
      if (cursor.hasFirstChild()) {
        final long key = cursor.getFirstChildKey();
        final long rightSiblNodeKey = cursor.getRightSiblingKey();
        if (cursor.hasRightSibling() && (mRightSiblingKeyStack.isEmpty()
            || (!mRightSiblingKeyStack.isEmpty() && mRightSiblingKeyStack.peek() != rightSiblNodeKey))) {
          mRightSiblingKeyStack.push(rightSiblNodeKey);
        }
        return key;
      }
    }

    // If visitor is present and result is not EVisitResult.SKIPSIBLINGS or
    // visitor is not present.
    if ((result != null && result != VisitResultType.SKIPSIBLINGS) || result == null) {
      // Then follow right sibling if there is one.
      if (cursor.hasRightSibling()) {
        final long nextKey = cursor.getRightSiblingKey();
        return hasNextNode(nextKey, cursor.getNodeKey());
      }
    }

    // Then follow right sibling on stack.
    if (mRightSiblingKeyStack.size() > 0) {
      final long nextKey = mRightSiblingKeyStack.pop();
      return hasNextNode(nextKey, cursor.getNodeKey());
    }

    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  /**
   * Determines if next node is not a right sibling of the current node.
   *
   * @param currKey node key of current node
   */
  private long hasNextNode(final @Nonnegative long nextKey, final @Nonnegative long currKey) {
    // Fail if the subtree is finished.
    final NodeCursor cursor = getCursor();
    cursor.moveTo(nextKey);
    if (cursor.getLeftSiblingKey() == getStartKey()) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    } else {
      cursor.moveTo(currKey);
      return nextKey;
    }
  }
}
