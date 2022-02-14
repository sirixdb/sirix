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

package org.sirix.service.xml.xpath;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.api.Axis;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.IncludeSelf;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.settings.Fixed;

/**
 *
 * <p>
 * Provide standard Java iterator capability compatible with the new enhanced for loop available
 * since Java 5.
 *
 * Override the "template method" {@code nextKey()} to implement an axis.
 * </p>
 */
public abstract class AbstractAxis implements Axis {

  /** Iterate over transaction exclusive to this step. */
  private final NodeCursor mRtx;

  /** Key of next node. */
  protected long mKey;

  /**
   * Make sure {@code next()} can only be called after {@code hasNext()} has been called.
   */
  private boolean mNext;

  /** Key of node where axis started. */
  private long mStartKey;

  /** Include self? */
  private final IncludeSelf mIncludeSelf;

  /** Determines if a next node follows or not. */
  private boolean mHasNext;

  /**
   * Bind axis step to transaction.
   *
   * @param rtx transaction to operate with
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  public AbstractAxis(final NodeCursor rtx) {
    mRtx = checkNotNull(rtx);
    mIncludeSelf = IncludeSelf.NO;
    mHasNext = true;
    reset(rtx.getNodeKey());
  }

  /**
   * Bind axis step to transaction.
   *
   * @param pRtx transaction to operate with
   * @param pIncludeSelf determines if self is included
   */
  public AbstractAxis(final NodeCursor pRtx, final IncludeSelf pIncludeSelf) {
    mRtx = checkNotNull(pRtx);
    mIncludeSelf = checkNotNull(pIncludeSelf);
    mHasNext = true;
    reset(pRtx.getNodeKey());
  }

  @Override
  public final Iterator<Long> iterator() {
    return this;
  }

  /**
   * Signals that axis traversal is done, that is {@code hasNext()} must return false. Can be called
   * from subclasses to signal that axis is done.
   *
   * @return null node key
   */
  protected long done() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  /**
   * {@inheritDoc}
   *
   * <p>
   * During the last call to {@code hasNext()}, that is {@code hasNext()} returns false, the
   * transaction is reset to the start key.
   * </p>
   *
   * <p>
   * <strong>Implementors should implement {@code nextKey()} instead which is a template method called
   * from this {@code hasNext()} method.</strong>
   * </p>
   */
  @Override
  public boolean hasNext() {
    if (!isHasNext()) {
      // End of the axis reached.
      return false;
    }
    if (isNext()) {
      // hasNext() has been called before without an intermediate next()-call.
      return true;
    }

    // Reset to last node key.
    resetToLastKey();

    // Template method.
    mKey = nextKey();

    if (mKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      // Reset to the start key before invoking the axis.
      resetToStartKey();
      return false;
    } else {
      return true;
    }
  }

  /**
   * Please do not override {@code hasNext()} directly. Use this template method instead. It
   * determines the next node key in the axis. Override this method to simplify {@code hasNext()}.
   * Simply return {@code EFixed.NULL_NODE_KEY.getStandardProperty()} if no more node is following in
   * the axis, otherwise return the node key of the next node.
   *
   * @return next node key
   */
  protected long nextKey() {
    return 0;
  }

  @Override
  public final Long next() {
    if (!mHasNext) {
      throw new NoSuchElementException("No more nodes in the axis!");
    }
    if (!mNext) {
      if (!hasNext()) {
        throw new NoSuchElementException("No more nodes in the axis!");
      }
    }
    if (mKey >= 0) {
      if (mRtx.hasNode(mKey)) {
        mRtx.moveTo(mKey);
      } else {
        throw new IllegalStateException("Failed to move to nodeKey: " + mKey);
      }
    } else {
      mRtx.moveTo(mKey);
    }
    mNext = false;
    return mKey;
  }

  @Override
  public final void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Resetting the nodekey of this axis to a given nodekey.
   *
   * @param pNodeKey the nodekey where the reset should occur to
   */
  @Override
  public void reset(@NonNegative final long pNodeKey) {
    mStartKey = pNodeKey;
    mKey = pNodeKey;
    mNext = false;
    mHasNext = true;
  }

  @Override
  public XmlNodeReadOnlyTrx asXdmNodeReadTrx() {
    if (mRtx instanceof NodeReadOnlyTrx) {
      return (XmlNodeReadOnlyTrx) mRtx;
    }
    throw new ClassCastException("Node cursor is no XDM node transaction.");
  }

  @Override
  public JsonNodeReadOnlyTrx asJsonNodeReadTrx() {
    throw new ClassCastException("Node cursor is no JSON node transaction.");
  }

  @Override
  public NodeReadOnlyTrx getTrx() {
    if (mRtx instanceof NodeReadOnlyTrx) {
      return (XmlNodeReadOnlyTrx) mRtx;
    }
    throw new IllegalStateException("Node cursor is no transaction cursor.");
  }

  @Override
  public PathSummaryReader asPathSummary() {
    if (mRtx instanceof PathSummaryReader) {
      return (PathSummaryReader) mRtx;
    }
    throw new ClassCastException("Node cursor is no path summary reader.");
  }

  @Override
  public NodeCursor getCursor() {
    return mRtx;
  }

  /**
   * Determines if axis might have more results.
   *
   * @return {@code true} if axis might have more results, {@code false} otherwise
   */
  public boolean isHasNext() {
    return mHasNext;
  }

  /**
   * Make sure the transaction points to the node it started with. This must be called just before
   * {@code hasNext() == false}.
   *
   * @return key of node where transaction was before the first call of {@code hasNext()}
   */
  protected final long resetToStartKey() {
    // No check because of IAxis Convention 4.
    mRtx.moveTo(mStartKey);
    mNext = false;
    mHasNext = false;
    return mStartKey;
  }

  /**
   * Make sure the transaction points to the node after the last hasNext(). This must be called first
   * in hasNext().
   *
   * @return key of node where transaction was after the last call of {@code hasNext()}
   */
  protected final long resetToLastKey() {
    // No check because of IAxis Convention 4.
    mRtx.moveTo(mKey);
    mNext = true;
    return mKey;
  }

  @Override
  public final long getStartKey() {
    return mStartKey;
  }

  @Override
  public final IncludeSelf includeSelf() {
    return mIncludeSelf;
  }

  @Override
  public Long peek() {
    return null;
  }

  /**
   * Get mNext which determines if {@code hasNext()} has at least been called once before the call to
   * {@code next()}.
   *
   * @return {@code true} if {@code hasNext()} has been called before calling {@code next()},
   *         {@code false} otherwise
   */
  public final boolean isNext() {
    return mNext;
  }

  /**
   * Implements a simple foreach-method.
   *
   * @param pVisitor {@link XmlNodeVisitor} implementation
   */
  @Override
  public final void foreach(final XmlNodeVisitor pVisitor) {
    checkNotNull(pVisitor);
    for (; hasNext(); next()) {
      ((XmlNodeReadOnlyTrx) mRtx).acceptVisitor(pVisitor);
    }
  }

  @Override
  public synchronized final long nextNode() {
    synchronized (mRtx) {
      long retVal = Fixed.NULL_NODE_KEY.getStandardProperty();
      if (hasNext()) {
        retVal = next();
      }
      return retVal;
    }
  }
}
