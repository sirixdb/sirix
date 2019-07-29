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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nonnegative;

import org.sirix.api.Axis;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.settings.Fixed;

import com.google.common.base.MoreObjects;

/**
 * <h1>AbstractAxis</h1>
 *
 * <p>
 * Provide standard Java iterator capability compatible with the new enhanced for loop available
 * since Java 5.
 *
 * Override the "template method" {@code nextKey()} to implement an axis. Return {@code done()} if
 * the axis has no more "elements".
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public abstract class AbstractAxis implements Axis {

  /** Iterate over transaction exclusive to this step. */
  protected final NodeCursor mNodeCursor;

  /** Key of next node. */
  private long mKey;

  /** Key of node where axis started. */
  private long mStartKey;

  /** Include self? */
  private final IncludeSelf mIncludeSelf;

  /** Current state. */
  private State mState = State.NOT_READY;

  /** State of the iterator. */
  private enum State {
    /** We have computed the next element and haven't returned it yet. */
    READY,

    /** We haven't yet computed or have already returned the element. */
    NOT_READY,

    /** We have reached the end of the data and are finished. */
    DONE,

    /** We've suffered an exception and are kaput. */
    FAILED,
  }

  /**
   * Bind axis step to transaction.
   *
   * @param nodeCursor node cursor
   * @throws NullPointerException if {@code nodeCursor} is {@code null}
   */
  public AbstractAxis(final NodeCursor nodeCursor) {
    mNodeCursor = checkNotNull(nodeCursor);
    mIncludeSelf = IncludeSelf.NO;
    reset(nodeCursor.getNodeKey());
  }

  /**
   * Bind axis step to transaction.
   *
   * @param nodeCursor node cursor
   * @param includeSelf determines if self is included
   * @throws NullPointerException if {@code nodeCursor} or {@code includeSelf} is {@code null}
   */
  public AbstractAxis(final NodeCursor nodeCursor, final IncludeSelf includeSelf) {
    mNodeCursor = checkNotNull(nodeCursor);
    mIncludeSelf = checkNotNull(includeSelf);
    reset(nodeCursor.getNodeKey());
  }

  @Override
  public final Iterator<Long> iterator() {
    return this;
  }

  /**
   * Signals that axis traversal is done, that is {@code hasNext()} must return false. Is callable
   * from subclasses which implement {@link #nextKey()} to signal that the axis-traversal is done and
   * {@link #hasNext()} must return false.
   *
   * @return null node key to indicate that the travesal is done
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
   * <strong>Implementors must implement {@code nextKey()} instead which is a template method called
   * from this {@code hasNext()} method.</strong>
   * </p>
   */
  @Override
  public final boolean hasNext() {
    // First check the state.
    checkState(mState != State.FAILED);
    switch (mState) {
      case DONE:
        return false;
      case READY:
        return true;
      case FAILED:
      case NOT_READY:
      default:
    }

    // Reset to last node key.
    resetToLastKey();

    final boolean hasNext = tryToComputeNext();
    if (hasNext) {
      return true;
    } else {
      // Reset to the start key before invoking the axis.
      resetToStartKey();
      return false;
    }
  }

  /**
   * Try to compute the next node key.
   *
   * @return {@code true} if next node key exists, {@code false} otherwise
   */
  private boolean tryToComputeNext() {
    mState = State.FAILED; // temporary pessimism
    // Template method.
    mKey = nextKey();
    if (mKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      mState = State.DONE;
    }
    if (mState == State.DONE) {
      return false;
    }
    mState = State.READY;
    return true;
  }

  /**
   * Returns the next node key. <strong>Note:</strong> the implementation must either call
   * {@link #done()} when there are no elements left in the iteration or return the node key
   * {@code EFixed.NULL_NODE.getStandardProperty()}.
   *
   * <p>
   * The initial invocation of {@link #hasNext()} or {@link #next()} calls this method, as does the
   * first invocation of {@code hasNext} or {@code next} following each successful call to
   * {@code next}. Once the implementation either invokes {@link #done()}, returns
   * {@code EFixed.NULL_NODE.getStandardProperty()} or throws an exception, {@code nextKey()} is
   * guaranteed to never be called again.
   * </p>
   *
   * <p>
   * If this method throws an exception, it will propagate outward to the {@code hasNext} or
   * {@code next} invocation that invoked this method. Any further attempts to use the iterator will
   * result in an {@link IllegalStateException}.
   * </p>
   *
   * <p>
   * The implementation of this method may not invoke the {@code hasNext}, {@code next}, or
   * {@link #peek()} methods on this instance; if it does, an {@code IllegalStateException} will
   * result.
   * </p>
   *
   * @return the next node key
   * @throws RuntimeException if any unrecoverable error happens. This exception will propagate
   *         outward to the {@code hasNext()}, {@code next()}, or {@code peek()} invocation that
   *         invoked this method. Any further attempts to use the iterator will result in an
   *         {@link IllegalStateException}.
   */
  protected abstract long nextKey();

  @Override
  public final Long next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    mState = State.NOT_READY;

    // Move to next.
    if (mKey >= 0) {
      if (!mNodeCursor.moveTo(mKey).hasMoved()) {
        throw new IllegalStateException("Failed to move to nodeKey: " + mKey);
      }
    } else {
      mNodeCursor.moveTo(mKey);
    }
    return mKey;
  }

  /**
   * Remove is not supported.
   */
  @Override
  public final void remove() {
    throw new UnsupportedOperationException();
  }

  /**
   * Resetting the nodekey of this axis to a given nodekey.
   *
   * @param nodeKey the nodekey where the reset should occur to
   */
  @Override
  public void reset(@Nonnegative final long nodeKey) {
    mStartKey = nodeKey;
    mKey = nodeKey;
    mState = State.NOT_READY;
  }

  @Override
  public XmlNodeReadOnlyTrx asXdmNodeReadTrx() {
    if (mNodeCursor instanceof XmlNodeReadOnlyTrx) {
      return (XmlNodeReadOnlyTrx) mNodeCursor;
    }
    throw new ClassCastException("Node cursor is no XDM node transaction.");
  }

  @Override
  public JsonNodeReadOnlyTrx asJsonNodeReadTrx() {
    if (mNodeCursor instanceof JsonNodeReadOnlyTrx) {
      return (JsonNodeReadOnlyTrx) mNodeCursor;
    }
    throw new ClassCastException("Node cursor is no JSON node transaction.");
  }

  @Override
  public PathSummaryReader asPathSummary() {
    if (mNodeCursor instanceof PathSummaryReader) {
      return (PathSummaryReader) mNodeCursor;
    }
    throw new ClassCastException("Node cursor is no path summary reader.");
  }

  @Override
  public NodeCursor getCursor() {
    return mNodeCursor;
  }

  @Override
  public NodeReadOnlyTrx getTrx() {
    if (mNodeCursor instanceof NodeReadOnlyTrx) {
      return (NodeReadOnlyTrx) mNodeCursor;
    }
    throw new ClassCastException("Node cursor is no transactional cursor.");
  }

  /**
   * Make sure the transaction points to the node it started with. This must be called just before
   * {@code hasNext() == false}.
   *
   * @return key of node where transaction was before the first call of {@code hasNext()}
   */
  private final long resetToStartKey() {
    // No check because of IAxis Convention 4.
    mNodeCursor.moveTo(mStartKey);
    return mStartKey;
  }

  /**
   * Make sure the transaction points to the node after the last hasNext(). This must be called first
   * in hasNext().
   *
   * @return key of node where transaction was after the last call of {@code hasNext()}
   */
  private final long resetToLastKey() {
    // No check because of IAxis Convention 4.
    mNodeCursor.moveTo(mKey);
    return mKey;
  }

  @Override
  public final Long peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return mKey;
  }

  @Override
  public final long getStartKey() {
    return mStartKey;
  }

  @Override
  public final IncludeSelf isSelfIncluded() {
    return mIncludeSelf;
  }

  /**
   * Implements a simple foreach-method.
   *
   * @param visitor {@link IVisitor} implementation
   */
  @Override
  public final void foreach(final XmlNodeVisitor visitor) {
    checkNotNull(visitor);
    if (mNodeCursor instanceof XmlNodeReadOnlyTrx) {
      while (hasNext()) {
        next();
        ((XmlNodeReadOnlyTrx) mNodeCursor).acceptVisitor(visitor);
      }
    }
  }

  @Override
  public synchronized final long nextNode() {
    synchronized (mNodeCursor) {
      long retVal = Fixed.NULL_NODE_KEY.getStandardProperty();
      if (hasNext()) {
        retVal = next();
      }
      return retVal;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("trx", mNodeCursor).toString();
  }
}
