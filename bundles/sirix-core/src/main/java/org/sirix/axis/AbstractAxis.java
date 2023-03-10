/*
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

import com.google.common.base.MoreObjects;
import it.unimi.dsi.fastutil.longs.LongIterator;
import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.api.Axis;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.settings.Fixed;

import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Provide standard Java iterator capability compatible with the new enhanced for loop available
 * since Java 5.
 * </p>
 * <p>
 * Override the "template method" {@code nextKey()} to implement an axis. Return {@code done()} if
 * the axis has no more "elements".
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public abstract class AbstractAxis implements Axis {

  /** Iterate over transaction exclusive to this step. */
  protected final NodeCursor nodeCursor;

  /** Key of next node. */
  private long nextNodeKey;

  /** Key of node where axis started. */
  private long startNodeKey;

  /** Include self? */
  private final IncludeSelf includeSelf;

  /** Current state. */
  private State state = State.NOT_READY;

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
    this.nodeCursor = requireNonNull(nodeCursor);
    includeSelf = IncludeSelf.NO;
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
    this.nodeCursor = requireNonNull(nodeCursor);
    this.includeSelf = requireNonNull(includeSelf);
    reset(nodeCursor.getNodeKey());
  }

  @Override
  public final LongIterator iterator() {
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
    checkState(state != State.FAILED);
    switch (state) {
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
    state = State.FAILED; // temporary pessimism
    // Template method.
    nextNodeKey = nextKey();
    if (nextNodeKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
      state = State.DONE;
    }
    if (state == State.DONE) {
      return false;
    }
    state = State.READY;
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
  public final long nextLong() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    state = State.NOT_READY;

    // Move to next.
    if (nextNodeKey >= 0) {
      if (!nodeCursor.moveTo(nextNodeKey)) {
        throw new IllegalStateException("Failed to move to nodeKey: " + nextNodeKey);
      }
    } else {
      nodeCursor.moveTo(nextNodeKey);
    }
    return nextNodeKey;
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
  public void reset(@NonNegative final long nodeKey) {
    startNodeKey = nodeKey;
    nextNodeKey = nodeKey;
    state = State.NOT_READY;
  }

  @Override
  public XmlNodeReadOnlyTrx asXmlNodeReadTrx() {
    if (nodeCursor instanceof XmlNodeReadOnlyTrx) {
      return (XmlNodeReadOnlyTrx) nodeCursor;
    }
    throw new ClassCastException("Node cursor is no XDM node transaction.");
  }

  @Override
  public JsonNodeReadOnlyTrx asJsonNodeReadTrx() {
    if (nodeCursor instanceof JsonNodeReadOnlyTrx) {
      return (JsonNodeReadOnlyTrx) nodeCursor;
    }
    throw new ClassCastException("Node cursor is no JSON node transaction.");
  }

  @Override
  public PathSummaryReader asPathSummary() {
    if (nodeCursor instanceof PathSummaryReader) {
      return (PathSummaryReader) nodeCursor;
    }
    throw new ClassCastException("Node cursor is no path summary reader.");
  }

  @Override
  public NodeCursor getCursor() {
    return nodeCursor;
  }

  @Override
  public NodeReadOnlyTrx getTrx() {
    if (nodeCursor instanceof NodeReadOnlyTrx) {
      return (NodeReadOnlyTrx) nodeCursor;
    }
    throw new ClassCastException("Node cursor is no transactional cursor.");
  }

  /**
   * Make sure the transaction points to the node it started with. This must be called just before
   * {@code hasNext() == false}.
   *
   * @return key of node where transaction was before the first call of {@code hasNext()}
   */
  private long resetToStartKey() {
    // No check because of Axis Convention 4.
    nodeCursor.moveTo(startNodeKey);
    return startNodeKey;
  }

  /**
   * Make sure the transaction points to the node after the last hasNext(). This must be called first
   * in hasNext().
   *
   * @return key of node where transaction was after the last call of {@code hasNext()}
   */
  protected final long resetToLastKey() {
    // No check because of Axis convention 4.
    if (nodeCursor.getNodeKey() != nextNodeKey) {
      nodeCursor.moveTo(nextNodeKey);
    }
    return nextNodeKey;
  }

  @Override
  public final long peek() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return nextNodeKey;
  }

  @Override
  public final long getStartKey() {
    return startNodeKey;
  }

  @Override
  public final IncludeSelf includeSelf() {
    return includeSelf;
  }

  /**
   * Implements a simple foreach-method.
   *
   * @param visitor {@link XmlNodeVisitor} implementation
   */
  @Override
  public final void foreach(final XmlNodeVisitor visitor) {
    requireNonNull(visitor);
    if (nodeCursor instanceof XmlNodeReadOnlyTrx) {
      while (hasNext()) {
        nextLong();
        ((XmlNodeReadOnlyTrx) nodeCursor).acceptVisitor(visitor);
      }
    }
  }

  @Override
  public synchronized final long nextNode() {
    synchronized (nodeCursor) {
      long retVal = Fixed.NULL_NODE_KEY.getStandardProperty();
      if (hasNext()) {
        retVal = nextLong();
      }
      return retVal;
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("trx", nodeCursor).toString();
  }
}
