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

package org.sirix.gui.view;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nonnull;

import org.sirix.api.NodeReadTrx;
import org.sirix.axis.IncludeSelf;

/**
 * <h1>AbsDiffAxis</h1>
 * 
 * <p>
 * Provide standard Java iterator capability compatible with the new enhanced for loop available since Java 5.
 * </p>
 * 
 * <p>
 * All users must make sure to call next() after hasNext() evaluated to true.
 * </p>
 */
public abstract class AbsDiffAxis implements Iterator<Long>, Iterable<Long> {

  /** Iterate over transaction exclusive to this step. */
  private NodeReadTrx mRtx;

  /** Key of last found node. */
  protected long mKey;

  /** Make sure next() can only be called after hasNext(). */
  private boolean mNext;

  /** Key of node where axis started. */
  private long mStartKey;

  /** Include self? */
  private final IncludeSelf mIncludeSelf;

  /**
   * Bind axis step to transaction.
   * 
   * @param pRtx
   *          transaction to operate with
   */
  public AbsDiffAxis(final NodeReadTrx pRtx) {
    mRtx = checkNotNull(pRtx);
    mIncludeSelf = IncludeSelf.NO;
    reset(pRtx.getNodeKey());
  }

  /**
   * Bind axis step to transaction.
   * 
   * @param pRtx
   *          transaction to operate with
   * @param pIncludeSelf
   *          determines if self is included
   */
  public AbsDiffAxis(final NodeReadTrx pRtx, final IncludeSelf pIncludeSelf) {
    checkNotNull(pRtx);
    mRtx = pRtx;
    mIncludeSelf = pIncludeSelf;
    reset(pRtx.getNodeKey());
  }

  @Override
  public final Iterator<Long> iterator() {
    return this;
  }

  @Override
  public Long next() {
    if (!mNext) {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
    }
    mRtx.moveTo(mKey);
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
   * @param pNodeKey
   *          the nodekey where the reset should occur to.
   */
  public void reset(final long pNodeKey) {
    mStartKey = pNodeKey;
    mKey = pNodeKey;
    mNext = false;
  }

  /**
   * Get current {@link NodeReadTrx}.
   * 
   * @return the {@link NodeReadTrx} used
   */
  public NodeReadTrx getTransaction() {
    return mRtx;
  }

  /**
   * Make sure the transaction points to the node it started with. This must
   * be called just before hasNext() == false.
   * 
   * @return Key of node where transaction was before the first call of
   *         hasNext().
   */
  protected final long resetToStartKey() {
    // No check because of IAxis Convention 4.
    mRtx.moveTo(mStartKey);
    mNext = false;
    return mStartKey;
  }

  /**
   * Make sure the transaction points to the node after the last hasNext().
   * This must be called first in hasNext().
   * 
   * @return Key of node where transaction was after the last call of
   *         hasNext().
   */
  protected final long resetToLastKey() {
    // No check because of IAxis Convention 4.
    mRtx.moveTo(mKey);
    mNext = true;
    return mKey;
  }

  /**
   * Get start key.
   * 
   * @return Start key.
   */
  protected final long getStartKey() {
    return mStartKey;
  }

  /**
   * Is self included?
   * 
   * @return {@link IncludeSelf} value
   */
  public final IncludeSelf isSelfIncluded() {
    return mIncludeSelf;
  }

  /**
   * Get {@code mNext} which determines if {@code hasNext()} has at least been called once before
   * {@code next()}.
   * 
   * @return mNext
   */
  public final boolean getNext() {
    return mNext;
  }

  /**
   * Set a new transaction.
   * 
   * @param pRtx
   *          sirix {@link NodeReadTrx}
   */
  public void setTransaction(@Nonnull final NodeReadTrx pRtx) {
    mRtx = checkNotNull(pRtx);
  }
}
