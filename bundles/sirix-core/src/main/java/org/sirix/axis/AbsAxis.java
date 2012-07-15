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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeTraversal;
import org.sirix.api.visitor.IVisitor;

/**
 * <h1>AbsAxis</h1>
 * 
 * <p>
 * Provide standard Java iterator capability compatible with the new enhanced for loop available since Java 5.
 * </p>
 */
public abstract class AbsAxis implements IAxis {

  /** Iterate over transaction exclusive to this step. */
  private final INodeTraversal mRtx;

  /** Key of last found node. */
  protected long mKey;

  /** Make sure next() can only be called after hasNext(). */
  private boolean mNext;

  /** Key of node where axis started. */
  private long mStartKey;

  /** Include self? */
  private final EIncludeSelf mIncludeSelf;

  private boolean mHasNext;

  /**
   * Bind axis step to transaction.
   * 
   * @param pRtx
   *          transaction to operate with
   * @throws NullPointerException
   *           if {@code paramRtx} is {@code null}
   */
  public AbsAxis(@Nonnull final INodeTraversal pRtx) {
    mRtx = checkNotNull(pRtx);
    mIncludeSelf = EIncludeSelf.NO;
    mHasNext = true;
    reset(pRtx.getNode().getNodeKey());
  }

  /**
   * Bind axis step to transaction.
   * 
   * @param pRtx
   *          transaction to operate with
   * @param pIncludeSelf
   *          determines if self is included
   */
  public AbsAxis(@Nonnull final INodeTraversal pRtx,
    @Nonnull final EIncludeSelf pIncludeSelf) {
    mRtx = checkNotNull(pRtx);
    mIncludeSelf = checkNotNull(pIncludeSelf);
    mHasNext = true;
    reset(pRtx.getNode().getNodeKey());
  }

  @Override
  public final Iterator<Long> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    if (mHasNext) {
      return true;
    } else {
      return false;
    }
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
      if (!mRtx.moveTo(mKey)) {
        throw new IllegalStateException();
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
   * @param paramNodeKey
   *          the nodekey where the reset should occur to
   */
  @Override
  public void reset(@Nonnegative final long pNodeKey) {
    mStartKey = pNodeKey;
    mKey = pNodeKey;
    mNext = false;
    mHasNext = true;
  }

  /**
   * Get current {@link INodeReadTrx}.
   * 
   * @return the {@link INodeReadTrx} used
   */
  @Override
  public INodeReadTrx getTransaction() {
    if (mRtx instanceof INodeReadTrx) {
      return (INodeReadTrx)mRtx;
    } else {
      return null;
    }
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
   * Make sure the transaction points to the node it started with. This must
   * be called just before {@code hasNext() == false}.
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
   * Make sure the transaction points to the node after the last hasNext().
   * This must be called first in hasNext().
   * 
   * @return key of node where transaction was after the last call of {@code hasNext()}
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

  @Override
  public final EIncludeSelf isSelfIncluded() {
    return mIncludeSelf;
  }

  /**
   * Get mNext which determines if {@code hasNext()} has at least been called once before the call to
   * {@code next()}.
   * 
   * @return {@code true} if {@code hasNext()} has been called before calling {@code next()}, {@code false}
   *         otherwise
   */
  public final boolean isNext() {
    return mNext;
  }

  /**
   * Implements a simple foreach-method.
   * 
   * @param pVisitor
   *          {@link IVisitor} implementation
   */
  @Override
  public final void foreach(@Nonnull final IVisitor pVisitor) {
    checkNotNull(pVisitor);
    for (; hasNext(); next()) {
      mRtx.getNode().acceptVisitor(pVisitor);
    }
  }

  @Override
  public synchronized final long nextNode() {
    synchronized (mRtx) {
      long retVal = -1;
      if (hasNext()) {
        retVal = next();
      }
      return retVal;
    }
  }
}
