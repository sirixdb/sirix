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

package org.sirix.service.xml.xpath.concurrent;

import org.sirix.api.INodeReadTrx;
import org.sirix.axis.AbsAxis;
import org.sirix.exception.TTXPathException;
import org.sirix.service.xml.xpath.EXPathError;
import org.sirix.settings.EFixed;

/**
 * <h1>ConcurrentIntersectAxis</h1>
 * <p>
 * Computes concurrently and returns an intersection of two operands. This axis takes two node sequences as
 * operands and returns a sequence containing all the nodes that occur in both operands. The result is in doc
 * order and duplicate free.
 * </p>
 */
public class ConcurrentIntersectAxis extends AbsAxis {

  /** First operand sequence. */
  private final ConcurrentAxis mOp1;

  /** Second operand sequence. */
  private final ConcurrentAxis mOp2;

  /** Is axis called for the first time? */
  private boolean mFirst;

  /** Current result of the 1st axis */
  private long mCurrentResult1;

  /** Current result of the 2nd axis. */
  private long mCurrentResult2;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx
   *          Exclusive (immutable) trx to iterate with.
   * @param operand1
   *          First operand
   * @param operand2
   *          Second operand
   */
  public ConcurrentIntersectAxis(final INodeReadTrx rtx, final AbsAxis operand1, final AbsAxis operand2) {

    super(rtx);
    mOp1 = new ConcurrentAxis(rtx, operand1);
    mOp2 = new ConcurrentAxis(rtx, operand2);
    mFirst = true;
    mCurrentResult1 = EFixed.NULL_NODE_KEY.getStandardProperty();
    mCurrentResult2 = EFixed.NULL_NODE_KEY.getStandardProperty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void reset(final long nodeKey) {

    super.reset(nodeKey);

    if (mOp1 != null) {
      mOp1.reset(nodeKey);
    }
    if (mOp2 != null) {
      mOp2.reset(nodeKey);
    }

    mFirst = true;
    mCurrentResult1 = EFixed.NULL_NODE_KEY.getStandardProperty();
    mCurrentResult2 = EFixed.NULL_NODE_KEY.getStandardProperty();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized boolean hasNext() {

    resetToLastKey();

    if (mFirst) {
      mFirst = false;
      mCurrentResult1 = getNext(mOp1);
      mCurrentResult2 = getNext(mOp2);
    }

    final long nodeKey;

    // if 1st axis has a result left that is not contained in the 2nd it is
    // returned
    while (!mOp1.isFinished()) {
      while (!mOp2.isFinished()) {

        // if both results are not equal get next values
        while (mCurrentResult1 != mCurrentResult2 && !mOp1.isFinished() && !mOp2.isFinished()) {

          // get next result from 1st axis, if current is smaller than
          // 2nd
          while (mCurrentResult1 < mCurrentResult2 && !mOp1.isFinished() && !mOp2.isFinished()) {
            mCurrentResult1 = getNext(mOp1);

          }

          // get next result from 2nd axis if current is smaller than
          // 1st
          while (mCurrentResult1 > mCurrentResult2 && !mOp1.isFinished() && !mOp2.isFinished()) {
            mCurrentResult2 = getNext(mOp2);

          }
        }

        if (!mOp1.isFinished() && !mOp2.isFinished()) {
          // if both results are equal return it
          assert (mCurrentResult1 == mCurrentResult2);
          nodeKey = mCurrentResult1;
          if (isValid(nodeKey)) {
            mCurrentResult1 = getNext(mOp1);
            mCurrentResult2 = getNext(mOp2);
            getTransaction().moveTo(nodeKey);
            return true;
          }
          // should never come here!
          throw new IllegalStateException(nodeKey + " is not valid!");

        }
        break;

      }
      break;

    }
    // no results left
    resetToStartKey();
    return false;

  }

  /**
   * @return the next result of the axis. If the axis has no next result, the
   *         null node key is returned.
   */
  private long getNext(final AbsAxis axis) {
    return (axis.hasNext()) ? axis.next() : (Long)EFixed.NULL_NODE_KEY.getStandardProperty();

  }

  /**
   * Checks, whether the given nodekey belongs to a node or an atomic value.
   * Returns true for a node and throws an exception for an atomic value,
   * because these are not allowed in the except expression.
   * 
   * @param nodeKey
   *          the nodekey to validate
   * @return true, if key is a key of a node, otherwise throws an exception
   */
  private boolean isValid(final long nodeKey) {
    if (nodeKey < 0) {
      try {
        throw EXPathError.XPTY0004.getEncapsulatedException();
      } catch (final TTXPathException mExp) {
        mExp.printStackTrace();
      }
    }
    return true;

  }
}
