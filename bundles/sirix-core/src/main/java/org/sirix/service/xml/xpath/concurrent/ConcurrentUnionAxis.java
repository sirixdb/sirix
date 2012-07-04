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

import javax.annotation.Nonnull;

import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.axis.AbsAxis;
import org.sirix.exception.TTXPathException;
import org.sirix.service.xml.xpath.EXPathError;
import org.sirix.settings.EFixed;

/**
 * <h1>ConcurrentUnionAxis</h1>
 * <p>
 * Computes concurrently and returns a union of two operands. This axis takes two node sequences as operands
 * and returns a sequence containing all the items that occur in either of the operands. A union of two
 * sequences may lead to a sequence containing duplicates. These duplicates are removed by the concept of ....
 * Additionally this guarantees the document order.
 * </p>
 */
public class ConcurrentUnionAxis extends AbsAxis {

  /** First operand sequence. */
  private final ConcurrentAxis mOp1;

  /** Second operand sequence. */
  private final ConcurrentAxis mOp2;

  private boolean mFirst;

  private long mCurrentResult1;

  private long mCurrentResult2;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param pOperand1
   *          first operand
   * @param pOperand2
   *          second operand
   */
  public ConcurrentUnionAxis(final INodeReadTrx pRtx, final IAxis pOperand1,
    final IAxis pOperand2) {
    super(pRtx);
    mOp1 = new ConcurrentAxis(pRtx, pOperand1);
    mOp2 = new ConcurrentAxis(pRtx, pOperand2);
    mFirst = true;
  }

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
  }

  @Override
  public synchronized boolean hasNext() {
    resetToLastKey();

    if (mFirst) {
      mFirst = false;
      mCurrentResult1 = getNext(mOp1);
      mCurrentResult2 = getNext(mOp2);
    }

    final long nodeKey;

    // if both operands have results left return the smallest value (doc
    // order)
    if (!mOp1.isFinished()) {
      if (!mOp2.isFinished()) {
        if (mCurrentResult1 < mCurrentResult2) {
          nodeKey = mCurrentResult1;
          mCurrentResult1 = getNext(mOp1);

        } else if (mCurrentResult1 > mCurrentResult2) {
          nodeKey = mCurrentResult2;
          mCurrentResult2 = getNext(mOp2);
        } else {
          // return only one of the values (prevent duplicates)
          nodeKey = mCurrentResult2;
          mCurrentResult1 = getNext(mOp1);
          mCurrentResult2 = getNext(mOp2);
        }

        if (nodeKey < 0) {
          try {
            throw EXPathError.XPTY0004.getEncapsulatedException();
          } catch (final TTXPathException mExp) {
            mExp.printStackTrace();
          }
        }
        mKey = nodeKey;
        return true;
      }

      // only operand1 has results left, so return all of them
      nodeKey = mCurrentResult1;
      if (isValid(nodeKey)) {
        mCurrentResult1 = getNext(mOp1);
        mKey = nodeKey;
        return true;
      }
      // should never come here!
      throw new IllegalStateException(nodeKey + " is not valid!");

    } else if (!mOp2.isFinished()) {
      // only operand1 has results left, so return all of them

      nodeKey = mCurrentResult2;
      if (isValid(nodeKey)) {
        mCurrentResult2 = getNext(mOp2);
        mKey = nodeKey;
        return true;
      }
      // should never come here!
      throw new IllegalStateException(nodeKey + " is not valid!");

    }
    // no results left
    resetToStartKey();
    return false;

  }

  /**
   * Get next result from axis.
   * 
   * @return the next result of the axis. If the axis has no next result, the
   *         null node key is returned.
   */
  private long getNext(@Nonnull final IAxis axis) {
    return (axis.hasNext()) ? axis.next() : EFixed.NULL_NODE_KEY
      .getStandardProperty();
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
      // throw new XPathError(ErrorType.XPTY0004);
      try {
        throw new TTXPathException(
          "err:XPTY0004 The type is not appropriate the expression or the "
            + "typedoes not match a required type as specified by the matching rules.");
      } catch (TTXPathException mExp) {
        mExp.printStackTrace();
      }
    }
    return true;

  }

}
