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

package org.sirix.axis.concurrent;

import org.sirix.api.Axis;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.axis.AbstractAxis;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.EXPathError;

/**
 * <p>
 * Computes concurrently and returns a union of two operands. This axis takes two node sequences as
 * operands and returns a sequence containing all the items that occur in either of the operands. A
 * union of two sequences may lead to a sequence containing duplicates. These duplicates are removed
 * by the concept of .... Additionally this guarantees the document order.
 * </p>
 */
public final class ConcurrentUnionAxis<R extends NodeCursor & NodeReadOnlyTrx> extends AbstractAxis {

  /** First operand sequence. */
  private final ConcurrentAxis<R> mOp1;

  /** Second operand sequence. */
  private final ConcurrentAxis<R> mOp2;

  /** First run. */
  private boolean mFirst;

  /** Result from first axis. */
  private long mCurrentResult1;

  /** Result from second axis. */
  private long mCurrentResult2;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param rtx exclusive (immutable) trx to iterate with
   * @param operand1 first operand
   * @param operand2 second operand
   * @throws NullPointerException if {@code rtx}, {@code operand1} or {@code operand2} is {@code null}
   */
  public ConcurrentUnionAxis(final R rtx, final Axis operand1, final Axis operand2) {
    super(rtx);
    mOp1 = new ConcurrentAxis<>(rtx, operand1);
    mOp2 = new ConcurrentAxis<>(rtx, operand2);
    mFirst = true;
  }

  @Override
  public void reset(final long nodeKey) {
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
  protected long nextKey() {
    if (mFirst) {
      mFirst = false;
      mCurrentResult1 = Util.getNext(mOp1);
      mCurrentResult2 = Util.getNext(mOp2);
    }

    final long nodeKey;

    // if both operands have results left return the smallest value (doc
    // order)
    if (!mOp1.isFinished()) {
      if (!mOp2.isFinished()) {
        if (mCurrentResult1 < mCurrentResult2) {
          nodeKey = mCurrentResult1;
          mCurrentResult1 = Util.getNext(mOp1);
        } else if (mCurrentResult1 > mCurrentResult2) {
          nodeKey = mCurrentResult2;
          mCurrentResult2 = Util.getNext(mOp2);
        } else {
          // return only one of the values (prevent duplicates)
          nodeKey = mCurrentResult2;
          mCurrentResult1 = Util.getNext(mOp1);
          mCurrentResult2 = Util.getNext(mOp2);
        }

        if (nodeKey < 0) {
          try {
            throw EXPathError.XPTY0004.getEncapsulatedException();
          } catch (final SirixXPathException mExp) {
            mExp.printStackTrace();
          }
        }
        return nodeKey;
      }

      // only operand1 has results left, so return all of them
      nodeKey = mCurrentResult1;
      if (Util.isValid(nodeKey)) {
        mCurrentResult1 = Util.getNext(mOp1);
        return nodeKey;
      }
      // should never come here!
      throw new IllegalStateException(nodeKey + " is not valid!");
    } else if (!mOp2.isFinished()) {
      // only operand2 has results left, so return all of them
      nodeKey = mCurrentResult2;
      if (Util.isValid(nodeKey)) {
        mCurrentResult2 = Util.getNext(mOp2);
        return nodeKey;
      }
      // should never come here!
      throw new IllegalStateException(nodeKey + " is not valid!");
    }

    return done();
  }
}
