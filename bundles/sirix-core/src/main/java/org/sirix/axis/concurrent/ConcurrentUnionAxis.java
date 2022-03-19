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
  private final ConcurrentAxis<R> op1;

  /** Second operand sequence. */
  private final ConcurrentAxis<R> op2;

  /** First run. */
  private boolean first;

  /** Result from first axis. */
  private long currentResult1;

  /** Result from second axis. */
  private long currentResult2;

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
    op1 = new ConcurrentAxis<>(rtx, operand1);
    op2 = new ConcurrentAxis<>(rtx, operand2);
    first = true;
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);

    if (op1 != null) {
      op1.reset(nodeKey);
    }
    if (op2 != null) {
      op2.reset(nodeKey);
    }

    first = true;
  }

  @Override
  protected long nextKey() {
    if (first) {
      first = false;
      currentResult1 = Util.getNext(op1);
      currentResult2 = Util.getNext(op2);
    }

    final long nodeKey;

    // if both operands have results left return the smallest value (doc order)
    if (!op1.isFinished()) {
      if (!op2.isFinished()) {
        if (currentResult1 < currentResult2) {
          nodeKey = currentResult1;
          currentResult1 = Util.getNext(op1);
        } else if (currentResult1 > currentResult2) {
          nodeKey = currentResult2;
          currentResult2 = Util.getNext(op2);
        } else {
          // return only one of the values (prevent duplicates)
          nodeKey = currentResult2;
          currentResult1 = Util.getNext(op1);
          currentResult2 = Util.getNext(op2);
        }

        if (nodeKey < 0) {
          throw EXPathError.XPTY0004.getEncapsulatedException();
        }
        return nodeKey;
      }

      // only operand1 has results left, so return all of them
      nodeKey = currentResult1;
      if (Util.isValid(nodeKey)) {
        currentResult1 = Util.getNext(op1);
        return nodeKey;
      }
      // should never come here!
      throw new IllegalStateException(nodeKey + " is not valid!");
    } else if (!op2.isFinished()) {
      // only operand2 has results left, so return all of them
      nodeKey = currentResult2;
      if (Util.isValid(nodeKey)) {
        currentResult2 = Util.getNext(op2);
        return nodeKey;
      }
      // should never come here!
      throw new IllegalStateException(nodeKey + " is not valid!");
    }

    return done();
  }
}
