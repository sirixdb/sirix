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

package io.sirix.axis.concurrent;

import io.sirix.api.Axis;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.axis.AbstractAxis;
import io.sirix.settings.Fixed;

/**
 * <p>
 * Computes concurrently and returns an intersection of two operands. This axis takes two node
 * sequences as operands and returns a sequence containing all the nodes that occur in both
 * operands. The result is in doc order and duplicate free.
 * </p>
 */
public final class ConcurrentIntersectAxis<R extends NodeCursor & NodeReadOnlyTrx> extends AbstractAxis {

  /** First operand sequence. */
  private final ConcurrentAxis<R> op1;

  /** Second operand sequence. */
  private final ConcurrentAxis<R> op2;

  /** Is axis called for the first time? */
  private boolean first;

  /** Current result of the 1st axis */
  private long currentResult1;

  /** Current result of the 2nd axis. */
  private long currentResult2;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param rtx exclusive (immutable) trx to iterate with.
   * @param operand1 first operand
   * @param operand2 second operand
   * @throws NullPointerException if {@code rtx}, {@code operand1} or {@code operand2} is {@code null}
   */
  public ConcurrentIntersectAxis(final R rtx, final Axis operand1, final Axis operand2) {
    super(rtx);
    op1 = new ConcurrentAxis<>(rtx, operand1);
    op2 = new ConcurrentAxis<>(rtx, operand2);
    first = true;
    currentResult1 = Fixed.NULL_NODE_KEY.getStandardProperty();
    currentResult2 = Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public synchronized void reset(final long nodeKey) {
    super.reset(nodeKey);

    if (op1 != null) {
      op1.reset(nodeKey);
    }
    if (op2 != null) {
      op2.reset(nodeKey);
    }

    first = true;
    currentResult1 = Fixed.NULL_NODE_KEY.getStandardProperty();
    currentResult2 = Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  protected long nextKey() {
    if (first) {
      first = false;
      currentResult1 = Util.getNext(op1);
      currentResult2 = Util.getNext(op2);
    }

    final long nodeKey;

    // if 1st axis has a result left that is not contained in the 2nd it is
    // returned
    while (!op1.isFinished()) {
      while (!op2.isFinished()) {
        // if both results are not equal get next values
        while (currentResult1 != currentResult2 && !op1.isFinished() && !op2.isFinished()) {

          // get next result from 1st axis, if current is smaller than
          // 2nd
          while (currentResult1 < currentResult2 && !op1.isFinished() && !op2.isFinished()) {
            currentResult1 = Util.getNext(op1);
          }

          // get next result from 2nd axis if current is smaller than
          // 1st
          while (currentResult1 > currentResult2 && !op1.isFinished() && !op2.isFinished()) {
            currentResult2 = Util.getNext(op2);
          }
        }

        if (!op1.isFinished() && !op2.isFinished()) {
          // if both results are equal return it
          assert (currentResult1 == currentResult2);
          nodeKey = currentResult1;
          if (Util.isValid(nodeKey)) {
            currentResult1 = Util.getNext(op1);
            currentResult2 = Util.getNext(op2);
            return nodeKey;
          }
          // should never come here!
          throw new IllegalStateException(nodeKey + " is not valid!");

        }
        break;

      }
      break;
    }

    return done();
  }
}
