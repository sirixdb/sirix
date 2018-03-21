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

import javax.annotation.Nonnull;
import org.sirix.api.Axis;

/**
 * <h1>ForAxis</h1>
 * <p>
 * Axis that handles a for expression.
 * </p>
 * <p>
 * This Axis represents only the single-variable for expression. A multiple variables for expression
 * is created by wrap for-Axes by first expanding the expression to a set of nested for expressions,
 * each of which uses only one variable. For example, the expression for $x in X, $y in Y return $x
 * + $y is expanded to for $x in X return for $y in Y return $x + $y.
 * </p>
 * <p>
 * In a single-variable for expression, the variable is called the range variable, the value of the
 * expression that follows the 'in' keyword is called the binding sequence, and the expression that
 * follows the 'return' keyword is called the return expression. The result of the for expression is
 * obtained by evaluating the return expression once for each item in the binding sequence, with the
 * range variable bound to that item. The resulting sequences are concatenated (as if by the comma
 * operator) in the order of the items in the binding sequence from which they were derived.
 * </p>
 */
public final class ForAxis extends AbstractAxis {

  /** The range expression. */
  private final Axis mRange;

  /** The result expression. */
  private final Axis mReturn;

  /** Defines, whether is first call of hasNext(). */
  private boolean mIsFirst;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param range the range variable that holds the binding sequence
   * @param returnExpr the return expression of the for expression
   */
  public ForAxis(final Axis range, @Nonnull final Axis returnExpr) {
    super(range.getTrx());
    mRange = range;
    mReturn = returnExpr;
    mIsFirst = true;
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    mIsFirst = true;
    if (mRange != null) {
      mRange.reset(nodeKey);
    }
  }

  @Override
  protected long nextKey() {
    if (mIsFirst) {
      /*
       * Makes sure, that mRange.hasNext() is called before the return statement, on the first call.
       */
      mIsFirst = false;
    } else {
      if (mReturn.hasNext()) {
        return mReturn.next();
      }
    }

    // Check for more items in the binding sequence.
    while (mRange.hasNext()) {
      mRange.next();

      mReturn.reset(getStartKey());
      if (mReturn.hasNext()) {
        return mReturn.next();
      }
    }

    return done();
  }
}
