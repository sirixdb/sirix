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

package org.sirix.service.xml.xpath.expr;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.utils.TypedValue;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>
 * IAxis that represents the quantified expression "some".
 * </p>
 * <p>
 * The quantified expression is true if at least one evaluation of the test expression has the
 * effective boolean value true; otherwise the quantified expression is false. This rule implies
 * that, if the in-clauses generate zero binding tuples, the value of the quantified expression is
 * false.
 * </p>
 */
public class SomeExpr extends AbstractExpression {

  private final List<Axis> mVars;

  private final Axis mSatisfy;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param pRtx exclusive (immutable) trx to iterate with.
   * @param pVars variables for which the condition must be satisfied
   * @param pSatisfy condition that must be satisfied by at least one item of the variable results
   *        in order to evaluate expression to true
   */
  public SomeExpr(final XmlNodeReadOnlyTrx pRtx, @NonNull final List<Axis> pVars,
      @NonNull final Axis pSatisfy) {
    super(pRtx);
    mVars = checkNotNull(pVars);
    mSatisfy = checkNotNull(pSatisfy);
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    if (mVars != null) {
      for (final Axis var : mVars) {
        var.reset(pNodeKey);
      }
    }

    if (mSatisfy != null) {
      mSatisfy.reset(pNodeKey);
    }
  }

  @Override
  public void evaluate() {
    boolean satisfiesCond = false;

    for (final Axis axis : mVars) {
      while (axis.hasNext()) {
        key = axis.next();
        mSatisfy.reset(key);
        if (mSatisfy.hasNext()) {
          key = mSatisfy.next();
          // condition is satisfied for this item -> expression is
          // true
          satisfiesCond = true;
          break;
        }
      }
    }

    final int itemKey = asXmlNodeReadTrx().getItemList().addItem(
        new AtomicValue(TypedValue.getBytes(Boolean.toString(satisfiesCond)),
                        asXmlNodeReadTrx().keyForName("xs:boolean")));
    key = itemKey;

  }

}
