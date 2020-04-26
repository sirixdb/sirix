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

import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.service.xml.xpath.functions.Function;

/**
 * <p>
 * IAxis that represents the conditional expression based on the keywords if, then, and else.
 * </p>
 * <p>
 * The first step in processing a conditional expression is to find the effective boolean value of
 * the test expression. If the effective boolean value of the test expression is true, the value of
 * the then-expression is returned. If the effective boolean value of the test expression is false,
 * the value of the else-expression is returned.
 * </p>
 * 
 */
public class IfAxis extends AbstractAxis {

  private final Axis mIf;
  private final Axis mThen;
  private final Axis mElse;
  private boolean mFirst;
  private Axis mResult;

  /**
   * 
   * Constructor. Initializes the internal state.
   * 
   * @param rtx Exclusive (immutable) trx to iterate with.
   * @param mIfAxis Test expression
   * @param mThenAxis Will be evaluated if test expression evaluates to true.
   * @param mElseAxis Will be evaluated if test expression evaluates to false.
   */
  public IfAxis(final XmlNodeReadOnlyTrx rtx, final Axis mIfAxis, final Axis mThenAxis,
      final Axis mElseAxis) {

    super(rtx);
    mIf = mIfAxis;
    mThen = mThenAxis;
    mElse = mElseAxis;
    mFirst = true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset(final long mNodeKey) {

    super.reset(mNodeKey);
    mFirst = true;

    if (mIf != null) {
      mIf.reset(mNodeKey);
    }

    if (mThen != null) {
      mThen.reset(mNodeKey);
    }

    if (mElse != null) {
      mElse.reset(mNodeKey);
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasNext() {

    resetToLastKey();

    if (mFirst) {
      mFirst = false;
      try {
        mResult = (Function.ebv(mIf)) ? mThen : mElse;
      } catch (SirixXPathException e) {
        throw new RuntimeException(e);
      }
    }

    if (mResult.hasNext()) {
      mKey = mResult.next();
      return true;
    } else {
      resetToStartKey();
      return false;
    }
  }

}
