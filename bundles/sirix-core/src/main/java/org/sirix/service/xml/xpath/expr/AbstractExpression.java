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

import org.sirix.api.Expression;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.AbstractAxis;

/**
 * <h1>AbstractExpression</h1>
 * <p>
 * Template for all expressions.
 * </p>
 * <p>
 * This class is a template for most complex expressions of the XPath 2.0 language. These
 * expressions work like an axis, as all other XPath 2.0 expressions in this implementation, but the
 * expression is only evaluated once. Therefore the axis returns true only for the first call and
 * false for all others.
 * </p>
 */
public abstract class AbstractExpression extends AbstractAxis implements Expression {

  /** Defines, whether {@code hasNext()} has already been called. */
  private boolean mIsFirst;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx exclusive (immutable) trx to iterate with
   */
  public AbstractExpression(final XdmNodeReadTrx pRtx) {
    super(pRtx);
    mIsFirst = true;
  }

  @Override
  public synchronized void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    mIsFirst = true;
  }

  @Override
  public synchronized boolean hasNext() {
    if (isNext()) {
      return true;
    }
    resetToLastKey();

    if (mIsFirst) {
      mIsFirst = false;

      // Evaluate expression.
      try {
        evaluate();
      } catch (final SirixXPathException e) {
        throw new RuntimeException(e);
      }

      return true;
    } else {
      // only the first call yields to true, all further calls will yield
      // to
      // false. Calling hasNext() makes no sense, since evaluating the
      // expression on the same input would always return the same result.
      resetToStartKey();
      return false;
    }

  }

  /**
   * Performs the expression dependent evaluation of the expression. (Template method)
   * 
   * @throws SirixXPathException if evaluation fails
   */
  @Override
  public abstract void evaluate() throws SirixXPathException;

}
