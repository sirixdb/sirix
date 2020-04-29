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
import org.sirix.service.xml.xpath.SingleType;
import org.sirix.service.xml.xpath.XPathError;
import org.sirix.service.xml.xpath.XPathError.ErrorType;
import org.sirix.service.xml.xpath.types.Type;

/**
 * <p>
 * The cast expression cast a given value into a given target type.
 * </p>
 * <p>
 * Occasionally it is necessary to convert a value to a specific datatype. For this purpose, XPath
 * provides a cast expression that creates a new value of a specific type based on an existing
 * value. A cast expression takes two operands: an input expression and a target type. The type of
 * the input expression is called the input or source type. The target type must be an atomic type
 * that is in the in-scope schema types [err:XPST0051]. In addition, the target type cannot be
 * xs:NOTATION or xs:anyAtomicType [err:XPST0080]. The optional occurrence indicator "?" denotes
 * that an empty sequence is permitted.
 * </p>
 */
public class CastExpr extends AbstractExpression {

  /** The input expression to cast to a specified target expression. */
  private final Axis mSourceExpr;

  /** The type, to which the input expression will be casted to. */
  private final Type mTargetType;

  /** Defines, whether an empty sequence will be casted to any target type. */
  private final boolean mPermitEmptySeq;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx Exclusive (immutable) trx to iterate with.
   * @param mInputExpr input expression, that will be casted.
   * @param mTarget Type the input expression will be casted to.
   */
  public CastExpr(final XmlNodeReadOnlyTrx rtx, final Axis mInputExpr, final SingleType mTarget) {

    super(rtx);
    mSourceExpr = mInputExpr;
    mTargetType = mTarget.getAtomic();
    mPermitEmptySeq = mTarget.hasInterogation();

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset(final long mNodeKey) {

    super.reset(mNodeKey);
    if (mSourceExpr != null) {
      mSourceExpr.reset(mNodeKey);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void evaluate() throws SirixXPathException {

    // atomic type must not be xs:anyAtomicType or xs:NOTATION
    if (mTargetType == Type.ANY_ATOMIC_TYPE || mTargetType == Type.NOTATION) {
      throw new XPathError(ErrorType.XPST0080);
    }

    if (mSourceExpr.hasNext()) {
      mSourceExpr.next();

      final Type sourceType = Type.getType(asXdmNodeReadTrx().getTypeKey());
      final String sourceValue = asXdmNodeReadTrx().getValue();

      // cast source to target type, if possible
      if (sourceType.isCastableTo(mTargetType, sourceValue)) {
        throw new IllegalStateException("casts not implemented yet.");
        // ((XPathReadTransaction)
        // getTransaction()).castTo(mTargetType);
      }

      // 2. if the result sequence of the input expression has more than
      // one
      // items, a type error is raised.
      if (mSourceExpr.hasNext()) {
        throw new XPathError(ErrorType.XPTY0004);
      }

    } else {
      // 3. if is empty sequence:
      if (!mPermitEmptySeq) {
        // if '?' is specified after the target type, the result is an
        // empty sequence (which means, nothing is changed),
        // otherwise an error is raised.
        throw new XPathError(ErrorType.XPTY0004);

      }
    }

  }
}
