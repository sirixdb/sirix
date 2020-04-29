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
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.SingleType;
import org.sirix.service.xml.xpath.XPathError;
import org.sirix.service.xml.xpath.XPathError.ErrorType;
import org.sirix.service.xml.xpath.types.Type;
import org.sirix.utils.TypedValue;

/**
 * <p>
 * The castable expression tests whether a given value is castable into a given target type. The
 * target type must be an atomic type that is in the in-scope schema types [err:XPST0051]. In
 * addition, the target type cannot be xs:NOTATION or xs:anyAtomicType [err:XPST0080]. The optional
 * occurrence indicator "?" denotes that an empty sequence is permitted.
 * </p>
 * <p>
 * The expression V castable as T returns true if the value V can be successfully cast into the
 * target type T by using a cast expression; otherwise it returns false. The castable expression can
 * be used as a predicate to avoid errors at evaluation time. It can also be used to select an
 * appropriate type for processing of a given value.
 * </p>
 */
public class CastableExpr extends AbstractExpression {

  /** The input expression to cast to a specified target expression. */
  private final Axis mSourceExpr;

  /** The type, to which the input expression should be cast to. */
  private final Type mTargetType;

  /** Defines, whether an empty sequence can be casted to any target type. */
  private final boolean mPermitEmptySeq;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx Exclusive (immutable) trx to iterate with.
   * @param inputExpr input expression, that's castablity will be tested.
   * @param mTarget Type to test, whether the input expression can be casted to.
   */
  public CastableExpr(final XmlNodeReadOnlyTrx rtx, final Axis inputExpr, final SingleType mTarget) {

    super(rtx);
    mSourceExpr = inputExpr;
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
   * 
   */
  @Override
  public void evaluate() throws SirixXPathException {

    // defines if current item is castable to the target type, or not
    boolean isCastable;

    // atomic type must not be xs:anyAtomicType or xs:NOTATION
    if (mTargetType == Type.ANY_ATOMIC_TYPE || mTargetType == Type.NOTATION) {
      throw new XPathError(ErrorType.XPST0080);
    }

    if (mSourceExpr.hasNext()) { // result sequence > 0
      mKey = mSourceExpr.next();

      final Type sourceType = Type.getType(asXdmNodeReadTrx().getTypeKey());
      final String sourceValue = asXdmNodeReadTrx().getValue();

      // determine castability
      isCastable = sourceType.isCastableTo(mTargetType, sourceValue);

      // if the result sequence of the input expression has more than one
      // item, a type error is raised.
      if (mSourceExpr.hasNext()) { // result sequence > 1
        throw new XPathError(ErrorType.XPTY0004);
      }

    } else { // result sequence = 0 (empty sequence)

      // empty sequence is allowed.
      isCastable = mPermitEmptySeq;

    }

    // create result item and move transaction to it.
    final int mItemKey = asXdmNodeReadTrx().getItemList().addItem(
        new AtomicValue(TypedValue.getBytes(Boolean.toString(isCastable)),
            asXdmNodeReadTrx().keyForName("xs:boolean")));
    mKey = mItemKey;
  }
}
