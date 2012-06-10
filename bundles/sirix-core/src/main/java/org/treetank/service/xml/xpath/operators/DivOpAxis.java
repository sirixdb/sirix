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

package org.treetank.service.xml.xpath.operators;

import org.treetank.api.IAxis;
import org.treetank.api.INodeReadTrx;
import org.treetank.exception.TTXPathException;
import org.treetank.node.interfaces.INode;
import org.treetank.service.xml.xpath.AtomicValue;
import org.treetank.service.xml.xpath.XPathError;
import org.treetank.service.xml.xpath.XPathError.ErrorType;
import org.treetank.service.xml.xpath.types.Type;
import org.treetank.utils.TypedValue;

/**
 * <h1>DivOpAxis</h1>
 * <p>
 * Performs an arithmetic division on two input operators.
 * </p>
 */
public class DivOpAxis extends AbsObAxis {

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx
   *          Exclusive (immutable) trx to iterate with.
   * @param mOp1
   *          First value of the operation
   * @param mOp2
   *          Second value of the operation
   */
  public DivOpAxis(final INodeReadTrx rtx, final IAxis mOp1, final IAxis mOp2) {

    super(rtx, mOp1, mOp2);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public INode operate(final AtomicValue mOperand1, final AtomicValue mOperand2) throws TTXPathException {

    final Type returnType = getReturnType(mOperand1.getTypeKey(), mOperand2.getTypeKey());
    final int typeKey = getTransaction().keyForName(returnType.getStringRepr());

    final byte[] value;

    switch (returnType) {
    case DECIMAL:
    case FLOAT:
    case DOUBLE:
      final double aD = Double.parseDouble(new String(mOperand1.getRawValue()));
      final double dValue;

      if (aD == 0.0 || aD == -0.0) {
        dValue = Double.NaN;
      } else {
        dValue = aD / Double.parseDouble(new String(mOperand2.getRawValue()));
      }

      value = TypedValue.getBytes(dValue);
      return new AtomicValue(value, typeKey);

    case INTEGER:
      try {
        final int iValue =
          (int)Double.parseDouble(new String(mOperand1.getRawValue()))
            / (int)Double.parseDouble(new String(mOperand2.getRawValue()));
        value = TypedValue.getBytes(iValue);
        return new AtomicValue(value, typeKey);
      } catch (final ArithmeticException e) {
        throw new XPathError(ErrorType.FOAR0001);
      }
    case YEAR_MONTH_DURATION:
    case DAY_TIME_DURATION:
      throw new IllegalStateException("Add operator is not implemented for the type "
        + returnType.getStringRepr() + " yet.");
    default:
      throw new XPathError(ErrorType.XPTY0004);

    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Type getReturnType(final int mOp1, final int mOp2) throws TTXPathException {

    Type type1;
    Type type2;
    try {
      type1 = Type.getType(mOp1).getPrimitiveBaseType();
      type2 = Type.getType(mOp2).getPrimitiveBaseType();
    } catch (final IllegalStateException e) {
      throw new XPathError(ErrorType.XPTY0004);
    }

    if (type1.isNumericType() && type2.isNumericType()) {

      // if both have the same numeric type, return it
      if (type1 == type2) {
        return type1;
      }

      if (type1 == Type.DOUBLE || type2 == Type.DOUBLE) {
        return Type.DOUBLE;
      } else if (type1 == Type.FLOAT || type2 == Type.FLOAT) {
        return Type.FLOAT;
      } else {
        assert (type1 == Type.DECIMAL || type2 == Type.DECIMAL);
        return Type.DECIMAL;
      }

    } else {

      switch (type1) {

      case YEAR_MONTH_DURATION:
        if (type2 == Type.YEAR_MONTH_DURATION) {
          return Type.DECIMAL;
        }
        if (type2.isNumericType()) {
          return type1;
        }
        break;
      case DAY_TIME_DURATION:
        if (type2 == Type.DAY_TIME_DURATION) {
          return Type.DECIMAL;
        }
        if (type2.isNumericType()) {
          return type1;
        }
        break;
      default:
        throw new XPathError(ErrorType.XPTY0004);
      }
      throw new XPathError(ErrorType.XPTY0004);
    }
  }

}
