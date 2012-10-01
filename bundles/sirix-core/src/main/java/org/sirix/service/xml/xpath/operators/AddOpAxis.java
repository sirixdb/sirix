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

package org.sirix.service.xml.xpath.operators;

import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.exception.SirixXPathException;
import org.sirix.node.interfaces.INode;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.EXPathError;
import org.sirix.service.xml.xpath.types.Type;
import org.sirix.utils.TypedValue;

/**
 * <h1>AddOpAxis</h1>
 * <p>
 * Performs an arithmetic addition on two input operators.
 * </p>
 */
public class AddOpAxis extends AbsObAxis {

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
  public AddOpAxis(final INodeReadTrx rtx, final IAxis mOp1, final IAxis mOp2) {

    super(rtx, mOp1, mOp2);
  }

  /**
   * {@inheritDoc}
   * 
   */
  @Override
  public INode operate(final AtomicValue mOperand1, final AtomicValue mOperand2) throws SirixXPathException {

    final Type returnType = getReturnType(mOperand1.getTypeKey(), mOperand2.getTypeKey());
    final int typeKey = getTrx().keyForName(returnType.getStringRepr());

    final byte[] value;

    switch (returnType) {
    case DOUBLE:
    case FLOAT:
    case DECIMAL:
    case INTEGER:
      final double dOp1 = Double.parseDouble(new String(mOperand1.getRawValue()));
      final double dOp2 = Double.parseDouble(new String(mOperand2.getRawValue()));
      value = TypedValue.getBytes(dOp1 + dOp2);
      break;
    case DATE:
    case TIME:
    case DATE_TIME:
    case YEAR_MONTH_DURATION:
    case DAY_TIME_DURATION:
      throw new IllegalStateException("Add operator is not implemented for the type "
        + returnType.getStringRepr() + " yet.");
    default:
      throw EXPathError.XPTY0004.getEncapsulatedException();

    }

    return new AtomicValue(value, typeKey);

  }

  /**
   * {@inheritDoc}
   * 
   */
  @Override
  protected Type getReturnType(final int mOp1, final int mOp2) throws SirixXPathException {

    final Type mType1 = Type.getType(mOp1).getPrimitiveBaseType();
    final Type mType2 = Type.getType(mOp2).getPrimitiveBaseType();

    if (mType1.isNumericType() && mType2.isNumericType()) {

      // if both have the same numeric type, return it
      if (mType1 == mType2) {
        return mType1;
      } else if (mType1 == Type.DOUBLE || mType2 == Type.DOUBLE) {
        return Type.DOUBLE;
      } else if (mType1 == Type.FLOAT || mType2 == Type.FLOAT) {
        return Type.FLOAT;
      } else {
        assert (mType1 == Type.DECIMAL || mType2 == Type.DECIMAL);
        return Type.DECIMAL;
      }

    } else {

      switch (mType1) {
      case DATE:
        if (mType2 == Type.YEAR_MONTH_DURATION || mType2 == Type.DAY_TIME_DURATION) {
          return mType1;
        }
        break;
      case TIME:
        if (mType2 == Type.DAY_TIME_DURATION) {
          return mType1;
        }
        break;
      case DATE_TIME:
        if (mType2 == Type.YEAR_MONTH_DURATION || mType2 == Type.DAY_TIME_DURATION) {
          return mType1;
        }
        break;
      case YEAR_MONTH_DURATION:
        if (mType2 == Type.DATE || mType2 == Type.DATE_TIME || mType2 == Type.YEAR_MONTH_DURATION) {
          return mType2;
        }
        break;
      case DAY_TIME_DURATION:
        if (mType2 == Type.DATE || mType2 == Type.TIME || mType2 == Type.DATE_TIME
          || mType2 == Type.DAY_TIME_DURATION) {
          return mType2;
        }
        break;
      default:
        throw EXPathError.XPTY0004.getEncapsulatedException();
      }
      throw EXPathError.XPTY0004.getEncapsulatedException();
    }
  }

}
