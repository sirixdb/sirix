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

package org.sirix.service.xml.xpath.comparators;

import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.exception.TTXPathException;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.EXPathError;
import org.sirix.service.xml.xpath.types.Type;

/**
 * <h1>ValueComp</h1>
 * <p>
 * Value comparisons are used for comparing single values.
 * </p>
 * 
 */
public class ValueComp extends AbsComparator {

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx
   *          Exclusive (immutable) trx to iterate with.
   * @param mOperand1
   *          First value of the comparison
   * @param mOperand2
   *          Second value of the comparison
   * @param mComp
   *          comparison kind
   */
  public ValueComp(final INodeReadTrx rtx, final IAxis mOperand1, final IAxis mOperand2, final CompKind mComp) {

    super(rtx, mOperand1, mOperand2, mComp);
  }

  /**
   * {@inheritDoc}
   * 
   */
  @Override
  protected boolean compare(final AtomicValue[] mOperand1, final AtomicValue[] mOperand2)
    throws TTXPathException {
    final Type type = getType(mOperand1[0].getTypeKey(), mOperand2[0].getTypeKey());
    final String op1 = new String(mOperand1[0].getRawValue());
    final String op2 = new String(mOperand2[0].getRawValue());

    return getCompKind().compare(op1, op2, type);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AtomicValue[] atomize(final IAxis mOperand) throws TTXPathException {

    final INodeReadTrx trx = getTransaction();

    int type = trx.getNode().getTypeKey();

    // (3.) if type is untypedAtomic, cast to string
    if (type == trx.keyForName("xs:unytpedAtomic")) {
      type = trx.keyForName("xs:string");
    }

    final AtomicValue atomized =
      new AtomicValue(mOperand.getTransaction().getValueOfCurrentNode().getBytes(), type);
    final AtomicValue[] op = {
      atomized
    };

    // (4.) the operands must be singletons in case of a value comparison
    if (mOperand.hasNext()) {
      throw EXPathError.XPTY0004.getEncapsulatedException();
    } else {
      return op;
    }

  }

  /**
   * {@inheritDoc}
   * 
   * @throws TTXPathException
   */
  @Override
  protected Type getType(final int mKey1, final int mKey2) throws TTXPathException {

    Type type1 = Type.getType(mKey1).getPrimitiveBaseType();
    Type type2 = Type.getType(mKey2).getPrimitiveBaseType();
    return Type.getLeastCommonType(type1, type2);

  }

}
