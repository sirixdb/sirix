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

package io.sirix.service.xml.xpath.comparators;

import io.sirix.api.Axis;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.exception.SirixXPathException;
import io.sirix.service.xml.xpath.AtomicValue;
import io.sirix.service.xml.xpath.EXPathError;
import io.sirix.service.xml.xpath.types.Type;

/**
 * <p>
 * Value comparisons are used for comparing single values.
 * </p>
 *
 */
public class ValueComp extends AbstractComparator {

  /**
   * Constructor. Initializes the internal state.
   *
   * @param rtx Exclusive (immutable) trx to iterate with.
   * @param mOperand1 First value of the comparison
   * @param mOperand2 Second value of the comparison
   * @param mComp comparison kind
   */
  public ValueComp(final XmlNodeReadOnlyTrx rtx, final Axis mOperand1, final Axis mOperand2, final CompKind mComp) {

    super(rtx, mOperand1, mOperand2, mComp);
  }

  @Override
  protected boolean compare(final AtomicValue[] mOperand1, final AtomicValue[] mOperand2) throws SirixXPathException {
    final Type type = getType(mOperand1[0].getTypeKey(), mOperand2[0].getTypeKey());
    final String op1 = new String(mOperand1[0].getRawValue());
    final String op2 = new String(mOperand2[0].getRawValue());

    return getCompKind().compare(op1, op2, type);
  }

  @Override
  protected AtomicValue[] atomize(final Axis mOperand) throws SirixXPathException {

    final XmlNodeReadOnlyTrx trx = asXmlNodeReadTrx();

    int type = trx.getTypeKey();

    // (3.) if type is untypedAtomic, cast to string
    if (type == trx.keyForName("xs:unytpedAtomic")) {
      type = trx.keyForName("xs:string");
    }

    final AtomicValue atomized = new AtomicValue(((XmlNodeReadOnlyTrx) mOperand.asXmlNodeReadTrx()).getValue().getBytes(), type);
    final AtomicValue[] op = {atomized};

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
   * @throws SirixXPathException
   */
  @Override
  protected Type getType(final int mKey1, final int mKey2) throws SirixXPathException {

    Type type1 = Type.getType(mKey1).getPrimitiveBaseType();
    Type type2 = Type.getType(mKey2).getPrimitiveBaseType();
    return Type.getLeastCommonType(type1, type2);

  }

}
