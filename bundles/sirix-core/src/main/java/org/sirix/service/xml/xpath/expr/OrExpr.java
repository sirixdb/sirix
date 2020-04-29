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
import org.sirix.service.xml.xpath.functions.Function;
import org.sirix.utils.TypedValue;

/**
 * <p>
 * The logical or expression performs a logical disjunction of the boolean values of two input
 * sequences. If a logical expression does not raise an error, its value is always one of the
 * boolean values true or false.
 * </p>
 * <p>
 * The value of an or-expression is determined by the effective boolean values of its operands, as
 * shown in the following table:
 * <table>
 *     <caption>
 *         The value of an or-expression is determined by the effective boolean values of its operands, as
 * shown in the following table:
 *  </caption>
 * <tr>
 * <th>OR</th>
 * <th>EBV2 = true</th>
 * <th>EBV2 = false</th>
 * <th>error in EBV2</th>
 * </tr>
 * <tr>
 * <th>EBV1 = true</th>
 * <th>true</th>
 * <th>true</th>
 * <th>true</th>
 * </tr>
 * <tr>
 * <th>EBV1 = false</th>
 * <th>true</th>
 * <th>false</th>
 * <th>error</th>
 * </tr>
 * <tr>
 * <th>error in EBV1</th>
 * <th>error</th>
 * <th>error</th>
 * <th>error</th>
 * </tr>
 * </table>
 */
public class OrExpr extends AbstractExpression {

  /** First operand of the logical expression. */
  private final Axis mOp1;

  /** Second operand of the logical expression. */
  private final Axis mOp2;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx Exclusive (immutable) transaction to iterate with.
   * @param mOperand1 First operand
   * @param mOperand2 Second operand
   */
  public OrExpr(final XmlNodeReadOnlyTrx rtx, final Axis mOperand1, final Axis mOperand2) {

    super(rtx);
    mOp1 = mOperand1;
    mOp2 = mOperand2;

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset(final long mNodeKey) {

    super.reset(mNodeKey);
    if (mOp1 != null) {
      mOp1.reset(mNodeKey);
    }
    if (mOp2 != null) {
      mOp2.reset(mNodeKey);
    }
  }

  /**
   * {@inheritDoc}
   * 
   * @throws SirixXPathException
   */
  @Override
  public void evaluate() throws SirixXPathException {

    // first find the effective boolean values of the two operands, then
    // determine value of the and-expression and store it in am item
    final boolean result = Function.ebv(mOp1) || Function.ebv(mOp2);
    // note: the error handling is implicitly done by the fnBoolean()
    // function.

    // add result item to list and set the item as the current item
    final int itemKey = asXdmNodeReadTrx().getItemList().addItem(
        new AtomicValue(TypedValue.getBytes(Boolean.toString(result)),
            asXdmNodeReadTrx().keyForName("xs:boolean")));
    mKey = itemKey;

  }

}
