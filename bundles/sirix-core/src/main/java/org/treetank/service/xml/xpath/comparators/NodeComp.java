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

package org.treetank.service.xml.xpath.comparators;

import org.treetank.api.IAxis;
import org.treetank.api.INodeReadTrx;
import org.treetank.exception.TTXPathException;
import org.treetank.service.xml.xpath.AtomicValue;
import org.treetank.service.xml.xpath.EXPathError;
import org.treetank.service.xml.xpath.types.Type;
import org.treetank.utils.TypedValue;

/**
 * <h1>NodeComp</h1>
 * <p>
 * Node comparisons are used to compare two nodes, by their identity or by their document order.
 * </p>
 */
public class NodeComp extends AbsComparator {

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
  public NodeComp(final INodeReadTrx rtx, final IAxis mOperand1, final IAxis mOperand2, final CompKind mComp) {

    super(rtx, mOperand1, mOperand2, mComp);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected AtomicValue[] atomize(final IAxis mOperand) throws TTXPathException {

    final INodeReadTrx rtx = getTransaction();
    // store item key as atomic value
    final AtomicValue mAtomized =
      new AtomicValue(TypedValue.getBytes(((Long)rtx.getNode().getNodeKey()).toString()), rtx
        .keyForName("xs:integer"));
    final AtomicValue[] op = {
      mAtomized
    };

    // the operands must be singletons in case of a node comparison
    if (mOperand.hasNext()) {
      throw EXPathError.XPTY0004.getEncapsulatedException();
    } else {
      return op;
    }

  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Type getType(final int mKey1, final int mKey2) {

    return Type.INTEGER;

  }

  /**
   * {@inheritDoc}
   * 
   */
  @Override
  protected boolean compare(final AtomicValue[] mOperand1, final AtomicValue[] mOperand2)
    throws TTXPathException {

    final String op1 = new String(mOperand1[0].getRawValue());
    final String op2 = new String(mOperand2[0].getRawValue());

    return getCompKind().compare(op1, op2, getType(mOperand1[0].getTypeKey(), mOperand2[0].getTypeKey()));
  }

}
