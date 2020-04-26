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

import java.util.HashSet;
import java.util.Set;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.service.xml.xpath.XPathError;
import org.sirix.service.xml.xpath.XPathError.ErrorType;

/**
 * <p>
 * Returns the nodes of the first operand except those of the second operand. This axis takes two
 * node sequences as operands and returns a sequence containing all the nodes that occur in the
 * first, but not in the second operand.
 * </p>
 */
public class ExceptAxis extends AbstractAxis {

  /** First operand sequence. */
  private final Axis mOp1;

  /** Second operand sequence. */
  private final Axis mOp2;

  /**
   * Set that is used to determine, whether an item of the first operand is also contained in the
   * result set of the second operand.
   */
  private final Set<Long> mDupSet;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx Exclusive (immutable) trx to iterate with.
   * @param mOperand1 First operand
   * @param mOperand2 Second operand
   */
  public ExceptAxis(final XmlNodeReadOnlyTrx rtx, final Axis mOperand1, final Axis mOperand2) {

    super(rtx);
    mOp1 = mOperand1;
    mOp2 = mOperand2;
    mDupSet = new HashSet<Long>();

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset(final long mNodeKey) {

    super.reset(mNodeKey);
    if (mDupSet != null) {
      mDupSet.clear();
    }

    if (mOp1 != null) {
      mOp1.reset(mNodeKey);
    }
    if (mOp2 != null) {
      mOp2.reset(mNodeKey);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasNext() {

    resetToLastKey();

    // first all items of the second operand are stored in the set.
    while (mOp2.hasNext()) {
      mKey = mOp2.next();
      if (asXdmNodeReadTrx().getNodeKey() < 0) { // only nodes are
        // allowed
        throw new XPathError(ErrorType.XPTY0004);
      }
      mDupSet.add(asXdmNodeReadTrx().getNodeKey());
    }

    while (mOp1.hasNext()) {
      mKey = mOp1.next();
      if (asXdmNodeReadTrx().getNodeKey() < 0) { // only nodes are
        // allowed
        throw new XPathError(ErrorType.XPTY0004);
      }

      // return true, if node is not already in the set, which means, that
      // it is
      // not also an item of the result set of the second operand
      // sequence.
      if (mDupSet.add(asXdmNodeReadTrx().getNodeKey())) {
        return true;
      }
    }

    return false;
  }

}
