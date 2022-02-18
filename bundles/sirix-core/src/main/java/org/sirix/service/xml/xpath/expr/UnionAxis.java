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

import static com.google.common.base.Preconditions.checkNotNull;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.service.xml.xpath.XPathError;
import org.sirix.service.xml.xpath.XPathError.ErrorType;

/**
 * <p>
 * Returns an union of two operands. This axis takes two node sequences as operands and returns a
 * sequence containing all the items that occur in either of the operands. A union of two sequences
 * may lead to a sequence containing duplicates. These duplicates can be removed by wrapping the
 * UnionAxis with a DupFilterAxis. The resulting sequence may also be out of document order.
 * </p>
 */
public class UnionAxis extends AbstractAxis {

  /** First operand sequence. */
  private final Axis mOp1;

  /** Second operand sequence. */
  private final Axis mOp2;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx exclusive (immutable) trx to iterate with
   * @param operand1 first operand
   * @param operand2 second operand
   */
  public UnionAxis(final XmlNodeReadOnlyTrx rtx, @NonNull final Axis operand1,
      @NonNull final Axis operand2) {
    super(rtx);
    mOp1 = checkNotNull(operand1);
    mOp2 = checkNotNull(operand2);
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);

    if (mOp1 != null) {
      mOp1.reset(nodeKey);
    }
    if (mOp2 != null) {
      mOp2.reset(nodeKey);
    }
  }

  @Override
  public boolean hasNext() {
    resetToLastKey();
    // first return all values of the first operand
    while (mOp1.hasNext()) {
      mKey = mOp1.next();

      if (asXdmNodeReadTrx().getNodeKey() < 0) { // only nodes are
        // allowed
        throw new XPathError(ErrorType.XPTY0004);
      }
      return true;
    }

    // then all values of the second operand.
    while (mOp2.hasNext()) {
      mKey = mOp2.next();

      if (asXdmNodeReadTrx().getNodeKey() < 0) { // only nodes are
        // allowed
        throw new XPathError(ErrorType.XPTY0004);
      }
      return true;
    }

    return false;
  }

}
