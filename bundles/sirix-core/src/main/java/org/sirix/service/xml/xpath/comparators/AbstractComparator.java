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

package org.sirix.service.xml.xpath.comparators;

import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.expr.LiteralExpr;
import org.sirix.service.xml.xpath.types.Type;

/**
 * <p>
 * Abstract axis that evaluates a comparison.
 * </p>
 */
public abstract class AbstractComparator extends AbstractAxis {

  /** Kind of comparison. */
  private final CompKind mComp;

  /** First value of the comparison. */
  private final Axis mOperand1;

  /** Second value of the comparison. */
  private final Axis mOperand2;

  /** Is first evaluation? */
  private boolean mIsFirst;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param mRtx Exclusive (immutable) trx to iterate with.
   * @param mOperand1 First value of the comparison
   * @param mOperand2 Second value of the comparison
   * @param mComp comparison kind
   */
  public AbstractComparator(final XmlNodeReadOnlyTrx mRtx, final Axis mOperand1, final Axis mOperand2,
      final CompKind mComp) {
    super(mRtx);
    this.mComp = mComp;
    this.mOperand1 = mOperand1;
    this.mOperand2 = mOperand2;
    mIsFirst = true;
  }

  @Override
  public final void reset(final long mNodeKey) {
    super.reset(mNodeKey);
    mIsFirst = true;
    if (mOperand1 != null) {
      mOperand1.reset(mNodeKey);
    }

    if (mOperand2 != null) {
      mOperand2.reset(mNodeKey);
    }
  }

  @Override
  public final boolean hasNext() {
    resetToLastKey();

    if (mIsFirst) {
      mIsFirst = false;

      // TODO: why?
      if (!(mOperand1 instanceof LiteralExpr)) {
        mOperand1.reset(asXdmNodeReadTrx().getNodeKey());
      }

      // TODO: why?
      if (!(mOperand2 instanceof LiteralExpr)) {
        mOperand2.reset(asXdmNodeReadTrx().getNodeKey());
      }

      /*
       * Evaluates the comparison. First atomizes both operands and then executes the comparison on
       * them. At the end, the transaction is set to the retrieved result item.
       */
      if (mOperand1.hasNext()) {
        key = mOperand1.next();
        try {
          // atomize operands
          final AtomicValue[] operandOne = atomize(mOperand1);
          if (mOperand2.hasNext()) {
            key = mOperand2.next();
            final AtomicValue[] operandTwo = atomize(mOperand2);

            hook(operandOne, operandTwo);
            try {
              // get comparison result
              final boolean resultValue = compare(operandOne, operandTwo);
              final AtomicValue result = new AtomicValue(resultValue);

              // add retrieved AtomicValue to item list
              final int itemKey = asXdmNodeReadTrx().getItemList().addItem(result);
              key = itemKey;
            } catch (SirixXPathException e) {
              throw new RuntimeException(e);
            }
            return true;

          }
        } catch (final SirixXPathException exc) {
          throw new RuntimeException(exc);
        }
      }
    }
    // return empty sequence or function called more than once
    resetToStartKey();
    return false;
  }

  /**
   * Allowes the general comparisons to do some extra functionality.
   * 
   * @param paramOperandOne first operand
   * @param paramOperandTwo second operand
   */
  protected void hook(final AtomicValue[] paramOperandOne, final AtomicValue[] paramOperandTwo) {
    // do nothing
  }

  /**
   * Performs the comparison of two atomic values.
   * 
   * @param paramOperandOne first comparison operand.
   * @param paramOperandTwo second comparison operand.
   * @return the result of the comparison
   */
  protected abstract boolean compare(final AtomicValue[] paramOperandOne,
      final AtomicValue[] paramOperandTwo) throws SirixXPathException;

  /**
   * Atomizes an operand according to the rules specified in the XPath specification.
   * 
   * @param paramOperand the operand that will be atomized.
   * @return the atomized operand. (always an atomic value)
   * @throws SirixXPathException if any goes wrong.
   */
  protected abstract AtomicValue[] atomize(final Axis paramOperand) throws SirixXPathException;

  /**
   * Returns the common comparable type of the two operands, or an error, if the two operands don't
   * have a common type on which a comparison is allowed according to the XPath 2.0 specification.
   * 
   * @param mKey1 first comparison operand's type key
   * @param mKey2 second comparison operand's type key
   * @return the type the comparison can be evaluated on
   */
  protected abstract Type getType(final int mKey1, final int mKey2) throws SirixXPathException;

  /**
   * Getting CompKind for this Comparator.
   * 
   * @return comparison kind
   */
  public final CompKind getCompKind() {
    return mComp;
  }

  /**
   * Factory method to implement the comparator.
   * 
   * @param paramRtx rtx for accessing data
   * @param paramOperandOne operand one to be compared
   * @param paramOperandTwo operand two to be compared
   * @param paramKind kind of comparison
   * @param paramVal string value to estimate
   * @return AbsComparator the comparator of two axis
   */
  public static final AbstractComparator getComparator(final XmlNodeReadOnlyTrx paramRtx,
      final Axis paramOperandOne, final Axis paramOperandTwo, final CompKind paramKind,
      final String paramVal) {
    if ("eq".equals(paramVal) || "lt".equals(paramVal) || "le".equals(paramVal)
        || "gt".equals(paramVal) || "ge".equals(paramVal)) {
      return new ValueComp(paramRtx, paramOperandOne, paramOperandTwo, paramKind);
    } else if ("=".equals(paramVal) || "!=".equals(paramVal) || "<".equals(paramVal)
        || "<=".equals(paramVal) || ">".equals(paramVal) || ">=".equals(paramVal)) {
      return new GeneralComp(paramRtx, paramOperandOne, paramOperandTwo, paramKind);
    } else if ("is".equals(paramVal) || "<<".equals(paramVal) || ">>".equals(paramVal)) {
      new NodeComp(paramRtx, paramOperandOne, paramOperandTwo, paramKind);
    }
    throw new IllegalStateException(paramVal + " is not a valid comparison.");
  }

}
