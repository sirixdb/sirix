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

import static org.sirix.service.xml.xpath.XPathAxis.XPATH_10_COMP;

import java.util.ArrayList;
import java.util.List;

import org.sirix.api.Axis;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.functions.Function;
import org.sirix.service.xml.xpath.types.Type;

/**
 * <h1>GeneralComp</h1>
 * <p>
 * General comparisons are existentially quantified comparisons that may be applied to operand
 * sequences of any length.
 * </p>
 */
public class GeneralComp extends AbstractComparator {

	/**
	 * Constructor. Initializes the internal state.
	 * 
	 * @param rtx Exclusive (immutable) trx to iterate with.
	 * @param mOperand1 First value of the comparison
	 * @param mOperand2 Second value of the comparison
	 * @param mCom comparison kind
	 */
	public GeneralComp(final XdmNodeReadTrx rtx, final Axis mOperand1, final Axis mOperand2,
			final CompKind mCom) {

		super(rtx, mOperand1, mOperand2, mCom);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected boolean compare(final AtomicValue[] mOperand1, final AtomicValue[] mOperand2)
			throws SirixXPathException {

		assert mOperand1.length >= 1 && mOperand2.length >= 1;

		for (AtomicValue op1 : mOperand1) {
			for (AtomicValue op2 : mOperand2) {
				String value1 = new String(op1.getRawValue());
				String value2 = new String(op2.getRawValue());
				if (getCompKind().compare(value1, value2, getType(op1.getTypeKey(), op2.getTypeKey()))) {
					return true;
				}
			}
		}

		return false;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected AtomicValue[] atomize(final Axis mOperand) {

		final XdmNodeReadTrx rtx = getTrx();
		final List<AtomicValue> op = new ArrayList<AtomicValue>();
		AtomicValue atomized;
		// cast to double, if compatible with XPath 1.0 and <, >, >=, <=
		final boolean convert =
				!(!XPATH_10_COMP || getCompKind() == CompKind.EQ || getCompKind() == CompKind.EQ);

		boolean first = true;
		do {
			if (first) {
				first = false;
			} else {
				mOperand.next();
			}
			if (convert) { // cast to double
				Function.fnnumber(rtx);
			}
			atomized = new AtomicValue(rtx.getValue().getBytes(), rtx.getTypeKey());
			op.add(atomized);
		} while (mOperand.hasNext());

		return op.toArray(new AtomicValue[op.size()]);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected Type getType(final int mKey1, final int mKey2) throws SirixXPathException {

		final Type mType1 = Type.getType(mKey1).getPrimitiveBaseType();
		final Type mType2 = Type.getType(mKey2).getPrimitiveBaseType();

		if (XPATH_10_COMP) {
			if (mType1.isNumericType() || mType2.isNumericType()) {
				return Type.DOUBLE;
			}

			if (mType1 == Type.STRING || mType2 == Type.STRING
					|| (mType1 == Type.UNTYPED_ATOMIC && mType2 == Type.UNTYPED_ATOMIC)) {
				return Type.STRING;
			}

			if (mType1 == Type.UNTYPED_ATOMIC || mType2 == Type.UNTYPED_ATOMIC) {
				return Type.UNTYPED_ATOMIC;

			}

		} else {
			if (mType1 == Type.UNTYPED_ATOMIC) {
				switch (mType2) {
					case UNTYPED_ATOMIC:
					case STRING:
						return Type.STRING;
					case INTEGER:
					case DECIMAL:
					case FLOAT:
					case DOUBLE:
						return Type.DOUBLE;

					default:
						return mType2;
				}
			}

			if (mType2 == Type.UNTYPED_ATOMIC) {
				switch (mType1) {
					case UNTYPED_ATOMIC:
					case STRING:
						return Type.STRING;
					case INTEGER:
					case DECIMAL:
					case FLOAT:
					case DOUBLE:
						return Type.DOUBLE;

					default:
						return mType1;
				}
			}

		}

		return Type.getLeastCommonType(mType1, mType2);

	}

	// protected void hook(final AtomicValue[] operand1, final AtomicValue[]
	// operand2) {
	//
	// if (operand1.length == 1
	// && operand1[0].getTypeKey() == getTransaction()
	// .keyForName("xs:boolean")) {
	// operand2 = new AtomicValue[1];
	// getOperand2().reset(startKey);
	// operand2[0] = new AtomicValue(Function.ebv(getOperand1()));
	// } else {
	// if (operand2.length == 1
	// && operand2[0].getTypeKey() == getTransaction().keyForName(
	// "xs:boolean")) {
	// operand1 = new AtomicValue[1];
	// getOperand1().reset(startKey);
	// operand1[0] = new AtomicValue(Function.ebv(getOperand2()));
	// }
	// }
	// }

}
