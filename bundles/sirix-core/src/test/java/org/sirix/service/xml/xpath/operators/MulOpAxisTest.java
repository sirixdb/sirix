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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.exception.SirixException;
import org.sirix.node.interfaces.Node;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.XPathError;
import org.sirix.service.xml.xpath.expr.LiteralExpr;
import org.sirix.service.xml.xpath.expr.SequenceAxis;
import org.sirix.service.xml.xpath.types.Type;

public class MulOpAxisTest {

	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		holder = Holder.generateRtx();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.deleteEverything();
	}

	@Test
	public final void testOperate() throws SirixException {

		AtomicValue item1 = new AtomicValue(3.0, Type.DOUBLE);
		AtomicValue item2 = new AtomicValue(2.0, Type.DOUBLE);

		AbstractAxis op1 = new LiteralExpr(holder.getRtx(), holder.getRtx()
				.getItemList().addItem(item1));
		AbstractAxis op2 = new LiteralExpr(holder.getRtx(), holder.getRtx()
				.getItemList().addItem(item2));
		AbstractObAxis axis = new MulOpAxis(holder.getRtx(), op1, op2);

		assertEquals(true, axis.hasNext());
		axis.next();
		assertThat(6.0, is(Double.parseDouble(holder.getRtx().getValue())));
		assertEquals(holder.getRtx().keyForName("xs:double"), holder.getRtx()
				.getTypeKey());
		assertEquals(false, axis.hasNext());

	}

	@Test
	public final void testGetReturnType() throws SirixException {

		AbstractAxis op1 = new SequenceAxis(holder.getRtx());
		AbstractAxis op2 = new SequenceAxis(holder.getRtx());
		AbstractObAxis axis = new MulOpAxis(holder.getRtx(), op1, op2);

		assertEquals(Type.DOUBLE, axis.getReturnType(
				holder.getRtx().keyForName("xs:double"),
				holder.getRtx().keyForName("xs:double")));
		assertEquals(Type.DOUBLE, axis.getReturnType(
				holder.getRtx().keyForName("xs:decimal"),
				holder.getRtx().keyForName("xs:double")));
		assertEquals(Type.FLOAT, axis.getReturnType(
				holder.getRtx().keyForName("xs:float"),
				holder.getRtx().keyForName("xs:decimal")));
		assertEquals(Type.DECIMAL, axis.getReturnType(
				holder.getRtx().keyForName("xs:decimal"),
				holder.getRtx().keyForName("xs:integer")));
		// assertEquals(Type.INTEGER,
		// axis.getReturnType(holder.getRtx().keyForName("xs:integer"),
		// holder.getRtx().keyForName("xs:integer")));
		assertEquals(Type.YEAR_MONTH_DURATION, axis.getReturnType(holder.getRtx()
				.keyForName("xs:yearMonthDuration"),
				holder.getRtx().keyForName("xs:double")));
		assertEquals(Type.YEAR_MONTH_DURATION, axis.getReturnType(holder.getRtx()
				.keyForName("xs:integer"),
				holder.getRtx().keyForName("xs:yearMonthDuration")));
		assertEquals(Type.DAY_TIME_DURATION, axis.getReturnType(holder.getRtx()
				.keyForName("xs:dayTimeDuration"),
				holder.getRtx().keyForName("xs:double")));
		assertEquals(Type.DAY_TIME_DURATION, axis.getReturnType(holder.getRtx()
				.keyForName("xs:integer"),
				holder.getRtx().keyForName("xs:dayTimeDuration")));

		try {

			axis.getReturnType(holder.getRtx().keyForName("xs:dateTime"), holder
					.getRtx().keyForName("xs:yearMonthDuration"));
			fail("Expected an XPathError-Exception.");
		} catch (XPathError e) {
			assertThat(
					e.getMessage(),
					is("err:XPTY0004 The type is not appropriate the expression or the "
							+ "typedoes not match a required type as specified by the matching rules."));
		}

		try {

			axis.getReturnType(holder.getRtx().keyForName("xs:dateTime"), holder
					.getRtx().keyForName("xs:double"));
			fail("Expected an XPathError-Exception.");
		} catch (XPathError e) {
			assertThat(
					e.getMessage(),
					is("err:XPTY0004 The type is not appropriate the expression or the "
							+ "typedoes not match a required type as specified by the matching rules."));
		}

		try {

			axis.getReturnType(holder.getRtx().keyForName("xs:string"), holder
					.getRtx().keyForName("xs:yearMonthDuration"));
			fail("Expected an XPathError-Exception.");
		} catch (XPathError e) {
			assertThat(
					e.getMessage(),
					is("err:XPTY0004 The type is not appropriate the expression or the "
							+ "typedoes not match a required type as specified by the matching rules."));
		}

		try {

			axis.getReturnType(holder.getRtx().keyForName("xs:yearMonthDuration"),
					holder.getRtx().keyForName("xs:yearMonthDuration"));
			fail("Expected an XPathError-Exception.");
		} catch (XPathError e) {
			assertThat(
					e.getMessage(),
					is("err:XPTY0004 The type is not appropriate the expression or the "
							+ "typedoes not match a required type as specified by the matching rules."));
		}

	}

}
