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

package org.sirix.service.xml.xpath.expr;

import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.types.Type;

/**
 * JUnit-test class to test the functionality of the LiteralExpr.
 * 
 * @author Tina Scherer
 */
public class LiteralExprTest {

	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		TestHelper.createTestDocument();
		holder = Holder.generateRtx();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();
	}

	@Test
	public void testLiteralExpr() throws SirixException {
		// Build simple test tree.
		final AtomicValue item1 = new AtomicValue(false);
		final AtomicValue item2 = new AtomicValue(14, Type.INTEGER);

		final int key1 = holder.getRtx().getItemList().addItem(item1);
		final int key2 = holder.getRtx().getItemList().addItem(item2);

		final AbstractAxis axis1 = new LiteralExpr(holder.getRtx(), key1);
		assertEquals(true, axis1.hasNext());
		axis1.next();
		assertEquals(key1, holder.getRtx().getNodeKey());
		assertEquals(holder.getRtx().keyForName("xs:boolean"), holder.getRtx()
				.getTypeKey());
		assertEquals(false, Boolean.parseBoolean(holder.getRtx().getValue()));
		assertEquals(false, axis1.hasNext());

		final AbstractAxis axis2 = new LiteralExpr(holder.getRtx(), key2);
		assertEquals(true, axis2.hasNext());
		axis2.next();
		assertEquals(key2, holder.getRtx().getNodeKey());
		assertEquals(holder.getRtx().keyForName("xs:integer"), holder.getRtx()
				.getTypeKey());
		assertEquals(14, Integer.parseInt(holder.getRtx().getValue()));
		assertEquals(false, axis2.hasNext());

	}

}
