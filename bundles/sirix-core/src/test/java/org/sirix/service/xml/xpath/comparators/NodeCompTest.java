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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.api.Axis;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.expr.LiteralExpr;
import org.sirix.service.xml.xpath.types.Type;

public class NodeCompTest {

  private AbstractComparator comparator;
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateRtx();
    comparator = new NodeComp(holder.getXmlNodeReadTrx(), new LiteralExpr(holder.getXmlNodeReadTrx(), -2),
                              new LiteralExpr(holder.getXmlNodeReadTrx(), -1), CompKind.IS);
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testCompare() throws SirixXPathException {

    AtomicValue[] op1 = {new AtomicValue(2, Type.INTEGER)};
    AtomicValue[] op2 = {new AtomicValue(3, Type.INTEGER)};
    AtomicValue[] op3 = {new AtomicValue(3, Type.INTEGER)};

    assertEquals(false, comparator.compare(op1, op2));
    assertEquals(true, comparator.compare(op3, op2));

    try {
      comparator = new NodeComp(holder.getXmlNodeReadTrx(), new LiteralExpr(holder.getXmlNodeReadTrx(), -2),
                                new LiteralExpr(holder.getXmlNodeReadTrx(), -1), CompKind.PRE);
      comparator.compare(op1, op2);
      fail("Expexcted not yet implemented exception.");
    } catch (IllegalStateException e) {
      assertEquals("Evaluation of node comparisons not possible", e.getMessage());
    }

    try {
      comparator = new NodeComp(holder.getXmlNodeReadTrx(), new LiteralExpr(holder.getXmlNodeReadTrx(), -2),
                                new LiteralExpr(holder.getXmlNodeReadTrx(), -1), CompKind.FO);
      comparator.compare(op1, op2);
      fail("Expexcted not yet implemented exception.");
    } catch (IllegalStateException e) {
      assertEquals("Evaluation of node comparisons not possible", e.getMessage());
    }

  }

  @Test
  public void testAtomize() throws SirixXPathException {
    Axis axis = new LiteralExpr(holder.getXmlNodeReadTrx(), -2);
    axis.hasNext();
    axis.next();
    AtomicValue[] value = comparator.atomize(axis);
    assertEquals(value.length, 1);
    assertEquals(holder.getXmlNodeReadTrx().getNodeKey(), value[0].getNodeKey());
    assertEquals("xs:integer", value[0].getType());

    try {
      axis = new DescendantAxis(holder.getXmlNodeReadTrx());
      axis.hasNext();
      comparator.atomize(axis);
    } catch (SirixXPathException e) {
      assertEquals(
          "err:XPTY0004 The type is not appropriate the expression or"
              + " the typedoes not match a required type as specified by the " + "matching rules.",
          e.getMessage());
    }

  }

  @Test
  public void testGetType() throws SirixXPathException {

    assertEquals(Type.INTEGER, comparator.getType(123, 2435));
  }
}
