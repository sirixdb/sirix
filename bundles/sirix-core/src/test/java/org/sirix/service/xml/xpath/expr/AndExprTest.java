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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import java.util.NoSuchElementException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.service.xml.xpath.XPathError;

/**
 * JUnit-test class to test the functionality of the AndExpr.
 * 
 * @author Tina Scherer
 * 
 */
public class AndExprTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test(expected = NoSuchElementException.class)
  public void testAnd() throws SirixException {
    long iTrue = holder.getXdmNodeReadTrx().getItemList().addItem(new AtomicValue(true));
    long iFalse = holder.getXdmNodeReadTrx().getItemList().addItem(new AtomicValue(false));

    AbstractAxis trueLit1 = new LiteralExpr(holder.getXdmNodeReadTrx(), iTrue);
    AbstractAxis trueLit2 = new LiteralExpr(holder.getXdmNodeReadTrx(), iTrue);
    AbstractAxis falseLit1 = new LiteralExpr(holder.getXdmNodeReadTrx(), iFalse);
    AbstractAxis falseLit2 = new LiteralExpr(holder.getXdmNodeReadTrx(), iFalse);

    AbstractAxis axis1 = new AndExpr(holder.getXdmNodeReadTrx(), trueLit1, trueLit2);
    assertEquals(true, axis1.hasNext());
    axis1.next();
    assertEquals(true, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis1.hasNext());

    AbstractAxis axis2 = new AndExpr(holder.getXdmNodeReadTrx(), trueLit1, falseLit1);
    assertEquals(true, axis2.hasNext());
    axis2.next();
    assertEquals(false, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis2.hasNext());

    AbstractAxis axis3 = new AndExpr(holder.getXdmNodeReadTrx(), falseLit1, trueLit1);
    assertEquals(true, axis3.hasNext());
    axis3.next();
    assertEquals(false, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis3.hasNext());

    AbstractAxis axis4 = new AndExpr(holder.getXdmNodeReadTrx(), falseLit1, falseLit2);
    assertEquals(true, axis4.hasNext());
    axis4.next();
    assertEquals(false, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis4.hasNext());
    axis4.next();
  }

  @Test
  public void testAndQuery() throws SirixException {

    holder.getXdmNodeReadTrx().moveTo(1L);

    final AbstractAxis axis1 = new XPathAxis(holder.getXdmNodeReadTrx(), "text() and node()");
    assertEquals(true, axis1.hasNext());
    axis1.next();
    assertEquals(true, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis1.hasNext());

    final AbstractAxis axis2 = new XPathAxis(holder.getXdmNodeReadTrx(), "comment() and node()");
    assertEquals(true, axis2.hasNext());
    axis2.next();
    assertEquals(false, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis2.hasNext());

    final AbstractAxis axis3 = new XPathAxis(holder.getXdmNodeReadTrx(), "1 eq 1 and 2 eq 2");
    assertEquals(true, axis3.hasNext());
    axis3.next();
    assertEquals(true, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis3.hasNext());

    final AbstractAxis axis4 = new XPathAxis(holder.getXdmNodeReadTrx(), "1 eq 1 and 2 eq 3");
    assertEquals(true, axis4.hasNext());
    axis4.next();
    assertEquals(false, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis4.hasNext());

    // is never evaluated.
    final AbstractAxis axis5 = new XPathAxis(holder.getXdmNodeReadTrx(), "1 eq 2 and (3 idiv 0 = 1)");
    assertEquals(true, axis5.hasNext());
    axis5.next();
    assertEquals(false, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis5.hasNext());

    final AbstractAxis axis6 = new XPathAxis(holder.getXdmNodeReadTrx(), "1 eq 1 and 3 idiv 0 = 1");
    try {
      assertEquals(true, axis6.hasNext());
      axis6.next();
      fail("Expected XPath exception, because of division by zero");
    } catch (XPathError e) {
      assertEquals("err:FOAR0001: Division by zero.", e.getMessage());
    }

  }

}
