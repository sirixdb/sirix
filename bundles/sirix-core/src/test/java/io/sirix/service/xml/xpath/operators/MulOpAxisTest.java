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

package io.sirix.service.xml.xpath.operators;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.exception.SirixException;
import io.sirix.service.xml.xpath.AbstractAxis;
import io.sirix.service.xml.xpath.AtomicValue;
import io.sirix.service.xml.xpath.XPathError;
import io.sirix.service.xml.xpath.expr.LiteralExpr;
import io.sirix.service.xml.xpath.expr.SequenceAxis;
import io.sirix.service.xml.xpath.types.Type;

public class MulOpAxisTest {

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
    XmlTestHelper.deleteEverything();
  }

  @Test
  public final void testOperate() throws SirixException {

    AtomicValue item1 = new AtomicValue(3.0, Type.DOUBLE);
    AtomicValue item2 = new AtomicValue(2.0, Type.DOUBLE);

    AbstractAxis op1 =
        new LiteralExpr(holder.getXmlNodeReadTrx(), holder.getXmlNodeReadTrx().getItemList().addItem(item1));
    AbstractAxis op2 =
        new LiteralExpr(holder.getXmlNodeReadTrx(), holder.getXmlNodeReadTrx().getItemList().addItem(item2));
    AbstractObAxis axis = new MulOpAxis(holder.getXmlNodeReadTrx(), op1, op2);

    assertEquals(true, axis.hasNext());
    axis.next();
    assertThat(6.0, is(Double.parseDouble(holder.getXmlNodeReadTrx().getValue())));
    assertEquals(holder.getXmlNodeReadTrx().keyForName("xs:double"), holder.getXmlNodeReadTrx().getTypeKey());
    assertEquals(false, axis.hasNext());

  }

  @Test
  public final void testGetReturnType() throws SirixException {

    AbstractAxis op1 = new SequenceAxis(holder.getXmlNodeReadTrx());
    AbstractAxis op2 = new SequenceAxis(holder.getXmlNodeReadTrx());
    AbstractObAxis axis = new MulOpAxis(holder.getXmlNodeReadTrx(), op1, op2);

    assertEquals(
        Type.DOUBLE,
        axis.getReturnType(
            holder.getXmlNodeReadTrx().keyForName("xs:double"),
            holder.getXmlNodeReadTrx().keyForName("xs:double")));
    assertEquals(
        Type.DOUBLE,
        axis.getReturnType(
            holder.getXmlNodeReadTrx().keyForName("xs:decimal"),
            holder.getXmlNodeReadTrx().keyForName("xs:double")));
    assertEquals(
        Type.FLOAT,
        axis.getReturnType(
            holder.getXmlNodeReadTrx().keyForName("xs:float"),
            holder.getXmlNodeReadTrx().keyForName("xs:decimal")));
    assertEquals(
        Type.DECIMAL,
        axis.getReturnType(
            holder.getXmlNodeReadTrx().keyForName("xs:decimal"),
            holder.getXmlNodeReadTrx().keyForName("xs:integer")));
    // assertEquals(Type.INTEGER,
    // axis.getReturnType(holder.getRtx().keyForName("xs:integer"),
    // holder.getRtx().keyForName("xs:integer")));
    assertEquals(
        Type.YEAR_MONTH_DURATION,
        axis.getReturnType(
            holder.getXmlNodeReadTrx().keyForName("xs:yearMonthDuration"),
            holder.getXmlNodeReadTrx().keyForName("xs:double")));
    assertEquals(
        Type.YEAR_MONTH_DURATION,
        axis.getReturnType(
            holder.getXmlNodeReadTrx().keyForName("xs:integer"),
            holder.getXmlNodeReadTrx().keyForName("xs:yearMonthDuration")));
    assertEquals(
        Type.DAY_TIME_DURATION,
        axis.getReturnType(
            holder.getXmlNodeReadTrx().keyForName("xs:dayTimeDuration"),
            holder.getXmlNodeReadTrx().keyForName("xs:double")));
    assertEquals(
        Type.DAY_TIME_DURATION,
        axis.getReturnType(
            holder.getXmlNodeReadTrx().keyForName("xs:integer"),
            holder.getXmlNodeReadTrx().keyForName("xs:dayTimeDuration")));

    try {

      axis.getReturnType(
          holder.getXmlNodeReadTrx().keyForName("xs:dateTime"),
          holder.getXmlNodeReadTrx().keyForName("xs:yearMonthDuration"));
      fail("Expected an XPathError-Exception.");
    } catch (XPathError e) {
      assertThat(
          e.getMessage(),
          is(
              "err:XPTY0004 The type is not appropriate the expression or the "
                  + "typedoes not match a required type as specified by the matching rules."));
    }

    try {

      axis.getReturnType(
          holder.getXmlNodeReadTrx().keyForName("xs:dateTime"), holder.getXmlNodeReadTrx().keyForName("xs:double"));
      fail("Expected an XPathError-Exception.");
    } catch (XPathError e) {
      assertThat(
          e.getMessage(),
          is(
              "err:XPTY0004 The type is not appropriate the expression or the "
                  + "typedoes not match a required type as specified by the matching rules."));
    }

    try {

      axis.getReturnType(
          holder.getXmlNodeReadTrx().keyForName("xs:string"),
          holder.getXmlNodeReadTrx().keyForName("xs:yearMonthDuration"));
      fail("Expected an XPathError-Exception.");
    } catch (XPathError e) {
      assertThat(
          e.getMessage(),
          is(
              "err:XPTY0004 The type is not appropriate the expression or the "
                  + "typedoes not match a required type as specified by the matching rules."));
    }

    try {

      axis.getReturnType(
          holder.getXmlNodeReadTrx().keyForName("xs:yearMonthDuration"),
          holder.getXmlNodeReadTrx().keyForName("xs:yearMonthDuration"));
      fail("Expected an XPathError-Exception.");
    } catch (XPathError e) {
      assertThat(
          e.getMessage(),
          is(
              "err:XPTY0004 The type is not appropriate the expression or the "
                  + "typedoes not match a required type as specified by the matching rules."));
    }

  }

}
