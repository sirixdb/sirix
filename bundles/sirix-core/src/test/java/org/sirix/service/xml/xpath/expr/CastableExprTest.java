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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.service.xml.xpath.XPathError;

/**
 * JUnit-test class to test the functionality of the CastableExpr.
 * 
 * @author Tina Scherer
 */
public class CastableExprTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
    XdmTestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XdmTestHelper.closeEverything();
  }

  @Test
  public void testCastableExpr() throws SirixException {

    final AbstractAxis axis1 = new XPathAxis(holder.getXdmNodeReadTrx(), "1 castable as xs:decimal");
    assertEquals(true, axis1.hasNext());
    axis1.next();
    assertEquals(holder.getXdmNodeReadTrx().keyForName("xs:boolean"), holder.getXdmNodeReadTrx().getTypeKey());
    assertEquals(true, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis1.hasNext());

    final AbstractAxis axis2 =
        new XPathAxis(holder.getXdmNodeReadTrx(), "10.0 castable as xs:anyAtomicType");
    try {
      assertEquals(true, axis2.hasNext());
      axis2.next();
    } catch (XPathError e) {
      assertThat(
          e.getMessage(),
          is(
              "err:XPST0080 " + "Target type of a cast or castable expression must not be "
                  + "xs:NOTATION or xs:anyAtomicType."));
    }

    // Token is not implemented yet.
    // final IAxis axis3 = new XPathAxis(holder.getRtx(),
    // "\"hello\" castable as xs:token");
    // assertEquals(true, axis3.hasNext());
    // assertEquals(Type.BOOLEAN, holder.getRtx().getValueTypeAsType());
    // assertEquals(true, holder.getRtx().getValueAsBoolean());
    // assertEquals(false, axis3.hasNext());

    final AbstractAxis axis4 = new XPathAxis(holder.getXdmNodeReadTrx(), "\"hello\" castable as xs:string");
    assertEquals(true, axis4.hasNext());
    axis4.next();
    assertEquals(holder.getXdmNodeReadTrx().keyForName("xs:boolean"), holder.getXdmNodeReadTrx().getTypeKey());
    assertEquals(true, Boolean.parseBoolean(holder.getXdmNodeReadTrx().getValue()));
    assertEquals(false, axis4.hasNext());

    // final IAxis axis5 = new XPathAxis(holder.getRtx(),
    // "\"hello\" castable as xs:decimal");
    // assertEquals(true, axis5.hasNext());
    // assertEquals(holder.getRtx().keyForName("xs:boolean"),
    // holder.getRtx().getTypeKey());
    // assertEquals(true, Boolean.parseBoolean(holder.getRtx().getValue()));
    // assertEquals(false, axis5.hasNext());

  }

}
