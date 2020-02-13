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

package org.sirix.service.xml.xpath.parser;

import static org.junit.Assert.assertEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.service.xml.xpath.XPathAxis;

public class XPathParserTest {

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
  public void testLiterals() throws SirixException {

    holder.getXmlNodeReadTrx().moveTo(2L);

    AbstractAxis axis;

    axis = new XPathAxis(holder.getXmlNodeReadTrx(), "\"12.5\"");
    assertEquals(true, axis.hasNext());
    axis.next();
    assertEquals("12.5", holder.getXmlNodeReadTrx().getValue());
    assertEquals(holder.getXmlNodeReadTrx().keyForName("xs:string"), holder.getXmlNodeReadTrx().getTypeKey());
    assertEquals(false, axis.hasNext());

    axis = new XPathAxis(holder.getXmlNodeReadTrx(), "\"He said, \"\"I don't like it\"\"\"");
    assertEquals(true, axis.hasNext());
    axis.next();
    assertEquals("He said, I don't like it", holder.getXmlNodeReadTrx().getValue());
    assertEquals(holder.getXmlNodeReadTrx().keyForName("xs:string"), holder.getXmlNodeReadTrx().getTypeKey());
    assertEquals(false, axis.hasNext());

    axis = new XPathAxis(holder.getXmlNodeReadTrx(), "12");
    assertEquals(true, axis.hasNext());
    axis.next();
    assertEquals(holder.getXmlNodeReadTrx().keyForName("xs:integer"), holder.getXmlNodeReadTrx().getTypeKey());
    assertEquals("12", holder.getXmlNodeReadTrx().getValue());
    assertEquals(false, axis.hasNext());

    axis = new XPathAxis(holder.getXmlNodeReadTrx(), "12.5");
    assertEquals(true, axis.hasNext());
    axis.next();
    assertEquals(holder.getXmlNodeReadTrx().keyForName("xs:decimal"), holder.getXmlNodeReadTrx().getTypeKey());
    assertEquals("12.5", holder.getXmlNodeReadTrx().getValue());
    assertEquals(false, axis.hasNext());

    axis = new XPathAxis(holder.getXmlNodeReadTrx(), "12.5E2");
    assertEquals(true, axis.hasNext());
    axis.next();
    assertEquals(holder.getXmlNodeReadTrx().keyForName("xs:double"), holder.getXmlNodeReadTrx().getTypeKey());
    assertEquals("12.5E2", holder.getXmlNodeReadTrx().getValue());
    assertEquals(false, axis.hasNext());

    axis = new XPathAxis(holder.getXmlNodeReadTrx(), "1");
    assertEquals(true, axis.hasNext());
    axis.next();
    assertEquals("1", holder.getXmlNodeReadTrx().getValue());
    assertEquals(holder.getXmlNodeReadTrx().keyForName("xs:integer"), holder.getXmlNodeReadTrx().getTypeKey());
    assertEquals(false, axis.hasNext());

  }

  @Test
  public void testEBNF() throws SirixException {

    XPathParser parser = new XPathParser(holder.getXmlNodeReadTrx(), "/p:a");
    parser.parseQuery();

    parser = new XPathParser(holder.getXmlNodeReadTrx(), "/p:a/node(), /b/descendant-or-self::adsfj");
    parser.parseQuery();

    parser = new XPathParser(holder.getXmlNodeReadTrx(), "for $i in /p:a return $i");
    parser.parseQuery();

    parser = new XPathParser(holder.getXmlNodeReadTrx(), "for $i in /p:a return /p:a");
    parser.parseQuery();

    parser = new XPathParser(holder.getXmlNodeReadTrx(), "child::element(person)");
    parser.parseQuery();

    parser = new XPathParser(holder.getXmlNodeReadTrx(), "child::element(person, xs:string)");
    parser.parseQuery();

    parser = new XPathParser(holder.getXmlNodeReadTrx(), " child::element(*, xs:string)");
    parser.parseQuery();

    parser = new XPathParser(holder.getXmlNodeReadTrx(), "child::element()");
    parser.parseQuery();

    // parser = new XPathParser(holder.getRtx(), ". treat as item()");
    // parser.parseQuery();

    parser = new XPathParser(holder.getXmlNodeReadTrx(), "/b instance of item()");
    parser.parseQuery();

  }

}
