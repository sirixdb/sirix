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

package org.sirix.saxon.wrapper;

import net.sf.saxon.s9api.XPathSelector;
import net.sf.saxon.s9api.XdmItem;
import org.custommonkey.xmlunit.XMLTestCase;
import org.custommonkey.xmlunit.XMLUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.IDatabase;
import org.sirix.exception.AbsTTException;
import org.sirix.saxon.evaluator.XPathEvaluator;

/**
 * Test XPath S9Api.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class TestNodeWrapperS9ApiXPath extends XMLTestCase {

  /**
   * sirix database on sirix test document {@link IDatabase}.
   */
  private transient Holder mHolder;

  @Override
  @Before
  public void setUp() throws AbsTTException {
    XMLUnit.setIgnoreWhitespace(true);
    TestHelper.deleteEverything();
    TestHelper.createTestDocument();
    mHolder = Holder.generateRtx();
  }

  @Override
  @After
  public void tearDown() throws AbsTTException {
    mHolder.close();
  }

  @Test
  public void testB1() throws Exception {
    final XPathSelector selector = new XPathEvaluator.Builder("//b[1]", mHolder.getSession()).build().call();

    final StringBuilder strBuilder = new StringBuilder();

    for (final XdmItem item : selector) {
      strBuilder.append(item.toString());
    }

    assertXMLEqual("expected pieces to be similar", "<b xmlns:p=\"ns\">foo<c xmlns:p=\"ns\"/></b>",
      strBuilder.toString());
  }

  @Test
  public void testB1String() throws Exception {
    final XPathSelector selector =
      new XPathEvaluator.Builder("//b[1]/text()", mHolder.getSession()).build().call();

    final StringBuilder strBuilder = new StringBuilder();

    for (final XdmItem item : selector) {
      strBuilder.append(item.toString());
    }

    assertEquals("foo", strBuilder.toString());
  }

  @Test
  public void testB2() throws Exception {
    final XPathSelector selector = new XPathEvaluator.Builder("//b[2]", mHolder.getSession()).build().call();

    final StringBuilder strBuilder = new StringBuilder();

    for (final XdmItem item : selector) {
      strBuilder.append(item.toString());
    }

    assertXMLEqual("expected pieces to be similar", "<b xmlns:p=\"ns\" p:x=\"y\"><c xmlns:p=\"ns\"/>bar</b>",
      strBuilder.toString());
  }

  @Test
  public void testB2Text() throws Exception {
    final XPathSelector selector =
      new XPathEvaluator.Builder("//b[2]/text()", mHolder.getSession()).build().call();

    final StringBuilder strBuilder = new StringBuilder();

    for (final XdmItem item : selector) {
      strBuilder.append(item.toString());
    }

    assertEquals("bar", strBuilder.toString());
  }

  @Test
  public void testB() throws Exception {
    final XPathSelector selector = new XPathEvaluator.Builder("//b", mHolder.getSession()).build().call();

    final StringBuilder strBuilder = new StringBuilder();

    strBuilder.append("<result>");
    for (final XdmItem item : selector) {
      strBuilder.append(item.toString());
    }
    strBuilder.append("</result>");

    assertXMLEqual("expected pieces to be similar",
      "<result><b xmlns:p=\"ns\">foo<c xmlns:p=\"ns\"/></b><b xmlns:p=\"ns\" p:x=\"y\">"
        + "<c xmlns:p=\"ns\"/>bar</b></result>", strBuilder.toString());
  }

  @Test
  public void testCountB() throws Exception {
    final XPathSelector selector =
      new XPathEvaluator.Builder("count(//b)", mHolder.getSession()).build().call();

    final StringBuilder sb = new StringBuilder();

    for (final XdmItem item : selector) {
      sb.append(item.getStringValue());
    }

    assertEquals("2", sb.toString());
  }

}
