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

package org.treetank.saxon.wrapper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Iterator;

import javax.xml.XMLConstants;
import javax.xml.namespace.NamespaceContext;
import javax.xml.namespace.QName;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathFactory;
import javax.xml.xpath.XPathFactoryConfigurationException;

import net.sf.saxon.Configuration;
import net.sf.saxon.lib.NamespaceConstant;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.xpath.XPathFactoryImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.treetank.Holder;
import org.treetank.TestHelper;
import org.treetank.api.INodeReadTrx;
import org.treetank.exception.AbsTTException;
import org.treetank.node.ENode;
import org.treetank.node.interfaces.INode;

/**
 * Test XPath Java API.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class TestNodeWrapperXPath {

  private Holder mHolder;

  /** XPath expression. */
  private static transient XPath xpe;

  /** Saxon configuration. */
  private static transient Configuration config;

  @Before
  public void setUp() throws AbsTTException, XPathFactoryConfigurationException {
    TestHelper.deleteEverything();
    TestHelper.createTestDocument();
    mHolder = Holder.generateRtx();

    // Saxon setup.
    System.setProperty("javax.xml.xpath.XPathFactory:" + NamespaceConstant.OBJECT_MODEL_SAXON,
      "net.sf.saxon.xpath.XPathFactoryImpl");

    XPathFactory xpf = XPathFactory.newInstance(NamespaceConstant.OBJECT_MODEL_SAXON);
    xpe = xpf.newXPath();
    config = ((XPathFactoryImpl)xpf).getConfiguration();
  }

  @After
  public void tearDown() throws Exception {
    mHolder.close();
    TestHelper.deleteEverything();
  }

  /**
   * XPath tests.
   * 
   * @param expressions
   *          Expressions, which are used.
   * @param doc
   *          The test document.
   * @param expectedResults
   *          Expected result for each expression.
   * @param result
   *          Array with the result types for each expression.
   * @param xpathConstants
   *          XPathConstants for each expression.
   * @param namespaces
   *          Array of boolean values for each expression.
   * @param namespace
   *          The namespace Context, which is used.
   * @throws Exception
   *           Any Exception which maybe occurs.
   */
  @SuppressWarnings("unchecked")
  @Ignore("Not a test, utility method only")
  public void test(final String[] expressions, final Object doc, final Object[] expectedResults,
    Object[] result, final QName[] xpathConstants, final boolean[] namespaces, final Object namespace)
    throws Exception {

    // For every expected result.
    for (int i = 0; i < expectedResults.length; i++) {

      // If namespace is required.
      if (namespaces[i]) {
        xpe.setNamespaceContext((NamespaceContext)namespace);
      }

      final XPathExpression findLine = xpe.compile(expressions[i]);

      // Cast the evaluated value.
      if (xpathConstants[i].equals(XPathConstants.NODESET)) {
        result[i] = findLine.evaluate(doc, xpathConstants[i]);
      } else if (xpathConstants[i].equals(XPathConstants.STRING)) {
        result[i] = (String)findLine.evaluate(doc, xpathConstants[i]);
      } else if (xpathConstants[i].equals(XPathConstants.NUMBER)) {
        result[i] = Double.parseDouble(findLine.evaluate(doc, xpathConstants[i]).toString());
      } else {
        throw new IllegalStateException("Unknown XPathConstant!");
      }

      assertNotNull(result);

      if (xpathConstants[i].equals(XPathConstants.NODESET)) {
        final ArrayList<INode> test = (ArrayList<INode>)result[i];

        final String res = (String)expectedResults[i];
        final String[] expRes = res.split(" ");

        // Iterate over expected result and the actual result and
        // compare it.
        for (int j = 0; j < test.size(); j++) {
          final INode item = test.get(j);

          mHolder.getRtx().moveTo(item.getNodeKey());

          final QName qName = mHolder.getRtx().getQNameOfCurrentNode();

          if (mHolder.getRtx().getNode().getKind() == ENode.ELEMENT_KIND) {
            assertEquals(expRes[j], qName.getPrefix() + ":" + qName.getLocalPart());
          } else if (mHolder.getRtx().getNode().getKind() == ENode.TEXT_KIND) {
            assertEquals(expRes[j], mHolder.getRtx().getValueOfCurrentNode());
          }

        }
      } else {
        assertEquals(expectedResults[i], result[i]);
      }
    }
  }

  @Test
  public void testExample() throws Exception {

    final String[] expressions =
      {
        "count(//b)", "count(//p:a)", "//p:a/@i", "//p:a/@p:i", "//b[1]/text()", "//b[2]", "//b[1]",
        "//b[2]/text()", "//p:a/text()"
      };

    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    final Object[] expectedResults = {
      2D, 1D, "j", "", "foo",
      // "<b p:x=\"y\"><c/>bar</b>",
      "bar", "foo",
      // "<b>foo<c/></b>",
      "bar", "oops1 oops2 oops3"
    };

    Object[] result = {
      "", "", "", 0D, "", "", "", "", ""
    };

    final QName[] xpathConstants =
      {
        XPathConstants.NUMBER, XPathConstants.NUMBER, XPathConstants.STRING, XPathConstants.STRING,
        XPathConstants.STRING, XPathConstants.STRING, XPathConstants.STRING, XPathConstants.STRING,
        XPathConstants.NODESET,
      };

    final boolean[] namespaces = {
      false, true, true, false, false, false, false, false, true
    };

    test(expressions, doc, expectedResults, result, xpathConstants, namespaces, new DocNamespaceContext());
  }

  @Test
  public void testElementBCount() throws Exception {

    final XPathExpression findLine = xpe.compile("count(//b)");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final double result = Double.parseDouble(findLine.evaluate(doc, XPathConstants.NUMBER).toString());
    assertNotNull(result);
    assertEquals(2D, result, 0D);
  }

  @Test
  public void testElementACount() throws Exception {

    final XPathExpression findLine = xpe.compile("count(//a)");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final double result = Double.parseDouble(findLine.evaluate(doc, XPathConstants.NUMBER).toString());
    assertNotNull(result);
    assertEquals(0D, result, 0D);
  }

  @Test
  public void testNamespaceElementCount() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("count(//p:a)");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final double result = Double.parseDouble(findLine.evaluate(doc, XPathConstants.NUMBER).toString());
    assertNotNull(result);
    assertEquals(1D, result, 0D);
  }

  @Test
  public void testAttributeCount() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("count(//p:a/@i)");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final double result = Double.parseDouble(findLine.evaluate(doc, XPathConstants.NUMBER).toString());
    assertNotNull(result);
    assertEquals(1D, result, 0D);
  }

  @Test
  public void testNamespaceAttributeCount() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("count(//p:a/@p:i)");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final double result = Double.parseDouble(findLine.evaluate(doc, XPathConstants.NUMBER).toString());

    assertNotNull(result);
    assertEquals(0D, result, 0D);
  }

  @Test
  public void testAttributeValue() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//p:a/@i");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final String result = findLine.evaluate(doc, XPathConstants.STRING).toString();

    assertNotNull(result);
    assertEquals("j", result);
  }

  @Test
  public void testNamespaceAttributeValue() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//p:a/@p:i");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final String result = findLine.evaluate(doc, XPathConstants.STRING).toString();

    assertNotNull(result);
    assertEquals("", result);
  }

  @Test
  public void testText() throws Exception {
    final XPathExpression findLine = xpe.compile("//b[1]/text()");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final String result = (String)findLine.evaluate(doc, XPathConstants.STRING);

    assertNotNull(result);
    assertEquals("foo", result);
  }

  @Test
  public void testText1() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//p:a[1]/text()[1]");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final String result = (String)findLine.evaluate(doc, XPathConstants.STRING);

    assertNotNull(result);
    assertEquals("oops1", result);
  }

  @Test
  public void testDefaultNamespaceText1() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//p:a/text()[1]");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final String result = (String)findLine.evaluate(doc, XPathConstants.STRING);

    assertNotNull(result);
    assertEquals("oops1", result);
  }

  @Test
  public void testDefaultNamespaceText2() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//p:a/text()[2]");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final String result = (String)findLine.evaluate(doc, XPathConstants.STRING);

    assertNotNull(result);
    assertEquals("oops2", result);
  }

  @Test
  public void testDefaultNamespaceText3() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//p:a/text()[3]");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final String result = (String)findLine.evaluate(doc, XPathConstants.STRING);

    assertNotNull(result);
    assertEquals("oops3", result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testDefaultNamespaceTextAll() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//p:a/text()");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final ArrayList<INode> result = (ArrayList<INode>)findLine.evaluate(doc, XPathConstants.NODESET);
    assertNotNull(result);

    final INodeReadTrx rtx = mHolder.getSession().beginNodeReadTrx();
    rtx.moveTo(result.get(0).getNodeKey());
    assertEquals("oops1", rtx.getValueOfCurrentNode());
    rtx.moveTo(result.get(1).getNodeKey());
    assertEquals("oops2", rtx.getValueOfCurrentNode());
    rtx.moveTo(result.get(2).getNodeKey());
    assertEquals("oops3", rtx.getValueOfCurrentNode());
    rtx.close();
  }

  @Test
  public void testB1() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//b[1]");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final String result = (String)findLine.evaluate(doc, XPathConstants.STRING);

    assertNotNull(result);
    assertEquals("foo", result);
  }

  @Test
  public void testB2() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//b[2]");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final String result = (String)findLine.evaluate(doc, XPathConstants.STRING);

    assertNotNull(result);
    assertEquals("bar", result);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testBAll() throws Exception {
    xpe.setNamespaceContext(new DocNamespaceContext());
    final XPathExpression findLine = xpe.compile("//b");
    final NodeInfo doc = new DocumentWrapper(mHolder.getSession(), config);

    // Execute XPath.
    final ArrayList<INode> result = (ArrayList<INode>)findLine.evaluate(doc, XPathConstants.NODESET);

    assertNotNull(result);
    assertEquals(5, result.get(0).getNodeKey());
    assertEquals(9, result.get(1).getNodeKey());

    final INodeReadTrx rtx = mHolder.getSession().beginNodeReadTrx();
    rtx.moveTo(result.get(0).getNodeKey());
    assertEquals("b", rtx.getQNameOfCurrentNode().getLocalPart());

    rtx.moveTo(result.get(1).getNodeKey());
    assertEquals("b", rtx.getQNameOfCurrentNode().getLocalPart());
  }

  /**
   * <h1>Namespace Context</h1>
   * 
   * <p>
   * Namespace Context to test the NodeInfo implementation ( <code>NodeWrapper</code>). It's written for the
   * test document which is written via <code></code>.
   * </p>
   * 
   * @author Johannes Lichtenberger, University of Konstanz
   */
  static class DocNamespaceContext implements NamespaceContext {

    /**
     * Get the Namespace URI.
     * 
     * @param prefix
     *          Prefix of the current node.
     * @return Return the Namespace URI.
     * @throws IllegalArgumentException
     *           if prefix is null.
     */
    public String getNamespaceURI(final String prefix) {
      if (prefix == null) {
        throw new IllegalArgumentException("Prefix may not be null!");
      } else if (prefix == XMLConstants.DEFAULT_NS_PREFIX) {
        return XMLConstants.NULL_NS_URI;
      } else if (prefix == XMLConstants.XML_NS_PREFIX) {
        return XMLConstants.XML_NS_URI;
      } else if (prefix == XMLConstants.XMLNS_ATTRIBUTE) {
        return XMLConstants.XMLNS_ATTRIBUTE_NS_URI;
      } else if ("p".equals(prefix)) {
        return "ns";
      } else {
        return XMLConstants.NULL_NS_URI;
      }
    }

    /**
     * Get the prefix of the namespace.
     * 
     * @param namespace
     *          Namespace of the current node.
     * @return "p" if the Namespace equals "ns", otherwise returns null.
     */
    public String getPrefix(final String namespace) {
      if (namespace == null) {
        throw new IllegalArgumentException("Namespace may not be null!");
      } else if (XMLConstants.XML_NS_URI.equals(namespace)) {
        return XMLConstants.XML_NS_PREFIX;
      } else if (XMLConstants.XMLNS_ATTRIBUTE_NS_URI.equals(namespace)) {
        return XMLConstants.XMLNS_ATTRIBUTE;
      } else if (namespace.equals("ns")) {
        return "p";
      } else {
        return null;
      }
    }

    /**
     * Not needed/supported (only one prefix exists for a NS_URI).
     */
    public Iterator<String> getPrefixes(final String namespace) {
      throw new UnsupportedOperationException("Currently not needed by the test document!");
    }
  }

}
