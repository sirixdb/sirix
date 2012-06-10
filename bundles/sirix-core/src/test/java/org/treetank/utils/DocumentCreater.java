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

package org.treetank.utils;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.treetank.TestHelper;
import org.treetank.access.conf.SessionConfiguration;
import org.treetank.api.IDatabase;
import org.treetank.api.INodeWriteTrx;
import org.treetank.exception.AbsTTException;
import org.treetank.service.xml.shredder.EInsert;
import org.treetank.service.xml.shredder.XMLShredder;

/**
 * <h1>TestDocument</h1>
 * 
 * <p>
 * This class creates an XML document that contains all features seen in the Extensible Markup Language (XML)
 * 1.1 (Second Edition) as well as the Namespaces in XML 1.1 (Second Edition).
 * </p>
 * 
 * <p>
 * The following figure describes the created test document (see <code>xml/test.xml</code>). The nodes are
 * described as follows:
 * 
 * <ul>
 * <li><code>ENode.ROOT_KIND     : doc()</code></li>
 * <li><code>ENode.ELEMENT_KIND  : &lt;prefix:localPart&gt;</code></li>
 * <li><code>ENode.NAMESPACE_KIND: §prefix:namespaceURI</code></li>
 * <li><code>ENode.ATTRIBUTE_KIND: &#64;prefix:localPart='value'</code></li>
 * <li><code>ENode.TEXT_KIND     : #value</code></li>
 * </ul>
 * 
 * <pre>
 * 0 doc()
 * |-  1 &lt;p:a §p:ns @i='j'&gt;
 *     |-  4 #oops1
 *     |-  5 &lt;b&gt;
 *     |   |-  6 #foo
 *     |   |-  7 &lt;c&gt;
 *     |-  8 #oops2
 *     |-  9 &lt;b @p:x='y'&gt;
 *     |   |- 11 &lt;c&gt;
 *     |   |- 12 #bar
 *     |- 13 #oops3
 * </pre>
 * 
 * </p>
 */
public final class DocumentCreater {

  /** String representation of revisioned xml file. */
  public static final String REVXML =
    "<article><title>A Test Document</title><para>This is para 1.</para><para>This is para 2<emphasis>"
      + "with emphasis</emphasis>in it.</para><para>This is para 3.</para><para id=\"p4\">This is "
      + "para 4.</para><para id=\"p5\">This is para 5.</para><para>This is para 6."
      + "</para><para>This is para 7.</para><para>This is para 8.</para><para>This is para 9."
      + "</para></article>";

  /** String representation of ID. */
  public static final String ID =
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?><p:a xmlns:p=\"ns\" "
      + "ttid=\"1\" i=\"j\">oops1<b ttid=\"5\">foo<c ttid=\"7\"/></b>oops2<b ttid=\"9\" p:x=\"y\">"
      + "<c ttid=\"11\"/>bar</b>oops3</p:a>";

  /** String representation of rest. */
  public static final String REST =
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
      + "<rest:sequence xmlns:rest=\"REST\"><rest:item>"
      + "<p:a xmlns:p=\"ns\" rest:ttid=\"1\" i=\"j\">oops1<b rest:ttid=\"5\">foo<c rest:ttid=\"7\"/></b>oops2<b rest:ttid=\"9\" p:x=\"y\">"
      + "<c rest:ttid=\"11\"/>bar</b>oops3</p:a>" + "</rest:item></rest:sequence>";

  /** String representation of test document. */
  public static final String XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
    + "<p:a xmlns:p=\"ns\" i=\"j\">oops1<b>foo<c/></b>oops2<b p:x=\"y\">" + "<c/>bar</b>oops3</p:a>";

  /** String representation of test document without xml declaration. */
  public static final String XML_WITHOUT_XMLDECL =
    "<p:a xmlns:p=\"ns\" i=\"j\">oops1<b>foo<c/></b>oops2<b p:x=\"y\"><c/>bar</b>oops3</p:a>";

  /** String representation of versioned test document. */
  public static final String VERSIONEDXML =
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
      + "<tt revision=\"0\"><p:a xmlns:p=\"ns\" i=\"j\">oops1<b>foo<c/></b>oops2<b p:x=\"y\"><c/>bar</b>oops3</p:a></tt>"
      + "<tt revision=\"1\"><p:a xmlns:p=\"ns\" i=\"j\"><p:a>OOPS4!</p:a>oops1<b>foo<c/></b>oops2<b p:x=\"y\"><c/>bar</b>oops3</p:a></tt>"
      + "<tt revision=\"2\"><p:a xmlns:p=\"ns\" i=\"j\"><p:a>OOPS4!</p:a><p:a>OOPS4!</p:a>oops1<b>foo<c/></b>oops2<b p:x=\"y\"><c/>bar</b>oops3</p:a></tt>";

  /** String representation of test document without attributes. */
  public static final String XMLWITHOUTATTRIBUTES =
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>" + "<p:a>oops1<b>foo<c></c></b>oops2<b>"
      + "<c></c>bar</b>oops3</p:a>";

  /** XML for the index structure. */
  public static final String XML_INDEX = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
    + "<t:o><t:oo><t:oop><t:oops><d:DOCUMENT_ROOT_KIND nodeID=\"0\"><d:p:a nodeID=\"1\">"
    + "<d:TEXT_KIND nodeID=\"4\"/></d:p:a></d:DOCUMENT_ROOT_KIND></t:oops></t:oop></t:oo>"
    + "</t:o><t:f><t:fo><t:foo><d:DOCUMENT_ROOT_KIND nodeID=\"0\"><d:p:a nodeID=\"1\">"
    + "<d:b nodeID=\"5\"><d:TEXT_KIND nodeID=\"6\"/></d:b></d:p:a></d:DOCUMENT_ROOT_KIND></t:foo>"
    + "</t:fo></t:f><t:b><t:ba><t:bar><d:DOCUMENT_ROOT_KIND nodeID=\"0\"><d:p:a nodeID=\"1\">"
    + "<d:b nodeID=\"9\"><d:TEXT_KIND nodeID=\"12\"/></d:b></d:p:a></d:DOCUMENT_ROOT_KIND></t:bar>"
    + "</t:ba></t:b>";

  /**
   * Private Constructor, not used.
   */
  private DocumentCreater() {
    throw new AssertionError("Not permitted to call constructor!");
  }

  /**
   * Create simple test document containing all supported node kinds.
   * 
   * @param paramWtx
   *          {@link INodeWriteTrx} to write to
   * @throws AbsTTException
   *           if anything weird happens
   */
  public static void create(final INodeWriteTrx paramWtx) throws AbsTTException {
    assertNotNull(paramWtx);
    assertTrue(paramWtx.moveToDocumentRoot());

    paramWtx.insertElementAsFirstChild(new QName("ns", "a", "p"));
    paramWtx.insertNamespace(new QName("ns", "xmlns", "p"));
    assertTrue(paramWtx.moveToParent());
    paramWtx.insertAttribute(new QName("i"), "j");
    assertTrue(paramWtx.moveToParent());

    paramWtx.insertTextAsFirstChild("oops1");

    paramWtx.insertElementAsRightSibling(new QName("b"));

    paramWtx.insertTextAsFirstChild("foo");
    paramWtx.insertElementAsRightSibling(new QName("c"));
    assertTrue(paramWtx.moveToParent());

    paramWtx.insertTextAsRightSibling("oops2");

    paramWtx.insertElementAsRightSibling(new QName("b"));
    paramWtx.insertAttribute(new QName("ns", "x", "p"), "y");
    assertTrue(paramWtx.moveToParent());

    paramWtx.insertElementAsFirstChild(new QName("c"));
    paramWtx.insertTextAsRightSibling("bar");
    assertTrue(paramWtx.moveToParent());

    paramWtx.insertTextAsRightSibling("oops3");

    paramWtx.moveToDocumentRoot();
  }

  /**
   * Create simple revision test in current database.
   * 
   * @param paramWtx
   *          {@link INodeWriteTrx} to write to
   * @throws AbsTTException
   *           if anything went wrong
   */
  public static void createVersioned(final INodeWriteTrx paramWtx) throws AbsTTException {
    assertNotNull(paramWtx);
    create(paramWtx);
    paramWtx.commit();
    for (int i = 0; i <= 1; i++) {
      paramWtx.moveToDocumentRoot();
      paramWtx.moveToFirstChild();
      paramWtx.insertElementAsFirstChild(new QName("ns", "a", "p"));
      paramWtx.insertTextAsFirstChild("OOPS4!");
      paramWtx.commit();
    }
  }

  /**
   * Create simple test document containing all supported node kinds except
   * the attributes.
   * 
   * @param paramWtx
   *          {@link INodeWriteTrx} to write to
   * @throws AbsTTException
   *           if anything went wrong
   */
  public static void createWithoutAttributes(final INodeWriteTrx paramWtx) throws AbsTTException {
    assertNotNull(paramWtx);
    paramWtx.moveToDocumentRoot();

    paramWtx.insertElementAsFirstChild(new QName("ns", "a", "p"));

    paramWtx.insertTextAsFirstChild("oops1");

    paramWtx.insertElementAsRightSibling(new QName("b"));

    paramWtx.insertTextAsFirstChild("foo");
    paramWtx.insertElementAsRightSibling(new QName("c"));
    paramWtx.moveToParent();

    paramWtx.insertTextAsRightSibling("oops2");

    paramWtx.insertElementAsRightSibling(new QName("b"));

    paramWtx.insertElementAsFirstChild(new QName("c"));
    paramWtx.insertTextAsRightSibling("bar");
    paramWtx.moveToParent();

    paramWtx.insertTextAsRightSibling("oops3");

    paramWtx.moveToDocumentRoot();
  }

  /**
   * Create simple test document containing all supported node kinds, but
   * ignoring their namespace prefixes.
   * 
   * @param paramWtx
   *          {@link INodeWriteTrx} to write to
   * @throws AbsTTException
   *           if anything went wrong
   */
  public static void createWithoutNamespace(final INodeWriteTrx paramWtx) throws AbsTTException {
    assertNotNull(paramWtx);
    paramWtx.moveToDocumentRoot();

    paramWtx.insertElementAsFirstChild(new QName("a"));
    paramWtx.insertAttribute(new QName("i"), "j");
    paramWtx.moveToParent();

    paramWtx.insertTextAsFirstChild("oops1");

    paramWtx.insertElementAsRightSibling(new QName("b"));

    paramWtx.insertTextAsFirstChild("foo");
    paramWtx.insertElementAsRightSibling(new QName("c"));
    paramWtx.moveToParent();

    paramWtx.insertTextAsRightSibling("oops2");

    paramWtx.insertElementAsRightSibling(new QName("b"));
    paramWtx.insertAttribute(new QName("x"), "y");
    paramWtx.moveToParent();

    paramWtx.insertElementAsFirstChild(new QName("c"));
    paramWtx.insertTextAsRightSibling("bar");
    paramWtx.moveToParent();

    paramWtx.insertTextAsRightSibling("oops3");

    paramWtx.moveToDocumentRoot();
  }

  /**
   * Create revisioned document.
   * 
   * @throws AbsTTException
   *           if shredding fails
   * @throws XMLStreamException
   *           if StAX reader couldn't be created
   * @throws IOException
   *           if reading XML string fails
   */
  public static void createRevisioned(final IDatabase paramDB) throws AbsTTException, IOException,
    XMLStreamException {

    final INodeWriteTrx firstWtx =
      paramDB.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE).build()).beginNodeWriteTrx();
    final XMLShredder shredder =
      new XMLShredder(firstWtx, XMLShredder.createStringReader(REVXML), EInsert.ASFIRSTCHILD);
    shredder.call();
    firstWtx.close();
    final INodeWriteTrx secondWtx =
      paramDB.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE).build()).beginNodeWriteTrx();
    secondWtx.moveToFirstChild();
    secondWtx.moveToFirstChild();
    secondWtx.moveToFirstChild();
    secondWtx.setValue("A Contrived Test Document");
    secondWtx.moveToParent();
    secondWtx.moveToRightSibling();
    secondWtx.moveToRightSibling();
    secondWtx.moveToFirstChild();
    secondWtx.moveToRightSibling();
    final long key = secondWtx.getNode().getNodeKey();
    secondWtx.insertAttribute(new QName("role"), "bold");
    secondWtx.moveTo(key);
    secondWtx.moveToRightSibling();
    secondWtx.setValue("changed in it.");
    secondWtx.moveToParent();
    secondWtx.insertElementAsRightSibling(new QName("para"));
    secondWtx.insertTextAsFirstChild("This is a new para 2b.");
    secondWtx.moveToParent();
    secondWtx.moveToRightSibling();
    secondWtx.moveToRightSibling();
    secondWtx.moveToFirstChild();
    secondWtx.setValue("This is a different para 4.");
    secondWtx.moveToParent();
    secondWtx.insertElementAsRightSibling(new QName("para"));
    secondWtx.insertTextAsFirstChild("This is a new para 4b.");
    secondWtx.moveToParent();
    secondWtx.moveToRightSibling();
    secondWtx.moveToRightSibling();
    secondWtx.remove();
    secondWtx.remove();
    secondWtx.commit();
    secondWtx.moveToDocumentRoot();
    secondWtx.moveToFirstChild();
    secondWtx.moveToFirstChild();
    secondWtx.remove();
    secondWtx.commit();
    secondWtx.close();
  }
}
