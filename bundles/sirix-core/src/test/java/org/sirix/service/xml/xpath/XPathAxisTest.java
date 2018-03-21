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

package org.sirix.service.xml.xpath;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.axis.AbsAxisTest;
import org.sirix.exception.SirixException;

/**
 * JUnit-test class to test the functionality of the XPathAxis.
 * 
 * @author Tina Scherer
 */
public class XPathAxisTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
    TestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    TestHelper.closeEverything();
    holder.close();
  }

  @Test
  public void testSteps() throws SirixException {
    // Verify.

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/text:p/b"), new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a/b"),
        new long[] {5L, 9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a/b/c"),
        new long[] {7L, 11L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a"), new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "child::p:a/child::b"),
        new long[] {5L, 9L});

    AbsAxisTest.testIAxisConventions(
        new XPathAxis(holder.getReader(), "child::p:" + "a/child::b/child::c"),
        new long[] {7L, 11L});

  }

  @Test
  public void testAttributes() throws SirixException {

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a[@i]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a/@i"), new long[] {3L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a/@i/@*"),
        new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a/b[@p:x]"),
        new long[] {9L});

    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getReader(), "descendant-or-self::node()/@p:x = 'y'"),
        new String[] {"true"});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[text()]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[element()]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[node()/text()]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[./node()]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(
        new XPathAxis(holder.getReader(), "p:a[./node()/node()/node()]"), new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[//element()]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[/text()]"),
        new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[16<65]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[13>=4]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[13.0>=4]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[4 = 4]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[3=4]"), new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[3.2 = 3.22]"),
        new long[] {});

    // TODO:error with XPath 1.0 compatibility because one operand is parsed
    // to
    // double
    // and with no compatibility error, because value can not be converted
    // to
    // string
    // from the byte array
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[(3.2 + 0.02) = 3.22]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[@i = \"j\"]"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(
        new XPathAxis(holder.getReader(), "descendant-or-self::node()[@p:x = \"y\"]"),
        new long[] {9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a[@i=\"k\"]"),
        new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a/b[@p:x=\"y\"]"),
        new long[] {9L});

  }

  @Test
  public void testNodeTests() throws SirixException {

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a/node()"),
        new long[] {4L, 5L, 8L, 9L, 13L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a/text()"),
        new long[] {4L, 8L, 13L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a/b/text()"),
        new long[] {6L, 12L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a/b/node()"),
        new long[] {6L, 7L, 11L, 12L});
  }

  @Test
  public void testDescendant() throws SirixException {

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a//b"),
        new long[] {5L, 9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "//p:a"), new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "descendant-or-self::p:a"),
        new long[] {1L});

    AbsAxisTest.testIAxisConventions(
        new XPathAxis(holder.getReader(), "/p:a/descendant-or-self::b"), new long[] {5L, 9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a/descendant::b"),
        new long[] {5L, 9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "p:a/descendant::p:a"),
        new long[] {});

  }

  @Test
  public void testAncestor() throws SirixException {
    // Find ancestor starting from nodeKey 8L.
    holder.getReader().moveTo(11L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "ancestor::p:a"),
        new long[] {1L});

    holder.getReader().moveTo(13L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "ancestor::p:a"),
        new long[] {1L});

    holder.getReader().moveTo(11L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "ancestor::node()"),
        new long[] {9L, 1L});

    holder.getReader().moveTo(11L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "ancestor-or-self::node()"),
        new long[] {11L, 9L, 1L});
  }

  @Test
  public void testParent() throws SirixException {
    // Find ancestor starting from nodeKey 8L.
    holder.getReader().moveTo(9L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "parent::p:a"),
        new long[] {1L});

    holder.getReader().moveTo(11L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "parent::b"),
        new long[] {9L});

    holder.getReader().moveTo(11L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "parent::node()"),
        new long[] {9L});

    holder.getReader().moveTo(13L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "parent::node()"),
        new long[] {1L});
  }

  @Test
  public void testPreceding() throws SirixException {
    // Find preceding nodes starting from nodeKey 13.
    holder.getReader().moveTo(13L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "preceding-sibling::node()"),
        new long[] {4L, 5L, 8L, 9L});
  }

  @Test
  public void testSelf() throws SirixException {
    holder.getReader().moveTo(1L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "self::p:a"),
        new long[] {1L});

    holder.getReader().moveTo(9L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "self::b"), new long[] {9L});

    holder.getReader().moveTo(11L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "./node()"), new long[] {});

    holder.getReader().moveTo(11L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "self::node()"),
        new long[] {11L});

    holder.getReader().moveTo(1L);
    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "./b/node()"),
        new long[] {6L, 7L, 11L, 12L});

  }

  @Test
  public void testPosition() throws SirixException {
    holder.getReader().moveTo(1L);

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "b"), new long[] {5L, 9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "b/c"),
        new long[] {7L, 11L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "b/text()"),
        new long[] {6L, 12L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a/b/c"),
        new long[] {7L, 11L});

  }

  //
  @Test
  public void testDupElemination() throws SirixException {
    holder.getReader().moveTo(1L);

    AbsAxisTest.testIAxisConventions(
        new XPathAxis(holder.getReader(), "child::node()/parent::node()"), new long[] {1L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "b/c"),
        new long[] {7L, 11L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "b/text()"),
        new long[] {6L, 12L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/p:a/b/c"),
        new long[] {7L, 11L});

  }

  @Test
  public void testUnabbreviate() throws SirixException {
    holder.getReader().moveTo(1L);

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "child::b"),
        new long[] {5L, 9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "child::*"),
        new long[] {5L, 9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "child::text()"),
        new long[] {4L, 8L, 13L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "attribute::i"),
        new long[] {3L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "attribute::*"),
        new long[] {3L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "parent::node()"),
        new long[] {0L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "self::blau"),
        new long[] {});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "/"), new long[] {0L});

    AbsAxisTest.testIAxisConventions(
        new XPathAxis(holder.getReader(), "child::b[attribute::p:x = \"y\"]"), new long[] {9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "child::b[child::c]"),
        new long[] {5L, 9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "child::*[text() or c]"),
        new long[] {5l, 9L});

    AbsAxisTest.testIAxisConventions(
        new XPathAxis(holder.getReader(), "child::*[text() or c], /node(), //c"),
        new long[] {5l, 9L, 1L, 7L, 11L});
  }

  @Test
  public void testMultiExpr() throws SirixException {
    holder.getReader().moveTo(1L);

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "b, b, b"),
        new long[] {5L, 9L, 5L, 9L, 5L, 9L});

    AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getReader(), "b/c, ., //c"),
        new long[] {7L, 11L, 1L, 7L, 11L});

    AbsAxisTest.testIAxisConventions(
        new XPathAxis(holder.getReader(), "b/text(), //text(), descendant-or-self::element()"),
        new long[] {6L, 12L, 4L, 8L, 13L, 6L, 12L, 1L, 5L, 7L, 9L, 11L});

    holder.getReader().moveTo(5L);
    AbsAxisTest.testIAxisConventions(
        new XPathAxis(holder.getReader(), "/p:a/b/c, ., .., .//text()"),
        new long[] {7L, 11L, 5L, 1L, 6L});
  }

  @Test
  public void testCount() throws SirixException {
    // Verify.
    holder.getReader().moveTo(1L);

    XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), "fn:count(//node())"),
        new String[] {"10"});
  }

}
