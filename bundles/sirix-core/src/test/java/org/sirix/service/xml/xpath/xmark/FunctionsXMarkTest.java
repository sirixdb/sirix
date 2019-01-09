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

package org.sirix.service.xml.xpath.xmark;

import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.service.xml.xpath.XPathAxis;
import org.sirix.service.xml.xpath.XPathStringChecker;

/**
 * This class performs tests for XQuery functions used for XMark bench test and XPathMark bench
 * test.
 *
 * @author Patrick Lang, Konstanz University
 *
 */
public class FunctionsXMarkTest {
  /** XML file name to test. */
  private static final String XMLFILE = "10mb.xml";
  /** Path to XML file. */
  private static final Path XML = Paths.get("src", "test", "resources", XMLFILE);

  private Holder holder;

  /**
   * Method is called once before each test. It deletes all states, shreds XML file to database and
   * initializes the required variables.
   *
   * @throws Exception
   */
  @Before
  @Ignore
  public void setUp() throws Exception {
    TestHelper.deleteEverything();
    XMLShredder.main(
        XML.toAbsolutePath().toString(), PATHS.PATH1.getFile().toAbsolutePath().toString());
    holder = Holder.generateRtx();
  }

  /**
   * Test function string().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testString() throws SirixXPathException {
    final String query = "fn:string(/site/people/person[@id=\"person3\"]/name)";
    final String result = "Limor Simone";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test comment.
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testComment() throws SirixXPathException {
    final String query = "2 (: this is a comment :)";
    final String result = "2";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function node().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testNode() throws SirixXPathException {
    final String query = "for $b in /site/people/person[@id=\"person1\"] return $b/name/node()";
    final String result = "Keung Yetim";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function text().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testText() throws SirixXPathException {
    final String query = "for $b in /site/people/person[@id=\"person0\"] return $b/name/text()";
    final String result = "Krishna Merle";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function count().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testCount() throws SirixXPathException {
    final String query =
        "fn:count(for $i in /site/closed_auctions/closed_auction[price/text() >= 40] return $i/price)";
    final String result = "670";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function position().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testPosition() throws SirixXPathException {
    final String query = "/site/open_auctions/open_auction/bidder/increase[position()=1]";
    final String result = "<increase>10.50</increase>";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function not().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testNot() throws SirixXPathException {
    final String query = "/site/people/person[not(homepage)][@id=\"person1\"]/name/text()";
    final String result = "<name>Keung Yetim</name>";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function id().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testId() throws SirixXPathException {
    final String query = "fn:id(/site/people/person[@id=\"person1\"]/watches/watch/@open_auction)";
    final String result = "";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function data().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testData() throws SirixXPathException {
    final String query = "for $b in /site/people/person[@id=\"person0\"] return fn:data($b/name)";
    final String result = "Krishna Merle";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function contains().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testContains() throws SirixXPathException {
    final String query =
        "/site/regions/*/item[contains(description,\"gold\")]/location[text()=\"El Salvador\"]";
    final String result = "<location>El Salvador</location>";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function exactly-one(). alternative query: exactly-one('a') -> result: a
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testExactlyOne() throws SirixXPathException {
    final String query = "exactly-one(/site/people/person[@id=\"person0\"]/name)";
    final String result = "<name>Krishna Merle</name>";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function sum().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testSum() throws SirixXPathException {
    final String query = "fn:sum(/site/open_auctions/open_auction/bidder/increase/text())";
    final String result = "96496.5";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function zero-or-one(). alternative query: zero-or-one('a') -> result: a
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testZeroOrOne() throws SirixXPathException {
    final String query =
        " for $i in /site/open_auctions/open_auction return zero-or-one($i/reserve[text()=\"20.54\"]/text())";
    final String result = "20.54";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function max().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testMax() throws SirixXPathException {
    final String query =
        "fn:max(for $i in /site/open_auctions/open_auction return $i/reserve/text())";
    final String result = "4701.79";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function min().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testMin() throws SirixXPathException {
    final String query =
        "fn:min(for $i in /site/open_auctions/open_auction return $i/reserve/text())";
    final String result = "0.43";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function empty().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testEmpty() throws SirixXPathException {
    final String query =
        "fn:empty(for $i in /site/open_auctions/open_auction return $i/reserve/text())";
    final String result = "false";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function one-or-more().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testOneOrMore() throws SirixXPathException {
    final String query = "fn:one-or-more(\"a\")";
    final String result = "a";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function exists().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testExists() throws SirixXPathException {
    final String query = "fn:exists( ('a', 'b', 'c') )";
    final String result = "true";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function substring-after().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testSubstringAfter() throws SirixXPathException {
    final String query = "fn:substring-after(\"query\", \"u\")";
    final String result = "ery";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function substring-before().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testSubstringBefore() throws SirixXPathException {
    final String query = "fn:substring-before(\"query\", \"r\")";
    final String result = "que";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function last().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testLast() throws SirixXPathException {
    final String query = "/site/open_auctions/open_auction/reserve[last()]";
    final String result = "<reserve>539.66</reserve>";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function boolean().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testBoolean() throws SirixXPathException {
    final String query = "fn:boolean(0)";
    final String result = "false";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function number().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testNumber() throws SirixXPathException {
    final String query =
        "/site/open_auctions/open_auction/bidder[personref[@person=\"person2436\"]]/increase/number()";
    final String result = "12 12";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function distinct-values().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testDistinctValues() throws SirixXPathException {
    final String query =
        "fn:distinct-values(/site/open_auctions/open_auction/bidder[personref[@person=\"person2436\"]]/increase)";
    final String result = "12.00";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function root().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testRoot() throws SirixXPathException {
    final String query = "fn:root()/site/people/person[@id=\"person0\"]/name/text()";
    final String result = "Krishna Merle";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test function floor().
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testFloor() throws SirixXPathException {
    final String query = "fn:floor(5.7)";
    final String result = "5";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Test <element attribute=""/> in return statement.
   *
   * @throws SirixXPathException
   */
  @Ignore
  @Test
  public final void testElementAttributeInReturn() throws SirixXPathException {
    final String query =
        "for $b in /site/open_auctions/open_auction/bidder[personref[@person=\"person2436\"]]/increase return <element attribute=\"{$b/text()}\"/>";
    final String result = "<element attribute=\"12.00\"/><element attribute=\"12.00\"/>";
    XPathStringChecker.testIAxisConventions(
        new XPathAxis(holder.getNodeReadTrx(), query), new String[] {result});
  }

  /**
   * Close all connections.
   *
   * @throws SirixException
   */
  @After
  @Ignore
  public void tearDown() throws SirixException {
    holder.close();
    TestHelper.closeEverything();

  }

}
