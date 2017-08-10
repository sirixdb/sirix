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
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixXPathException;

/**
 * This class contains test cases for not yet implemented xpath/xquery functions with test.xml file.
 * 
 * @author Patrick Lang, Konstanz University
 */
public class FunctionsTest {

	private Holder holder;

	/**
	 * Method is called once before each test. It deletes all states, shreds XML file to database and
	 * initializes the required variables.
	 * 
	 * @throws Exception of any kind
	 */
	@Before
	public final void setUp() throws Exception {
		TestHelper.deleteEverything();
		TestHelper.createTestDocument();
		holder = Holder.generateRtx();
	}

	/**
	 * Test function boolean().
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testBoolean() throws SirixXPathException {
		final String query = "fn:boolean(0)";
		final String result = "false";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function boolean() for XPath 1.0.
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testBooleanXPath10() throws SirixXPathException {
		final String query = "boolean(1)";
		final String result = "true";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function count().
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testCount() throws SirixXPathException {
		final String query = "fn:count(//p:a/b)";
		final String result = "2";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function count() for XPath 1.0.
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testCountXPath10() throws SirixXPathException {
		final String query = "count(//p:a/b)";
		final String result = "2";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function string().
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testString() throws SirixXPathException {
		final String query = "fn:string(/p:a/b)";
		final String result = "foo bar";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function string() for XPath 1.0.
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testStringXPath10() throws SirixXPathException {
		final String query = "string(/p:a/b)";
		final String result = "foo bar";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test comment.
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testComment() throws SirixXPathException {
		final String query = "2 (: this is a comment :)";
		final String result = "2";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function node().
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testNode() throws SirixXPathException {
		final String query = "p:a[./node()/node()]";
		final String result = "p:a";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function text().
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testText() throws SirixXPathException {
		final String query = "p:a[./text()]";
		final String result = "p:a";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function not().
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testNot() throws SirixXPathException {
		final String query = "fn:not(//b)";
		final String result = "false";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function not() for XPath 1.0.
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testNotXPath10() throws SirixXPathException {
		final String query = "not(//b)";
		final String result = "false";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function sum().
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testSum() throws SirixXPathException {
		final String query = "fn:sum(5)";
		final String result = "1";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function sum() for XPath 1.0.
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	public final void testSumXPath10() throws SirixXPathException {
		final String query = "sum(5)";
		final String result = "1";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function position().
	 * 
	 * @throws SirixXPathException
	 */
	@Test
	@Ignore
	public final void testPosition() throws SirixXPathException {
		final String query = "//b[position()=2]";
		final String result = "b";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function id().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testId() throws SirixXPathException {
		final String query = "//b/fn:id()";
		final String result = "";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function data().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testData() throws SirixXPathException {
		final String query = "fn:data(//b)";
		final String result = "foo bar";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function contains().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testContains() throws SirixXPathException {
		final String query = "fn:contains(/p:a/b, \"\")";
		final String result = "true";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function exactly-one().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testExactlyOne() throws SirixXPathException {
		final String query = "fn:exactly-one(\"a\")";
		final String result = "a";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function zero-or-one().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testZeroOrOne() throws SirixXPathException {
		final String query = "fn:zero-or-one(\"a\")";
		final String result = "a";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function max().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testMax() throws SirixXPathException {
		final String query = "fn:max((2, 1, 5, 4, 3))";
		final String result = "5";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function min().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testMin() throws SirixXPathException {
		final String query = "fn:min((2, 1, 5, 4, 3))";
		final String result = "1";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function empty().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testEmpty() throws SirixXPathException {
		final String query = "fn:empty(/p:a)";
		final String result = "true";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function one-or-more().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testOneOrMore() throws SirixXPathException {
		final String query = "fn:one-or-more(//b/c)";
		final String result = "<c xmlns:p=\"ns\"/><c xmlns:p=\"ns\"/>";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function exists().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testExists() throws SirixXPathException {
		final String query = "fn:exists(('a', 'b', 'c'))";
		final String result = "true";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
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
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
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
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function last().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testLast() throws SirixXPathException {
		final String query = "//b[last()]";
		final String result = "<b xmlns:p=\"ns\" p:x=\"y\"><c/>bar</b>";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function number().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testNumber() throws SirixXPathException {
		final String query = "fn:number('29.99')";
		final String result = "29.99";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function distinct-values().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testDistinctValues() throws SirixXPathException {
		final String query = "fn:distinct-values(('a', 'a'))";
		final String result = "a";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test function root().
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testRoot() throws SirixXPathException {
		final String query = "fn:root()//c";
		final String result = "<c xmlns:p=\"ns\"/><c xmlns:p=\"ns\"/>";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
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
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Test <element attribute=""/> in return statement.
	 * 
	 * @throws SirixXPathException
	 */
	@Ignore
	@Test
	public final void testElementAttributeInReturn() throws SirixXPathException {
		final String query = "for $x in //b/text() return <element attr=\"{$x}\"/>";
		final String result = "<element attr=\"foo\"/><element attr=\"bar\"/>";
		XPathStringChecker.testIAxisConventions(new XPathAxis(holder.getReader(), query),
				new String[] {result});
	}

	/**
	 * Close all connections.
	 * 
	 * @throws SirixException
	 */
	@After
	public final void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();

	}

}
