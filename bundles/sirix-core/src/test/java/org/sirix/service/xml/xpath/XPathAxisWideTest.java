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

package org.sirix.service.xml.xpath;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.axis.AbsAxisTest;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.XMLShredder;

public class XPathAxisWideTest {

	private static final String XML = "src" + File.separator + "test"
			+ File.separator + "resources" + File.separator + "factbook.xml";

	@Before
	public void setUp() throws Exception {
		TestHelper.deleteEverything();
		// Setup parsed session.
		XMLShredder.main(XML, PATHS.PATH1.getFile().getAbsolutePath());

	}

	@After
	public void tearDown() throws SirixException {
		TestHelper.closeEverything();
	}

	@Test
	public void testIterateFactbook() throws Exception {
		// Verify.
		final Holder holder = Holder.generateRtx();
		holder.getRtx().moveToDocumentRoot();

		AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(),
				"/mondial/continent[@id]"), new long[] { 2L, 5L, 8L, 11L, 14L });

		AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(),
				"mondial/continent[@name]"), new long[] { 2L, 5L, 8L, 11L, 14L });

		AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(),
				"mondial/continent[@id=\"f0_119\"]"), new long[] { 2L });

		AbsAxisTest.testIAxisConventions(new XPathAxis(holder.getRtx(),
				"/mondial/continent[@name = \"Africa\"]"), new long[] { 14L });

		final AbstractAxis axis5 = new XPathAxis(holder.getRtx(),
				"mondial/lake/node()");
		for (int i = 0; i < 61; i++) {
			assertEquals(true, axis5.hasNext());
			axis5.next();
		}
		// assertEquals(29891L, axis5.next());
		assertEquals(false, axis5.hasNext());

		final AbstractAxis axis6 = new XPathAxis(holder.getRtx(),
				"mondial/country/religions/node()");
		for (int i = 0; i < 446; i++) {
			assertEquals(true, axis6.hasNext());
			axis6.next();
		}
		assertEquals(false, axis6.hasNext());

		final AbstractAxis axis7 = new XPathAxis(holder.getRtx(),
				"child::mondial/child::lake/child::node()");
		for (int i = 0; i < 60; i++) {
			assertEquals(true, axis7.hasNext());
			axis7.next();
		}
		assertEquals(true, axis7.hasNext());
		// assertEquals(29891L, axis7.next());
		axis7.next();
		assertEquals(false, axis7.hasNext());

		final AbstractAxis axis8 = new XPathAxis(holder.getRtx(), "//*[@id]");
		for (int i = 0; i < 5562; i++) {
			assertEquals(true, axis8.hasNext());
			axis8.next();
		}
		assertEquals(false, axis8.hasNext());

		final AbstractAxis axis9 = new XPathAxis(holder.getRtx(),
				"/mondial/country/attribute::car_code");
		for (int i = 0; i < 194; i++) {
			assertEquals(true, axis9.hasNext());
			axis9.next();
		}
		assertEquals(false, axis9.hasNext());

		final AbstractAxis axis10 = new XPathAxis(holder.getRtx(), "//country[@*]");
		for (int i = 0; i < 231; i++) {
			assertEquals(true, axis10.hasNext());
			axis10.next();
		}
		assertEquals(false, axis10.hasNext());

		holder.close();
	}
	//
	// // lasts too long
	// @Test
	// public void testIterateShakespreare() throws IOException,
	// XMLStreamException {
	// // Setup parsed session.
	// XMLShredder.shred(XML2, new SessionConfiguration(PATH2));
	//
	// // Verify.
	// final ISession session2 = Session.beginSession(PATH2);
	// final IReadTransaction rtx2 = session2.beginReadTransaction();
	// rtx2.moveToDocumentRoot();
	//
	// XPathStringChecker.testIAxisConventions(
	// new XPathAxis(rtx2, "fn:count(//PLAY)"),
	// new String[] { "37" });
	//
	// rtx2.moveTo(2L);
	// XPathStringChecker.testIAxisConventions(
	// new XPathAxis(rtx2, "fn:count(TITLE)"),
	// new String[] { "1" });
	//
	// XPathStringChecker.testIAxisConventions(
	// new XPathAxis(rtx2, "fn:string(TITLE)"),
	// new String[] { "The Tragedy of Antony and Cleopatra" });
	//
	// XPathStringChecker.testIAxisConventions(
	// new XPathAxis(rtx2, "fn:count(//*)"),
	// new String[] { "179619" });
	//
	// XPathStringChecker.testIAxisConventions(new XPathAxis(
	// rtx2,
	// "fn:count(/PLAYS/PLAY/PERSONAE)"), new String[] { "37" });
	//
	// final IAxis axis1 = new XPathAxis(rtx2, "//PERSONA");
	// for (int i = 0; i < 969; i++) {
	// assertEquals(true, axis1.hasNext());
	// axis1.next();
	// }
	// assertEquals(false, axis1.hasNext());
	//
	// final IAxis axis2 = new XPathAxis(rtx2, "/PLAYS/PLAY//SPEECH");
	// for (int i = 0; i < 31014; i++) {
	// assertEquals(true, axis2.hasNext());
	// axis2.next();
	// }
	// assertEquals(false, axis2.hasNext());
	//
	// final IAxis axis3 = new XPathAxis(rtx2, "/PLAYS/PLAY//STAGEDIR");
	// for (int i = 0; i < 6257; i++) {
	// assertEquals(true, axis3.hasNext());
	// axis3.next();
	// }
	// assertEquals(false, axis3.hasNext());
	//
	// rtx2.moveToDocumentRoot();
	// final IAxis axis4 = new XPathAxis(rtx2, "PLAYS/PLAY//STAGEDIR/text()");
	// for (int i = 0; i < 6257; i++) {
	// assertEquals(true, axis4.hasNext());
	// axis4.next();
	// }
	// assertEquals(false, axis4.hasNext());
	//
	// final IAxis axis5 = new XPathAxis(rtx2, "/PLAYS/PLAY//SCNDESCR");
	// for (int i = 0; i < 37; i++) {
	// assertEquals(true, axis5.hasNext());
	// axis5.next();
	// }
	// assertEquals(false, axis5.hasNext());
	//
	// final IAxis axis6 = new XPathAxis(rtx2, "//SPEECH/SPEAKER");
	// for (int i = 0; i < 31067; i++) {
	// assertEquals(true, axis6.hasNext());
	// axis6.next();
	// }
	// assertEquals(false, axis6.hasNext());
	//
	// XPathStringChecker.testIAxisConventions(new XPathAxis(
	// rtx2,
	// "PLAYS/PLAY/TITLE/text()"), new String[] {
	// "The Tragedy of Antony and Cleopatra",
	// "All's Well That Ends Well",
	// "As You Like It",
	// "The Comedy of Errors",
	// "The Tragedy of Coriolanus",
	// "Cymbeline",
	// "A Midsummer Night's Dream",
	// "The Tragedy of Hamlet, Prince of Denmark",
	// "The First Part of Henry the Fourth",
	// "The Second Part of Henry the Fourth",
	// "The Life of Henry the Fifth",
	// "The First Part of Henry the Sixth",
	// "The Second Part of Henry the Sixth",
	// "The Third Part of Henry the Sixth",
	// "The Famous History of the Life of Henry the Eighth",
	// "The Tragedy of Julius Caesar",
	// "The Life and Death of King John",
	// "The Tragedy of King Lear",
	// "Love's Labor's Lost",
	// "Measure for Measure",
	// "The Merry Wives of Windsor",
	// "The Tragedy of Macbeth",
	// "The Merchant of Venice",
	// "Much Ado about Nothing",
	// "The Tragedy of Othello, the Moor of Venice",
	// "Pericles, Prince of Tyre",
	// "The Tragedy of Romeo and Juliet",
	// "The Tragedy of King Richard the Second",
	// "The Tragedy of Richard the Third",
	// "Twelfth Night, or What You Will",
	// "The Taming of the Shrew",
	// "The Tempest",
	// "The Life of Timon of Athens",
	// "The Tragedy of Titus Andronicus",
	// "The History of Troilus and Cressida",
	// "The Two Gentlemen of Verona",
	// "The Winter's Tale" });
	//
	// final IAxis axis7 = new XPathAxis(rtx2, "PLAYS/PLAY");
	// for (int i = 0; i < 37; i++) {
	// assertEquals(true, axis7.hasNext());
	// axis7.next();
	// }
	// assertEquals(false, axis7.hasNext());
	//
	// rtx2.close();
	// session2.close();
	// }
}
