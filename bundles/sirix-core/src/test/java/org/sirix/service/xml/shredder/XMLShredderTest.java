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

package org.sirix.service.xml.shredder;

import java.io.File;
import java.util.Iterator;

import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.events.XMLEvent;

import org.custommonkey.xmlunit.XMLTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.utils.DocumentCreater;

public class XMLShredderTest extends XMLTestCase {

	public static final String XML = "src" + File.separator + "test"
			+ File.separator + "resources" + File.separator + "test.xml";

	public static final String XML2 = "src" + File.separator + "test"
			+ File.separator + "resources" + File.separator + "test2.xml";

	public static final String XML3 = "src" + File.separator + "test"
			+ File.separator + "resources" + File.separator + "test3.xml";

	private Holder holder;

	@Override
	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		holder = Holder.generateWtx();
	}

	@Override
	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();
	}

	@Test
	public void testSTAXShredder() throws Exception {

		// Setup parsed session.
		XMLShredder.main(XML, PATHS.PATH2.getFile().getAbsolutePath());
		final NodeReadTrx expectedTrx = holder.getWtx();

		// Verify.
		final Database database2 = TestHelper.getDatabase(PATHS.PATH2.getFile());
		database2.createResource(new ResourceConfiguration.Builder(
				TestHelper.RESOURCE, PATHS.PATH2.getConfig()).build());
		final Session session = database2
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final NodeReadTrx rtx = session.beginNodeReadTrx();
		rtx.moveToDocumentRoot();
		final Iterator<Long> expectedDescendants = new DescendantAxis(expectedTrx);
		final Iterator<Long> descendants = new DescendantAxis(rtx);

		while (expectedDescendants.hasNext() && descendants.hasNext()) {
			assertEquals(expectedTrx.getNodeKey(), rtx.getNodeKey());
			assertEquals(expectedTrx.getParentKey(), rtx.getParentKey());
			assertEquals(expectedTrx.getFirstChildKey(), rtx.getFirstChildKey());
			assertEquals(expectedTrx.getLeftSiblingKey(), rtx.getLeftSiblingKey());
			assertEquals(expectedTrx.getRightSiblingKey(), rtx.getRightSiblingKey());
			assertEquals(expectedTrx.getChildCount(), rtx.getChildCount());
			if (expectedTrx.getKind() == Kind.ELEMENT
					|| rtx.getKind() == Kind.ELEMENT) {

				assertEquals(expectedTrx.getAttributeCount(), rtx.getAttributeCount());
				assertEquals(expectedTrx.getNamespaceCount(), rtx.getNamespaceCount());
			}
			assertEquals(expectedTrx.getKind(), rtx.getKind());
			assertEquals(expectedTrx.getName(), rtx.getName());
			assertEquals(expectedTrx.getValue(), expectedTrx.getValue());
		}

		rtx.close();
		session.close();
		database2.close();
		expectedTrx.close();
	}

	@Test
	public void testShredIntoExisting() throws Exception {
		final NodeWriteTrx wtx = holder.getWtx();
		final XMLShredder shredder = new XMLShredder.Builder(wtx,
				XMLShredder.createFileReader(new File(XML)), Insert.ASFIRSTCHILD)
				.includeComments(true).commitAfterwards().build();
		shredder.call();
		assertEquals(2, wtx.getRevisionNumber());
		wtx.moveToDocumentRoot();
		wtx.moveToFirstChild();
		wtx.remove();
		final XMLShredder shredder2 = new XMLShredder.Builder(wtx,
				XMLShredder.createFileReader(new File(XML)), Insert.ASFIRSTCHILD)
				.includeComments(true).commitAfterwards().build();
		shredder2.call();
		assertEquals(3, wtx.getRevisionNumber());
		wtx.close();

		// Setup expected session.
		final Database database2 = TestHelper.getDatabase(PATHS.PATH2.getFile());
		final Session expectedSession = database2
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());

		final NodeWriteTrx expectedTrx = expectedSession.beginNodeWriteTrx();
		DocumentCreater.create(expectedTrx);
		expectedTrx.commit();
		expectedTrx.moveToDocumentRoot();

		// Verify.
		final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();

		final Iterator<Long> descendants = new DescendantAxis(rtx);
		final Iterator<Long> expectedDescendants = new DescendantAxis(expectedTrx);

		while (expectedDescendants.hasNext()) {
			expectedDescendants.next();
			descendants.hasNext();
			descendants.next();
			assertEquals(expectedTrx.getName(), rtx.getName());
			assertEquals(expectedTrx.getValue(), rtx.getValue());
		}

		// expectedTrx.moveToDocumentRoot();
		// final Iterator<Long> expectedDescendants2 = new
		// DescendantAxis(expectedTrx);
		// while (expectedDescendants2.hasNext()) {
		// expectedDescendants2.next();
		// descendants.hasNext();
		// descendants.next();
		// assertEquals(expectedTrx.getQNameOfCurrentNode(),
		// rtx.getQNameOfCurrentNode());
		// }

		expectedTrx.close();
		expectedSession.close();
		rtx.close();
	}

	@Test
	public void testAttributesNSPrefix() throws Exception {
		// Setup expected session.
		final NodeWriteTrx expectedTrx2 = holder.getWtx();
		DocumentCreater.createWithoutNamespace(expectedTrx2);
		expectedTrx2.commit();

		// Setup parsed session.
		final Database database2 = TestHelper.getDatabase(PATHS.PATH2.getFile());
		final Session session2 = database2
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final NodeWriteTrx wtx = session2.beginNodeWriteTrx();
		final XMLShredder shredder = new XMLShredder.Builder(wtx,
				XMLShredder.createFileReader(new File(XML2)), Insert.ASFIRSTCHILD)
				.commitAfterwards().build();
		shredder.call();
		wtx.commit();
		wtx.close();

		// Verify.
		final NodeReadTrx rtx = session2.beginNodeReadTrx();
		rtx.moveToDocumentRoot();
		final Iterator<Long> expectedAttributes = new DescendantAxis(expectedTrx2);
		final Iterator<Long> attributes = new DescendantAxis(rtx);

		while (expectedAttributes.hasNext() && attributes.hasNext()) {
			expectedAttributes.next();
			attributes.next();
			if (expectedTrx2.getKind() == Kind.ELEMENT
					|| rtx.getKind() == Kind.ELEMENT) {
				assertEquals(expectedTrx2.getNamespaceCount(), rtx.getNamespaceCount());
				assertEquals(expectedTrx2.getAttributeCount(), rtx.getAttributeCount());
				for (int i = 0; i < expectedTrx2.getAttributeCount(); i++) {
					assertEquals(expectedTrx2.getName(), rtx.getName());
				}
			}
		}
		attributes.hasNext();

		assertEquals(expectedAttributes.hasNext(), attributes.hasNext());

		expectedTrx2.close();
		rtx.close();
		session2.close();
	}

	@Test
	public void testShreddingLargeText() throws Exception {
		final Database database = TestHelper.getDatabase(PATHS.PATH2.getFile());
		final Session session = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final NodeWriteTrx wtx = session.beginNodeWriteTrx();
		final XMLShredder shredder = new XMLShredder.Builder(wtx,
				XMLShredder.createFileReader(new File(XML3)), Insert.ASFIRSTCHILD)
				.commitAfterwards().build();
		shredder.call();
		wtx.close();

		final NodeReadTrx rtx = session.beginNodeReadTrx();
		assertTrue(rtx.moveToFirstChild().hasMoved());
		assertTrue(rtx.moveToFirstChild().hasMoved());

		final StringBuilder tnkBuilder = new StringBuilder();
		do {
			tnkBuilder.append(rtx.getValue());
		} while (rtx.moveToRightSibling().hasMoved());

		final String tnkString = tnkBuilder.toString();

		rtx.close();
		session.close();

		final XMLEventReader validater = XMLShredder
				.createFileReader(new File(XML3));
		final StringBuilder xmlBuilder = new StringBuilder();
		while (validater.hasNext()) {
			final XMLEvent event = validater.nextEvent();
			switch (event.getEventType()) {
			case XMLStreamConstants.CHARACTERS:
				final String text = event.asCharacters().getData().trim();
				if (text.length() > 0) {
					xmlBuilder.append(text);
				}
				break;
			}
		}

		assertEquals(xmlBuilder.toString(), tnkString);
	}
}
