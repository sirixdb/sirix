/*
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

package io.sirix.access.node.xml;

import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.node.NodeKind;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;
import io.sirix.utils.XmlDocumentCreator;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class XmlResourceSessionTest {

	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		XmlTestHelper.deleteEverything();
		holder = Holder.generateRtx();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		XmlTestHelper.closeEverything();
	}

	@Test
	public void testSingleton() {
		try (final var database = Holder.openResourceManager().getDatabase()) {
			assertEquals(database, holder.getDatabase());

			try (final XmlResourceSession manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
				assertEquals(manager, holder.getResourceManager());
			}

			try (final XmlResourceSession manager2 = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
				assertNotSame(manager2, holder.getResourceManager());
			}
		}
	}

	@Test
	public void testClosed() {
		final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
		rtx.close();

		try {
			rtx.getAttributeCount();
			fail();
		} catch (final IllegalStateException e) {
			// Must fail.
		} finally {
			holder.getResourceManager().close();
		}
	}

	@Test
	public void testNonExisting() {
		final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
		final var database2 = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
		assertSame(database, database2);
	}

	@Test
	public void testInsertChild() {
		try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
			XmlDocumentCreator.create(wtx);
			wtx.moveToDocumentRoot();
			Assert.assertEquals(NodeKind.XML_DOCUMENT, wtx.getKind());

			wtx.moveToFirstChild();
			Assert.assertEquals(NodeKind.ELEMENT, wtx.getKind());
			assertEquals("p:a", wtx.getName().getPrefix() + ":" + wtx.getName().getLocalName());

			wtx.rollback();
		}
	}

	@Test
	public void testRevision() {
		XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
		Assert.assertEquals(0L, rtx.getRevisionNumber());

		try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
			Assert.assertEquals(1L, wtx.getRevisionNumber());

			// Commit and check.
			wtx.commit();
		}

		try {
			rtx = holder.getResourceManager().beginNodeReadOnlyTrx(Constants.UBP_ROOT_REVISION_NUMBER);

			assertEquals(Constants.UBP_ROOT_REVISION_NUMBER, rtx.getRevisionNumber());
		} finally {
			rtx.close();
		}

		try (final XmlNodeReadOnlyTrx rtx2 = holder.getResourceManager().beginNodeReadOnlyTrx()) {
			Assert.assertEquals(1L, rtx2.getRevisionNumber());
		}
	}

	@Test
	public void testShreddedRevision() {
		try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
			XmlDocumentCreator.create(wtx);
			Assert.assertEquals(1L, wtx.getRevisionNumber());
			wtx.commit();
		}

		try (final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx()) {
			Assert.assertEquals(1L, rtx.getRevisionNumber());
			rtx.moveTo(12L);
			assertEquals("bar", rtx.getValue());

			try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
				Assert.assertEquals(2L, wtx.getRevisionNumber());
				wtx.moveTo(12L);
				wtx.setValue("bar2");

				assertEquals("bar", rtx.getValue());
				assertEquals("bar2", wtx.getValue());
				wtx.rollback();
			}
		}

		try (final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx()) {
			Assert.assertEquals(1L, rtx.getRevisionNumber());
			rtx.moveTo(12L);
			assertEquals("bar", rtx.getValue());
		}
	}

	@Test
	public void testExisting() {
		final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
		final XmlResourceSession resource = database.beginResourceSession(XmlTestHelper.RESOURCE);

		final XmlNodeTrx wtx1 = resource.beginNodeTrx();
		XmlDocumentCreator.create(wtx1);
		Assert.assertEquals(1L, wtx1.getRevisionNumber());
		wtx1.commit();
		wtx1.close();
		resource.close();

		final XmlResourceSession resource2 = database.beginResourceSession(XmlTestHelper.RESOURCE);
		final XmlNodeReadOnlyTrx rtx1 = resource2.beginNodeReadOnlyTrx();
		Assert.assertEquals(1L, rtx1.getRevisionNumber());
		rtx1.moveTo(12L);
		assertEquals("bar", rtx1.getValue());

		final XmlNodeTrx wtx2 = resource2.beginNodeTrx();
		Assert.assertEquals(2L, wtx2.getRevisionNumber());
		wtx2.moveTo(12L);
		wtx2.setValue("bar2");

		assertEquals("bar", rtx1.getValue());
		assertEquals("bar2", wtx2.getValue());

		rtx1.close();
		wtx2.commit();
		wtx2.close();

		final var database2 = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
		final XmlResourceSession resource3 = database2.beginResourceSession(XmlTestHelper.RESOURCE);
		final XmlNodeReadOnlyTrx rtx2 = resource3.beginNodeReadOnlyTrx();
		Assert.assertEquals(2L, rtx2.getRevisionNumber());
		rtx2.moveTo(12L);
		assertEquals("bar2", rtx2.getValue());

		rtx2.close();
		resource3.close();
	}

	@Test
	public void testIdempotentClose() {
		final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
		XmlDocumentCreator.create(wtx);
		wtx.commit();
		wtx.close();
		wtx.close();

		final NodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
		assertFalse(rtx.moveTo(14L));
		rtx.close();
		rtx.close();
		holder.getResourceManager().close();
	}

	@Test
	public void testAutoCommitWithNodeThreshold() {
		// After each bunch of 5 nodes commit.
		try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx(5)) {
			XmlDocumentCreator.create(wtx);
			wtx.commit();
			Assert.assertEquals(4, wtx.getRevisionNumber());
		}
	}

	@Test
	public void testAutoCommitWithScheduler() throws InterruptedException {
		// After 500 milliseconds commit.
		try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx(500, TimeUnit.MILLISECONDS)) {
			TimeUnit.MILLISECONDS.sleep(1500);
			assertTrue(wtx.getRevisionNumber() >= 3);
		}
	}

	@Test
	public void testFetchingOfClosestRevisionToAGivenPointInTime() throws InterruptedException {
		final Instant start = Instant.now();
		final Instant afterAllCommits;
		final Instant afterFirstCommit;
		final Instant afterSecondCommit;
		try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx(2000, TimeUnit.MILLISECONDS)) {
			TimeUnit.MILLISECONDS.sleep(2100);
			afterFirstCommit = Instant.now();
			TimeUnit.MILLISECONDS.sleep(2100);
			afterSecondCommit = Instant.now();
			TimeUnit.MILLISECONDS.sleep(2100);
			assertTrue(wtx.getRevisionNumber() >= 3);
			afterAllCommits = Instant.now();
		}

		try (final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(start)) {
			Assert.assertEquals(0, rtx.getRevisionNumber());
		}

		try (final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(afterFirstCommit)) {
			Assert.assertEquals(1, rtx.getRevisionNumber());
		}

		try (final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(afterSecondCommit)) {
			Assert.assertEquals(2, rtx.getRevisionNumber());
		}

		try (final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(afterAllCommits)) {
			assertEquals(holder.getResourceManager().getMostRecentRevisionNumber(), rtx.getRevisionNumber());
		}
	}
}
