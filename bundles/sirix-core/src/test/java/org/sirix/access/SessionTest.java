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

package org.sirix.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.settings.Constants;
import org.sirix.utils.DocumentCreater;

public class SessionTest {

	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		holder = Holder.generateWtx();
		holder.getWtx().commit();
		holder.getWtx().close();
		holder = Holder.generateRtx();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();
	}

	@Test
	public void testSingleton() throws SirixException {
		final Database database = Holder.generateSession().getDatabase();
		assertEquals(database, holder.getDatabase());
		final Session session = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		assertEquals(session, holder.getSession());
		session.close();
		final Session session2 = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		assertNotSame(session2, holder.getSession());
		database.close();

	}

	@Test
	public void testClosed() throws SirixException {
		NodeReadTrx rtx = holder.getRtx();
		rtx.close();

		try {
			rtx.getAttributeCount();
			fail();
		} catch (final IllegalStateException e) {
			// Must fail.
		} finally {
			holder.getSession().close();
		}
	}

	@Test
	public void testNonExisting() throws SirixException, InterruptedException {
		final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
		final Database database2 = TestHelper.getDatabase(PATHS.PATH1.getFile());
		assertTrue(database == database2);
	}

	@Test
	public void testInsertChild() throws SirixException {
		final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
		DocumentCreater.create(wtx);
		assertNotNull(wtx.moveToDocumentRoot());
		assertEquals(Kind.DOCUMENT, wtx.getKind());

		assertNotNull(wtx.moveToFirstChild());
		assertEquals(Kind.ELEMENT, wtx.getKind());
		assertEquals("p:a", new StringBuilder(wtx.getName().getPrefix())
				.append(":").append(wtx.getName().getLocalName()).toString());

		wtx.abort();
		wtx.close();
	}

	@Test
	public void testRevision() throws SirixException {
		NodeReadTrx rtx = holder.getRtx();
		assertEquals(0L, rtx.getRevisionNumber());

		final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
		assertEquals(1L, wtx.getRevisionNumber());

		// Commit and check.
		wtx.commit();
		wtx.close();

		rtx = holder.getSession().beginNodeReadTrx(Constants.UBP_ROOT_REVISION_NUMBER);

		assertEquals(Constants.UBP_ROOT_REVISION_NUMBER, rtx.getRevisionNumber());
		rtx.close();

		final NodeReadTrx rtx2 = holder.getSession().beginNodeReadTrx();
		assertEquals(1L, rtx2.getRevisionNumber());
		rtx2.close();
	}

	@Test
	public void testShreddedRevision() throws SirixException {

		final NodeWriteTrx wtx1 = holder.getSession().beginNodeWriteTrx();
		DocumentCreater.create(wtx1);
		assertEquals(1L, wtx1.getRevisionNumber());
		wtx1.commit();
		wtx1.close();

		final NodeReadTrx rtx1 = holder.getSession().beginNodeReadTrx();
		assertEquals(1L, rtx1.getRevisionNumber());
		rtx1.moveTo(12L);
		assertEquals("bar", rtx1.getValue());

		final NodeWriteTrx wtx2 = holder.getSession().beginNodeWriteTrx();
		assertEquals(2L, wtx2.getRevisionNumber());
		wtx2.moveTo(12L);
		wtx2.setValue("bar2");

		assertEquals("bar", rtx1.getValue());
		assertEquals("bar2", wtx2.getValue());
		rtx1.close();
		wtx2.abort();
		wtx2.close();

		final NodeReadTrx rtx2 = holder.getSession().beginNodeReadTrx();
		assertEquals(1L, rtx2.getRevisionNumber());
		rtx2.moveTo(12L);
		assertEquals("bar", rtx2.getValue());
		rtx2.close();
	}

	@Test
	public void testExisting() throws SirixException {
		final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
		final Session session1 = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());

		final NodeWriteTrx wtx1 = session1.beginNodeWriteTrx();
		DocumentCreater.create(wtx1);
		assertEquals(1L, wtx1.getRevisionNumber());
		wtx1.commit();
		wtx1.close();
		session1.close();

		final Session session2 = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final NodeReadTrx rtx1 = session2.beginNodeReadTrx();
		assertEquals(1L, rtx1.getRevisionNumber());
		rtx1.moveTo(12L);
		assertEquals("bar", rtx1.getValue());

		final NodeWriteTrx wtx2 = session2.beginNodeWriteTrx();
		assertEquals(2L, wtx2.getRevisionNumber());
		wtx2.moveTo(12L);
		wtx2.setValue("bar2");

		assertEquals("bar", rtx1.getValue());
		assertEquals("bar2", wtx2.getValue());

		rtx1.close();
		wtx2.commit();
		wtx2.close();
		session2.close();

		final Database database2 = TestHelper.getDatabase(PATHS.PATH1.getFile());
		final Session session3 = database2
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final NodeReadTrx rtx2 = session3.beginNodeReadTrx();
		assertEquals(2L, rtx2.getRevisionNumber());
		rtx2.moveTo(12L);
		assertEquals("bar2", rtx2.getValue());

		rtx2.close();
		session3.close();

	}

	@Test
	public void testIdempotentClose() throws SirixException {
		final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
		DocumentCreater.create(wtx);
		wtx.commit();
		wtx.close();
		wtx.close();

		final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
		assertEquals(false, rtx.moveTo(14L).hasMoved());
		rtx.close();
		rtx.close();
		holder.getSession().close();
	}

	@Test
	public void testAutoCommit() throws SirixException {
		final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
		DocumentCreater.create(wtx);
	}

	@Test
	public void testAutoClose() throws SirixException {

		final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
		DocumentCreater.create(wtx);
		wtx.commit();
		holder.getSession().beginNodeReadTrx();
	}
}
