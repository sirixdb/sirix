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
import static org.junit.Assert.assertFalse;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.IDatabase;
import org.sirix.api.INodeReadTrx;
import org.sirix.api.ISession;
import org.sirix.exception.SirixException;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.IStructNode;

public class ReadTransactionTest {

	private Holder holder;

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
		TestHelper.createTestDocument();
		holder = Holder.generateRtx();
	}

	@After
	public void tearDown() throws SirixException {
		holder.close();
		TestHelper.closeEverything();
	}

	@Test
	public void testEmptyRtx() throws SirixException {
		assertFalse(PATHS.PATH2.getFile().exists());
		Database.createDatabase(PATHS.PATH2.getConfig());
		final IDatabase db = Database.openDatabase(PATHS.PATH2.getFile());
		db.createResource(new ResourceConfiguration.Builder(TestHelper.RESOURCE,
				PATHS.PATH2.getConfig()).build());
		final ISession session = db.getSession(new SessionConfiguration.Builder(
				TestHelper.RESOURCE).build());
		final INodeReadTrx rtx = session.beginNodeReadTrx();
		rtx.getRevisionNumber();
		rtx.close();
		session.close();
		db.close();
	}

	@Test
	public void testDocumentRoot() throws SirixException {
		assertEquals(true, holder.getRtx().moveToDocumentRoot().hasMoved());
		assertEquals(EKind.DOCUMENT_ROOT, holder.getRtx().getKind());
		assertEquals(false, holder.getRtx().hasParent());
		assertEquals(false, holder.getRtx().hasLeftSibling());
		assertEquals(false, holder.getRtx().hasRightSibling());
		assertEquals(true, holder.getRtx().hasFirstChild());
		holder.getRtx().close();
	}

	@Test
	public void testConventions() throws SirixException {

		// IReadTransaction Convention 1.
		assertEquals(true, holder.getRtx().moveToDocumentRoot().hasMoved());
		long key = holder.getRtx().getNodeKey();

		// IReadTransaction Convention 2.
		assertEquals(holder.getRtx().hasParent(), holder.getRtx().moveToParent()
				.hasMoved());
		assertEquals(key, holder.getRtx().getNodeKey());

		assertEquals(holder.getRtx().hasFirstChild(), holder
				.getRtx().moveToFirstChild().hasMoved());
		assertEquals(1L, holder.getRtx().getNodeKey());

		assertEquals(false, holder.getRtx().moveTo(Integer.MAX_VALUE).hasMoved());
		assertEquals(false, holder.getRtx().moveTo(Integer.MIN_VALUE).hasMoved());
		assertEquals(false, holder.getRtx().moveTo(Long.MAX_VALUE).hasMoved());
		assertEquals(false, holder.getRtx().moveTo(Long.MIN_VALUE).hasMoved());
		assertEquals(1L, holder.getRtx().getNodeKey());

		assertEquals(holder.getRtx().hasRightSibling(), holder.getRtx()
				.moveToRightSibling().hasMoved());
		assertEquals(1L, holder.getRtx().getNodeKey());

		assertEquals(holder.getRtx().hasFirstChild(), holder.getRtx()
				.moveToFirstChild().hasMoved());
		assertEquals(4L, holder.getRtx().getNodeKey());

		assertEquals(holder.getRtx().hasRightSibling(), holder.getRtx()
				.moveToRightSibling().hasMoved());
		assertEquals(5L, holder.getRtx().getNodeKey());

		assertEquals(holder.getRtx().hasLeftSibling(), holder.getRtx()
				.moveToLeftSibling().hasMoved());
		assertEquals(4L, holder.getRtx().getNodeKey());

		assertEquals(holder.getRtx().hasParent(), holder.getRtx().moveToParent().hasMoved());
		assertEquals(1L, holder.getRtx().getNodeKey());

		holder.getRtx().close();
	}

}
