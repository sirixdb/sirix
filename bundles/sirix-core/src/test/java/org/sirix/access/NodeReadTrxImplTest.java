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

package org.sirix.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.nio.file.Files;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.ResourceManagerConfiguration;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.api.XdmNodeReadTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;

public final class NodeReadTrxImplTest {

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
		assertFalse(Files.exists(PATHS.PATH2.getFile()));
		Databases.createDatabase(PATHS.PATH2.getConfig());

		try (final Database db = Databases.openDatabase(PATHS.PATH2.getFile())) {
			db.createResource(
					new ResourceConfiguration.Builder(TestHelper.RESOURCE, PATHS.PATH2.getConfig()).build());
			try (
					final ResourceManager resMgr = db.getResourceManager(
							new ResourceManagerConfiguration.Builder(TestHelper.RESOURCE).build());
					final XdmNodeReadTrx rtx = resMgr.beginNodeReadTrx()) {
				rtx.getRevisionNumber();
			}
		}
	}

	@Test
	public void testDocumentRoot() throws SirixException {
		assertEquals(true, holder.getReader().moveToDocumentRoot().hasMoved());
		assertEquals(Kind.DOCUMENT, holder.getReader().getKind());
		assertEquals(false, holder.getReader().hasParent());
		assertEquals(false, holder.getReader().hasLeftSibling());
		assertEquals(false, holder.getReader().hasRightSibling());
		assertEquals(true, holder.getReader().hasFirstChild());
	}

	@Test
	public void testConventions() throws SirixException {

		// ReadTransaction Convention 1.
		assertEquals(true, holder.getReader().moveToDocumentRoot().hasMoved());
		long key = holder.getReader().getNodeKey();

		// ReadTransaction Convention 2.
		assertEquals(holder.getReader().hasParent(), holder.getReader().moveToParent().hasMoved());
		assertEquals(key, holder.getReader().getNodeKey());

		assertEquals(holder.getReader().hasFirstChild(),
				holder.getReader().moveToFirstChild().hasMoved());
		assertEquals(1L, holder.getReader().getNodeKey());

		assertEquals(false, holder.getReader().moveTo(Integer.MAX_VALUE).hasMoved());
		assertEquals(false, holder.getReader().moveTo(Integer.MIN_VALUE).hasMoved());
		assertEquals(false, holder.getReader().moveTo(Long.MAX_VALUE).hasMoved());
		assertEquals(false, holder.getReader().moveTo(Long.MIN_VALUE).hasMoved());
		assertEquals(1L, holder.getReader().getNodeKey());

		assertEquals(holder.getReader().hasRightSibling(),
				holder.getReader().moveToRightSibling().hasMoved());
		assertEquals(1L, holder.getReader().getNodeKey());

		assertEquals(holder.getReader().hasFirstChild(),
				holder.getReader().moveToFirstChild().hasMoved());
		assertEquals(4L, holder.getReader().getNodeKey());

		assertEquals(holder.getReader().hasRightSibling(),
				holder.getReader().moveToRightSibling().hasMoved());
		assertEquals(5L, holder.getReader().getNodeKey());

		assertEquals(holder.getReader().hasLeftSibling(),
				holder.getReader().moveToLeftSibling().hasMoved());
		assertEquals(4L, holder.getReader().getNodeKey());

		assertEquals(holder.getReader().hasParent(), holder.getReader().moveToParent().hasMoved());
		assertEquals(1L, holder.getReader().getNodeKey());
	}

}
