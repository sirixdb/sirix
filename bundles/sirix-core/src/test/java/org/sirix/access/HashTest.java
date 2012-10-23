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

import javax.xml.namespace.QName;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.access.conf.SessionConfiguration;
import org.sirix.api.Database;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.Session;
import org.sirix.exception.SirixException;
import org.sirix.settings.Fixed;

public class HashTest {

	private final static String NAME1 = "a";
	private final static String NAME2 = "b";

	@Before
	public void setUp() throws SirixException {
		TestHelper.deleteEverything();
	}

	@Test
	public void testPostorderInsertRemove() throws SirixException {
		final NodeWriteTrx wtx = createWtx(HashKind.POSTORDER);
		testHashTreeWithInsertAndRemove(wtx);
	}

	@Test
	public void testPostorderDeep() throws SirixException {
		final NodeWriteTrx wtx = createWtx(HashKind.POSTORDER);
		testDeepTree(wtx);
	}

	@Test
	public void testPostorderSetter() throws SirixException {
		final NodeWriteTrx wtx = createWtx(HashKind.POSTORDER);
		testSetter(wtx);
	}

	@Test
	public void testRollingInsertRemove() throws SirixException {
		final NodeWriteTrx wtx = createWtx(HashKind.ROLLING);
		testHashTreeWithInsertAndRemove(wtx);
	}

	@Test
	public void testRollingDeep() throws SirixException {
		final NodeWriteTrx wtx = createWtx(HashKind.ROLLING);
		testDeepTree(wtx);
	}

	@Test
	public void testRollingSetter() throws SirixException {
		final NodeWriteTrx wtx = createWtx(HashKind.ROLLING);
		testSetter(wtx);
	}

	/**
	 * Inserting nodes and removing them.
	 * 
	 * <pre>
	 * -a (1)
	 *  '-test (5)
	 *  '-a (6)
	 *    '-attr(7)
	 *    '-a (8)
	 *      '-attr (9)
	 *  '-text (2)
	 *  '-a (3(x))
	 *    '-attr(4(x))
	 * </pre>
	 * 
	 * @param wtx
	 * @throws TTException
	 */
	@Ignore
	private void testHashTreeWithInsertAndRemove(final NodeWriteTrx wtx)
			throws SirixException {

		// inserting a element as root
		wtx.insertElementAsFirstChild(new QName(NAME1));
		final long rootKey = wtx.getNodeKey();
		final long firstRootHash = wtx.getHash();

		// inserting a text as second child of root
		wtx.moveTo(rootKey);
		wtx.insertTextAsFirstChild(NAME1);
		wtx.moveTo(wtx.getParentKey());
		final long secondRootHash = wtx.getHash();

		// inserting a second element on level 2 under the only element
		wtx.moveTo(wtx.getFirstChildKey());
		wtx.insertElementAsRightSibling(new QName(NAME2));
		wtx.insertAttribute(new QName(NAME2), NAME1);
		wtx.moveTo(rootKey);
		final long thirdRootHash = wtx.getHash();

		// Checking that all hashes are different
		assertFalse(firstRootHash == secondRootHash);
		assertFalse(firstRootHash == thirdRootHash);
		assertFalse(secondRootHash == thirdRootHash);

		// removing the second element
		wtx.moveTo(wtx.getFirstChildKey());
		wtx.moveTo(wtx.getRightSiblingKey());
		wtx.remove();
		wtx.moveTo(rootKey);
		assertEquals(secondRootHash, wtx.getHash());

		// adding additional element for showing that hashes are computed
		// incrementilly
		wtx.insertTextAsFirstChild(NAME1);
		wtx.insertElementAsRightSibling(new QName(NAME1));
		wtx.insertAttribute(new QName(NAME1), NAME2);
		wtx.moveToParent();
		wtx.insertElementAsFirstChild(new QName(NAME1));
		wtx.insertAttribute(new QName(NAME2), NAME1);

		wtx.moveTo(rootKey);
		wtx.moveToFirstChild();
		wtx.remove();
		wtx.remove();

		wtx.moveTo(rootKey);
		assertEquals(firstRootHash, wtx.getHash());
	}

	@Ignore
	private void testDeepTree(final NodeWriteTrx wtx) throws SirixException {

		wtx.insertElementAsFirstChild(new QName(NAME1));
		final long oldHash = wtx.getHash();

		wtx.insertElementAsFirstChild(new QName(NAME1));
		wtx.insertElementAsFirstChild(new QName(NAME2));
		wtx.insertElementAsFirstChild(new QName(NAME1));
		wtx.insertElementAsFirstChild(new QName(NAME2));
		wtx.insertElementAsFirstChild(new QName(NAME1));
		wtx.remove();
		wtx.insertElementAsFirstChild(new QName(NAME2));
		wtx.insertElementAsFirstChild(new QName(NAME2));
		wtx.insertElementAsFirstChild(new QName(NAME1));

		wtx.moveTo(1);
		wtx.moveTo(wtx.getFirstChildKey());
		wtx.remove();
		assertEquals(oldHash, wtx.getHash());
	}

	@Ignore
	private void testSetter(final NodeWriteTrx wtx) throws SirixException {

		// Testing node inheritance
		wtx.insertElementAsFirstChild(new QName(NAME1));
		wtx.insertElementAsFirstChild(new QName(NAME1));
		wtx.insertElementAsFirstChild(new QName(NAME1));
		wtx.moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
		wtx.moveTo(wtx.getFirstChildKey());
		final long hashRoot1 = wtx.getHash();
		wtx.moveTo(wtx.getFirstChildKey());
		wtx.moveTo(wtx.getFirstChildKey());
		final long hashLeaf1 = wtx.getHash();
		wtx.setName(new QName(NAME2));
		final long hashLeaf2 = wtx.getHash();
		wtx.moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
		wtx.moveTo(wtx.getFirstChildKey());
		final long hashRoot2 = wtx.getHash();
		assertFalse(hashRoot1 == hashRoot2);
		assertFalse(hashLeaf1 == hashLeaf2);
		wtx.moveTo(wtx.getFirstChildKey());
		wtx.moveTo(wtx.getFirstChildKey());
		wtx.setName(new QName(NAME1));
		final long hashLeaf3 = wtx.getHash();
		assertEquals(hashLeaf1, hashLeaf3);
		wtx.moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
		wtx.moveTo(wtx.getFirstChildKey());
		final long hashRoot3 = wtx.getHash();
		assertEquals(hashRoot1, hashRoot3);

		// Testing root inheritance
		wtx.moveTo(Fixed.DOCUMENT_NODE_KEY.getStandardProperty());
		wtx.moveTo(wtx.getFirstChildKey());
		wtx.setName(new QName(NAME2));
		final long hashRoot4 = wtx.getHash();
		assertFalse(hashRoot4 == hashRoot2);
		assertFalse(hashRoot4 == hashRoot1);
		assertFalse(hashRoot4 == hashRoot3);
		assertFalse(hashRoot4 == hashLeaf1);
		assertFalse(hashRoot4 == hashLeaf2);
		assertFalse(hashRoot4 == hashLeaf3);
	}

	private NodeWriteTrx createWtx(final HashKind kind) throws SirixException {
		final Database database = TestHelper.getDatabase(TestHelper.PATHS.PATH1
				.getFile());
		database.createResource(new ResourceConfiguration.Builder(
				TestHelper.RESOURCE, PATHS.PATH1.getConfig()).build());
		final Session session = database
				.getSession(new SessionConfiguration.Builder(TestHelper.RESOURCE)
						.build());
		final NodeWriteTrx wTrx = session.beginNodeWriteTrx();
		return wTrx;
	}

	@After
	public void tearDown() throws SirixException {
		TestHelper.closeEverything();
	}

}
