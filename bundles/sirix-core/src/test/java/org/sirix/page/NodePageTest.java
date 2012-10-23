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

package org.sirix.page;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixException;
import org.sirix.node.ElementNode;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.NameNode;
import org.sirix.utils.NamePageHash;

import com.google.common.base.Optional;
import com.google.common.collect.HashBiMap;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

/**
 * Node page test.
 */
public class NodePageTest {

	/** {@link Holder} instance. */
	private Holder mHolder;

	/** Sirix {@link PageReadTrx} instance. */
	private PageReadTrx mPageReadTrx;

	@Before
	public void setUp() throws SirixException {
		TestHelper.closeEverything();
		TestHelper.deleteEverything();
		mHolder = Holder.generateDeweyIDSession();
		mPageReadTrx = mHolder.getSession().beginPageReadTrx();
	}

	@After
	public void tearDown() throws SirixException {
		mPageReadTrx.close();
		mHolder.close();
	}

	@Test
	public void testSerializeDeserialize() {
		final NodePage page1 = new NodePage(0L, 0, mPageReadTrx);
		assertEquals(0L, page1.getNodePageKey());

		final NodeDelegate del = new NodeDelegate(0, 1, 0, 0,
				Optional.of(SirixDeweyID.newRootID()));
		final StructNodeDelegate strucDel = new StructNodeDelegate(del, 12l, 4l,
				3l, 1l, 0l);
		final NameNodeDelegate nameDel = new NameNodeDelegate(del, 6, 7, 1);

		final ElementNode node1 = new ElementNode(strucDel, nameDel,
				new ArrayList<Long>(), HashBiMap.<Integer, Long> create(),
				new ArrayList<Long>());
		node1.insertAttribute(88L, 100);
		node1.insertAttribute(87L, 101);
		node1.insertNamespace(99L);
		node1.insertNamespace(98L);
		assertEquals(0L, node1.getNodeKey());
		page1.setNode(node1);

		final ByteArrayDataOutput out = ByteStreams.newDataOutput();
		PagePersistenter.serializePage(out, page1);
		final ByteArrayDataInput in = ByteStreams.newDataInput(out.toByteArray());
		final NodePage page2 = (NodePage) PagePersistenter.deserializePage(in,
				mPageReadTrx);
		// assertEquals(position, out.position());
		assertEquals(0L, page2.getNode(0).getNodeKey());
		assertEquals(1L, ((ElementNode) page2.getNode(0)).getParentKey());
		assertEquals(12L, ((ElementNode) page2.getNode(0)).getFirstChildKey());
		assertEquals(3L, ((ElementNode) page2.getNode(0)).getLeftSiblingKey());
		assertEquals(4L, ((ElementNode) page2.getNode(0)).getRightSiblingKey());
		assertEquals(1, ((ElementNode) page2.getNode(0)).getChildCount());
		assertEquals(2, ((ElementNode) page2.getNode(0)).getAttributeCount());
		assertEquals(2, ((ElementNode) page2.getNode(0)).getNamespaceCount());
		assertEquals(88L, ((ElementNode) page2.getNode(0)).getAttributeKey(0));
		assertEquals(87L, ((ElementNode) page2.getNode(0)).getAttributeKey(1));
		assertEquals(99L, ((ElementNode) page2.getNode(0)).getNamespaceKey(0));
		assertEquals(98L, ((ElementNode) page2.getNode(0)).getNamespaceKey(1));
		assertEquals(6, ((NameNode) page2.getNode(0)).getNameKey());
		assertEquals(7, ((NameNode) page2.getNode(0)).getURIKey());
		assertEquals(NamePageHash.generateHashForString("xs:untyped"),
				((ElementNode) page2.getNode(0)).getTypeKey());
	}
}
