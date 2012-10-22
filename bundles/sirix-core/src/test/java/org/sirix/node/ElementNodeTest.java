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

package org.sirix.node;

import static org.junit.Assert.assertEquals;

import com.google.common.base.Optional;
import com.google.common.collect.HashBiMap;
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.File;
import java.util.ArrayList;

import org.junit.Test;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceConfiguration;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.utils.NamePageHash;

public class ElementNodeTest {

	@Test
	public void testElementNode() {
		final NodeDelegate del = new NodeDelegate(13, 14, 0, 0, Optional.of(SirixDeweyID.newRootID()));
		final StructNodeDelegate strucDel = new StructNodeDelegate(del, 12l, 17l,
				16l, 1l, 0);
		final NameNodeDelegate nameDel = new NameNodeDelegate(del, 18, 19, 1);

		final ElementNode node1 = new ElementNode(strucDel, nameDel,
				new ArrayList<Long>(), HashBiMap.<Integer, Long> create(),
				new ArrayList<Long>());

		// Create empty node.
		node1.insertAttribute(97, 100);
		node1.insertAttribute(98, 101);
		node1.insertNamespace(99);
		node1.insertNamespace(100);
		check(node1);

		// Serialize and deserialize node.
		final ResourceConfiguration resourceConfig = new ResourceConfiguration.Builder(
				"", new DatabaseConfiguration(new File(""))).build();
		final ByteArrayDataOutput out = ByteStreams.newDataOutput();
		node1.getKind().serialize(out, node1, resourceConfig);
		final ByteArrayDataInput in = ByteStreams.newDataInput(out.toByteArray());
		final ElementNode node2 = (ElementNode) Kind.ELEMENT.deserialize(in, resourceConfig);
		check(node2);
	}

	private final static void check(final ElementNode node) {
		// Now compare.
		assertEquals(13L, node.getNodeKey());
		assertEquals(14L, node.getParentKey());
		assertEquals(12L, node.getFirstChildKey());
		assertEquals(16L, node.getLeftSiblingKey());
		assertEquals(17L, node.getRightSiblingKey());
		assertEquals(1, node.getChildCount());
		assertEquals(2, node.getAttributeCount());
		assertEquals(2, node.getNamespaceCount());
		assertEquals(18, node.getNameKey());
		assertEquals(19, node.getURIKey());
		assertEquals(NamePageHash.generateHashForString("xs:untyped"),
				node.getTypeKey());
		assertEquals(Kind.ELEMENT, node.getKind());
		assertEquals(true, node.hasFirstChild());
		assertEquals(true, node.hasParent());
		assertEquals(true, node.hasLeftSibling());
		assertEquals(true, node.hasRightSibling());
		assertEquals(97L, node.getAttributeKey(0));
		assertEquals(98L, node.getAttributeKey(1));
		assertEquals(99L, node.getNamespaceKey(0));
		assertEquals(100L, node.getNamespaceKey(1));
	}

}
