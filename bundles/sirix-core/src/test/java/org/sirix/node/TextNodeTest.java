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

import org.junit.Test;
import org.sirix.io.file.ByteBufferSinkAndSource;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.settings.EFixed;
import org.sirix.utils.NamePageHash;

public class TextNodeTest {

  @Test
  public void testTextRootNode() {

    // Create empty node.
    final byte[] value = {
      (byte)17, (byte)18
    };
    final NodeDelegate del = new NodeDelegate(13, 14, 0);
    final ValNodeDelegate valDel = new ValNodeDelegate(del, value, false);
    final StructNodeDelegate strucDel =
      new StructNodeDelegate(del, EFixed.NULL_NODE_KEY.getStandardProperty(), 16l, 15l, 0l, 0l);
    final TextNode node1 = new TextNode(del, valDel, strucDel);
    check(node1);

    // Serialize and deserialize node.
    final ByteBufferSinkAndSource out = new ByteBufferSinkAndSource();
    node1.getKind().serialize(out, node1);
    out.position(0);
    final TextNode node2 = (TextNode)EKind.TEXT.deserialize(out);
    check(node2);

  }

  private final static void check(final TextNode node) {

    // Now compare.
    assertEquals(13L, node.getNodeKey());
    assertEquals(14L, node.getParentKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), node.getFirstChildKey());
    assertEquals(15L, node.getLeftSiblingKey());
    assertEquals(16L, node.getRightSiblingKey());
    assertEquals(NamePageHash.generateHashForString("xs:untyped"), node.getTypeKey());
    assertEquals(2, node.getRawValue().length);
    assertEquals(EKind.TEXT, node.getKind());
    assertEquals(false, node.hasFirstChild());
    assertEquals(true, node.hasParent());
    assertEquals(true, node.hasLeftSibling());
    assertEquals(true, node.hasRightSibling());
  }

}
