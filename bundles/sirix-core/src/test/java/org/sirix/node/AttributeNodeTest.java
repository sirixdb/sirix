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
import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import org.junit.Test;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.utils.NamePageHash;

public class AttributeNodeTest {

  @Test
  public void testAttributeNode() {
    final byte[] value = {
      (byte)17, (byte)18
    };

    final NodeDelegate del = new NodeDelegate(99, 13, 0, 0);
    final NameNodeDelegate nameDel = new NameNodeDelegate(del, 14, 15, 1);
    final ValNodeDelegate valDel = new ValNodeDelegate(del, value, false);

    final AttributeNode node1 = new AttributeNode(del, nameDel, valDel);

    // Create empty node.
    check(node1);

    // Serialize and deserialize node.
    final ByteArrayDataOutput out = ByteStreams.newDataOutput();
    node1.getKind().serialize(out, node1);
    final ByteArrayDataInput in = ByteStreams.newDataInput(out.toByteArray());
    final AttributeNode node2 = (AttributeNode)EKind.ATTRIBUTE.deserialize(in);
    check(node2);

  }

  private final static void check(final AttributeNode node) {
    // Now compare.
    assertEquals(99L, node.getNodeKey());
    assertEquals(13L, node.getParentKey());

    assertEquals(14, node.getNameKey());
    assertEquals(15, node.getURIKey());
    assertEquals(NamePageHash.generateHashForString("xs:untyped"), node.getTypeKey());
    assertEquals(2, node.getRawValue().length);
    assertEquals(EKind.ATTRIBUTE, node.getKind());
    assertEquals(true, node.hasParent());
    assertEquals(EKind.ATTRIBUTE, node.getKind());
  }

}
