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

package io.sirix.access.node.xml;

import java.nio.file.Files;

import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.xml.InternalXmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.axis.DescendantAxis;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import io.sirix.settings.Fixed;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;

import static org.junit.Assert.*;

public final class XmlNodeReadOnlyTrxImplTest {

  private Holder holder;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testEmptyRtx() {
    assertFalse(Files.exists(PATHS.PATH2.getFile()));
    Databases.createXmlDatabase(PATHS.PATH2.getConfig());

    try (final var db = Databases.openXmlDatabase(PATHS.PATH2.getFile())) {
      db.createResource(new ResourceConfiguration.Builder(XmlTestHelper.RESOURCE).build());
      try (final XmlResourceSession resMgr = db.beginResourceSession(XmlTestHelper.RESOURCE);
          final XmlNodeReadOnlyTrx rtx = resMgr.beginNodeReadOnlyTrx()) {
        Assert.assertEquals(0, rtx.getRevisionNumber());
      }
    }
  }

  @Test
  public void testDocumentRoot() {
    assertTrue(holder.getXmlNodeReadTrx().moveToDocumentRoot());
    assertEquals(NodeKind.XML_DOCUMENT, holder.getXmlNodeReadTrx().getKind());
    assertFalse(holder.getXmlNodeReadTrx().hasParent());
    assertFalse(holder.getXmlNodeReadTrx().hasLeftSibling());
    assertFalse(holder.getXmlNodeReadTrx().hasRightSibling());
    assertTrue(holder.getXmlNodeReadTrx().hasFirstChild());
  }

  @Test
  public void testConventions() {

    // ReadTransaction Convention 1.
    assertTrue(holder.getXmlNodeReadTrx().moveToDocumentRoot());
    long key = holder.getXmlNodeReadTrx().getNodeKey();

    // ReadTransaction Convention 2.
    assertEquals(holder.getXmlNodeReadTrx().hasParent(), holder.getXmlNodeReadTrx().moveToParent());
    assertEquals(key, holder.getXmlNodeReadTrx().getNodeKey());

    assertEquals(holder.getXmlNodeReadTrx().hasFirstChild(), holder.getXmlNodeReadTrx().moveToFirstChild());
    assertEquals(1L, holder.getXmlNodeReadTrx().getNodeKey());

    assertFalse(holder.getXmlNodeReadTrx().moveTo(Integer.MAX_VALUE));
    assertFalse(holder.getXmlNodeReadTrx().moveTo(Integer.MIN_VALUE));
    assertFalse(holder.getXmlNodeReadTrx().moveTo(Long.MAX_VALUE));
    assertFalse(holder.getXmlNodeReadTrx().moveTo(Long.MIN_VALUE));
    assertEquals(1L, holder.getXmlNodeReadTrx().getNodeKey());

    assertEquals(holder.getXmlNodeReadTrx().hasRightSibling(), holder.getXmlNodeReadTrx().moveToRightSibling());
    assertEquals(1L, holder.getXmlNodeReadTrx().getNodeKey());

    assertEquals(holder.getXmlNodeReadTrx().hasFirstChild(), holder.getXmlNodeReadTrx().moveToFirstChild());
    assertEquals(4L, holder.getXmlNodeReadTrx().getNodeKey());

    assertEquals(holder.getXmlNodeReadTrx().hasRightSibling(), holder.getXmlNodeReadTrx().moveToRightSibling());
    assertEquals(5L, holder.getXmlNodeReadTrx().getNodeKey());

    assertEquals(holder.getXmlNodeReadTrx().hasLeftSibling(), holder.getXmlNodeReadTrx().moveToLeftSibling());
    assertEquals(4L, holder.getXmlNodeReadTrx().getNodeKey());

    assertEquals(holder.getXmlNodeReadTrx().hasParent(), holder.getXmlNodeReadTrx().moveToParent());
    assertEquals(1L, holder.getXmlNodeReadTrx().getNodeKey());
  }

  @Test
  public void testMoveToReusesXmlElementSingleton() {
    final InternalXmlNodeReadOnlyTrx rtx = (InternalXmlNodeReadOnlyTrx) holder.getXmlNodeReadTrx();
    assertTrue(rtx.moveToDocumentRoot());

    long firstElementKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    long secondElementKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    final DescendantAxis axis = new DescendantAxis(rtx);
    while (axis.hasNext()) {
      axis.nextLong();
      if (rtx.getKind() == NodeKind.ELEMENT) {
        if (firstElementKey == Fixed.NULL_NODE_KEY.getStandardProperty()) {
          firstElementKey = rtx.getNodeKey();
        } else {
          secondElementKey = rtx.getNodeKey();
          break;
        }
      }
    }

    assertTrue(firstElementKey != Fixed.NULL_NODE_KEY.getStandardProperty());
    assertTrue(secondElementKey != Fixed.NULL_NODE_KEY.getStandardProperty());

    assertTrue(rtx.moveTo(firstElementKey));
    final StructNode firstSingleton = rtx.getStructuralNode();
    assertEquals(firstElementKey, firstSingleton.getNodeKey());

    assertTrue(rtx.moveTo(secondElementKey));
    final StructNode secondSingleton = rtx.getStructuralNode();
    assertSame(firstSingleton, secondSingleton);
    assertEquals(secondElementKey, secondSingleton.getNodeKey());
  }

}
