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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.XmlTestHelper.PATHS;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.node.NodeKind;

import java.nio.file.Files;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public final class NodeReadTrxImplTest {

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
      try (final XmlResourceManager resMgr = db.openResourceManager(XmlTestHelper.RESOURCE);
          final XmlNodeReadOnlyTrx rtx = resMgr.beginNodeReadOnlyTrx()) {
        assertEquals(0, rtx.getRevisionNumber());
      }
    }
  }

  @Test
  public void testDocumentRoot() {
    assertEquals(true, holder.getXdmNodeReadTrx().moveToDocumentRoot().hasMoved());
    assertEquals(NodeKind.XDM_DOCUMENT, holder.getXdmNodeReadTrx().getKind());
    assertEquals(false, holder.getXdmNodeReadTrx().hasParent());
    assertEquals(false, holder.getXdmNodeReadTrx().hasLeftSibling());
    assertEquals(false, holder.getXdmNodeReadTrx().hasRightSibling());
    assertEquals(true, holder.getXdmNodeReadTrx().hasFirstChild());
  }

  @Test
  public void testConventions() {

    // ReadTransaction Convention 1.
    assertEquals(true, holder.getXdmNodeReadTrx().moveToDocumentRoot().hasMoved());
    long key = holder.getXdmNodeReadTrx().getNodeKey();

    // ReadTransaction Convention 2.
    assertEquals(holder.getXdmNodeReadTrx().hasParent(), holder.getXdmNodeReadTrx().moveToParent().hasMoved());
    assertEquals(key, holder.getXdmNodeReadTrx().getNodeKey());

    assertEquals(holder.getXdmNodeReadTrx().hasFirstChild(), holder.getXdmNodeReadTrx().moveToFirstChild().hasMoved());
    assertEquals(1L, holder.getXdmNodeReadTrx().getNodeKey());

    assertEquals(false, holder.getXdmNodeReadTrx().moveTo(Integer.MAX_VALUE).hasMoved());
    assertEquals(false, holder.getXdmNodeReadTrx().moveTo(Integer.MIN_VALUE).hasMoved());
    assertEquals(false, holder.getXdmNodeReadTrx().moveTo(Long.MAX_VALUE).hasMoved());
    assertEquals(false, holder.getXdmNodeReadTrx().moveTo(Long.MIN_VALUE).hasMoved());
    assertEquals(1L, holder.getXdmNodeReadTrx().getNodeKey());

    assertEquals(holder.getXdmNodeReadTrx().hasRightSibling(), holder.getXdmNodeReadTrx().moveToRightSibling().hasMoved());
    assertEquals(1L, holder.getXdmNodeReadTrx().getNodeKey());

    assertEquals(holder.getXdmNodeReadTrx().hasFirstChild(), holder.getXdmNodeReadTrx().moveToFirstChild().hasMoved());
    assertEquals(4L, holder.getXdmNodeReadTrx().getNodeKey());

    assertEquals(holder.getXdmNodeReadTrx().hasRightSibling(), holder.getXdmNodeReadTrx().moveToRightSibling().hasMoved());
    assertEquals(5L, holder.getXdmNodeReadTrx().getNodeKey());

    assertEquals(holder.getXdmNodeReadTrx().hasLeftSibling(), holder.getXdmNodeReadTrx().moveToLeftSibling().hasMoved());
    assertEquals(4L, holder.getXdmNodeReadTrx().getNodeKey());

    assertEquals(holder.getXdmNodeReadTrx().hasParent(), holder.getXdmNodeReadTrx().moveToParent().hasMoved());
    assertEquals(1L, holder.getXdmNodeReadTrx().getNodeKey());
  }

}
