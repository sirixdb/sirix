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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.XdmTestHelper.PATHS;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.settings.Constants;
import org.sirix.utils.XdmDocumentCreator;

public class XdmResourceManagerTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XdmTestHelper.closeEverything();
  }

  @Test
  public void testSingleton() {
    try (final var database = Holder.openResourceManager().getDatabase()) {
      assertEquals(database, holder.getDatabase());

      try (final XdmResourceManager manager = database.openResourceManager(XdmTestHelper.RESOURCE)) {
        assertEquals(manager, holder.getResourceManager());
      }

      try (final XdmResourceManager manager2 = database.openResourceManager(XdmTestHelper.RESOURCE)) {
        assertNotSame(manager2, holder.getResourceManager());
      }
    }
  }

  @Test
  public void testClosed() {
    final XdmNodeReadOnlyTrx rtx = holder.getNodeReadTrx();
    rtx.close();

    try {
      rtx.getAttributeCount();
      fail();
    } catch (final IllegalStateException e) {
      // Must fail.
    } finally {
      holder.getResourceManager().close();
    }
  }

  @Test
  public void testNonExisting() throws SirixException, InterruptedException {
    final var database = XdmTestHelper.getDatabase(PATHS.PATH1.getFile());
    final var database2 = XdmTestHelper.getDatabase(PATHS.PATH1.getFile());
    assertTrue(database == database2);
  }

  @Test
  public void testInsertChild() {
    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
      XdmDocumentCreator.create(wtx);
      assertNotNull(wtx.moveToDocumentRoot());
      assertEquals(Kind.XDM_DOCUMENT, wtx.getKind());

      assertNotNull(wtx.moveToFirstChild());
      assertEquals(Kind.ELEMENT, wtx.getKind());
      assertEquals("p:a",
          new StringBuilder(wtx.getName().getPrefix()).append(":").append(wtx.getName().getLocalName()).toString());

      wtx.rollback();
    }
  }

  @Test
  public void testRevision() {
    XdmNodeReadOnlyTrx rtx = holder.getNodeReadTrx();
    assertEquals(0L, rtx.getRevisionNumber());

    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
      assertEquals(1L, wtx.getRevisionNumber());

      // Commit and check.
      wtx.commit();
    }

    try {
      rtx = holder.getResourceManager().beginNodeReadOnlyTrx(Constants.UBP_ROOT_REVISION_NUMBER);

      assertEquals(Constants.UBP_ROOT_REVISION_NUMBER, rtx.getRevisionNumber());
    } finally {
      rtx.close();
    }

    try (final XdmNodeReadOnlyTrx rtx2 = holder.getResourceManager().beginNodeReadOnlyTrx()) {
      assertEquals(1L, rtx2.getRevisionNumber());
    }
  }

  @Test
  public void testShreddedRevision() {
    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
      XdmDocumentCreator.create(wtx);
      assertEquals(1L, wtx.getRevisionNumber());
      wtx.commit();
    }

    try (final XdmNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx()) {
      assertEquals(1L, rtx.getRevisionNumber());
      rtx.moveTo(12L);
      assertEquals("bar", rtx.getValue());

      try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
        assertEquals(2L, wtx.getRevisionNumber());
        wtx.moveTo(12L);
        wtx.setValue("bar2");

        assertEquals("bar", rtx.getValue());
        assertEquals("bar2", wtx.getValue());
        wtx.rollback();
      }
    }

    try (final XdmNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx()) {
      assertEquals(1L, rtx.getRevisionNumber());
      rtx.moveTo(12L);
      assertEquals("bar", rtx.getValue());
    }
  }

  @Test
  public void testExisting() {
    final var database = XdmTestHelper.getDatabase(PATHS.PATH1.getFile());
    final XdmResourceManager resource = database.openResourceManager(XdmTestHelper.RESOURCE);

    final XdmNodeTrx wtx1 = resource.beginNodeTrx();
    XdmDocumentCreator.create(wtx1);
    assertEquals(1L, wtx1.getRevisionNumber());
    wtx1.commit();
    wtx1.close();
    resource.close();

    final XdmResourceManager resource2 = database.openResourceManager(XdmTestHelper.RESOURCE);
    final XdmNodeReadOnlyTrx rtx1 = resource2.beginNodeReadOnlyTrx();
    assertEquals(1L, rtx1.getRevisionNumber());
    rtx1.moveTo(12L);
    assertEquals("bar", rtx1.getValue());

    final XdmNodeTrx wtx2 = resource2.beginNodeTrx();
    assertEquals(2L, wtx2.getRevisionNumber());
    wtx2.moveTo(12L);
    wtx2.setValue("bar2");

    assertEquals("bar", rtx1.getValue());
    assertEquals("bar2", wtx2.getValue());

    rtx1.close();
    wtx2.commit();
    wtx2.close();

    final var database2 = XdmTestHelper.getDatabase(PATHS.PATH1.getFile());
    final XdmResourceManager resource3 = database2.openResourceManager(XdmTestHelper.RESOURCE);
    final XdmNodeReadOnlyTrx rtx2 = resource3.beginNodeReadOnlyTrx();
    assertEquals(2L, rtx2.getRevisionNumber());
    rtx2.moveTo(12L);
    assertEquals("bar2", rtx2.getValue());

    rtx2.close();
    resource3.close();
  }

  @Test
  public void testIdempotentClose() {
    final XdmNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XdmDocumentCreator.create(wtx);
    wtx.commit();
    wtx.close();
    wtx.close();

    final NodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    assertEquals(false, rtx.moveTo(14L).hasMoved());
    rtx.close();
    rtx.close();
    holder.getResourceManager().close();
  }

  @Test
  public void testAutoCommitWithNodeThreshold() {
    // After each bunch of 5 nodes commit.
    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeTrx(5)) {
      XdmDocumentCreator.create(wtx);
      wtx.commit();
      assertEquals(4, wtx.getRevisionNumber());
    }
  }

  @Test
  public void testAutoCommitWithScheduler() throws InterruptedException {
    // After 500 milliseconds commit.
    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeTrx(TimeUnit.MILLISECONDS, 500)) {
      TimeUnit.MILLISECONDS.sleep(1500);
      assertTrue(wtx.getRevisionNumber() >= 3);
    }
  }

  @Test
  public void testFetchingOfClosestRevisionToAGivenPointInTime() throws InterruptedException {
    final Instant start = Instant.now();
    final Instant afterAllCommits;
    final Instant afterFirstCommit;
    final Instant afterSecondCommit;
    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeTrx(TimeUnit.MILLISECONDS, 1000)) {
      TimeUnit.MILLISECONDS.sleep(1100);
      afterFirstCommit = Instant.now();
      TimeUnit.MILLISECONDS.sleep(1100);
      afterSecondCommit = Instant.now();
      TimeUnit.MILLISECONDS.sleep(1100);
      assertTrue(wtx.getRevisionNumber() >= 3);
      afterAllCommits = Instant.now();
    }

    try (final XdmNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(start)) {
      assertEquals(0, rtx.getRevisionNumber());
    }

    try (final XdmNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(afterFirstCommit)) {
      assertEquals(1, rtx.getRevisionNumber());
    }

    try (final XdmNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(afterSecondCommit)) {
      assertEquals(2, rtx.getRevisionNumber());
    }

    try (final XdmNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx(afterAllCommits)) {
      assertEquals(holder.getResourceManager().getMostRecentRevisionNumber(), rtx.getRevisionNumber());
    }
  }
}
