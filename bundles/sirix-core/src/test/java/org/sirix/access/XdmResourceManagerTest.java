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
import org.sirix.TestHelper;
import org.sirix.TestHelper.PATHS;
import org.sirix.api.Database;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.api.xdm.XdmNodeWriteTrx;
import org.sirix.api.xdm.XdmResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.node.Kind;
import org.sirix.settings.Constants;
import org.sirix.utils.DocumentCreator;

public class XdmResourceManagerTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void testSingleton() {
    final Database database = Holder.openResourceManager().getDatabase();
    assertEquals(database, holder.getDatabase());
    final XdmResourceManager manager = database.getXdmResourceManager(TestHelper.RESOURCE);
    assertEquals(manager, holder.getResourceManager());
    manager.close();
    final XdmResourceManager manager2 = database.getXdmResourceManager(TestHelper.RESOURCE);
    assertNotSame(manager2, holder.getResourceManager());
    database.close();
  }

  @Test
  public void testClosed() {
    final XdmNodeReadTrx rtx = holder.getNodeReadTrx();
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
    final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    final Database database2 = TestHelper.getDatabase(PATHS.PATH1.getFile());
    assertTrue(database == database2);
  }

  @Test
  public void testInsertChild() {
    try (final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx()) {
      DocumentCreator.create(wtx);
      assertNotNull(wtx.moveToDocumentRoot());
      assertEquals(Kind.DOCUMENT, wtx.getKind());

      assertNotNull(wtx.moveToFirstChild());
      assertEquals(Kind.ELEMENT, wtx.getKind());
      assertEquals("p:a",
          new StringBuilder(wtx.getName().getPrefix()).append(":").append(wtx.getName().getLocalName()).toString());

      wtx.rollback();
    }
  }

  @Test
  public void testRevision() {
    XdmNodeReadTrx rtx = holder.getNodeReadTrx();
    assertEquals(0L, rtx.getRevisionNumber());

    try (final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx()) {
      assertEquals(1L, wtx.getRevisionNumber());

      // Commit and check.
      wtx.commit();
    }

    try {
      rtx = holder.getResourceManager().beginNodeReadTrx(Constants.UBP_ROOT_REVISION_NUMBER);

      assertEquals(Constants.UBP_ROOT_REVISION_NUMBER, rtx.getRevisionNumber());
    } finally {
      rtx.close();
    }

    try (final XdmNodeReadTrx rtx2 = holder.getResourceManager().beginNodeReadTrx()) {
      assertEquals(1L, rtx2.getRevisionNumber());
    }
  }

  @Test
  public void testShreddedRevision() {
    try (final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx()) {
      DocumentCreator.create(wtx);
      assertEquals(1L, wtx.getRevisionNumber());
      wtx.commit();
    }

    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx()) {
      assertEquals(1L, rtx.getRevisionNumber());
      rtx.moveTo(12L);
      assertEquals("bar", rtx.getValue());

      try (final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx()) {
        assertEquals(2L, wtx.getRevisionNumber());
        wtx.moveTo(12L);
        wtx.setValue("bar2");

        assertEquals("bar", rtx.getValue());
        assertEquals("bar2", wtx.getValue());
        wtx.rollback();
      }
    }

    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx()) {
      assertEquals(1L, rtx.getRevisionNumber());
      rtx.moveTo(12L);
      assertEquals("bar", rtx.getValue());
    }
  }

  @Test
  public void testExisting() {
    final Database database = TestHelper.getDatabase(PATHS.PATH1.getFile());
    final XdmResourceManager resource = database.getXdmResourceManager(TestHelper.RESOURCE);

    final XdmNodeWriteTrx wtx1 = resource.beginNodeWriteTrx();
    DocumentCreator.create(wtx1);
    assertEquals(1L, wtx1.getRevisionNumber());
    wtx1.commit();
    wtx1.close();
    resource.close();

    final XdmResourceManager resource2 = database.getXdmResourceManager(TestHelper.RESOURCE);
    final XdmNodeReadTrx rtx1 = resource2.beginNodeReadTrx();
    assertEquals(1L, rtx1.getRevisionNumber());
    rtx1.moveTo(12L);
    assertEquals("bar", rtx1.getValue());

    final XdmNodeWriteTrx wtx2 = resource2.beginNodeWriteTrx();
    assertEquals(2L, wtx2.getRevisionNumber());
    wtx2.moveTo(12L);
    wtx2.setValue("bar2");

    assertEquals("bar", rtx1.getValue());
    assertEquals("bar2", wtx2.getValue());

    rtx1.close();
    wtx2.commit();
    wtx2.close();

    final Database database2 = TestHelper.getDatabase(PATHS.PATH1.getFile());
    final XdmResourceManager resource3 = database2.getXdmResourceManager(TestHelper.RESOURCE);
    final XdmNodeReadTrx rtx2 = resource3.beginNodeReadTrx();
    assertEquals(2L, rtx2.getRevisionNumber());
    rtx2.moveTo(12L);
    assertEquals("bar2", rtx2.getValue());

    rtx2.close();
    resource3.close();
  }

  @Test
  public void testIdempotentClose() {
    final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx();
    DocumentCreator.create(wtx);
    wtx.commit();
    wtx.close();
    wtx.close();

    final NodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx();
    assertEquals(false, rtx.moveTo(14L).hasMoved());
    rtx.close();
    rtx.close();
    holder.getResourceManager().close();
  }

  @Test
  public void testAutoCommitWithNodeThreshold() {
    // After each bunch of 5 nodes commit.
    try (final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx(5)) {
      DocumentCreator.create(wtx);
      wtx.commit();
      assertEquals(4, wtx.getRevisionNumber());
    }
  }

  @Test
  public void testAutoCommitWithScheduler() throws InterruptedException {
    // After 500 milliseconds commit.
    try (final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx(TimeUnit.MILLISECONDS, 500)) {
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
    try (final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx(TimeUnit.MILLISECONDS, 1000)) {
      TimeUnit.MILLISECONDS.sleep(1100);
      afterFirstCommit = Instant.now();
      TimeUnit.MILLISECONDS.sleep(1100);
      afterSecondCommit = Instant.now();
      TimeUnit.MILLISECONDS.sleep(1100);
      assertTrue(wtx.getRevisionNumber() >= 3);
      afterAllCommits = Instant.now();
    }

    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx(start)) {
      assertEquals(0, rtx.getRevisionNumber());
    }

    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx(afterFirstCommit)) {
      assertEquals(1, rtx.getRevisionNumber());
    }

    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx(afterSecondCommit)) {
      assertEquals(2, rtx.getRevisionNumber());
    }

    try (final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx(afterAllCommits)) {
      assertEquals(holder.getResourceManager().getMostRecentRevisionNumber(), rtx.getRevisionNumber());
    }
  }
}
