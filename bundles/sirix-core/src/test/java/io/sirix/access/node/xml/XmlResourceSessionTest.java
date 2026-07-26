/*
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

import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.node.NodeKind;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.XmlTestHelper.PATHS;
import io.sirix.exception.SirixException;
import io.sirix.settings.Constants;
import io.sirix.utils.XmlDocumentCreator;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class XmlResourceSessionTest {

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testSingleton() {
    try (final var database = Holder.openResourceSession().getDatabase()) {
      assertEquals(database, holder.getDatabase());

      try (final XmlResourceSession manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
        assertEquals(manager, holder.getResourceSession());
      }

      try (final XmlResourceSession manager2 = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
        assertNotSame(manager2, holder.getResourceSession());
      }
    }
  }

  @Test
  public void testClosed() {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
    rtx.close();

    try {
      rtx.getAttributeCount();
      fail();
    } catch (final IllegalStateException e) {
      // assertNotClosed() throws IllegalStateException when transaction is closed.
    } finally {
      holder.getResourceSession().close();
    }
  }

  @Test
  public void testNonExisting() {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    final var database2 = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    assertSame(database, database2);
  }

  @Test
  public void testInsertChild() {
    try (final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx()) {
      XmlDocumentCreator.create(wtx);
      wtx.moveToDocumentRoot();
      Assert.assertEquals(NodeKind.XML_DOCUMENT, wtx.getKind());

      wtx.moveToFirstChild();
      Assert.assertEquals(NodeKind.ELEMENT, wtx.getKind());
      assertEquals("p:a", wtx.getName().getPrefix() + ":" + wtx.getName().getLocalName());

      wtx.rollback();
    }
  }

  @Test
  public void testRevision() {
    XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
    Assert.assertEquals(0L, rtx.getRevisionNumber());

    try (final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx()) {
      Assert.assertEquals(1L, wtx.getRevisionNumber());

      // Commit and check.
      wtx.commit();
    }

    try {
      rtx = holder.getResourceSession().beginNodeReadOnlyTrx(Constants.UBP_ROOT_REVISION_NUMBER);

      assertEquals(Constants.UBP_ROOT_REVISION_NUMBER, rtx.getRevisionNumber());
    } finally {
      rtx.close();
    }

    try (final XmlNodeReadOnlyTrx rtx2 = holder.getResourceSession().beginNodeReadOnlyTrx()) {
      Assert.assertEquals(1L, rtx2.getRevisionNumber());
    }
  }

  @Test
  public void testShreddedRevision() {
    try (final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx()) {
      XmlDocumentCreator.create(wtx);
      Assert.assertEquals(1L, wtx.getRevisionNumber());
      wtx.commit();
    }

    try (final XmlNodeReadOnlyTrx rtx = holder.getResourceSession().beginNodeReadOnlyTrx()) {
      Assert.assertEquals(1L, rtx.getRevisionNumber());
      rtx.moveTo(12L);
      assertEquals("bar", rtx.getValue());

      try (final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx()) {
        Assert.assertEquals(2L, wtx.getRevisionNumber());
        wtx.moveTo(12L);
        wtx.setValue("bar2");

        assertEquals("bar", rtx.getValue());
        assertEquals("bar2", wtx.getValue());
        wtx.rollback();
      }
    }

    try (final XmlNodeReadOnlyTrx rtx = holder.getResourceSession().beginNodeReadOnlyTrx()) {
      Assert.assertEquals(1L, rtx.getRevisionNumber());
      rtx.moveTo(12L);
      assertEquals("bar", rtx.getValue());
    }
  }

  @Test
  public void testExisting() {
    final var database = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    final XmlResourceSession resource = database.beginResourceSession(XmlTestHelper.RESOURCE);

    final XmlNodeTrx wtx1 = resource.beginNodeTrx();
    XmlDocumentCreator.create(wtx1);
    Assert.assertEquals(1L, wtx1.getRevisionNumber());
    wtx1.commit();
    wtx1.close();
    resource.close();

    final XmlResourceSession resource2 = database.beginResourceSession(XmlTestHelper.RESOURCE);
    final XmlNodeReadOnlyTrx rtx1 = resource2.beginNodeReadOnlyTrx();
    Assert.assertEquals(1L, rtx1.getRevisionNumber());
    rtx1.moveTo(12L);
    assertEquals("bar", rtx1.getValue());

    final XmlNodeTrx wtx2 = resource2.beginNodeTrx();
    Assert.assertEquals(2L, wtx2.getRevisionNumber());
    wtx2.moveTo(12L);
    wtx2.setValue("bar2");

    assertEquals("bar", rtx1.getValue());
    assertEquals("bar2", wtx2.getValue());

    rtx1.close();
    wtx2.commit();
    wtx2.close();

    final var database2 = XmlTestHelper.getDatabase(PATHS.PATH1.getFile());
    final XmlResourceSession resource3 = database2.beginResourceSession(XmlTestHelper.RESOURCE);
    final XmlNodeReadOnlyTrx rtx2 = resource3.beginNodeReadOnlyTrx();
    Assert.assertEquals(2L, rtx2.getRevisionNumber());
    rtx2.moveTo(12L);
    assertEquals("bar2", rtx2.getValue());

    rtx2.close();
    resource3.close();
  }

  @Test
  public void testIdempotentClose() {
    final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.close();
    wtx.close();

    final NodeReadOnlyTrx rtx = holder.getResourceSession().beginNodeReadOnlyTrx();
    assertFalse(rtx.moveTo(14L));
    rtx.close();
    rtx.close();
    holder.getResourceSession().close();
  }

  @Test
  public void testAutoCommitWithNodeThreshold() {
    // After each bunch of 5 nodes commit.
    try (final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx(5)) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();
      Assert.assertEquals(4, wtx.getRevisionNumber());
    }
  }

  @Test
  public void testAutoCommitWithScheduler() throws InterruptedException {
    // After 500 milliseconds commit.
    final XmlResourceSession session = holder.getResourceSession();
    try (final XmlNodeTrx wtx = session.beginNodeTrx(500, TimeUnit.MILLISECONDS)) {
      // The timer fires on a background thread; sleeping a fixed 1500ms and expecting two
      // commits already happened is a wall-clock race that flaked on loaded runners. Poll the
      // session's committed revision instead and only assert that the scheduler commits
      // repeatedly — bounded so a wedged timer fails loudly instead of hanging the test.
      final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
      while (session.getMostRecentRevisionNumber() < 2) {
        if (System.nanoTime() - deadlineNanos > 0) {
          fail("Auto-commit scheduler did not commit two revisions within 60s (still at "
              + session.getMostRecentRevisionNumber() + ")");
        }
        TimeUnit.MILLISECONDS.sleep(10);
      }
      // The transaction's own view advances a beat later (the timer thread is still inside
      // commitInternal between publishing the committed uber page and re-instantiating the
      // transaction) — give it the same bounded grace instead of asserting the instant value.
      while (wtx.getRevisionNumber() < 3) {
        if (System.nanoTime() - deadlineNanos > 0) {
          break;
        }
        TimeUnit.MILLISECONDS.sleep(10);
      }
      assertTrue(wtx.getRevisionNumber() >= 3);
    }
  }

  @Test
  public void testFetchingRevisionValidAtPointInTime() throws InterruptedException {
    final Instant start = Instant.now();
    final Instant afterAllCommits;
    final Instant afterFirstCommit;
    final Instant afterSecondCommit;
    final XmlResourceSession session = holder.getResourceSession();
    try (final XmlNodeTrx wtx = session.beginNodeTrx(2000, TimeUnit.MILLISECONDS)) {
      // The auto-commit timer fires every 2000ms on a background thread. Sleeping a fixed
      // 2100ms and assuming the commit already happened is a wall-clock race — on a loaded
      // machine the timer fires late and the point-in-time lookups below see one revision
      // less than expected. Poll the session's committed revision instead (the transaction's
      // own getRevisionNumber() races the post-commit reader swap): once it advances, the
      // commit is durable and its timestamp lies before the captured instant, while the NEXT
      // commit is a full timer period away.
      awaitCommittedRevision(session, 1);
      afterFirstCommit = Instant.now();
      awaitCommittedRevision(session, 2);
      afterSecondCommit = Instant.now();
      awaitCommittedRevision(session, 3);
      afterAllCommits = Instant.now();
    }
    assertTrue(session.getMostRecentRevisionNumber() >= 3);

    try (final XmlNodeReadOnlyTrx rtx = holder.getResourceSession().beginNodeReadOnlyTrx(start)) {
      Assert.assertEquals(0, rtx.getRevisionNumber());
    }

    try (final XmlNodeReadOnlyTrx rtx = holder.getResourceSession().beginNodeReadOnlyTrx(afterFirstCommit)) {
      Assert.assertEquals(1, rtx.getRevisionNumber());
    }

    try (final XmlNodeReadOnlyTrx rtx = holder.getResourceSession().beginNodeReadOnlyTrx(afterSecondCommit)) {
      Assert.assertEquals(2, rtx.getRevisionNumber());
    }

    try (final XmlNodeReadOnlyTrx rtx = holder.getResourceSession().beginNodeReadOnlyTrx(afterAllCommits)) {
      assertEquals(holder.getResourceSession().getMostRecentRevisionNumber(), rtx.getRevisionNumber());
    }
  }

  /**
   * Wait until the auto-commit timer has durably committed the given revision (observed via
   * the session, which is safe to read while the transaction's internals are swapped after a
   * commit). Bounded so a wedged timer fails loudly instead of hanging the test.
   */
  private static void awaitCommittedRevision(final XmlResourceSession session, final int expectedCommittedRevision)
      throws InterruptedException {
    final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (session.getMostRecentRevisionNumber() < expectedCommittedRevision) {
      if (System.nanoTime() - deadlineNanos > 0) {
        fail("Auto-commit timer did not commit revision " + expectedCommittedRevision + " within 60s (still at "
            + session.getMostRecentRevisionNumber() + ")");
      }
      TimeUnit.MILLISECONDS.sleep(10);
    }
  }
}
