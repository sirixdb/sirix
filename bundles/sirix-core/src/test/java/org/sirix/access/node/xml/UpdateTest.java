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

package org.sirix.access.node.xml;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.access.Databases;
import org.sirix.access.User;
import org.sirix.access.trx.node.xml.XmlNodeReadOnlyTrxImpl;
import org.sirix.api.Axis;
import org.sirix.api.Movement;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlNodeTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.IncludeSelf;
import org.sirix.axis.NonStructuralWrapperAxis;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixUsageException;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.service.xml.shredder.XmlShredder;
import org.sirix.settings.Constants;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;
import org.sirix.utils.XmlDocumentCreator;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;

/** Test update operations. */
public class UpdateTest {

  /** {@link Holder} reference. */
  private Holder holder;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    holder = Holder.generateDeweyIDResourceMgr();
  }

  @After
  public void tearDown() {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testUserNamePersistence() {
    final var user = new User("Johannes Lichtenberger", UUID.randomUUID());
    try (final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      XmlDocumentCreator.create(wtx);
      assertEquals(Optional.of(user), wtx.getUser());
      wtx.commit();
      assertEquals(Optional.of(user), wtx.getUser());

      try (final var rtx = manager.beginNodeReadOnlyTrx()) {
        assertEquals(Optional.of(user), rtx.getUser());
      }
    }
  }

  @Test
  public void testUserNameRetrievalWhenReverting() {
    final var user = new User("Johannes Lichtenberger", UUID.randomUUID());

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      XmlDocumentCreator.createVersioned(wtx);
      assertEquals(Optional.of(user), wtx.getUser());
      wtx.commit();
      assertEquals(Optional.of(user), wtx.getUser());
    }

    final var newUser = new User("Marc Kramis", UUID.randomUUID());

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), newUser);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      assertEquals(Optional.of(user), wtx.getUserOfRevisionToRepresent());
      assertEquals(Optional.of(newUser), wtx.getUser());
      wtx.revertTo(1);
      assertEquals(Optional.of(user), wtx.getUserOfRevisionToRepresent());
      assertEquals(Optional.of(newUser), wtx.getUser());
    }
  }

  @Test
  public void testGettingHistory() {
    final var user = new User("Johannes Lichtenberger", UUID.randomUUID());
    try (final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      XmlDocumentCreator.createVersioned(wtx);
    }

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE)) {
      final var history = manager.getHistory();

      assertEquals(3, history.size());

      assertEquals(3, history.get(0).getRevision());
      assertNotNull(history.get(0).getRevisionTimestamp());
      assertTrue(history.get(0).getCommitMessage().isEmpty());
      assertEquals("Johannes Lichtenberger", history.get(0).getUser().getName());

      assertEquals(2, history.get(1).getRevision());
      assertNotNull(history.get(1).getRevisionTimestamp());
      assertTrue(history.get(1).getCommitMessage().isEmpty());
      assertEquals("Johannes Lichtenberger", history.get(1).getUser().getName());

      assertEquals(1, history.get(2).getRevision());
      assertNotNull(history.get(2).getRevisionTimestamp());
      assertTrue(history.get(2).getCommitMessage().isEmpty());
      assertEquals("Johannes Lichtenberger", history.get(2).getUser().getName());
    }
  }

  @Test
  public void testGettingHistoryWithCommitMessageAndDifferentUser() {
    final var user = setupCommitHistoryTest();

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE)) {
      final var history = manager.getHistory();

      assertEquals(3, history.size());

      assertEquals(3, history.get(0).getRevision());
      assertNotNull(history.get(0).getRevisionTimestamp());
      assertEquals("Insert a second element and text node", history.get(0).getCommitMessage().get());
      assertEquals("Marc Kramis", history.get(0).getUser().getName());

      assertEquals(2, history.get(1).getRevision());
      assertNotNull(history.get(1).getRevisionTimestamp());
      assertEquals("Insert element and text nodes", history.get(1).getCommitMessage().get());
      assertEquals("Johannes Lichtenberger", history.get(1).getUser().getName());

      assertEquals(1, history.get(2).getRevision());
      assertNotNull(history.get(2).getRevisionTimestamp());
      assertTrue(history.get(2).getCommitMessage().isEmpty());
      assertEquals("Johannes Lichtenberger", history.get(2).getUser().getName());
    }
  }

  @Test
  public void testGettingHistoryWithPaging() {
    final var user = setupCommitHistoryTest();

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE)) {
      final var history = manager.getHistory(3, 1);

      assertEquals(3, history.size());

      assertEquals(3, history.get(0).getRevision());
      assertNotNull(history.get(0).getRevisionTimestamp());
      assertEquals("Insert a second element and text node", history.get(0).getCommitMessage().get());
      assertEquals("Marc Kramis", history.get(0).getUser().getName());

      assertEquals(2, history.get(1).getRevision());
      assertNotNull(history.get(1).getRevisionTimestamp());
      assertEquals("Insert element and text nodes", history.get(1).getCommitMessage().get());
      assertEquals("Johannes Lichtenberger", history.get(1).getUser().getName());

      assertEquals(1, history.get(2).getRevision());
      assertNotNull(history.get(2).getRevisionTimestamp());
      assertTrue(history.get(2).getCommitMessage().isEmpty());
      assertEquals("Johannes Lichtenberger", history.get(2).getUser().getName());
    }
  }

  @Test
  public void testGettingHistoryWithNumberOfRevisionsToRetrieve() {
    final var user = setupCommitHistoryTest();

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE)) {
      final var history = manager.getHistory(2);

      assertEquals(2, history.size());

      assertEquals(3, history.get(0).getRevision());
      assertNotNull(history.get(0).getRevisionTimestamp());
      assertEquals("Insert a second element and text node", history.get(0).getCommitMessage().get());
      assertEquals("Marc Kramis", history.get(0).getUser().getName());

      assertEquals(2, history.get(1).getRevision());
      assertNotNull(history.get(1).getRevisionTimestamp());
      assertEquals("Insert element and text nodes", history.get(1).getCommitMessage().get());
      assertEquals("Johannes Lichtenberger", history.get(1).getUser().getName());
    }
  }

  private User setupCommitHistoryTest() {
    final var user = new User("Johannes Lichtenberger", UUID.randomUUID());
    try (final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      XmlDocumentCreator.create(wtx);
      wtx.commit();
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertElementAsFirstChild(new QNm("ns", "p", "a"));
      wtx.insertTextAsFirstChild("OOPS4!");
      wtx.commit("Insert element and text nodes");
    }

    final var newUser = new User("Marc Kramis", UUID.randomUUID());

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), newUser);
        final var manager = database.openResourceManager(XmlTestHelper.RESOURCE);
        final var wtx = manager.beginNodeTrx()) {
      wtx.moveToFirstChild();
      wtx.insertElementAsFirstChild(new QNm("ns", "p", "a"));
      wtx.insertTextAsFirstChild("OOPS4!");
      wtx.commit("Insert a second element and text node");
    }
    return user;
  }

  @Test
  public void testDelete() {
    try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
      XmlDocumentCreator.create(wtx);
      wtx.moveTo(4);
      wtx.insertElementAsRightSibling(new QNm("blabla"));
      wtx.moveTo(5);
      wtx.remove();
      assertEquals(8, wtx.getNodeKey());
      wtx.moveTo(4);
      testDelete(wtx);
      wtx.commit();
      testDelete(wtx);
      wtx.close();
      try (final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx()) {
        testDelete(rtx);
      }
    }
  }

  private static void testDelete(final XmlNodeReadOnlyTrx rtx) {
    assertFalse(rtx.moveTo(5).hasMoved());
    assertTrue(rtx.moveTo(1).hasMoved());
    assertEquals(5, rtx.getChildCount());
    assertEquals(7, rtx.getDescendantCount());
    assertTrue(rtx.moveToDocumentRoot().hasMoved());
    assertEquals(8, rtx.getDescendantCount());
  }

  @Test
  public void testInsert() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    testInsert(wtx);
    wtx.commit();
    testInsert(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testInsert(rtx);
    rtx.close();
  }

  private static void testInsert(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot().hasMoved());
    assertEquals(10, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(1).hasMoved());
    assertEquals(9, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4).hasMoved());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(2, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(2, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(0, rtx.getDescendantCount());
  }

  @Test
  public void testNodeTransactionIsolation() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    wtx.insertElementAsFirstChild(new QNm(""));
    testNodeTransactionIsolation(wtx);
    wtx.commit();
    testNodeTransactionIsolation(wtx);
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testNodeTransactionIsolation(rtx);
    wtx.moveToFirstChild();
    wtx.insertElementAsFirstChild(new QNm(""));
    testNodeTransactionIsolation(rtx);
    wtx.commit();
    testNodeTransactionIsolation(rtx);
    rtx.close();
    wtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testNodeTransactionIsolation()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testNodeTransactionIsolation(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot().hasMoved());
    assertEquals(0, rtx.getNodeKey());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(1, rtx.getNodeKey());
    assertEquals(0, rtx.getChildCount());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getRightSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getFirstChildKey());
  }

  /** Test NamePage. */
  @Test
  public void testNamePage() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.moveTo(7);
    wtx.remove();
    wtx.moveTo(11);
    wtx.remove();
    wtx.moveTo(5);
    wtx.commit();
    wtx.close();
    XmlNodeReadOnlyTrxImpl rtx = (XmlNodeReadOnlyTrxImpl) holder.getResourceManager().beginNodeReadOnlyTrx(1);
    assertEquals(1, rtx.getRevisionNumber());
    assertTrue(rtx.moveTo(7).hasMoved());
    assertEquals("c", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(11).hasMoved());
    assertEquals("c", rtx.getName().getLocalName());
    rtx = (XmlNodeReadOnlyTrxImpl) holder.getResourceManager().beginNodeReadOnlyTrx();
    assertEquals(2, rtx.getRevisionNumber());
    assertEquals(null, rtx.getPageTransaction().getName(NamePageHash.generateHashForString("c"), NodeKind.ELEMENT));
    assertEquals(0, rtx.getNameCount("blablabla", NodeKind.ATTRIBUTE));
    rtx.moveTo(5);
    assertEquals(2, rtx.getNameCount("b", NodeKind.ELEMENT));
    rtx.close();
  }

  /**
   * Test update of text value in case two adjacent text nodes would be the result of an insert.
   */
  @Test
  public void testInsertAsFirstChildUpdateText() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.moveTo(1L);
    wtx.insertTextAsFirstChild("foo");
    wtx.commit();
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    assertTrue(rtx.moveTo(1L).hasMoved());
    assertEquals(4L, rtx.getFirstChildKey());
    assertEquals(5L, rtx.getChildCount());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4L).hasMoved());
    assertEquals("foooops1", rtx.getValue());
    rtx.close();
  }

  /**
   * Test update of text value in case two adjacent text nodes would be the result of an insert.
   */
  @Test
  public void testInsertAsRightSiblingUpdateTextFirst() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.moveTo(4L);
    wtx.insertTextAsRightSibling("foo");
    wtx.commit();
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    assertTrue(rtx.moveTo(1L).hasMoved());
    assertEquals(4L, rtx.getFirstChildKey());
    assertEquals(5L, rtx.getChildCount());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4L).hasMoved());
    assertEquals("oops1foo", rtx.getValue());
    rtx.close();
  }

  /**
   * Test update of text value in case two adjacent text nodes would be the result of an insert.
   */
  @Test
  public void testInsertAsRightSiblingUpdateTextSecond() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.moveTo(5L);
    wtx.insertTextAsRightSibling("foo");
    wtx.commit();
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    assertTrue(rtx.moveTo(1L).hasMoved());
    assertEquals(4L, rtx.getFirstChildKey());
    assertEquals(5L, rtx.getChildCount());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(8L).hasMoved());
    assertEquals("foooops2", rtx.getValue());
    rtx.close();
  }

  /** Ordinary remove test. */
  @Test
  public void testRemoveDescendantFirst() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.moveTo(4L);
    wtx.remove();
    wtx.commit();
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    assertEquals(0, rtx.getNodeKey());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(1, rtx.getNodeKey());
    assertEquals(5, rtx.getFirstChildKey());
    assertEquals(4, rtx.getChildCount());
    assertEquals(8, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(5, rtx.getNodeKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(8, rtx.getNodeKey());
    assertEquals(5, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(9, rtx.getNodeKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(13, rtx.getNodeKey());
    rtx.close();
  }

  @Test
  public void testInsertChild() {
    XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    wtx.insertElementAsFirstChild(new QNm("foo"));
    wtx.commit();
    wtx.close();

    XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    assertEquals(1L, rtx.getRevisionNumber());
    rtx.close();

    // Insert 100 children.
    for (int i = 1; i <= 50; i++) {
      wtx = holder.getResourceManager().beginNodeTrx();
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertElementAsFirstChild(new QNm("bar"));
      wtx.insertTextAsRightSibling(Integer.toString(i));
      wtx.commit();
      wtx.close();

      rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      rtx.moveToFirstChild();
      rtx.moveToRightSibling();
      assertEquals(Integer.toString(i), rtx.getValue());
      assertEquals(i + 1, rtx.getRevisionNumber());
      rtx.close();
    }

    rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveToDocumentRoot();
    rtx.moveToFirstChild();
    rtx.moveToFirstChild();
    rtx.moveToRightSibling();
    assertEquals("50", rtx.getValue());
    assertEquals(51L, rtx.getRevisionNumber());
    rtx.close();
  }

  @Test
  public void testInsertPath() {
    XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    wtx.commit();
    wtx.close();

    wtx = holder.getResourceManager().beginNodeTrx();
    assertTrue(wtx.moveToDocumentRoot().hasMoved());
    assertEquals(1L, wtx.insertElementAsFirstChild(new QNm("")).getNodeKey());
    assertEquals(2L, wtx.insertElementAsFirstChild(new QNm("")).getNodeKey());
    assertEquals(3L, wtx.insertElementAsFirstChild(new QNm("")).getNodeKey());
    assertTrue(wtx.moveToParent().hasMoved());
    assertEquals(4L, wtx.insertElementAsRightSibling(new QNm("")).getNodeKey());
    wtx.commit();
    wtx.close();

    final XmlNodeTrx wtx2 = holder.getResourceManager().beginNodeTrx();
    assertTrue(wtx2.moveToDocumentRoot().hasMoved());
    assertTrue(wtx2.moveToFirstChild().hasMoved());
    assertEquals(5L, wtx2.insertElementAsFirstChild(new QNm("")).getNodeKey());
    wtx2.commit();
    wtx2.close();
  }

  @Test
  public void testPageBoundary() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();

    // Document root.
    wtx.insertElementAsFirstChild(new QNm(""));
    wtx.insertElementAsFirstChild(new QNm(""));
    for (int i = 0; i < Constants.NDP_NODE_COUNT << 1 + 1; i++) {
      wtx.insertElementAsRightSibling(new QNm(""));
    }

    testPageBoundary(wtx);
    wtx.commit();
    testPageBoundary(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testPageBoundary(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testPageBoundary()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testPageBoundary(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(2L).hasMoved());
    assertEquals(2L, rtx.getNodeKey());
  }

  @Test(expected = SirixUsageException.class)
  public void testRemoveDocument() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveToDocumentRoot();
    try {
      wtx.remove();
    } finally {
      wtx.rollback();
      wtx.close();
    }
  }

  /** Test for text concatenation. */
  @Test
  public void testRemoveDescendant() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(0L);
    // assertEquals(10L, wtx.getDescendantCount());
    wtx.commit();
    assertEquals(10L, wtx.getDescendantCount());
    wtx.moveTo(5L);
    wtx.remove();
    testRemoveDescendant(wtx);
    wtx.commit();
    testRemoveDescendant(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testRemoveDescendant(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testRemoveDescendant()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testRemoveDescendant(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot().hasMoved());
    assertEquals(0, rtx.getNodeKey());
    assertEquals(6, rtx.getDescendantCount());
    assertEquals(1, rtx.getChildCount());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(1, rtx.getNodeKey());
    assertEquals(3, rtx.getChildCount());
    assertEquals(5, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(4, rtx.getNodeKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(9, rtx.getNodeKey());
    assertEquals(4, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(13, rtx.getNodeKey());
  }

  /** Test for text concatenation. */
  @Test
  public void testRemoveDescendantTextConcat2() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.moveTo(9L);
    wtx.remove();
    wtx.moveTo(5L);
    wtx.remove();
    testRemoveDescendantTextConcat2(wtx);
    wtx.commit();
    testRemoveDescendantTextConcat2(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testRemoveDescendantTextConcat2(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testRemoveDescendant()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testRemoveDescendantTextConcat2(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot().hasMoved());
    assertEquals(0, rtx.getNodeKey());
    assertEquals(2, rtx.getDescendantCount());
    assertEquals(1, rtx.getChildCount());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(1, rtx.getNodeKey());
    assertEquals(1, rtx.getChildCount());
    assertEquals(1, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(4, rtx.getNodeKey());
    assertFalse(rtx.moveToRightSibling().hasMoved());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getRightSiblingKey());
  }

  @Test
  public void testReplaceElementWithTwoSiblingTextNodesWithTextNode() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveTo(12);
    wtx.moveTo(5);
    wtx.replaceNode(rtx);
    testReplaceElementWithTextNode(wtx);
    wtx.commit();
    testReplaceElementWithTextNode(wtx);
    wtx.close();
    rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testReplaceElementWithTextNode(rtx);
    rtx.close();
  }

  private void testReplaceElementWithTextNode(final XmlNodeReadOnlyTrx rtx) {
    assertFalse(rtx.moveTo(5).hasMoved());
    assertTrue(rtx.moveTo(4).hasMoved());
    assertEquals("oops1baroops2", rtx.getValue());
    assertEquals(9, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(4, rtx.getLeftSiblingKey());
    assertEquals(13, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(9, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(1).hasMoved());
    assertEquals(3, rtx.getChildCount());
    assertEquals(5, rtx.getDescendantCount());
  }

  @Test
  public void testReplaceTextNodeWithTextNode() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveTo(12);
    wtx.moveTo(4);
    wtx.replaceNode(rtx);
    testReplaceTextNode(wtx);
    wtx.commit();
    testReplaceTextNode(wtx);
    wtx.close();
    rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testReplaceTextNode(rtx);
    rtx.close();
  }

  private void testReplaceTextNode(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(14).hasMoved());
    assertEquals("bar", rtx.getValue());
    assertEquals(5, rtx.getRightSiblingKey());
  }

  @Test
  public void testReplaceElementNode() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveTo(11);
    wtx.moveTo(5);
    wtx.replaceNode(rtx);
    testReplaceElementNode(wtx);
    wtx.commit();
    testReplaceElementNode(wtx);
    wtx.close();
    rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testReplaceElementNode(rtx);
    rtx.close();
  }

  private void testReplaceElementNode(final XmlNodeReadOnlyTrx rtx) {
    assertFalse(rtx.moveTo(5).hasMoved());
    assertTrue(rtx.moveTo(4).hasMoved());
    assertEquals(14, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(4, rtx.getLeftSiblingKey());
    assertEquals(8, rtx.getRightSiblingKey());
    assertEquals("c", rtx.getName().getLocalName());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(14, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(1).hasMoved());
    assertEquals(5, rtx.getChildCount());
    assertEquals(7, rtx.getDescendantCount());
  }

  @Test
  public void testReplaceElement() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(5);
    wtx.replaceNode(XmlShredder.createStringReader("<d>foobar</d>"));
    testReplaceElement(wtx);
    wtx.commit();
    testReplaceElement(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testReplaceElement(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveToFirstChild()}.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testReplaceElement(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(14).hasMoved());
    assertEquals("d", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(4).hasMoved());
    assertEquals(14, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(8, rtx.getRightSiblingKey());
    assertEquals(1, rtx.getChildCount());
    assertEquals(1, rtx.getDescendantCount());
    assertEquals(15, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(15).hasMoved());
    assertEquals(0, rtx.getChildCount());
    assertEquals(0, rtx.getDescendantCount());
    assertEquals(14, rtx.getParentKey());
    assertTrue(rtx.moveTo(14).hasMoved());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(14, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(1).hasMoved());
    assertEquals(8, rtx.getDescendantCount());
  }

  // @Test
  // public void testReplaceElementMergeTextNodes() {
  // final XdmNodeWriteTrx wtx = holder.getResourceManager().beginNodeWriteTrx();
  // DocumentCreator.create(wtx);
  // wtx.moveTo(5);
  // wtx.replaceNode(XMLShredder.createStringReader("foo"));
  // testReplaceElementMergeTextNodes(wtx);
  // wtx.commit();
  // testReplaceElementMergeTextNodes(wtx);
  // wtx.close();
  // final XdmNodeReadTrx rtx = holder.getResourceManager().beginNodeReadTrx();
  // testReplaceElementMergeTextNodes(rtx);
  // rtx.close();
  // }
  //
  // /**
  // * Testmethod for {@link UpdateTest#testFirstMoveToFirstChild()}.
  // *
  // * @param rtx to test with
  // * @throws SirixException
  // */
  // private static void testReplaceElementMergeTextNodes(final XdmNodeReadTrx rtx) {
  // assertTrue(rtx.moveTo(4).hasMoved());
  // assertEquals("oops1foooops2", rtx.getValue());
  // assertEquals(9, rtx.getRightSiblingKey());
  // assertTrue(rtx.moveToRightSibling().hasMoved());
  // assertEquals(4, rtx.getLeftSiblingKey());
  // assertTrue(rtx.moveTo(1).hasMoved());
  // assertEquals(3, rtx.getChildCount());
  // assertEquals(5, rtx.getDescendantCount());
  // }

  @Test
  public void testFirstMoveToFirstChild() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(7);
    wtx.moveSubtreeToFirstChild(6);
    testFirstMoveToFirstChild(wtx);
    wtx.commit();
    testFirstMoveToFirstChild(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testFirstMoveToFirstChild(rtx);
    rtx.moveToDocumentRoot();
    final Builder<SirixDeweyID> builder = ImmutableSet.builder();
    final ImmutableSet<SirixDeweyID> ids = builder.add(new SirixDeweyID("1"))
                                                  .add(new SirixDeweyID("1.3"))
                                                  .add(new SirixDeweyID("1.3.0.3"))
                                                  .add(new SirixDeweyID("1.3.1.3"))
                                                  .add(new SirixDeweyID("1.3.3"))
                                                  .add(new SirixDeweyID("1.3.5"))
                                                  .add(new SirixDeweyID("1.3.5.5"))
                                                  .add(new SirixDeweyID("1.3.5.5.3"))
                                                  .add(new SirixDeweyID("1.3.7"))
                                                  .add(new SirixDeweyID("1.3.9"))
                                                  .add(new SirixDeweyID("1.3.9.1.3"))
                                                  .add(new SirixDeweyID("1.3.9.3"))
                                                  .add(new SirixDeweyID("1.3.9.5"))
                                                  .add(new SirixDeweyID("1.3.11"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  private void test(final Iterator<SirixDeweyID> ids, final Axis axis) {
    while (ids.hasNext()) {
      assertTrue(axis.hasNext());
      axis.next();
//      System.out.println(axis.asXdmNodeReadTrx().getDeweyID());
      assertEquals(ids.next(), axis.asXdmNodeReadTrx().getDeweyID());
    }
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveToFirstChild()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testFirstMoveToFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot().hasMoved());
    assertEquals(10L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4).hasMoved());
    assertEquals(rtx.getValue(), "oops1");
    assertTrue(rtx.moveTo(7).hasMoved());
    assertEquals(1, rtx.getChildCount());
    assertEquals(1, rtx.getDescendantCount());
    assertFalse(rtx.hasLeftSibling());
    assertTrue(rtx.hasFirstChild());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertFalse(rtx.hasFirstChild());
    assertFalse(rtx.hasLeftSibling());
    assertFalse(rtx.hasRightSibling());
    assertEquals("foo", rtx.getValue());
    assertTrue(rtx.moveTo(5).hasMoved());
    assertEquals(1, rtx.getChildCount());
    assertEquals(2, rtx.getDescendantCount());
  }

  @Test
  public void testSecondMoveToFirstChild() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(5);
    wtx.moveSubtreeToFirstChild(4);
    testSecondMoveToFirstChild(wtx);
    wtx.commit();
    testSecondMoveToFirstChild(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testSecondMoveToFirstChild(rtx);
    rtx.moveToDocumentRoot();
    final Builder<SirixDeweyID> builder = ImmutableSet.builder();
    final ImmutableSet<SirixDeweyID> ids = builder.add(new SirixDeweyID("1"))
                                                  .add(new SirixDeweyID("1.3"))
                                                  .add(new SirixDeweyID("1.3.0.3"))
                                                  .add(new SirixDeweyID("1.3.1.3"))
                                                  .add(new SirixDeweyID("1.3.5"))
                                                  .add(new SirixDeweyID("1.3.5.3"))
                                                  .add(new SirixDeweyID("1.3.5.5"))
                                                  .add(new SirixDeweyID("1.3.7"))
                                                  .add(new SirixDeweyID("1.3.9"))
                                                  .add(new SirixDeweyID("1.3.9.1.3"))
                                                  .add(new SirixDeweyID("1.3.9.3"))
                                                  .add(new SirixDeweyID("1.3.9.5"))
                                                  .add(new SirixDeweyID("1.3.11"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveToFirstChild()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testSecondMoveToFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(1).hasMoved());
    assertEquals(4L, rtx.getChildCount());
    assertEquals(8L, rtx.getDescendantCount());
    assertEquals(5L, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(5).hasMoved());
    assertEquals(2L, rtx.getChildCount());
    assertEquals(2L, rtx.getDescendantCount());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertEquals(4L, rtx.getFirstChildKey());
    assertFalse(rtx.moveTo(6).hasMoved());
    assertTrue(rtx.moveTo(4).hasMoved());
    assertEquals("oops1foo", rtx.getValue());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertEquals(5L, rtx.getParentKey());
    assertEquals(7L, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(7).hasMoved());
    assertEquals(4L, rtx.getLeftSiblingKey());
  }

  @Test
  public void testThirdMoveToFirstChild() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(5);
    wtx.moveSubtreeToFirstChild(11);
    testThirdMoveToFirstChild(wtx);
    wtx.commit();
    testThirdMoveToFirstChild(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testThirdMoveToFirstChild(rtx);
    rtx.moveToDocumentRoot();
    final Builder<SirixDeweyID> builder = ImmutableSet.<SirixDeweyID>builder();
    final ImmutableSet<SirixDeweyID> ids = builder.add(new SirixDeweyID("1"))
                                                  .add(new SirixDeweyID("1.3"))
                                                  .add(new SirixDeweyID("1.3.0.3"))
                                                  .add(new SirixDeweyID("1.3.1.3"))
                                                  .add(new SirixDeweyID("1.3.3"))
                                                  .add(new SirixDeweyID("1.3.5"))
                                                  .add(new SirixDeweyID("1.3.5.2.3"))
                                                  .add(new SirixDeweyID("1.3.5.3"))
                                                  .add(new SirixDeweyID("1.3.5.5"))
                                                  .add(new SirixDeweyID("1.3.7"))
                                                  .add(new SirixDeweyID("1.3.9"))
                                                  .add(new SirixDeweyID("1.3.9.1.3"))
                                                  .add(new SirixDeweyID("1.3.9.5"))
                                                  .add(new SirixDeweyID("1.3.11"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveToFirstChild()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testThirdMoveToFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(0).hasMoved());
    assertEquals(10L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(1).hasMoved());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(5).hasMoved());
    assertEquals(11L, rtx.getFirstChildKey());
    assertEquals(3L, rtx.getChildCount());
    assertEquals(3L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(11).hasMoved());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertEquals(5L, rtx.getParentKey());
    assertEquals(6L, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(6L).hasMoved());
    assertEquals(11L, rtx.getLeftSiblingKey());
    assertEquals(7L, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(9L).hasMoved());
    assertEquals(1L, rtx.getChildCount());
    assertEquals(1L, rtx.getDescendantCount());
  }

  @Test
  public void testFourthMoveToFirstChild() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(9);
    wtx.insertAttribute(new QNm("ns", "p", "hiphip"), "hurray");
    wtx.moveTo(11);
    wtx.insertAttribute(new QNm("ns", "p", "yes"), "yo", Movement.TOPARENT);
    wtx.insertTextAsFirstChild("foobarbazbaba");
    wtx.moveTo(7);
    wtx.moveSubtreeToFirstChild(9);
    testFourthMoveToFirstChild(wtx);
    wtx.commit();
    testFourthMoveToFirstChild(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testFourthMoveToFirstChild(rtx);
    rtx.moveToDocumentRoot();
    final Builder<SirixDeweyID> builder = ImmutableSet.builder();
    final ImmutableSet<SirixDeweyID> ids = builder.add(new SirixDeweyID("1"))
                                                  .add(new SirixDeweyID("1.3"))
                                                  .add(new SirixDeweyID("1.3.0.3"))
                                                  .add(new SirixDeweyID("1.3.1.3"))
                                                  .add(new SirixDeweyID("1.3.3"))
                                                  .add(new SirixDeweyID("1.3.5"))
                                                  .add(new SirixDeweyID("1.3.5.3"))
                                                  .add(new SirixDeweyID("1.3.5.5"))
                                                  .add(new SirixDeweyID("1.3.5.5.3"))
                                                  .add(new SirixDeweyID("1.3.5.5.3.1.3"))
                                                  .add(new SirixDeweyID("1.3.5.5.3.1.5"))
                                                  .add(new SirixDeweyID("1.3.5.5.3.3"))
                                                  .add(new SirixDeweyID("1.3.5.5.3.3.1.3"))
                                                  .add(new SirixDeweyID("1.3.5.5.3.3.3"))
                                                  .add(new SirixDeweyID("1.3.5.5.3.5"))
                                                  .add(new SirixDeweyID("1.3.7"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveToFirstChild()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testFourthMoveToFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(0).hasMoved());
    assertEquals(10L, rtx.getDescendantCount()); // due to text node merge
    assertTrue(rtx.moveTo(1).hasMoved());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(7).hasMoved());
    assertEquals(9L, rtx.getFirstChildKey());
    assertEquals(1L, rtx.getChildCount());
    assertEquals(4L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(9L).hasMoved());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertEquals(7L, rtx.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(10L).hasMoved());
    assertTrue(rtx.isAttribute());
    assertTrue(rtx.moveTo(14L).hasMoved());
    assertTrue(rtx.isAttribute());
  }


  @Test(expected = SirixUsageException.class)
  public void testFifthMoveToFirstChild() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(4);
    wtx.moveSubtreeToFirstChild(11);
    wtx.commit();
    wtx.close();
  }

  @Test
  public void testFirstMoveSubtreeToRightSibling() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);

    wtx.moveToDocumentRoot();
    for (final long nodeKey : new NonStructuralWrapperAxis(new DescendantAxis(wtx, IncludeSelf.YES))) {
      System.out.println(nodeKey + ": " + wtx.getDeweyID());
    }

    wtx.moveTo(7);
    wtx.moveSubtreeToRightSibling(6);
    testFirstMoveSubtreeToRightSibling(wtx);

    wtx.moveToDocumentRoot();
    for (final long nodeKey : new NonStructuralWrapperAxis(new DescendantAxis(wtx, IncludeSelf.YES))) {
      System.out.println(nodeKey + ": " + wtx.getDeweyID());
    }

    wtx.commit();

    wtx.moveToDocumentRoot();
    for (final long nodeKey : new NonStructuralWrapperAxis(new DescendantAxis(wtx, IncludeSelf.YES))) {
      System.out.println(nodeKey + ": " + wtx.getDeweyID());
    }

    testFirstMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testFirstMoveSubtreeToRightSibling(rtx);
    rtx.moveToDocumentRoot();
    final Builder<SirixDeweyID> builder = ImmutableSet.<SirixDeweyID>builder();
    final ImmutableSet<SirixDeweyID> ids = builder.add(new SirixDeweyID("1"))
                                                  .add(new SirixDeweyID("1.3"))
                                                  .add(new SirixDeweyID("1.3.0.3"))
                                                  .add(new SirixDeweyID("1.3.1.3"))
                                                  .add(new SirixDeweyID("1.3.3"))
                                                  .add(new SirixDeweyID("1.3.5"))
                                                  .add(new SirixDeweyID("1.3.5.5"))
                                                  .add(new SirixDeweyID("1.3.5.7"))
                                                  .add(new SirixDeweyID("1.3.7"))
                                                  .add(new SirixDeweyID("1.3.9"))
                                                  .add(new SirixDeweyID("1.3.9.1.3"))
                                                  .add(new SirixDeweyID("1.3.9.3"))
                                                  .add(new SirixDeweyID("1.3.9.5"))
                                                  .add(new SirixDeweyID("1.3.11"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveSubtreeToRightSibling()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testFirstMoveSubtreeToRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(7).hasMoved());
    assertFalse(rtx.hasLeftSibling());
    assertTrue(rtx.hasRightSibling());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals(6L, rtx.getNodeKey());
    assertEquals("foo", rtx.getValue());
    assertTrue(rtx.hasLeftSibling());
    assertEquals(7L, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(5).hasMoved());
    assertEquals(2L, rtx.getChildCount());
    assertEquals(2L, rtx.getDescendantCount());
    assertTrue(rtx.moveToDocumentRoot().hasMoved());
    assertEquals(10L, rtx.getDescendantCount());
  }

  @Test
  public void testSecondMoveSubtreeToRightSibling() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(9);
    wtx.moveSubtreeToRightSibling(5);
    // wtx.moveTo(5);
    // wtx.moveSubtreeToRightSibling(4);
    testSecondMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testSecondMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testSecondMoveSubtreeToRightSibling(rtx);
    rtx.moveToDocumentRoot();
    final Builder<SirixDeweyID> builder = ImmutableSet.<SirixDeweyID>builder();
    builder.add(new SirixDeweyID("1"))
           .add(new SirixDeweyID("1.3"))
           .add(new SirixDeweyID("1.3.0.3"))
           .add(new SirixDeweyID("1.3.1.3"))
           .add(new SirixDeweyID("1.3.3"))
           .add(new SirixDeweyID("1.3.5"))
           .add(new SirixDeweyID("1.3.5.5"))
           .add(new SirixDeweyID("1.3.5.7"))
           .add(new SirixDeweyID("1.3.7"))
           .add(new SirixDeweyID("1.3.9"))
           .add(new SirixDeweyID("1.3.9.1.3"))
           .add(new SirixDeweyID("1.3.9.3"))
           .add(new SirixDeweyID("1.3.9.5"))
           .add(new SirixDeweyID("1.3.11"))
           .build();
    // test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx,
    // IncludeSelf.YES)));
    rtx.moveToDocumentRoot();
    for (final long nodeKey : new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES))) {
      System.out.println(nodeKey + ": " + rtx.getDeweyID());
    }
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testSecondMoveSubtreeToRightSibling()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testSecondMoveSubtreeToRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot().hasMoved());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(4L, rtx.getChildCount());
    assertEquals(8L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4).hasMoved());
    // Assert that oops1 and oops2 text nodes merged.
    assertEquals("oops1oops2", rtx.getValue());
    assertFalse(rtx.moveTo(8).hasMoved());
    assertTrue(rtx.moveTo(9).hasMoved());
    assertEquals(5L, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(5).hasMoved());
    assertEquals(9L, rtx.getLeftSiblingKey());
    assertEquals(13L, rtx.getRightSiblingKey());
  }

  @Test
  public void testThirdMoveSubtreeToRightSibling() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(9);
    wtx.moveSubtreeToRightSibling(4);
    testThirdMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testThirdMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testThirdMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveSubtreeToRightSibling()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testThirdMoveSubtreeToRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot().hasMoved());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(4, rtx.getChildCount());
    assertEquals(8, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4).hasMoved());
    // Assert that oops1 and oops3 text nodes merged.
    assertEquals("oops1oops3", rtx.getValue());
    assertFalse(rtx.moveTo(13).hasMoved());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getRightSiblingKey());
    assertEquals(9L, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(9).hasMoved());
    assertEquals(4L, rtx.getRightSiblingKey());
  }

  @Test
  public void testFourthMoveSubtreeToRightSibling() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(8);
    wtx.moveSubtreeToRightSibling(4);
    testFourthMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testFourthMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testFourthMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFourthMoveSubtreeToRightSibling()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testFourthMoveSubtreeToRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(4).hasMoved());
    // Assert that oops2 and oops1 text nodes merged.
    assertEquals("oops2oops1", rtx.getValue());
    assertFalse(rtx.moveTo(8).hasMoved());
    assertEquals(9L, rtx.getRightSiblingKey());
    assertEquals(5L, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(5L).hasMoved());
    assertEquals(4L, rtx.getRightSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(9).hasMoved());
    assertEquals(4L, rtx.getLeftSiblingKey());
  }

  @Test
  public void testFirstCopySubtreeAsFirstChild() {
    // Test for one node.
    XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.close();
    XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveTo(4);
    wtx = holder.getResourceManager().beginNodeTrx();
    wtx.moveTo(9);
    wtx.copySubtreeAsFirstChild(rtx);
    testFirstCopySubtreeAsFirstChild(wtx);
    wtx.commit();
    wtx.close();
    rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testFirstCopySubtreeAsFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstCopySubtreeAsFirstChild()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testFirstCopySubtreeAsFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(9).hasMoved());
    assertEquals(14, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(14).hasMoved());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertEquals(11, rtx.getRightSiblingKey());
    assertEquals("oops1", rtx.getValue());
    assertTrue(rtx.moveTo(1).hasMoved());
    assertEquals(4, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(4).hasMoved());
    assertEquals("oops1", rtx.getValue());
  }

  @Test
  public void testSecondCopySubtreeAsFirstChild() {
    // Test for more than one node.
    XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.close();
    XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveTo(5);
    wtx = holder.getResourceManager().beginNodeTrx();
    wtx.moveTo(9);
    wtx.copySubtreeAsFirstChild(rtx);
    testSecondCopySubtreeAsFirstChild(wtx);
    wtx.commit();
    wtx.close();
    rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testSecondCopySubtreeAsFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testSecondCopySubtreeAsFirstChild()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testSecondCopySubtreeAsFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(9).hasMoved());
    assertEquals(14, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(14).hasMoved());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertEquals(11, rtx.getRightSiblingKey());
    assertEquals("b", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(4).hasMoved());
    assertEquals(5, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(5).hasMoved());
    assertEquals("b", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(14).hasMoved());
    assertEquals(15, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(15).hasMoved());
    assertEquals("foo", rtx.getValue());
    assertTrue(rtx.moveTo(16).hasMoved());
    assertEquals("c", rtx.getName().getLocalName());
    assertFalse(rtx.moveTo(17).hasMoved());
    assertEquals(16, rtx.getNodeKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getRightSiblingKey());
  }

  @Test
  public void testFirstCopySubtreeAsRightSibling() {
    // Test for more than one node.
    XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.commit();
    wtx.close();
    XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveTo(5);
    wtx = holder.getResourceManager().beginNodeTrx();
    wtx.moveTo(9);
    wtx.copySubtreeAsRightSibling(rtx);
    testFirstCopySubtreeAsRightSibling(wtx);
    wtx.commit();
    wtx.close();
    rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    testFirstCopySubtreeAsRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstCopySubtreeAsRightSibling()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testFirstCopySubtreeAsRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(9).hasMoved());
    assertEquals(14, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(14).hasMoved());
    assertEquals(13, rtx.getRightSiblingKey());
    assertEquals(15, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(15).hasMoved());
    assertEquals(15, rtx.getNodeKey());
    assertEquals("foo", rtx.getValue());
    assertEquals(16, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling().hasMoved());
    assertEquals("c", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(4).hasMoved());
    assertEquals(5, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(5).hasMoved());
    assertEquals(8, rtx.getRightSiblingKey());
  }

  @Test
  public void testSubtreeInsertAsFirstChildFirst() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(5);
    wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(XmlDocumentCreator.XML_WITHOUT_XMLDECL));
    testSubtreeInsertAsFirstChildFirst(wtx);
    wtx.commit();
    wtx.moveTo(14);
    testSubtreeInsertAsFirstChildFirst(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveTo(14);
    testSubtreeInsertAsFirstChildFirst(rtx);
    rtx.close();
  }

  @Test
  public void testSubtreeInsertWithFirstNodeBeingAComment() throws FileNotFoundException, IOException {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    final Path pomFile = Paths.get("src", "test", "resources", "pom.xml");
    try (final var fis = new FileInputStream(pomFile.toFile())) {
      wtx.insertSubtreeAsFirstChild(XmlShredder.createFileReader(fis));
    }
  }

  /**
   * Testmethod for {@link UpdateTest#testSubtreeInsertAsFirstChildFirst()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testSubtreeInsertAsFirstChildFirst(final XmlNodeReadOnlyTrx rtx) {
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertEquals(12L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertEquals(19L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertEquals(20L, rtx.getDescendantCount());
  }

  @Test
  public void testSubtreeInsertAsFirstChildSecond() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(11);
    wtx.insertSubtreeAsFirstChild(XmlShredder.createStringReader(XmlDocumentCreator.XML_WITHOUT_XMLDECL));
    testSubtreeInsertAsFirstChildSecond(wtx);
    wtx.commit();
    wtx.moveTo(14);
    testSubtreeInsertAsFirstChildSecond(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveTo(14);
    testSubtreeInsertAsFirstChildSecond(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testSubtreeInsertAsFirstChildSecond()} for having different rtx.
   *
   * @param rtx to test with
   * @throws SirixException
   */
  private static void testSubtreeInsertAsFirstChildSecond(final XmlNodeReadOnlyTrx rtx) {
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertEquals(10L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertEquals(12L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertEquals(19L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertEquals(20L, rtx.getDescendantCount());
  }

  @Test
  public void testSubtreeInsertAsRightSibling() {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    XmlDocumentCreator.create(wtx);
    wtx.moveTo(5);
    wtx.insertSubtreeAsRightSibling(XmlShredder.createStringReader(XmlDocumentCreator.XML_WITHOUT_XMLDECL));
    testSubtreeInsertAsRightSibling(wtx);
    wtx.commit();
    wtx.moveTo(14);
    testSubtreeInsertAsRightSibling(wtx);
    wtx.close();
    final XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveTo(14);
    testSubtreeInsertAsRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testSubtreeInsertAsRightSibling()} for having different rtx.
   *
   * @param rtx to test with
   */
  private static void testSubtreeInsertAsRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertEquals(19L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent().hasMoved());
    assertEquals(20L, rtx.getDescendantCount());
  }

}
