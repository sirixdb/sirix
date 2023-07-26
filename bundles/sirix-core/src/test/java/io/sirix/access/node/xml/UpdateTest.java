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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import io.sirix.access.Databases;
import io.sirix.access.User;
import io.sirix.api.Axis;
import io.sirix.api.Movement;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.DescendantAxis;
import io.sirix.axis.IncludeSelf;
import io.sirix.axis.NonStructuralWrapperAxis;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.utils.NamePageHash;
import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.access.trx.node.xml.XmlNodeReadOnlyTrxImpl;
import io.sirix.exception.SirixUsageException;
import io.sirix.service.xml.shredder.XmlShredder;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.XmlDocumentCreator;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;

/** Test update operations. */
@SuppressWarnings("OptionalGetWithoutIsPresent")
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
         final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
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
         final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      XmlDocumentCreator.createVersioned(wtx);
      Assert.assertEquals(Optional.of(user), wtx.getUser());
      wtx.commit();
      Assert.assertEquals(Optional.of(user), wtx.getUser());
    }

    final var newUser = new User("Marc Kramis", UUID.randomUUID());

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), newUser);
         final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      Assert.assertEquals(Optional.of(user), wtx.getUserOfRevisionToRepresent());
      Assert.assertEquals(Optional.of(newUser), wtx.getUser());
      wtx.revertTo(1);
      Assert.assertEquals(Optional.of(user), wtx.getUserOfRevisionToRepresent());
      Assert.assertEquals(Optional.of(newUser), wtx.getUser());
    }
  }

  @Test
  public void testGettingHistory() {
    final var user = new User("Johannes Lichtenberger", UUID.randomUUID());
    try (final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
         final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
         final var wtx = manager.beginNodeTrx()) {
      XmlDocumentCreator.createVersioned(wtx);
    }

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      final var history = manager.getHistory();

      Assert.assertEquals(3, history.size());

      Assert.assertEquals(3, history.get(0).getRevision());
      assertNotNull(history.get(0).getRevisionTimestamp());
      assertTrue(history.get(0).getCommitMessage().isEmpty());
      Assert.assertEquals("Johannes Lichtenberger", history.get(0).getUser().getName());

      Assert.assertEquals(2, history.get(1).getRevision());
      assertNotNull(history.get(1).getRevisionTimestamp());
      assertTrue(history.get(1).getCommitMessage().isEmpty());
      Assert.assertEquals("Johannes Lichtenberger", history.get(1).getUser().getName());

      Assert.assertEquals(1, history.get(2).getRevision());
      assertNotNull(history.get(2).getRevisionTimestamp());
      assertTrue(history.get(2).getCommitMessage().isEmpty());
      Assert.assertEquals("Johannes Lichtenberger", history.get(2).getUser().getName());
    }
  }

  @Test
  public void testGettingHistoryWithCommitMessageAndDifferentUser() {
    final var user = setupCommitHistoryTest();

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      final var history = manager.getHistory();

      Assert.assertEquals(3, history.size());

      Assert.assertEquals(3, history.get(0).getRevision());
      assertNotNull(history.get(0).getRevisionTimestamp());
      Assert.assertEquals("Insert a second element and text node", history.get(0).getCommitMessage().get());
      Assert.assertEquals("Marc Kramis", history.get(0).getUser().getName());

      Assert.assertEquals(2, history.get(1).getRevision());
      assertNotNull(history.get(1).getRevisionTimestamp());
      Assert.assertEquals("Insert element and text nodes", history.get(1).getCommitMessage().get());
      Assert.assertEquals("Johannes Lichtenberger", history.get(1).getUser().getName());

      Assert.assertEquals(1, history.get(2).getRevision());
      assertNotNull(history.get(2).getRevisionTimestamp());
      assertTrue(history.get(2).getCommitMessage().isEmpty());
      Assert.assertEquals("Johannes Lichtenberger", history.get(2).getUser().getName());
    }
  }

  @Test
  public void testGettingHistoryWithPaging() {
    final var user = setupCommitHistoryTest();

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      final var history = manager.getHistory(3, 1);

      Assert.assertEquals(3, history.size());

      Assert.assertEquals(3, history.get(0).getRevision());
      assertNotNull(history.get(0).getRevisionTimestamp());
      Assert.assertEquals("Insert a second element and text node", history.get(0).getCommitMessage().get());
      Assert.assertEquals("Marc Kramis", history.get(0).getUser().getName());

      Assert.assertEquals(2, history.get(1).getRevision());
      assertNotNull(history.get(1).getRevisionTimestamp());
      Assert.assertEquals("Insert element and text nodes", history.get(1).getCommitMessage().get());
      Assert.assertEquals("Johannes Lichtenberger", history.get(1).getUser().getName());

      Assert.assertEquals(1, history.get(2).getRevision());
      assertNotNull(history.get(2).getRevisionTimestamp());
      assertTrue(history.get(2).getCommitMessage().isEmpty());
      Assert.assertEquals("Johannes Lichtenberger", history.get(2).getUser().getName());
    }
  }

  @Test
  public void testGettingHistoryWithNumberOfRevisionsToRetrieve() {
    final var user = setupCommitHistoryTest();

    try (final var database = Databases.openXmlDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
        final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE)) {
      final var history = manager.getHistory(2);

      Assert.assertEquals(2, history.size());

      Assert.assertEquals(3, history.get(0).getRevision());
      assertNotNull(history.get(0).getRevisionTimestamp());
      Assert.assertEquals("Insert a second element and text node", history.get(0).getCommitMessage().get());
      Assert.assertEquals("Marc Kramis", history.get(0).getUser().getName());

      Assert.assertEquals(2, history.get(1).getRevision());
      assertNotNull(history.get(1).getRevisionTimestamp());
      Assert.assertEquals("Insert element and text nodes", history.get(1).getCommitMessage().get());
      Assert.assertEquals("Johannes Lichtenberger", history.get(1).getUser().getName());
    }
  }

  private User setupCommitHistoryTest() {
    final var user = new User("Johannes Lichtenberger", UUID.randomUUID());
    try (final var database = XmlTestHelper.getDatabase(XmlTestHelper.PATHS.PATH1.getFile(), user);
         final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
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
         final var manager = database.beginResourceSession(XmlTestHelper.RESOURCE);
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
      Assert.assertEquals(8, wtx.getNodeKey());
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
    assertFalse(rtx.moveTo(5));
    assertTrue(rtx.moveTo(1));
    assertEquals(5, rtx.getChildCount());
    assertEquals(7, rtx.getDescendantCount());
    assertTrue(rtx.moveToDocumentRoot());
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
    assertTrue(rtx.moveToDocumentRoot());
    assertEquals(10, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(1));
    assertEquals(9, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4));
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToRightSibling());
    assertEquals(2, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToRightSibling());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
    assertTrue(rtx.moveToRightSibling());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToRightSibling());
    assertEquals(2, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToRightSibling());
    assertEquals(0, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
    assertTrue(rtx.moveToRightSibling());
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

  private static void testNodeTransactionIsolation(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot());
    Assert.assertEquals(0, rtx.getNodeKey());
    assertTrue(rtx.moveToFirstChild());
    Assert.assertEquals(1, rtx.getNodeKey());
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
    assertTrue(rtx.moveTo(7));
    assertEquals("c", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(11));
    assertEquals("c", rtx.getName().getLocalName());
    rtx = (XmlNodeReadOnlyTrxImpl) holder.getResourceManager().beginNodeReadOnlyTrx();
    assertEquals(2, rtx.getRevisionNumber());
    assertNull(rtx.getPageTransaction().getName(NamePageHash.generateHashForString("c"), NodeKind.ELEMENT));
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
    assertTrue(rtx.moveTo(1L));
    Assert.assertEquals(4L, rtx.getFirstChildKey());
    assertEquals(5L, rtx.getChildCount());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4L));
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
    assertTrue(rtx.moveTo(1L));
    Assert.assertEquals(4L, rtx.getFirstChildKey());
    assertEquals(5L, rtx.getChildCount());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4L));
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
    assertTrue(rtx.moveTo(1L));
    Assert.assertEquals(4L, rtx.getFirstChildKey());
    assertEquals(5L, rtx.getChildCount());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(8L));
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
    Assert.assertEquals(0, rtx.getNodeKey());
    assertTrue(rtx.moveToFirstChild());
    Assert.assertEquals(1, rtx.getNodeKey());
    Assert.assertEquals(5, rtx.getFirstChildKey());
    assertEquals(4, rtx.getChildCount());
    assertEquals(8, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild());
    Assert.assertEquals(5, rtx.getNodeKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(8, rtx.getNodeKey());
    Assert.assertEquals(5, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(9, rtx.getNodeKey());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(13, rtx.getNodeKey());
    rtx.close();
  }

  @Test
  public void testInsertChild() {
    XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    wtx.insertElementAsFirstChild(new QNm("foo"));
    wtx.commit();
    wtx.close();

    XmlNodeReadOnlyTrx rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    Assert.assertEquals(1L, rtx.getRevisionNumber());
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
      Assert.assertEquals(i + 1, rtx.getRevisionNumber());
      rtx.close();
    }

    rtx = holder.getResourceManager().beginNodeReadOnlyTrx();
    rtx.moveToDocumentRoot();
    rtx.moveToFirstChild();
    rtx.moveToFirstChild();
    rtx.moveToRightSibling();
    assertEquals("50", rtx.getValue());
    Assert.assertEquals(51L, rtx.getRevisionNumber());
    rtx.close();
  }

  @Test
  public void testInsertPath() {
    XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    wtx.commit();
    wtx.close();

    wtx = holder.getResourceManager().beginNodeTrx();
    assertTrue(wtx.moveToDocumentRoot());
    Assert.assertEquals(1L, wtx.insertElementAsFirstChild(new QNm("")).getNodeKey());
    Assert.assertEquals(2L, wtx.insertElementAsFirstChild(new QNm("")).getNodeKey());
    Assert.assertEquals(3L, wtx.insertElementAsFirstChild(new QNm("")).getNodeKey());
    assertTrue(wtx.moveToParent());
    Assert.assertEquals(4L, wtx.insertElementAsRightSibling(new QNm("")).getNodeKey());
    wtx.commit();
    wtx.close();

    final XmlNodeTrx wtx2 = holder.getResourceManager().beginNodeTrx();
    assertTrue(wtx2.moveToDocumentRoot());
    assertTrue(wtx2.moveToFirstChild());
    Assert.assertEquals(5L, wtx2.insertElementAsFirstChild(new QNm("")).getNodeKey());
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

  private static void testPageBoundary(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(2L));
    Assert.assertEquals(2L, rtx.getNodeKey());
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

  private static void testRemoveDescendant(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot());
    Assert.assertEquals(0, rtx.getNodeKey());
    assertEquals(6, rtx.getDescendantCount());
    assertEquals(1, rtx.getChildCount());
    assertTrue(rtx.moveToFirstChild());
    Assert.assertEquals(1, rtx.getNodeKey());
    assertEquals(3, rtx.getChildCount());
    assertEquals(5, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild());
    Assert.assertEquals(4, rtx.getNodeKey());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(9, rtx.getNodeKey());
    Assert.assertEquals(4, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(13, rtx.getNodeKey());
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

  private static void testRemoveDescendantTextConcat2(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot());
    Assert.assertEquals(0, rtx.getNodeKey());
    assertEquals(2, rtx.getDescendantCount());
    assertEquals(1, rtx.getChildCount());
    assertTrue(rtx.moveToFirstChild());
    Assert.assertEquals(1, rtx.getNodeKey());
    assertEquals(1, rtx.getChildCount());
    assertEquals(1, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild());
    Assert.assertEquals(4, rtx.getNodeKey());
    assertFalse(rtx.moveToRightSibling());
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
    assertFalse(rtx.moveTo(5));
    assertTrue(rtx.moveTo(4));
    assertEquals("oops1baroops2", rtx.getValue());
    Assert.assertEquals(9, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(4, rtx.getLeftSiblingKey());
    Assert.assertEquals(13, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(9, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(1));
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
    assertTrue(rtx.moveTo(14));
    assertEquals("bar", rtx.getValue());
    Assert.assertEquals(5, rtx.getRightSiblingKey());
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
    assertFalse(rtx.moveTo(5));
    assertTrue(rtx.moveTo(4));
    Assert.assertEquals(14, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(4, rtx.getLeftSiblingKey());
    Assert.assertEquals(8, rtx.getRightSiblingKey());
    assertEquals("c", rtx.getName().getLocalName());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(14, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(1));
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

  private static void testReplaceElement(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(14));
    assertEquals("d", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(4));
    Assert.assertEquals(14, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(8, rtx.getRightSiblingKey());
    assertEquals(1, rtx.getChildCount());
    assertEquals(1, rtx.getDescendantCount());
    Assert.assertEquals(15, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(15));
    assertEquals(0, rtx.getChildCount());
    assertEquals(0, rtx.getDescendantCount());
    Assert.assertEquals(14, rtx.getParentKey());
    assertTrue(rtx.moveTo(14));
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(14, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(1));
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
  // assertTrue(rtx.moveTo(4));
  // assertEquals("oops1foooops2", rtx.getValue());
  // assertEquals(9, rtx.getRightSiblingKey());
  // assertTrue(rtx.moveToRightSibling());
  // assertEquals(4, rtx.getLeftSiblingKey());
  // assertTrue(rtx.moveTo(1));
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
                                                  .add(new SirixDeweyID("1.17"))
                                                  .add(new SirixDeweyID("1.17.0.17"))
                                                  .add(new SirixDeweyID("1.17.1.17"))
                                                  .add(new SirixDeweyID("1.17.17"))
                                                  .add(new SirixDeweyID("1.17.33"))
                                                  .add(new SirixDeweyID("1.17.33.33"))
                                                  .add(new SirixDeweyID("1.17.33.33.17"))
                                                  .add(new SirixDeweyID("1.17.49"))
                                                  .add(new SirixDeweyID("1.17.65"))
                                                  .add(new SirixDeweyID("1.17.65.1.17"))
                                                  .add(new SirixDeweyID("1.17.65.17"))
                                                  .add(new SirixDeweyID("1.17.65.33"))
                                                  .add(new SirixDeweyID("1.17.81"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  private void test(final Iterator<SirixDeweyID> ids, final Axis axis) {
    while (ids.hasNext()) {
      assertTrue(axis.hasNext());
      axis.nextLong();
      System.out.println(axis.asXmlNodeReadTrx().getDeweyID());
      assertEquals(ids.next(), axis.asXmlNodeReadTrx().getDeweyID());
    }
  }

  private static void testFirstMoveToFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot());
    assertEquals(10L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4));
    assertEquals(rtx.getValue(), "oops1");
    assertTrue(rtx.moveTo(7));
    assertEquals(1, rtx.getChildCount());
    assertEquals(1, rtx.getDescendantCount());
    assertFalse(rtx.hasLeftSibling());
    assertTrue(rtx.hasFirstChild());
    assertTrue(rtx.moveToFirstChild());
    assertFalse(rtx.hasFirstChild());
    assertFalse(rtx.hasLeftSibling());
    assertFalse(rtx.hasRightSibling());
    assertEquals("foo", rtx.getValue());
    assertTrue(rtx.moveTo(5));
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
                                                  .add(new SirixDeweyID("1.17"))
                                                  .add(new SirixDeweyID("1.17.0.17"))
                                                  .add(new SirixDeweyID("1.17.1.17"))
                                                  .add(new SirixDeweyID("1.17.33"))
                                                  .add(new SirixDeweyID("1.17.33.17"))
                                                  .add(new SirixDeweyID("1.17.33.33"))
                                                  .add(new SirixDeweyID("1.17.49"))
                                                  .add(new SirixDeweyID("1.17.65"))
                                                  .add(new SirixDeweyID("1.17.65.1.17"))
                                                  .add(new SirixDeweyID("1.17.65.17"))
                                                  .add(new SirixDeweyID("1.17.65.33"))
                                                  .add(new SirixDeweyID("1.17.81"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  private static void testSecondMoveToFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(1));
    assertEquals(4L, rtx.getChildCount());
    assertEquals(8L, rtx.getDescendantCount());
    Assert.assertEquals(5L, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(5));
    assertEquals(2L, rtx.getChildCount());
    assertEquals(2L, rtx.getDescendantCount());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    Assert.assertEquals(4L, rtx.getFirstChildKey());
    assertFalse(rtx.moveTo(6));
    assertTrue(rtx.moveTo(4));
    assertEquals("oops1foo", rtx.getValue());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    Assert.assertEquals(5L, rtx.getParentKey());
    Assert.assertEquals(7L, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(7));
    Assert.assertEquals(4L, rtx.getLeftSiblingKey());
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
    final Builder<SirixDeweyID> builder = ImmutableSet.builder();
    final ImmutableSet<SirixDeweyID> ids = builder.add(new SirixDeweyID("1"))
                                                  .add(new SirixDeweyID("1.17"))
                                                  .add(new SirixDeweyID("1.17.0.17"))
                                                  .add(new SirixDeweyID("1.17.1.17"))
                                                  .add(new SirixDeweyID("1.17.17"))
                                                  .add(new SirixDeweyID("1.17.33"))
                                                  .add(new SirixDeweyID("1.17.33.9"))
                                                  .add(new SirixDeweyID("1.17.33.17"))
                                                  .add(new SirixDeweyID("1.17.33.33"))
                                                  .add(new SirixDeweyID("1.17.49"))
                                                  .add(new SirixDeweyID("1.17.65"))
                                                  .add(new SirixDeweyID("1.17.65.1.17"))
                                                  .add(new SirixDeweyID("1.17.65.33"))
                                                  .add(new SirixDeweyID("1.17.81"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  private static void testThirdMoveToFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(0));
    assertEquals(10L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(1));
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(5));
    Assert.assertEquals(11L, rtx.getFirstChildKey());
    assertEquals(3L, rtx.getChildCount());
    assertEquals(3L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(11));
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    Assert.assertEquals(5L, rtx.getParentKey());
    Assert.assertEquals(6L, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(6L));
    Assert.assertEquals(11L, rtx.getLeftSiblingKey());
    Assert.assertEquals(7L, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(9L));
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
                                                  .add(new SirixDeweyID("1.17"))
                                                  .add(new SirixDeweyID("1.17.0.17"))
                                                  .add(new SirixDeweyID("1.17.1.17"))
                                                  .add(new SirixDeweyID("1.17.17"))
                                                  .add(new SirixDeweyID("1.17.33"))
                                                  .add(new SirixDeweyID("1.17.33.17"))
                                                  .add(new SirixDeweyID("1.17.33.33"))
                                                  .add(new SirixDeweyID("1.17.33.33.17"))
                                                  .add(new SirixDeweyID("1.17.33.33.17.1.17"))
                                                  .add(new SirixDeweyID("1.17.33.33.17.1.33"))
                                                  .add(new SirixDeweyID("1.17.33.33.17.17"))
                                                  .add(new SirixDeweyID("1.17.33.33.17.17.1.17"))
                                                  .add(new SirixDeweyID("1.17.33.33.17.17.17"))
                                                  .add(new SirixDeweyID("1.17.33.33.17.33"))
                                                  .add(new SirixDeweyID("1.17.49"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  private static void testFourthMoveToFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(0));
    assertEquals(10L, rtx.getDescendantCount()); // due to text node merge
    assertTrue(rtx.moveTo(1));
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(7));
    Assert.assertEquals(9L, rtx.getFirstChildKey());
    assertEquals(1L, rtx.getChildCount());
    assertEquals(4L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(9L));
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    Assert.assertEquals(7L, rtx.getParentKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(10L));
    assertTrue(rtx.isAttribute());
    assertTrue(rtx.moveTo(14L));
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
    final Builder<SirixDeweyID> builder = ImmutableSet.builder();
    final ImmutableSet<SirixDeweyID> ids = builder.add(new SirixDeweyID("1"))
                                                  .add(new SirixDeweyID("1.17"))
                                                  .add(new SirixDeweyID("1.17.0.17"))
                                                  .add(new SirixDeweyID("1.17.1.17"))
                                                  .add(new SirixDeweyID("1.17.17"))
                                                  .add(new SirixDeweyID("1.17.33"))
                                                  .add(new SirixDeweyID("1.17.33.33"))
                                                  .add(new SirixDeweyID("1.17.33.49"))
                                                  .add(new SirixDeweyID("1.17.49"))
                                                  .add(new SirixDeweyID("1.17.65"))
                                                  .add(new SirixDeweyID("1.17.65.1.17"))
                                                  .add(new SirixDeweyID("1.17.65.17"))
                                                  .add(new SirixDeweyID("1.17.65.33"))
                                                  .add(new SirixDeweyID("1.17.81"))
                                                  .build();
    test(ids.iterator(), new NonStructuralWrapperAxis(new DescendantAxis(rtx, IncludeSelf.YES)));
    rtx.close();
  }

  private static void testFirstMoveSubtreeToRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(7));
    assertFalse(rtx.hasLeftSibling());
    assertTrue(rtx.hasRightSibling());
    assertTrue(rtx.moveToRightSibling());
    Assert.assertEquals(6L, rtx.getNodeKey());
    assertEquals("foo", rtx.getValue());
    assertTrue(rtx.hasLeftSibling());
    Assert.assertEquals(7L, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(5));
    assertEquals(2L, rtx.getChildCount());
    assertEquals(2L, rtx.getDescendantCount());
    assertTrue(rtx.moveToDocumentRoot());
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
    final Builder<SirixDeweyID> builder = ImmutableSet.builder();
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

  private static void testSecondMoveSubtreeToRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild());
    assertEquals(4L, rtx.getChildCount());
    assertEquals(8L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4));
    // Assert that oops1 and oops2 text nodes merged.
    assertEquals("oops1oops2", rtx.getValue());
    assertFalse(rtx.moveTo(8));
    assertTrue(rtx.moveTo(9));
    Assert.assertEquals(5L, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(5));
    Assert.assertEquals(9L, rtx.getLeftSiblingKey());
    Assert.assertEquals(13L, rtx.getRightSiblingKey());
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

  private static void testThirdMoveSubtreeToRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveToDocumentRoot());
    assertTrue(rtx.moveToFirstChild());
    assertEquals(4, rtx.getChildCount());
    assertEquals(8, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4));
    // Assert that oops1 and oops3 text nodes merged.
    assertEquals("oops1oops3", rtx.getValue());
    assertFalse(rtx.moveTo(13));
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getRightSiblingKey());
    Assert.assertEquals(9L, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(9));
    Assert.assertEquals(4L, rtx.getRightSiblingKey());
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

  private static void testFourthMoveSubtreeToRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(4));
    // Assert that oops2 and oops1 text nodes merged.
    assertEquals("oops2oops1", rtx.getValue());
    assertFalse(rtx.moveTo(8));
    Assert.assertEquals(9L, rtx.getRightSiblingKey());
    Assert.assertEquals(5L, rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(5L));
    Assert.assertEquals(4L, rtx.getRightSiblingKey());
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    assertTrue(rtx.moveTo(9));
    Assert.assertEquals(4L, rtx.getLeftSiblingKey());
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

  private static void testFirstCopySubtreeAsFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(9));
    Assert.assertEquals(14, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(14));
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    Assert.assertEquals(11, rtx.getRightSiblingKey());
    assertEquals("oops1", rtx.getValue());
    assertTrue(rtx.moveTo(1));
    Assert.assertEquals(4, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(4));
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

  private static void testSecondCopySubtreeAsFirstChild(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(9));
    Assert.assertEquals(14, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(14));
    assertEquals(Fixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
    Assert.assertEquals(11, rtx.getRightSiblingKey());
    assertEquals("b", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(4));
    Assert.assertEquals(5, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(5));
    assertEquals("b", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(14));
    Assert.assertEquals(15, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(15));
    assertEquals("foo", rtx.getValue());
    assertTrue(rtx.moveTo(16));
    assertEquals("c", rtx.getName().getLocalName());
    assertFalse(rtx.moveTo(17));
    Assert.assertEquals(16, rtx.getNodeKey());
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

  private static void testFirstCopySubtreeAsRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertTrue(rtx.moveTo(9));
    Assert.assertEquals(14, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(14));
    Assert.assertEquals(13, rtx.getRightSiblingKey());
    Assert.assertEquals(15, rtx.getFirstChildKey());
    assertTrue(rtx.moveTo(15));
    Assert.assertEquals(15, rtx.getNodeKey());
    assertEquals("foo", rtx.getValue());
    Assert.assertEquals(16, rtx.getRightSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    assertEquals("c", rtx.getName().getLocalName());
    assertTrue(rtx.moveTo(4));
    Assert.assertEquals(5, rtx.getRightSiblingKey());
    assertTrue(rtx.moveTo(5));
    Assert.assertEquals(8, rtx.getRightSiblingKey());
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
  public void testSubtreeInsertWithFirstNodeBeingAComment() throws IOException {
    final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx();
    final Path pomFile = Paths.get("src", "test", "resources", "pom.xml");
    try (final var fis = new FileInputStream(pomFile.toFile())) {
      wtx.insertSubtreeAsFirstChild(XmlShredder.createFileReader(fis));
    }
  }

  private static void testSubtreeInsertAsFirstChildFirst(final XmlNodeReadOnlyTrx rtx) {
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
    assertEquals(12L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
    assertEquals(19L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
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

  private static void testSubtreeInsertAsFirstChildSecond(final XmlNodeReadOnlyTrx rtx) {
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
    assertEquals(10L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
    assertEquals(12L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
    assertEquals(19L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
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

  private static void testSubtreeInsertAsRightSibling(final XmlNodeReadOnlyTrx rtx) {
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
    assertEquals(19L, rtx.getDescendantCount());
    assertTrue(rtx.moveToParent());
    assertEquals(20L, rtx.getDescendantCount());
  }

}
