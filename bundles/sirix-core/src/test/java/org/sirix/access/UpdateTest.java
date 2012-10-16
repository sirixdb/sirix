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

package org.sirix.access;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLStreamException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixUsageException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.service.xml.shredder.Insert;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.settings.EFixed;
import org.sirix.utils.DocumentCreater;
import org.sirix.utils.NamePageHash;

/** Test update operations. */
public class UpdateTest {

	/** {@link Holder} reference. */
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
    holder = Holder.generateSession();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void testDelete() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(4);
    wtx.insertElementAsRightSibling(new QName("blabla"));
    wtx.moveTo(5);
    wtx.remove();
    assertEquals(8, wtx.getNodeKey());
    wtx.moveTo(4);
    testDelete(wtx);
    wtx.commit();
    testDelete(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testDelete(rtx);
    rtx.close();
  }

  private final static void testDelete(final NodeReadTrx pRtx) {
    assertFalse(pRtx.moveTo(5).hasMoved());
    assertTrue(pRtx.moveTo(1).hasMoved());
    assertEquals(5, pRtx.getChildCount());
    assertEquals(7, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToDocumentRoot().hasMoved());
    assertEquals(8, pRtx.getDescendantCount());
  }

  @Test
  public void testInsert() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    testInsert(wtx);
    wtx.commit();
    testInsert(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testInsert(rtx);
    rtx.close();
  }

  private final static void testInsert(final NodeReadTrx pRtx) {
    assertTrue(pRtx.moveToDocumentRoot().hasMoved());
    assertEquals(10, pRtx.getDescendantCount());
    assertTrue(pRtx.moveTo(1).hasMoved());
    assertEquals(9, pRtx.getDescendantCount());
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals(0, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(2, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertEquals(0, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(0, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(0, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(2, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertEquals(0, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(0, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(0, pRtx.getDescendantCount());
  }

  @Test
  public void testNodeTransactionIsolation() throws SirixException {
    NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    wtx.insertElementAsFirstChild(new QName(""));
    testNodeTransactionIsolation(wtx);
    wtx.commit();
    testNodeTransactionIsolation(wtx);
    NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testNodeTransactionIsolation(rtx);
    wtx.moveToFirstChild();
    wtx.insertElementAsFirstChild(new QName(""));
    testNodeTransactionIsolation(rtx);
    wtx.commit();
    testNodeTransactionIsolation(rtx);
    rtx.close();
    wtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testNodeTransactionIsolation()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testNodeTransactionIsolation(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveToDocumentRoot().hasMoved());
    assertEquals(0, pRtx.getNodeKey());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertEquals(1, pRtx.getNodeKey());
    assertEquals(0, pRtx.getChildCount());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getLeftSiblingKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getRightSiblingKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getFirstChildKey());
  }

  /** Test NamePage. */
  @Test
  public void testNamePage() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(7);
    wtx.remove();
    wtx.moveTo(11);
    wtx.remove();
    wtx.moveTo(5);
    wtx.commit();
    wtx.close();
    NodeReadTrxImpl rtx = (NodeReadTrxImpl)holder.getSession().beginNodeReadTrx(0);
    assertEquals(0, rtx.getRevisionNumber());
    assertTrue(rtx.moveTo(7).hasMoved());
    assertEquals("c", rtx.getName().getLocalPart());
    assertTrue(rtx.moveTo(11).hasMoved());
    assertEquals("c", rtx.getName().getLocalPart());
    rtx = (NodeReadTrxImpl)holder.getSession().beginNodeReadTrx();
    assertEquals(1, rtx.getRevisionNumber());
    assertEquals(null, rtx.getPageTransaction().getName(NamePageHash.generateHashForString("c"),
      Kind.ELEMENT));
    assertEquals(0, rtx.getNameCount("blablabla", Kind.ATTRIBUTE));
    rtx.moveTo(5);
    assertEquals(2, rtx.getNameCount("b", Kind.ELEMENT));
    rtx.close();
  }

  /** Test update of text value in case two adjacent text nodes would be the result of an insert. */
  @Test
  public void testInsertAsFirstChildUpdateText() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(1L);
    wtx.insertTextAsFirstChild("foo");
    wtx.commit();
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    assertTrue(rtx.moveTo(1L).hasMoved());
    assertEquals(4L, rtx.getFirstChildKey());
    assertEquals(5L, rtx.getChildCount());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4L).hasMoved());
    assertEquals("foooops1", rtx.getValue());
    rtx.close();
  }

  /** Test update of text value in case two adjacent text nodes would be the result of an insert. */
  @Test
  public void testInsertAsRightSiblingUpdateTextFirst() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(4L);
    wtx.insertTextAsRightSibling("foo");
    wtx.commit();
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    assertTrue(rtx.moveTo(1L).hasMoved());
    assertEquals(4L, rtx.getFirstChildKey());
    assertEquals(5L, rtx.getChildCount());
    assertEquals(9L, rtx.getDescendantCount());
    assertTrue(rtx.moveTo(4L).hasMoved());
    assertEquals("oops1foo", rtx.getValue());
    rtx.close();
  }

  /** Test update of text value in case two adjacent text nodes would be the result of an insert. */
  @Test
  public void testInsertAsRightSiblingUpdateTextSecond() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(5L);
    wtx.insertTextAsRightSibling("foo");
    wtx.commit();
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
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
  public void testRemoveDescendantFirst() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(4L);
    wtx.remove();
    wtx.commit();
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    assertEquals(0, rtx.getNodeKey());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(1, rtx.getNodeKey());
    assertEquals(5, rtx.getFirstChildKey());
    assertEquals(4, rtx.getChildCount());
    assertEquals(8, rtx.getDescendantCount());
    assertTrue(rtx.moveToFirstChild().hasMoved());
    assertEquals(5, rtx.getNodeKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), rtx.getLeftSiblingKey());
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
  public void testInsertChild() throws SirixException {
    NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    wtx.insertElementAsFirstChild(new QName("foo"));
    wtx.commit();
    wtx.close();

    NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    assertEquals(0L, rtx.getRevisionNumber());
    rtx.close();

    // Insert 100 children.
    for (int i = 1; i <= 50; i++) {
      wtx = holder.getSession().beginNodeWriteTrx();
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      wtx.insertElementAsFirstChild(new QName("bar"));
      wtx.insertTextAsRightSibling(Integer.toString(i));
      wtx.commit();
      wtx.close();

      rtx = holder.getSession().beginNodeReadTrx();
      rtx.moveToDocumentRoot();
      rtx.moveToFirstChild();
      rtx.moveToFirstChild();
      rtx.moveToRightSibling();
      assertEquals(Integer.toString(i), rtx.getValue());
      assertEquals(i, rtx.getRevisionNumber());
      rtx.close();
    }

    rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveToDocumentRoot();
    rtx.moveToFirstChild();
    rtx.moveToFirstChild();
    rtx.moveToRightSibling();
    assertEquals("50", rtx.getValue());
    assertEquals(50L, rtx.getRevisionNumber());
    rtx.close();
  }

  @Test
  public void testInsertPath() throws SirixException {
    NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    wtx.commit();
    wtx.close();

    wtx = holder.getSession().beginNodeWriteTrx();
    assertTrue(wtx.moveToDocumentRoot().hasMoved());
    assertEquals(1L, wtx.insertElementAsFirstChild(new QName("")).getNodeKey());
    assertEquals(2L, wtx.insertElementAsFirstChild(new QName("")).getNodeKey());
    assertEquals(3L, wtx.insertElementAsFirstChild(new QName("")).getNodeKey());
    assertTrue(wtx.moveToParent().hasMoved());
    assertEquals(4L, wtx.insertElementAsRightSibling(new QName("")).getNodeKey());
    wtx.commit();
    wtx.close();

    final NodeWriteTrx wtx2 = holder.getSession().beginNodeWriteTrx();
    assertTrue(wtx2.moveToDocumentRoot().hasMoved());
    assertTrue(wtx2.moveToFirstChild().hasMoved());
    assertEquals(5L, wtx2.insertElementAsFirstChild(new QName("")).getNodeKey());
    wtx2.commit();
    wtx2.close();
  }

  @Test
  public void testPageBoundary() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();

    // Document root.
    wtx.insertElementAsFirstChild(new QName(""));
    wtx.insertElementAsFirstChild(new QName(""));
    for (int i = 0; i < 256 * 256 + 1; i++) {
      wtx.insertElementAsRightSibling(new QName(""));
    }

    testPageBoundary(wtx);
    wtx.commit();
    testPageBoundary(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testPageBoundary(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testPageBoundary()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testPageBoundary(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveTo(2L).hasMoved());
    assertEquals(2L, pRtx.getNodeKey());
  }

  @Test(expected = SirixUsageException.class)
  public void testRemoveDocument() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveToDocumentRoot();
    try {
      wtx.remove();
    } finally {
      wtx.abort();
      wtx.close();
    }
  }

  /** Test for text concatenation. */
  @Test
  public void testRemoveDescendant() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
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
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testRemoveDescendant(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testRemoveDescendant()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testRemoveDescendant(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveToDocumentRoot().hasMoved());
    assertEquals(0, pRtx.getNodeKey());
    assertEquals(6, pRtx.getDescendantCount());
    assertEquals(1, pRtx.getChildCount());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertEquals(1, pRtx.getNodeKey());
    assertEquals(3, pRtx.getChildCount());
    assertEquals(5, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertEquals(4, pRtx.getNodeKey());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(9, pRtx.getNodeKey());
    assertEquals(4, pRtx.getLeftSiblingKey());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(13, pRtx.getNodeKey());
  }

  /** Test for text concatenation. */
  @Test
  public void testRemoveDescendantTextConcat2() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(9L);
    wtx.remove();
    wtx.moveTo(5L);
    wtx.remove();
    testRemoveDescendantTextConcat2(wtx);
    wtx.commit();
    testRemoveDescendantTextConcat2(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testRemoveDescendantTextConcat2(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testRemoveDescendant()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testRemoveDescendantTextConcat2(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveToDocumentRoot().hasMoved());
    assertEquals(0, pRtx.getNodeKey());
    assertEquals(2, pRtx.getDescendantCount());
    assertEquals(1, pRtx.getChildCount());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertEquals(1, pRtx.getNodeKey());
    assertEquals(1, pRtx.getChildCount());
    assertEquals(1, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertEquals(4, pRtx.getNodeKey());
    assertFalse(pRtx.moveToRightSibling().hasMoved());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getRightSiblingKey());
  }

  @Test
  public void testReplaceTextNode() throws SirixException, IOException, XMLStreamException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(12);
    wtx.moveTo(5);
    wtx.replaceNode(rtx);
    testReplaceTextNode(wtx);
    wtx.commit();
    testReplaceTextNode(wtx);
    wtx.close();
    rtx = holder.getSession().beginNodeReadTrx();
    testReplaceTextNode(rtx);
    rtx.close();
  }

  private void testReplaceTextNode(final NodeReadTrx pRtx) throws SirixException {
    assertFalse(pRtx.moveTo(5).hasMoved());
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals("oops1baroops2", pRtx.getValue());
    assertEquals(9, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(4, pRtx.getLeftSiblingKey());
    assertEquals(13, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(9, pRtx.getLeftSiblingKey());
    assertTrue(pRtx.moveTo(1).hasMoved());
    assertEquals(3, pRtx.getChildCount());
    assertEquals(5, pRtx.getDescendantCount());
  }

  @Test
  public void testReplaceElementNode() throws SirixException, IOException, XMLStreamException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(11);
    wtx.moveTo(5);
    wtx.replaceNode(rtx);
    testReplaceElementNode(wtx);
    wtx.commit();
    testReplaceElementNode(wtx);
    wtx.close();
    rtx = holder.getSession().beginNodeReadTrx();
    testReplaceElementNode(rtx);
    rtx.close();
  }

  private void testReplaceElementNode(final NodeReadTrx pRtx) throws SirixException {
    assertFalse(pRtx.moveTo(5).hasMoved());
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals(14, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(4, pRtx.getLeftSiblingKey());
    assertEquals(8, pRtx.getRightSiblingKey());
    assertEquals("c", pRtx.getName().getLocalPart());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(14, pRtx.getLeftSiblingKey());
    assertTrue(pRtx.moveTo(1).hasMoved());
    assertEquals(5, pRtx.getChildCount());
    assertEquals(7, pRtx.getDescendantCount());
  }

  @Test
  public void testReplaceElement() throws SirixException, IOException, XMLStreamException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.replaceNode("<d>foobar</d>");
    testReplaceElement(wtx);
    wtx.commit();
    testReplaceElement(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testReplaceElement(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveToFirstChild()}.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testReplaceElement(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveTo(14).hasMoved());
    assertEquals("d", pRtx.getName().getLocalPart());
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals(14, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(8, pRtx.getRightSiblingKey());
    assertEquals(1, pRtx.getChildCount());
    assertEquals(1, pRtx.getDescendantCount());
    assertEquals(15, pRtx.getFirstChildKey());
    assertTrue(pRtx.moveTo(15).hasMoved());
    assertEquals(0, pRtx.getChildCount());
    assertEquals(0, pRtx.getDescendantCount());
    assertEquals(14, pRtx.getParentKey());
    assertTrue(pRtx.moveTo(14).hasMoved());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(14, pRtx.getLeftSiblingKey());
    assertTrue(pRtx.moveTo(1).hasMoved());
    assertEquals(8, pRtx.getDescendantCount());
  }

  @Test
  public void testReplaceElementMergeTextNodes() throws SirixException, IOException, XMLStreamException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.replaceNode("foo");
    testReplaceElementMergeTextNodes(wtx);
    wtx.commit();
    testReplaceElementMergeTextNodes(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testReplaceElementMergeTextNodes(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveToFirstChild()}.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testReplaceElementMergeTextNodes(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals("oops1foooops2", pRtx.getValue());
    assertEquals(9, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(4, pRtx.getLeftSiblingKey());
    assertTrue(pRtx.moveTo(1).hasMoved());
    assertEquals(3, pRtx.getChildCount());
    assertEquals(5, pRtx.getDescendantCount());
  }

  @Test
  public void testFirstMoveToFirstChild() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(7);
    wtx.moveSubtreeToFirstChild(6);
    testFirstMoveToFirstChild(wtx);
    wtx.commit();
    testFirstMoveToFirstChild(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testFirstMoveToFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveToFirstChild()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testFirstMoveToFirstChild(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveToDocumentRoot().hasMoved());
    assertEquals(10L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals(pRtx.getValue(), "oops1");
    assertTrue(pRtx.moveTo(7).hasMoved());
    assertEquals(1, pRtx.getChildCount());
    assertEquals(1, pRtx.getDescendantCount());
    assertFalse(pRtx.hasLeftSibling());
    assertTrue(pRtx.hasFirstChild());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertFalse(pRtx.hasFirstChild());
    assertFalse(pRtx.hasLeftSibling());
    assertFalse(pRtx.hasRightSibling());
    assertEquals("foo", pRtx.getValue());
    assertTrue(pRtx.moveTo(5).hasMoved());
    assertEquals(1, pRtx.getChildCount());
    assertEquals(2, pRtx.getDescendantCount());
  }

  @Test
  public void testSecondMoveToFirstChild() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.moveSubtreeToFirstChild(4);
    testSecondMoveToFirstChild(wtx);
    wtx.commit();
    testSecondMoveToFirstChild(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testSecondMoveToFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveToFirstChild()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testSecondMoveToFirstChild(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveTo(1).hasMoved());
    assertEquals(4L, pRtx.getChildCount());
    assertEquals(8L, pRtx.getDescendantCount());
    assertEquals(5L, pRtx.getFirstChildKey());
    assertTrue(pRtx.moveTo(5).hasMoved());
    assertEquals(2L, pRtx.getChildCount());
    assertEquals(2L, pRtx.getDescendantCount());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getLeftSiblingKey());
    assertEquals(4L, pRtx.getFirstChildKey());
    assertFalse(pRtx.moveTo(6).hasMoved());
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals("oops1foo", pRtx.getValue());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getLeftSiblingKey());
    assertEquals(5L, pRtx.getParentKey());
    assertEquals(7L, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveTo(7).hasMoved());
    assertEquals(4L, pRtx.getLeftSiblingKey());
  }

  @Test
  public void testThirdMoveToFirstChild() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.moveSubtreeToFirstChild(11);
    testThirdMoveToFirstChild(wtx);
    wtx.commit();
    testThirdMoveToFirstChild(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testThirdMoveToFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveToFirstChild()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testThirdMoveToFirstChild(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveTo(0).hasMoved());
    assertEquals(10L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveTo(1).hasMoved());
    assertEquals(9L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveTo(5).hasMoved());
    assertEquals(11L, pRtx.getFirstChildKey());
    assertEquals(3L, pRtx.getChildCount());
    assertEquals(3L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveTo(11).hasMoved());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getLeftSiblingKey());
    assertEquals(5L, pRtx.getParentKey());
    assertEquals(6L, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveTo(6L).hasMoved());
    assertEquals(11L, pRtx.getLeftSiblingKey());
    assertEquals(7L, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveTo(9L).hasMoved());
    assertEquals(1L, pRtx.getChildCount());
    assertEquals(1L, pRtx.getDescendantCount());
  }

  @Test(expected = SirixUsageException.class)
  public void testFourthMoveToFirstChild() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(4);
    wtx.moveSubtreeToFirstChild(11);
    wtx.commit();
    wtx.close();
  }

  @Test
  public void testFirstMoveSubtreeToRightSibling() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(7);
    wtx.moveSubtreeToRightSibling(6);
    testFirstMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testFirstMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testFirstMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveSubtreeToRightSibling()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testFirstMoveSubtreeToRightSibling(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveTo(7).hasMoved());
    assertFalse(pRtx.hasLeftSibling());
    assertTrue(pRtx.hasRightSibling());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals(6L, pRtx.getNodeKey());
    assertEquals("foo", pRtx.getValue());
    assertTrue(pRtx.hasLeftSibling());
    assertEquals(7L, pRtx.getLeftSiblingKey());
    assertTrue(pRtx.moveTo(5).hasMoved());
    assertEquals(2L, pRtx.getChildCount());
    assertEquals(2L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToDocumentRoot().hasMoved());
    assertEquals(10L, pRtx.getDescendantCount());
  }

  @Test
  public void testSecondMoveSubtreeToRightSibling() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(9);
    wtx.moveSubtreeToRightSibling(5);
    testSecondMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testSecondMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testSecondMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testSecondMoveSubtreeToRightSibling()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testSecondMoveSubtreeToRightSibling(final NodeReadTrx pRtx)
    throws SirixException {
    assertTrue(pRtx.moveToDocumentRoot().hasMoved());
    assertEquals(9L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertEquals(4L, pRtx.getChildCount());
    assertEquals(8L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveTo(4).hasMoved());
    // Assert that oops1 and oops2 text nodes merged.
    assertEquals("oops1oops2", pRtx.getValue());
    assertFalse(pRtx.moveTo(8).hasMoved());
    assertTrue(pRtx.moveTo(9).hasMoved());
    assertEquals(5L, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveTo(5).hasMoved());
    assertEquals(9L, pRtx.getLeftSiblingKey());
    assertEquals(13L, pRtx.getRightSiblingKey());
  }

  @Test
  public void testThirdMoveSubtreeToRightSibling() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(9);
    wtx.moveSubtreeToRightSibling(4);
    testThirdMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testThirdMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testThirdMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveSubtreeToRightSibling()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testThirdMoveSubtreeToRightSibling(final NodeReadTrx pRtx) throws SirixException {
    assertTrue(pRtx.moveToDocumentRoot().hasMoved());
    assertTrue(pRtx.moveToFirstChild().hasMoved());
    assertEquals(4, pRtx.getChildCount());
    assertEquals(8, pRtx.getDescendantCount());
    assertTrue(pRtx.moveTo(4).hasMoved());
    // Assert that oops1 and oops3 text nodes merged.
    assertEquals("oops1oops3", pRtx.getValue());
    assertFalse(pRtx.moveTo(13).hasMoved());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getRightSiblingKey());
    assertEquals(9L, pRtx.getLeftSiblingKey());
    assertTrue(pRtx.moveTo(9).hasMoved());
    assertEquals(4L, pRtx.getRightSiblingKey());
  }

  @Test
  public void testFourthMoveSubtreeToRightSibling() throws SirixException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(8);
    wtx.moveSubtreeToRightSibling(4);
    testFourthMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testFourthMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testFourthMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFourthMoveSubtreeToRightSibling()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testFourthMoveSubtreeToRightSibling(final NodeReadTrx pRtx)
    throws SirixException {
    assertTrue(pRtx.moveTo(4).hasMoved());
    // Assert that oops2 and oops1 text nodes merged.
    assertEquals("oops2oops1", pRtx.getValue());
    assertFalse(pRtx.moveTo(8).hasMoved());
    assertEquals(9L, pRtx.getRightSiblingKey());
    assertEquals(5L, pRtx.getLeftSiblingKey());
    assertTrue(pRtx.moveTo(5L).hasMoved());
    assertEquals(4L, pRtx.getRightSiblingKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getLeftSiblingKey());
    assertTrue(pRtx.moveTo(9).hasMoved());
    assertEquals(4L, pRtx.getLeftSiblingKey());
  }

  @Test
  public void testFirstCopySubtreeAsFirstChild() throws SirixException {
    // Test for one node.
    NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();
    NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(4);
    wtx = holder.getSession().beginNodeWriteTrx();
    wtx.moveTo(9);
    wtx.copySubtreeAsFirstChild(rtx);
    testFirstCopySubtreeAsFirstChild(wtx);
    wtx.commit();
    wtx.close();
    rtx = holder.getSession().beginNodeReadTrx();
    testFirstCopySubtreeAsFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstCopySubtreeAsFirstChild()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testFirstCopySubtreeAsFirstChild(final NodeReadTrx pRtx) {
    assertTrue(pRtx.moveTo(9).hasMoved());
    assertEquals(14, pRtx.getFirstChildKey());
    assertTrue(pRtx.moveTo(14).hasMoved());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getLeftSiblingKey());
    assertEquals(11, pRtx.getRightSiblingKey());
    assertEquals("oops1", pRtx.getValue());
    assertTrue(pRtx.moveTo(1).hasMoved());
    assertEquals(4, pRtx.getFirstChildKey());
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals("oops1", pRtx.getValue());
  }

  @Test
  public void testSecondCopySubtreeAsFirstChild() throws SirixException {
    // Test for more than one node.
    NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();
    NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(5);
    wtx = holder.getSession().beginNodeWriteTrx();
    wtx.moveTo(9);
    wtx.copySubtreeAsFirstChild(rtx);
    testSecondCopySubtreeAsFirstChild(wtx);
    wtx.commit();
    wtx.close();
    rtx = holder.getSession().beginNodeReadTrx();
    testSecondCopySubtreeAsFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testSecondCopySubtreeAsFirstChild()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testSecondCopySubtreeAsFirstChild(final NodeReadTrx pRtx) {
    assertTrue(pRtx.moveTo(9).hasMoved());
    assertEquals(14, pRtx.getFirstChildKey());
    assertTrue(pRtx.moveTo(14).hasMoved());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getLeftSiblingKey());
    assertEquals(11, pRtx.getRightSiblingKey());
    assertEquals("b", pRtx.getName().getLocalPart());
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals(5, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveTo(5).hasMoved());
    assertEquals("b", pRtx.getName().getLocalPart());
    assertTrue(pRtx.moveTo(14).hasMoved());
    assertEquals(15, pRtx.getFirstChildKey());
    assertTrue(pRtx.moveTo(15).hasMoved());
    assertEquals("foo", pRtx.getValue());
    assertTrue(pRtx.moveTo(16).hasMoved());
    assertEquals("c", pRtx.getName().getLocalPart());
    assertFalse(pRtx.moveTo(17).hasMoved());
    assertEquals(16, pRtx.getNodeKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getRightSiblingKey());
  }

  @Test
  public void testFirstCopySubtreeAsRightSibling() throws SirixException {
    // Test for more than one node.
    NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();
    NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(5);
    wtx = holder.getSession().beginNodeWriteTrx();
    wtx.moveTo(9);
    wtx.copySubtreeAsRightSibling(rtx);
    testFirstCopySubtreeAsRightSibling(wtx);
    wtx.commit();
    wtx.close();
    rtx = holder.getSession().beginNodeReadTrx();
    testFirstCopySubtreeAsRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstCopySubtreeAsRightSibling()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testFirstCopySubtreeAsRightSibling(final NodeReadTrx pRtx) {
    assertTrue(pRtx.moveTo(9).hasMoved());
    assertEquals(14, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveTo(14).hasMoved());
    assertEquals(13, pRtx.getRightSiblingKey());
    assertEquals(15, pRtx.getFirstChildKey());
    assertTrue(pRtx.moveTo(15).hasMoved());
    assertEquals(15, pRtx.getNodeKey());
    assertEquals("foo", pRtx.getValue());
    assertEquals(16, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling().hasMoved());
    assertEquals("c", pRtx.getName().getLocalPart());
    assertTrue(pRtx.moveTo(4).hasMoved());
    assertEquals(5, pRtx.getRightSiblingKey());
    assertTrue(pRtx.moveTo(5).hasMoved());
    assertEquals(8, pRtx.getRightSiblingKey());
  }

  @Test
  public void testSubtreeInsertAsFirstChildFirst() throws SirixException, IOException, XMLStreamException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.insertSubtree(XMLShredder.createStringReader(DocumentCreater.XML_WITHOUT_XMLDECL),
      Insert.ASFIRSTCHILD);
    testSubtreeInsertAsFirstChildFirst(wtx);
    wtx.commit();
    wtx.moveTo(14);
    testSubtreeInsertAsFirstChildFirst(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(14);
    testSubtreeInsertAsFirstChildFirst(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testSubtreeInsertFirst()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testSubtreeInsertAsFirstChildFirst(final NodeReadTrx pRtx) {
    assertEquals(9L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertEquals(12L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertEquals(19L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertEquals(20L, pRtx.getDescendantCount());
  }
  
  @Test
  public void testSubtreeInsertAsFirstChildSecond() throws SirixException, IOException, XMLStreamException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(11);
    wtx.insertSubtree(XMLShredder
      .createStringReader(DocumentCreater.XML_WITHOUT_XMLDECL),
      Insert.ASFIRSTCHILD);
    testSubtreeInsertAsFirstChildSecond(wtx);
    wtx.commit();
    wtx.moveTo(14);
    testSubtreeInsertAsFirstChildSecond(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(14);
    testSubtreeInsertAsFirstChildSecond(rtx);
    rtx.close();
  }
  
  /**
   * Testmethod for {@link UpdateTest#testSubtreeInsertAsFirstChildSecond()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testSubtreeInsertAsFirstChildSecond(final NodeReadTrx pRtx) {
    assertEquals(9L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertEquals(10L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertEquals(12L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertEquals(19L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertEquals(20L, pRtx.getDescendantCount());
  }
  
  @Test
  public void testSubtreeInsertAsRightSibling() throws SirixException, IOException, XMLStreamException {
    final NodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.insertSubtree(XMLShredder.createStringReader(DocumentCreater.XML_WITHOUT_XMLDECL),
      Insert.ASRIGHTSIBLING);
    testSubtreeInsertAsRightSibling(wtx);
    wtx.commit();
    wtx.moveTo(14);
    testSubtreeInsertAsRightSibling(wtx);
    wtx.close();
    final NodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(14);
    testSubtreeInsertAsRightSibling(rtx);
    rtx.close();
  }
  
  /**
   * Testmethod for {@link UpdateTest#testSubtreeInsert()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws SirixException
   */
  private final static void testSubtreeInsertAsRightSibling(final NodeReadTrx pRtx) {
    assertEquals(9L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertEquals(19L, pRtx.getDescendantCount());
    assertTrue(pRtx.moveToParent().hasMoved());
    assertEquals(20L, pRtx.getDescendantCount());
  }

}
