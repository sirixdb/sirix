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
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.exception.TTUsageException;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.service.xml.shredder.EInsert;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.settings.EFixed;
import org.sirix.utils.DocumentCreater;
import org.sirix.utils.NamePageHash;

public class UpdateTest {

  private Holder holder;

  @Before
  public void setUp() throws AbsTTException {
    TestHelper.deleteEverything();
    holder = Holder.generateSession();
  }

  @After
  public void tearDown() throws AbsTTException {
    holder.close();
    TestHelper.closeEverything();
  }

  @Test
  public void testDelete() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(4);
    wtx.insertElementAsRightSibling(new QName("blabla"));
    wtx.moveTo(5);
    wtx.remove();
    assertEquals(8, wtx.getNode().getNodeKey());
    wtx.moveTo(9);
//    wtx.setQName(new QName("foobarbaz"));
    wtx.moveTo(4);
    testDelete(wtx);
    wtx.commit();
    testDelete(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testDelete(rtx);
    rtx.close();
  }

  private final static void testDelete(final INodeReadTrx pRtx) {
    assertFalse(pRtx.moveTo(5));
    assertTrue(pRtx.moveTo(1));
    assertEquals(5, pRtx.getStructuralNode().getChildCount());
    assertEquals(7, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToDocumentRoot());
    assertEquals(8, pRtx.getStructuralNode().getDescendantCount());
  }

  @Test
  public void testInsert() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    testInsert(wtx);
    wtx.commit();
    testInsert(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testInsert(rtx);
    rtx.close();
  }

  private final static void testInsert(final INodeReadTrx pRtx) {
    assertTrue(pRtx.moveToDocumentRoot());
    assertEquals(10, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveTo(1));
    assertEquals(9, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveTo(4));
    assertEquals(0, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(2, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToFirstChild());
    assertEquals(0, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(0, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(0, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(2, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToFirstChild());
    assertEquals(0, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(0, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(0, pRtx.getStructuralNode().getDescendantCount());
  }

  @Test
  public void testNodeTransactionIsolation() throws AbsTTException {
    INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    wtx.insertElementAsFirstChild(new QName(""));
    testNodeTransactionIsolation(wtx);
    wtx.commit();
    testNodeTransactionIsolation(wtx);
    INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
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
   * @throws AbsTTException
   */
  private final static void testNodeTransactionIsolation(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveToDocumentRoot());
    assertEquals(0, pRtx.getNode().getNodeKey());
    assertTrue(pRtx.moveToFirstChild());
    assertEquals(1, pRtx.getNode().getNodeKey());
    assertEquals(0, ((IStructNode)pRtx.getNode()).getChildCount());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getRightSiblingKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getFirstChildKey());
  }

  /** Test NamePage. */
  @Test
  public void testNamePage() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(7);
    wtx.remove();
    wtx.moveTo(11);
    wtx.remove();
    wtx.moveTo(5);
    wtx.commit();
    wtx.close();
    NodeReadTrx rtx = (NodeReadTrx)holder.getSession().beginNodeReadTrx(0);
    assertEquals(0, rtx.getRevisionNumber());
    assertTrue(rtx.moveTo(7));
    assertEquals("c", rtx.getQNameOfCurrentNode().getLocalPart());
    assertTrue(rtx.moveTo(11));
    assertEquals("c", rtx.getQNameOfCurrentNode().getLocalPart());
    rtx = (NodeReadTrx)holder.getSession().beginNodeReadTrx();
    assertEquals(1, rtx.getRevisionNumber());
    assertEquals(null, rtx.getPageTransaction().getName(NamePageHash.generateHashForString("c"),
      EKind.ELEMENT));
    assertEquals(0, rtx.getNameCount("blablabla", EKind.ATTRIBUTE));
    rtx.moveTo(5);
    assertEquals(2, rtx.getNameCount("b", EKind.ELEMENT));
    rtx.close();
  }

  /** Test update of text value in case two adjacent text nodes would be the result of an insert. */
  @Test
  public void testInsertAsFirstChildUpdateText() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(1L);
    wtx.insertTextAsFirstChild("foo");
    wtx.commit();
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    assertTrue(rtx.moveTo(1L));
    assertEquals(4L, rtx.getStructuralNode().getFirstChildKey());
    assertEquals(5L, rtx.getStructuralNode().getChildCount());
    assertEquals(9L, rtx.getStructuralNode().getDescendantCount());
    assertTrue(rtx.moveTo(4L));
    assertEquals("foooops1", rtx.getValueOfCurrentNode());
    rtx.close();
  }

  /** Test update of text value in case two adjacent text nodes would be the result of an insert. */
  @Test
  public void testInsertAsRightSiblingUpdateTextFirst() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(4L);
    wtx.insertTextAsRightSibling("foo");
    wtx.commit();
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    assertTrue(rtx.moveTo(1L));
    assertEquals(4L, rtx.getStructuralNode().getFirstChildKey());
    assertEquals(5L, rtx.getStructuralNode().getChildCount());
    assertEquals(9L, rtx.getStructuralNode().getDescendantCount());
    assertTrue(rtx.moveTo(4L));
    assertEquals("oops1foo", rtx.getValueOfCurrentNode());
    rtx.close();
  }

  /** Test update of text value in case two adjacent text nodes would be the result of an insert. */
  @Test
  public void testInsertAsRightSiblingUpdateTextSecond() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(5L);
    wtx.insertTextAsRightSibling("foo");
    wtx.commit();
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    assertTrue(rtx.moveTo(1L));
    assertEquals(4L, rtx.getStructuralNode().getFirstChildKey());
    assertEquals(5L, rtx.getStructuralNode().getChildCount());
    assertEquals(9L, rtx.getStructuralNode().getDescendantCount());
    assertTrue(rtx.moveTo(8L));
    assertEquals("foooops2", rtx.getValueOfCurrentNode());
    rtx.close();
  }

  /** Ordinary remove test. */
  @Test
  public void testRemoveDescendantFirst() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.moveTo(4L);
    wtx.remove();
    wtx.commit();
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    assertEquals(0, rtx.getNode().getNodeKey());
    assertTrue(rtx.moveToFirstChild());
    assertEquals(1, rtx.getNode().getNodeKey());
    assertEquals(5, rtx.getStructuralNode().getFirstChildKey());
    assertEquals(4, rtx.getStructuralNode().getChildCount());
    assertEquals(8, rtx.getStructuralNode().getDescendantCount());
    assertTrue(rtx.moveToFirstChild());
    assertEquals(5, rtx.getNode().getNodeKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), rtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    assertEquals(8, rtx.getNode().getNodeKey());
    assertEquals(5, rtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(rtx.moveToRightSibling());
    assertEquals(9, rtx.getNode().getNodeKey());
    assertTrue(rtx.moveToRightSibling());
    assertEquals(13, rtx.getNode().getNodeKey());
    rtx.close();
  }

  @Test
  public void testInsertChild() throws AbsTTException {
    INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    wtx.insertElementAsFirstChild(new QName("foo"));
    wtx.commit();
    wtx.close();

    INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
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
      assertEquals(Integer.toString(i), rtx.getValueOfCurrentNode());
      assertEquals(i, rtx.getRevisionNumber());
      rtx.close();
    }

    rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveToDocumentRoot();
    rtx.moveToFirstChild();
    rtx.moveToFirstChild();
    rtx.moveToRightSibling();
    assertEquals("50", rtx.getValueOfCurrentNode());
    assertEquals(50L, rtx.getRevisionNumber());
    rtx.close();
  }

  @Test
  public void testInsertPath() throws AbsTTException {
    INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    wtx.commit();
    wtx.close();

    wtx = holder.getSession().beginNodeWriteTrx();
    assertTrue(wtx.moveToDocumentRoot());
    assertEquals(1L, wtx.insertElementAsFirstChild(new QName("")).getNode().getNodeKey());
    assertEquals(2L, wtx.insertElementAsFirstChild(new QName("")).getNode().getNodeKey());
    assertEquals(3L, wtx.insertElementAsFirstChild(new QName("")).getNode().getNodeKey());
    assertTrue(wtx.moveToParent());
    assertEquals(4L, wtx.insertElementAsRightSibling(new QName("")).getNode().getNodeKey());
    wtx.commit();
    wtx.close();

    final INodeWriteTrx wtx2 = holder.getSession().beginNodeWriteTrx();
    assertTrue(wtx2.moveToDocumentRoot());
    assertTrue(wtx2.moveToFirstChild());
    assertEquals(5L, wtx2.insertElementAsFirstChild(new QName("")).getNode().getNodeKey());
    wtx2.commit();
    wtx2.close();
  }

  @Test
  public void testPageBoundary() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();

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
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testPageBoundary(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testPageBoundary()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testPageBoundary(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveTo(2L));
    assertEquals(2L, pRtx.getNode().getNodeKey());
  }

  @Test(expected = TTUsageException.class)
  public void testRemoveDocument() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
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
  public void testRemoveDescendant() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(0L);
    // assertEquals(10L, wtx.getStructuralNode().getDescendantCount());
    wtx.commit();
    assertEquals(10L, wtx.getStructuralNode().getDescendantCount());
    wtx.moveTo(5L);
    wtx.remove();
    testRemoveDescendant(wtx);
    wtx.commit();
    testRemoveDescendant(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testRemoveDescendant(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testRemoveDescendant()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testRemoveDescendant(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveToDocumentRoot());
    assertEquals(0, pRtx.getNode().getNodeKey());
    assertEquals(6, pRtx.getStructuralNode().getDescendantCount());
    assertEquals(1, pRtx.getStructuralNode().getChildCount());
    assertTrue(pRtx.moveToFirstChild());
    assertEquals(1, pRtx.getNode().getNodeKey());
    assertEquals(3, pRtx.getStructuralNode().getChildCount());
    assertEquals(5, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToFirstChild());
    assertEquals(4, pRtx.getNode().getNodeKey());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(9, pRtx.getNode().getNodeKey());
    assertEquals(4, pRtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(13, pRtx.getNode().getNodeKey());
  }

  /** Test for text concatenation. */
  @Test
  public void testRemoveDescendantTextConcat2() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
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
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testRemoveDescendantTextConcat2(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testRemoveDescendant()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testRemoveDescendantTextConcat2(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveToDocumentRoot());
    assertEquals(0, pRtx.getNode().getNodeKey());
    assertEquals(2, pRtx.getStructuralNode().getDescendantCount());
    assertEquals(1, pRtx.getStructuralNode().getChildCount());
    assertTrue(pRtx.moveToFirstChild());
    assertEquals(1, pRtx.getNode().getNodeKey());
    assertEquals(1, pRtx.getStructuralNode().getChildCount());
    assertEquals(1, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToFirstChild());
    assertEquals(4, pRtx.getNode().getNodeKey());
    assertFalse(pRtx.moveToRightSibling());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getRightSiblingKey());
  }

  @Test
  public void testReplaceTextNode() throws AbsTTException, IOException, XMLStreamException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
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

  private void testReplaceTextNode(final INodeReadTrx pRtx) throws AbsTTException {
    assertFalse(pRtx.moveTo(5));
    assertTrue(pRtx.moveTo(4));
    assertEquals("oops1baroops2", pRtx.getValueOfCurrentNode());
    assertEquals(9, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(4, pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(13, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(9, pRtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(pRtx.moveTo(1));
    assertEquals(3, pRtx.getStructuralNode().getChildCount());
    assertEquals(5, pRtx.getStructuralNode().getDescendantCount());
  }

  @Test
  public void testReplaceElementNode() throws AbsTTException, IOException, XMLStreamException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
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

  private void testReplaceElementNode(final INodeReadTrx pRtx) throws AbsTTException {
    assertFalse(pRtx.moveTo(5));
    assertTrue(pRtx.moveTo(4));
    assertEquals(14, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(4, pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(8, pRtx.getStructuralNode().getRightSiblingKey());
    assertEquals("c", pRtx.getQNameOfCurrentNode().getLocalPart());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(14, pRtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(pRtx.moveTo(1));
    assertEquals(5, pRtx.getStructuralNode().getChildCount());
    assertEquals(7, pRtx.getStructuralNode().getDescendantCount());
  }

  @Test
  public void testReplaceElement() throws AbsTTException, IOException, XMLStreamException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.replaceNode("<d>foobar</d>");
    testReplaceElement(wtx);
    wtx.commit();
    testReplaceElement(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testReplaceElement(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveToFirstChild()}.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testReplaceElement(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveTo(14));
    assertEquals("d", pRtx.getQNameOfCurrentNode().getLocalPart());
    assertTrue(pRtx.moveTo(4));
    assertEquals(14, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(8, pRtx.getStructuralNode().getRightSiblingKey());
    assertEquals(1, pRtx.getStructuralNode().getChildCount());
    assertEquals(1, pRtx.getStructuralNode().getDescendantCount());
    assertEquals(15, pRtx.getStructuralNode().getFirstChildKey());
    assertTrue(pRtx.moveTo(15));
    assertEquals(0, pRtx.getStructuralNode().getChildCount());
    assertEquals(0, pRtx.getStructuralNode().getDescendantCount());
    assertEquals(14, pRtx.getNode().getParentKey());
    assertTrue(pRtx.moveTo(14));
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(14, pRtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(pRtx.moveTo(1));
    assertEquals(8, pRtx.getStructuralNode().getDescendantCount());
  }

  @Test
  public void testReplaceElementMergeTextNodes() throws AbsTTException, IOException, XMLStreamException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.replaceNode("foo");
    testReplaceElementMergeTextNodes(wtx);
    wtx.commit();
    testReplaceElementMergeTextNodes(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testReplaceElementMergeTextNodes(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveToFirstChild()}.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testReplaceElementMergeTextNodes(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveTo(4));
    assertEquals("oops1foooops2", pRtx.getValueOfCurrentNode());
    assertEquals(9, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(4, pRtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(pRtx.moveTo(1));
    assertEquals(3, pRtx.getStructuralNode().getChildCount());
    assertEquals(5, pRtx.getStructuralNode().getDescendantCount());
  }

  @Test
  public void testFirstMoveToFirstChild() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(7);
    wtx.moveSubtreeToFirstChild(6);
    testFirstMoveToFirstChild(wtx);
    wtx.commit();
    testFirstMoveToFirstChild(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testFirstMoveToFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveToFirstChild()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testFirstMoveToFirstChild(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveToDocumentRoot());
    assertEquals(10L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveTo(4));
    assertEquals(pRtx.getValueOfCurrentNode(), "oops1");
    assertTrue(pRtx.moveTo(7));
    assertEquals(1, pRtx.getStructuralNode().getChildCount());
    assertEquals(1, pRtx.getStructuralNode().getDescendantCount());
    assertFalse(pRtx.getStructuralNode().hasLeftSibling());
    assertTrue(pRtx.getStructuralNode().hasFirstChild());
    assertTrue(pRtx.moveToFirstChild());
    assertFalse(pRtx.getStructuralNode().hasFirstChild());
    assertFalse(pRtx.getStructuralNode().hasLeftSibling());
    assertFalse(pRtx.getStructuralNode().hasRightSibling());
    assertEquals("foo", pRtx.getValueOfCurrentNode());
    assertTrue(pRtx.moveTo(5));
    assertEquals(1, pRtx.getStructuralNode().getChildCount());
    assertEquals(2, pRtx.getStructuralNode().getDescendantCount());
  }

  @Test
  public void testSecondMoveToFirstChild() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.moveSubtreeToFirstChild(4);
    testSecondMoveToFirstChild(wtx);
    wtx.commit();
    testSecondMoveToFirstChild(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testSecondMoveToFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveToFirstChild()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testSecondMoveToFirstChild(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveTo(1));
    assertEquals(4L, pRtx.getStructuralNode().getChildCount());
    assertEquals(8L, pRtx.getStructuralNode().getDescendantCount());
    assertEquals(5L, pRtx.getStructuralNode().getFirstChildKey());
    assertTrue(pRtx.moveTo(5));
    assertEquals(2L, pRtx.getStructuralNode().getChildCount());
    assertEquals(2L, pRtx.getStructuralNode().getDescendantCount());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(4L, pRtx.getStructuralNode().getFirstChildKey());
    assertFalse(pRtx.moveTo(6));
    assertTrue(pRtx.moveTo(4));
    assertEquals("oops1foo", pRtx.getValueOfCurrentNode());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(5L, pRtx.getStructuralNode().getParentKey());
    assertEquals(7L, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveTo(7));
    assertEquals(4L, pRtx.getStructuralNode().getLeftSiblingKey());
  }

  @Test
  public void testThirdMoveToFirstChild() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.moveSubtreeToFirstChild(11);
    testThirdMoveToFirstChild(wtx);
    wtx.commit();
    testThirdMoveToFirstChild(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testThirdMoveToFirstChild(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveToFirstChild()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testThirdMoveToFirstChild(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveTo(0));
    assertEquals(10L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveTo(1));
    assertEquals(9L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveTo(5));
    assertEquals(11L, pRtx.getStructuralNode().getFirstChildKey());
    assertEquals(3L, pRtx.getStructuralNode().getChildCount());
    assertEquals(3L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveTo(11));
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(5L, pRtx.getStructuralNode().getParentKey());
    assertEquals(6L, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveTo(6L));
    assertEquals(11L, pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(7L, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveTo(9L));
    assertEquals(1L, pRtx.getStructuralNode().getChildCount());
    assertEquals(1L, pRtx.getStructuralNode().getDescendantCount());
  }

  @Test(expected = TTUsageException.class)
  public void testFourthMoveToFirstChild() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(4);
    wtx.moveSubtreeToFirstChild(11);
    wtx.commit();
    wtx.close();
  }

  @Test
  public void testFirstMoveSubtreeToRightSibling() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(7);
    wtx.moveSubtreeToRightSibling(6);
    testFirstMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testFirstMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testFirstMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFirstMoveSubtreeToRightSibling()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testFirstMoveSubtreeToRightSibling(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveTo(7));
    assertFalse(pRtx.getStructuralNode().hasLeftSibling());
    assertTrue(pRtx.getStructuralNode().hasRightSibling());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals(6L, pRtx.getNode().getNodeKey());
    assertEquals("foo", pRtx.getValueOfCurrentNode());
    assertTrue(pRtx.getStructuralNode().hasLeftSibling());
    assertEquals(7L, pRtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(pRtx.moveTo(5));
    assertEquals(2L, pRtx.getStructuralNode().getChildCount());
    assertEquals(2L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToDocumentRoot());
    assertEquals(10L, pRtx.getStructuralNode().getDescendantCount());
  }

  @Test
  public void testSecondMoveSubtreeToRightSibling() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(9);
    wtx.moveSubtreeToRightSibling(5);
    testSecondMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testSecondMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testSecondMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testSecondMoveSubtreeToRightSibling()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testSecondMoveSubtreeToRightSibling(final INodeReadTrx pRtx)
    throws AbsTTException {
    assertTrue(pRtx.moveToDocumentRoot());
    assertEquals(9L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToFirstChild());
    assertEquals(4L, pRtx.getStructuralNode().getChildCount());
    assertEquals(8L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveTo(4));
    // Assert that oops1 and oops2 text nodes merged.
    assertEquals("oops1oops2", pRtx.getValueOfCurrentNode());
    assertFalse(pRtx.moveTo(8));
    assertTrue(pRtx.moveTo(9));
    assertEquals(5L, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveTo(5));
    assertEquals(9L, pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(13L, pRtx.getStructuralNode().getRightSiblingKey());
  }

  @Test
  public void testThirdMoveSubtreeToRightSibling() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(9);
    wtx.moveSubtreeToRightSibling(4);
    testThirdMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testThirdMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testThirdMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testThirdMoveSubtreeToRightSibling()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testThirdMoveSubtreeToRightSibling(final INodeReadTrx pRtx) throws AbsTTException {
    assertTrue(pRtx.moveToDocumentRoot());
    assertTrue(pRtx.moveToFirstChild());
    assertEquals(4, pRtx.getStructuralNode().getChildCount());
    assertEquals(8, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveTo(4));
    // Assert that oops1 and oops3 text nodes merged.
    assertEquals("oops1oops3", pRtx.getValueOfCurrentNode());
    assertFalse(pRtx.moveTo(13));
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getRightSiblingKey());
    assertEquals(9L, pRtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(pRtx.moveTo(9));
    assertEquals(4L, pRtx.getStructuralNode().getRightSiblingKey());
  }

  @Test
  public void testFourthMoveSubtreeToRightSibling() throws AbsTTException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(8);
    wtx.moveSubtreeToRightSibling(4);
    testFourthMoveSubtreeToRightSibling(wtx);
    wtx.commit();
    testFourthMoveSubtreeToRightSibling(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    testFourthMoveSubtreeToRightSibling(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testFourthMoveSubtreeToRightSibling()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testFourthMoveSubtreeToRightSibling(final INodeReadTrx pRtx)
    throws AbsTTException {
    assertTrue(pRtx.moveTo(4));
    // Assert that oops2 and oops1 text nodes merged.
    assertEquals("oops2oops1", pRtx.getValueOfCurrentNode());
    assertFalse(pRtx.moveTo(8));
    assertEquals(9L, pRtx.getStructuralNode().getRightSiblingKey());
    assertEquals(5L, pRtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(pRtx.moveTo(5L));
    assertEquals(4L, pRtx.getStructuralNode().getRightSiblingKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getLeftSiblingKey());
    assertTrue(pRtx.moveTo(9));
    assertEquals(4L, pRtx.getStructuralNode().getLeftSiblingKey());
  }

  @Test
  public void testFirstCopySubtreeAsFirstChild() throws AbsTTException {
    // Test for one node.
    INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();
    INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
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
   * @throws AbsTTException
   */
  private final static void testFirstCopySubtreeAsFirstChild(final INodeReadTrx pRtx) {
    assertTrue(pRtx.moveTo(9));
    assertEquals(14, pRtx.getStructuralNode().getFirstChildKey());
    assertTrue(pRtx.moveTo(14));
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(11, pRtx.getStructuralNode().getRightSiblingKey());
    assertEquals("oops1", pRtx.getValueOfCurrentNode());
    assertTrue(pRtx.moveTo(1));
    assertEquals(4, pRtx.getStructuralNode().getFirstChildKey());
    assertTrue(pRtx.moveTo(4));
    assertEquals("oops1", pRtx.getValueOfCurrentNode());
  }

  @Test
  public void testSecondCopySubtreeAsFirstChild() throws AbsTTException {
    // Test for more than one node.
    INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();
    INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
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
   * @throws AbsTTException
   */
  private final static void testSecondCopySubtreeAsFirstChild(final INodeReadTrx pRtx) {
    assertTrue(pRtx.moveTo(9));
    assertEquals(14, pRtx.getStructuralNode().getFirstChildKey());
    assertTrue(pRtx.moveTo(14));
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getLeftSiblingKey());
    assertEquals(11, pRtx.getStructuralNode().getRightSiblingKey());
    assertEquals("b", pRtx.getQNameOfCurrentNode().getLocalPart());
    assertTrue(pRtx.moveTo(4));
    assertEquals(5, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveTo(5));
    assertEquals("b", pRtx.getQNameOfCurrentNode().getLocalPart());
    assertTrue(pRtx.moveTo(14));
    assertEquals(15, pRtx.getStructuralNode().getFirstChildKey());
    assertTrue(pRtx.moveTo(15));
    assertEquals("foo", pRtx.getValueOfCurrentNode());
    assertTrue(pRtx.moveTo(16));
    assertEquals("c", pRtx.getQNameOfCurrentNode().getLocalPart());
    assertFalse(pRtx.moveTo(17));
    assertEquals(16, pRtx.getStructuralNode().getNodeKey());
    assertEquals(EFixed.NULL_NODE_KEY.getStandardProperty(), pRtx.getStructuralNode().getRightSiblingKey());
  }

  @Test
  public void testFirstCopySubtreeAsRightSibling() throws AbsTTException {
    // Test for more than one node.
    INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.commit();
    wtx.close();
    INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
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
   * @throws AbsTTException
   */
  private final static void testFirstCopySubtreeAsRightSibling(final INodeReadTrx pRtx) {
    assertTrue(pRtx.moveTo(9));
    assertEquals(14, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveTo(14));
    assertEquals(13, pRtx.getStructuralNode().getRightSiblingKey());
    assertEquals(15, pRtx.getStructuralNode().getFirstChildKey());
    assertTrue(pRtx.moveTo(15));
    assertEquals(15, pRtx.getStructuralNode().getNodeKey());
    assertEquals("foo", pRtx.getValueOfCurrentNode());
    assertEquals(16, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveToRightSibling());
    assertEquals("c", pRtx.getQNameOfCurrentNode().getLocalPart());
    assertTrue(pRtx.moveTo(4));
    assertEquals(5, pRtx.getStructuralNode().getRightSiblingKey());
    assertTrue(pRtx.moveTo(5));
    assertEquals(8, pRtx.getStructuralNode().getRightSiblingKey());
  }

  @Test
  public void testSubtreeInsertAsFirstChildFirst() throws AbsTTException, IOException, XMLStreamException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.insertSubtree(XMLShredder.createStringReader(DocumentCreater.XML_WITHOUT_XMLDECL),
      EInsert.ASFIRSTCHILD);
    testSubtreeInsertAsFirstChildFirst(wtx);
    wtx.commit();
    wtx.moveTo(14);
    testSubtreeInsertAsFirstChildFirst(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(14);
    testSubtreeInsertAsFirstChildFirst(rtx);
    rtx.close();
  }

  /**
   * Testmethod for {@link UpdateTest#testSubtreeInsertFirst()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testSubtreeInsertAsFirstChildFirst(final INodeReadTrx pRtx) {
    assertEquals(9L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertEquals(12L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertEquals(19L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertEquals(20L, pRtx.getStructuralNode().getDescendantCount());
  }
  
  @Test
  public void testSubtreeInsertAsFirstChildSecond() throws AbsTTException, IOException, XMLStreamException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(11);
    wtx.insertSubtree(XMLShredder
      .createStringReader(DocumentCreater.XML_WITHOUT_XMLDECL),
      EInsert.ASFIRSTCHILD);
    testSubtreeInsertAsFirstChildSecond(wtx);
    wtx.commit();
    wtx.moveTo(14);
    testSubtreeInsertAsFirstChildSecond(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(14);
    testSubtreeInsertAsFirstChildSecond(rtx);
    rtx.close();
  }
  
  /**
   * Testmethod for {@link UpdateTest#testSubtreeInsertAsFirstChildSecond()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testSubtreeInsertAsFirstChildSecond(final INodeReadTrx pRtx) {
    assertEquals(9L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertEquals(10L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertEquals(12L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertEquals(19L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertEquals(20L, pRtx.getStructuralNode().getDescendantCount());
  }
  
  @Test
  public void testSubtreeInsertAsRightSibling() throws AbsTTException, IOException, XMLStreamException {
    final INodeWriteTrx wtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(wtx);
    wtx.moveTo(5);
    wtx.insertSubtree(XMLShredder.createStringReader(DocumentCreater.XML_WITHOUT_XMLDECL),
      EInsert.ASRIGHTSIBLING);
    testSubtreeInsertAsRightSibling(wtx);
    wtx.commit();
    wtx.moveTo(14);
    testSubtreeInsertAsRightSibling(wtx);
    wtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.moveTo(14);
    testSubtreeInsertAsRightSibling(rtx);
    rtx.close();
  }
  
  /**
   * Testmethod for {@link UpdateTest#testSubtreeInsert()} for having different rtx.
   * 
   * @param pRtx
   *          to test with
   * @throws AbsTTException
   */
  private final static void testSubtreeInsertAsRightSibling(final INodeReadTrx pRtx) {
    assertEquals(9L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertEquals(19L, pRtx.getStructuralNode().getDescendantCount());
    assertTrue(pRtx.moveToParent());
    assertEquals(20L, pRtx.getStructuralNode().getDescendantCount());
  }

}
