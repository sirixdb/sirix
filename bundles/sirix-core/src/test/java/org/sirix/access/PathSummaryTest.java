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
import static org.junit.Assert.assertTrue;

import javax.annotation.Nonnull;
import javax.xml.namespace.QName;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.TestHelper;
import org.sirix.api.IAxis;
import org.sirix.api.INodeWriteTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.AbsTTException;
import org.sirix.index.path.PathNode;
import org.sirix.index.path.PathSummary;
import org.sirix.node.EKind;
import org.sirix.utils.DocumentCreater;

/**
 * Test the {@link PathSummary}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 *         TODO: Provide a method for all asserts with parameters.
 */
public class PathSummaryTest {

  /** {@link Holder} reference. */
  private Holder holder;

  /** {@link INodeWriteTrx} implementation. */
  private INodeWriteTrx mWtx;

  @Before
  public void setUp() throws AbsTTException {
    TestHelper.deleteEverything();
    holder = Holder.generateSession();
    mWtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(mWtx);
  }

  @After
  public void tearDown() throws AbsTTException {
    holder.close();
    TestHelper.closeEverything();
  }

  /**
   * Test insert on test document.
   * 
   * @throws AbsTTException
   *           if Sirix fails
   */
  @Test
  public void testInsert() throws AbsTTException {
    PathSummary pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testInsertHelper(pathSummary);
    mWtx.commit();
    mWtx.close();
    pathSummary = holder.getSession().openPathSummary();
    testInsertHelper(pathSummary);
    pathSummary.close();
  }

  private void testInsertHelper(final @Nonnull PathSummary pSummary) throws AbsTTException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathNode node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(1L, node.getNodeKey());
    assertEquals(4L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(1, node.getLevel());
    assertEquals(3, node.getChildCount());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(4L, node.getNodeKey());
    assertEquals(6L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(3L, node.getRightSiblingKey());
    assertEquals("b", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(2, node.getLevel());
    assertEquals(2, node.getChildCount());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ATTRIBUTE, node.getPathKind());
    assertEquals(6L, node.getNodeKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(5L, node.getRightSiblingKey());
    assertEquals("{ns}x", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(3, node.getLevel());
    assertEquals(0, node.getChildCount());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(5L, node.getNodeKey());
    assertEquals(6L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("c", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(3, node.getLevel());
    assertEquals(0, node.getChildCount());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ATTRIBUTE, node.getPathKind());
    assertEquals(3L, node.getNodeKey());
    assertEquals(4L, node.getLeftSiblingKey());
    assertEquals(2L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("i", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(2, node.getLevel());
    assertEquals(0, node.getChildCount());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.NAMESPACE, node.getPathKind());
    assertEquals(2L, node.getNodeKey());
    assertEquals(3L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("{ns}p", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(2, node.getLevel());
    assertEquals(0, node.getChildCount());
    node = next(axis);
    assertTrue(node == null);
  }

  /**
   * Test delete on test document.
   * 
   * @throws AbsTTException
   *           if Sirix fails
   */
  @Test
  public void testDelete() throws AbsTTException {
    PathSummary pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testInsertHelper(pathSummary);
    mWtx.commit();
    mWtx.moveTo(9);
    mWtx.remove();
    pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testDeleteHelper(pathSummary);
    mWtx.commit();
    mWtx.close();
    pathSummary = holder.getSession().openPathSummary();
    testDeleteHelper(pathSummary);
    pathSummary.close();
  }

  private void testDeleteHelper(final @Nonnull PathSummary pSummary) throws AbsTTException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathNode node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(1L, node.getNodeKey());
    assertEquals(4L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(1, node.getLevel());
    assertEquals(3, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(4L, node.getNodeKey());
    assertEquals(5L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(3L, node.getRightSiblingKey());
    assertEquals("b", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(2, node.getLevel());
    assertEquals(1, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(5L, node.getNodeKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals("c", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(3, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ATTRIBUTE, node.getPathKind());
    assertEquals(3L, node.getNodeKey());
    assertEquals(4L, node.getLeftSiblingKey());
    assertEquals(2L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("i", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(2, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.NAMESPACE, node.getPathKind());
    assertEquals(2L, node.getNodeKey());
    assertEquals(3L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("{ns}p", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(2, node.getLevel());
    assertEquals(0, node.getChildCount());
    node = next(axis);
    assertTrue(node == null);
  }

  /**
   * Test setQName on test document (does not find a corresponding path node after rename).
   * 
   * @throws AbsTTException
   *           if Sirix fails
   */
  @Test
  public void testSetQNameFirst() throws AbsTTException {
    mWtx.moveTo(9);
    mWtx.setQName(new QName("foo"));
    PathSummary pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testSetQNameFirstHelper(pathSummary);
    mWtx.commit();
    mWtx.close();
    pathSummary = holder.getSession().openPathSummary();
    testSetQNameFirstHelper(pathSummary);
    pathSummary.close();
  }
  
  private void testSetQNameFirstHelper(final @Nonnull PathSummary pSummary) throws AbsTTException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathNode node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(1L, node.getNodeKey());
    assertEquals(7L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(1, node.getLevel());
    assertEquals(4, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(7L, node.getNodeKey());
    assertEquals(9L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(4L, node.getRightSiblingKey());
    assertEquals("foo", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(2, node.getLevel());
    assertEquals(2, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(9L, node.getNodeKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(8L, node.getRightSiblingKey());
    assertEquals("c", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(3, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ATTRIBUTE, node.getPathKind());
    assertEquals(8L, node.getNodeKey());
    assertEquals(9L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("{ns}x", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(3, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(4L, node.getNodeKey());
    assertEquals(7L, node.getLeftSiblingKey());
    assertEquals(3L, node.getRightSiblingKey());
    assertEquals(5L, node.getFirstChildKey());
    assertEquals("b", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(2, node.getLevel());
    assertEquals(1, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(5L, node.getNodeKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("c", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(3, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ATTRIBUTE, node.getPathKind());
    assertEquals(3L, node.getNodeKey());
    assertEquals(4L, node.getLeftSiblingKey());
    assertEquals(2L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("i", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(2, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.NAMESPACE, node.getPathKind());
    assertEquals(2L, node.getNodeKey());
    assertEquals(3L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("{ns}p", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(2, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node == null);
  }
  
  /**
   * Test setQName on test document (finds a corresponding path node after rename).
   * 
   * @throws AbsTTException
   *           if Sirix fails
   */
  @Test
  public void testSetQNameSecond() throws AbsTTException {
    mWtx.moveTo(9);
    mWtx.setQName(new QName("d"));
    mWtx.setQName(new QName("b"));
    PathSummary pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testSetQNameSecondHelper(pathSummary);
    mWtx.commit();
    mWtx.close();
    pathSummary = holder.getSession().openPathSummary();
    testSetQNameSecondHelper(pathSummary);
    pathSummary.close();
  }
  
  private void testSetQNameSecondHelper(final @Nonnull PathSummary pSummary) throws AbsTTException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathNode node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(1L, node.getNodeKey());
    assertEquals(4L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(1, node.getLevel());
    assertEquals(3, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(4L, node.getNodeKey());
    assertEquals(10L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(3L, node.getRightSiblingKey());
    assertEquals("b", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(2, node.getLevel());
    assertEquals(2, node.getChildCount());
    assertEquals(2, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ATTRIBUTE, node.getPathKind());
    assertEquals(10L, node.getNodeKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals(-1L, node.getLeftSiblingKey());
    assertEquals(5L, node.getRightSiblingKey());
    assertEquals("{ns}x", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(3, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ELEMENT, node.getPathKind());
    assertEquals(5L, node.getNodeKey());
    assertEquals(10L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("c", axis.getTransaction().getQNameOfCurrentNode().toString());
    assertEquals(3, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(2, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.ATTRIBUTE, node.getPathKind());
    assertEquals(3L, node.getNodeKey());
    assertEquals(4L, node.getLeftSiblingKey());
    assertEquals(2L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("i", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(2, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node != null);
    assertEquals(EKind.NAMESPACE, node.getPathKind());
    assertEquals(2L, node.getNodeKey());
    assertEquals(3L, node.getLeftSiblingKey());
    assertEquals(-1L, node.getRightSiblingKey());
    assertEquals(-1L, node.getFirstChildKey());
    assertEquals("{ns}p", axis.getTransaction().getQNameOfCurrentNode()
      .toString());
    assertEquals(2, node.getLevel());
    assertEquals(0, node.getChildCount());
    assertEquals(1, node.getReferences());
    node = next(axis);
    assertTrue(node == null);
  }

  /**
   * Get the next node.
   * 
   * @param axis
   *          the axis to use
   * @return the next path node
   */
  private PathNode next(final @Nonnull IAxis axis) {
    if (axis.hasNext()) {
      axis.next();
      return (PathNode)axis.getTransaction().getNode();
    }
    return null;
  }
}
