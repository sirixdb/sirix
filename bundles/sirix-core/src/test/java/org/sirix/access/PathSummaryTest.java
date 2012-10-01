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
import org.sirix.api.INodeReadTrx;
import org.sirix.api.INodeWriteTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.index.path.PathSummary;
import org.sirix.node.EKind;
import org.sirix.utils.DocumentCreater;

/**
 * Test the {@link PathSummary}.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 *         TODO: Provide a method for all assertions with parameters.
 */
public class PathSummaryTest {

  /** {@link Holder} reference. */
  private Holder holder;

  /** {@link IsummaryWriteTrx} implementation. */
  private INodeWriteTrx mWtx;

  @Before
  public void setUp() throws SirixException {
    TestHelper.deleteEverything();
    holder = Holder.generateSession();
    mWtx = holder.getSession().beginNodeWriteTrx();
    DocumentCreater.create(mWtx);
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    TestHelper.closeEverything();
  }

  /**
   * Test insert on test document.
   * 
   * @throws SirixException
   *           if Sirix fails
   */
  @Test
  public void testInsert() throws SirixException {
    PathSummary pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testInsertHelper(pathSummary);
    mWtx.commit();
    mWtx.close();
    pathSummary = holder.getSession().openPathSummary();
    testInsertHelper(pathSummary);
    pathSummary.close();
  }

  private void testInsertHelper(final @Nonnull PathSummary pSummary)
    throws SirixException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathSummary summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(4L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTrx().getQName()
      .toString());
    assertEquals(1, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(6L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals("b", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(6L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(5L, summary.getRightSiblingKey());
    assertEquals("{ns}x", axis.getTrx().getQName()
      .toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(6L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("c", axis.getTrx().getQName().toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("i", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("{ns}p", axis.getTrx().getQName()
      .toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertTrue(summary == null);
  }

  /**
   * Test delete on test document.
   * 
   * @throws SirixException
   *           if Sirix fails
   */
  @Test
  public void testDelete() throws SirixException {
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

  private void testDeleteHelper(final @Nonnull PathSummary pSummary)
    throws SirixException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathSummary summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(4L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTrx().getQName()
      .toString());
    assertEquals(1, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(5L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals("b", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals("c", axis.getTrx().getQName().toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("i", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("{ns}p", axis.getTrx().getQName()
      .toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertTrue(summary == null);
  }

  /**
   * Test setQName on test document (does not find a corresponding path summary after rename).
   * 
   * @throws SirixException
   *           if Sirix fails
   */
  @Test
  public void testSetQNameFirst() throws SirixException {
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

  private void testSetQNameFirstHelper(final @Nonnull PathSummary pSummary)
    throws SirixException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathSummary summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(7L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTrx().getQName()
      .toString());
    assertEquals(1, summary.getLevel());
    assertEquals(4, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(7L, summary.getNodeKey());
    assertEquals(9L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(4L, summary.getRightSiblingKey());
    assertEquals("foo", axis.getTrx().getQName()
      .toString());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(9L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(8L, summary.getRightSiblingKey());
    assertEquals("c", axis.getTrx().getQName().toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(8L, summary.getNodeKey());
    assertEquals(9L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("{ns}x", axis.getTrx().getQName()
      .toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(7L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(5L, summary.getFirstChildKey());
    assertEquals("b", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("c", axis.getTrx().getQName().toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("i", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("{ns}p", axis.getTrx().getQName()
      .toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary == null);
  }

  /**
   * Test setQName on test document (finds a corresponding path summary after rename).
   * 
   * @throws SirixException
   *           if Sirix fails
   */
  @Test
  public void testSetQNameSecond() throws SirixException {
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

  private void testSetQNameSecondHelper(final @Nonnull PathSummary pSummary)
    throws SirixException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathSummary summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(4L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTrx().getQName()
      .toString());
    assertEquals(1, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(10L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals("b", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(10L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(5L, summary.getRightSiblingKey());
    assertEquals("{ns}x", axis.getTrx().getQName()
      .toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(10L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("c", axis.getTrx().getQName().toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("i", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("{ns}p", axis.getTrx().getQName()
      .toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary == null);
  }

  /**
   * Test setQName on test document (finds no corresponding path summary after rename -- after references dropped
   * to 0).
   * 
   * @throws SirixException
   *           if Sirix fails
   */
  @Test
  public void testSetQNameThird() throws SirixException {
    mWtx.moveTo(9);
    mWtx.setQName(new QName("d"));
    mWtx.moveTo(5);
    mWtx.setQName(new QName("t"));
    PathSummary pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testSetQNameThirdHelper(pathSummary);
    mWtx.commit();
    mWtx.close();
    pathSummary = holder.getSession().openPathSummary();
    testSetQNameThirdHelper(pathSummary);
    pathSummary.close();
  }

  private void testSetQNameThirdHelper(final @Nonnull PathSummary pSummary)
    throws SirixException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathSummary summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(7L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTrx().getQName()
      .toString());
    assertEquals(1, summary.getLevel());
    assertEquals(4, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(7L, summary.getNodeKey());
    assertEquals(9L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(4L, summary.getRightSiblingKey());
    assertEquals("d", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(9L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(8L, summary.getRightSiblingKey());
    assertEquals("c", axis.getTrx().getQName().toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(8L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(9L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals("{ns}x", axis.getTrx().getQName()
      .toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(7L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(5L, summary.getFirstChildKey());
    assertEquals("t", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("c", axis.getTrx().getQName().toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("i", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("{ns}p", axis.getTrx().getQName()
      .toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary == null);
  }

  /**
   * Test setQName on test document (finds no corresponding path summary after rename -- after references dropped
   * to 0).
   * 
   * @throws SirixException
   *           if Sirix fails
   */
  @Test
  public void testSetQNameFourth() throws SirixException {
    mWtx.moveTo(1);
    mWtx.insertElementAsFirstChild(new QName("b"));
    mWtx.moveTo(5);
    mWtx.setQName(new QName("d"));
    PathSummary pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testSetQNameFourthHelper(pathSummary);
    mWtx.commit();
    mWtx.close();
    pathSummary = holder.getSession().openPathSummary();
    testSetQNameFourthHelper(pathSummary);
    pathSummary.close();
  }

  private void testSetQNameFourthHelper(final @Nonnull PathSummary pSummary)
    throws SirixException {
    final IAxis axis = new DescendantAxis(pSummary);
    PathSummary summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(7L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals("{ns}a", axis.getTrx().getQName()
      .toString());
    assertEquals(1, summary.getLevel());
    assertEquals(4, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(7L, summary.getNodeKey());
    assertEquals(8L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(4L, summary.getRightSiblingKey());
    assertEquals("d", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(8L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals("c", axis.getTrx().getQName().toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(7L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(6L, summary.getFirstChildKey());
    assertEquals("b", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(6L, summary.getNodeKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(5L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("{ns}x", axis.getTrx().getQName()
      .toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(6L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("c", axis.getTrx().getQName().toString());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("i", axis.getTrx().getQName().toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary != null);
    assertEquals(EKind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals("{ns}p", axis.getTrx().getQName()
      .toString());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertTrue(summary == null);
  }

  @Test
  public void testFirstMoveToFirstChild() throws SirixException {
    mWtx.moveTo(5);
    mWtx.moveSubtreeToFirstChild(9);
    PathSummary pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    mWtx.commit();
    mWtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.close();
  }
  
  @Test
  public void testSecondMoveToFirstChild() throws SirixException {
    mWtx.moveTo(9);
    mWtx.insertElementAsFirstChild(new QName("foo"));
    mWtx.insertElementAsFirstChild(new QName("bar"));
    PathSummary pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    mWtx.moveTo(5);
    mWtx.moveSubtreeToRightSibling(9);
    pathSummary = mWtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    mWtx.commit();
    mWtx.close();
    final INodeReadTrx rtx = holder.getSession().beginNodeReadTrx();
    rtx.close();
  }

  /**
   * Get the next summary.
   * 
   * @param axis
   *          the axis to use
   * @return the next path summary
   */
  private PathSummary next(final @Nonnull IAxis axis) {
    if (axis.hasNext()) {
      axis.next();
      return (PathSummary)axis.getTrx();
    }
    return null;
  }
}
