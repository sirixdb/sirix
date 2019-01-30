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
import static org.junit.Assert.assertNull;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import org.brackit.xquery.atomic.QNm;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.api.Axis;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.axis.DescendantAxis;
import org.sirix.exception.SirixException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.Kind;
import org.sirix.service.xml.serialize.XmlSerializer;
import org.sirix.service.xml.serialize.XmlSerializer.XmlSerializerBuilder;
import org.sirix.utils.XdmDocumentCreator;

/**
 * Test the {@link PathSummaryReader}.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 *         TODO: Provide a method for all assertions with parameters.
 */
public class PathSummaryTest {

  /** {@link Holder} reference. */
  private Holder holder;

  /** {@link XdmNodeTrx} implementation. */
  private XdmNodeTrx wtx;

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
    holder = Holder.generatePathSummary();
    wtx = holder.getResourceManager().beginNodeTrx();
    XdmDocumentCreator.create(wtx);
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XdmTestHelper.closeEverything();
  }

  /**
   * Test insert on test document.
   */
  @Test
  public void testInsert() throws SirixException {
    PathSummaryReader pathSummary = wtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testInsertHelper(pathSummary);
    wtx.commit();
    wtx.close();
    pathSummary = holder.getResourceManager().openPathSummary();
    testInsertHelper(pathSummary);
    pathSummary.close();
  }

  private void testInsertHelper(final PathSummaryReader summaryReader) {
    final Axis axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(4L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "a"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(6L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(new QNm("b"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(6L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(5L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "x"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(6L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("i"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("ns", "p", ""), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    summary = next(axis);
    assertNull(summary);
  }

  /**
   * Test delete on test document.
   */
  @Test
  public void testDelete() {
    PathSummaryReader pathSummary = wtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testInsertHelper(pathSummary);
    wtx.commit();
    wtx.moveTo(9);
    wtx.remove();
    pathSummary = wtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testDeleteHelper(pathSummary);
    wtx.commit();
    wtx.close();
    pathSummary = holder.getResourceManager().openPathSummary();
    testDeleteHelper(pathSummary);
    pathSummary.close();
  }

  private void testDeleteHelper(final PathSummaryReader summaryReader) {
    final Axis axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(4L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "a"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(5L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(new QNm("b"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("i"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("ns", "p", ""), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());

    summary = next(axis);
    assertNull(summary);
  }

  /**
   * Test setQNm on test document (does not find a corresponding path summary after rename).
   */
  @Test
  public void testSetQNmFirst() {
    wtx.moveTo(9);
    wtx.setName(new QNm("foo"));
    PathSummaryReader pathSummary = wtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testSetQNmFirstHelper(pathSummary);
    wtx.commit();
    wtx.close();
    pathSummary = holder.getResourceManager().openPathSummary();
    testSetQNmFirstHelper(pathSummary);
    pathSummary.close();
  }

  private void testSetQNmFirstHelper(final PathSummaryReader summaryReader) {
    final Axis axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(7L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "a"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(4, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(7L, summary.getNodeKey());
    assertEquals(9L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(4L, summary.getRightSiblingKey());
    assertEquals(new QNm("foo"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(9L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(8L, summary.getRightSiblingKey());
    assertEquals(new QNm("c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(8L, summary.getNodeKey());
    assertEquals(9L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("ns", "p", "x"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(7L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(5L, summary.getFirstChildKey());
    assertEquals(new QNm("b"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("i"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("ns", "p", ""), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNull(summary);
  }

  /**
   * Test setQNm on test document (finds a corresponding path summary after rename).
   */
  @Test
  public void testSetQNmSecond() {
    wtx.moveTo(9);
    wtx.setName(new QNm("d"));
    wtx.commit();

    // System.out.println("nodes");
    //
    // OutputStream out = new ByteArrayOutputStream();
    // XMLSerializer serializer =
    // new XMLSerializerBuilder(holder.getResourceManager(), out).prettyPrint().build();
    // serializer.call();
    // System.out.println(out.toString());
    //
    // System.out.println("summary");
    //
    // PathSummaryReader pathSummary = wtx.getPathSummary();
    // Axis pathSummaryAxis = new DescendantAxis(pathSummary);
    //
    // while (pathSummaryAxis.hasNext()) {
    // pathSummaryAxis.next();
    //
    // System.out.println("nodeKey: " + pathSummary.getNodeKey());
    // System.out.println("path: " + pathSummary.getPath());
    // System.out.println("references: " + pathSummary.getReferences());
    // System.out.println("level: " + pathSummary.getLevel());
    // }

    wtx.moveTo(9);
    wtx.setName(new QNm("b"));
    wtx.commit();

    // System.out.println("");
    // System.out.println("nodes");
    //
    // out = new ByteArrayOutputStream();
    // serializer = new XMLSerializerBuilder(holder.getResourceManager(),
    // out).prettyPrint().build();
    // serializer.call();
    // System.out.println(out.toString());
    //
    // System.out.println("summary");
    //
    // pathSummary = wtx.getPathSummary();
    // pathSummaryAxis = new DescendantAxis(pathSummary);
    //
    // while (pathSummaryAxis.hasNext()) {
    // pathSummaryAxis.next();
    //
    // System.out.println("nodeKey: " + pathSummary.getNodeKey());
    // System.out.println("path: " + pathSummary.getPath());
    // System.out.println("references: " + pathSummary.getReferences());
    // System.out.println("level: " + pathSummary.getLevel());
    // }

    testSetQNmSecondHelper(wtx.getPathSummary());
    wtx.commit();
    wtx.close();

    try (final PathSummaryReader pathSummaryOnMostRecentRev = holder.getResourceManager().openPathSummary()) {
      testSetQNmSecondHelper(pathSummaryOnMostRecentRev);
    }
  }

  private void testSetQNmSecondHelper(final PathSummaryReader summaryReader) {
    summaryReader.moveToDocumentRoot();

    final Axis axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(4L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "a"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(10L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(new QNm("b"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    summary = next(axis);

    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(10L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(5L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "x"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(10L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(2, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("i"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("ns", "p", ""), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNull(summary);
  }

  /**
   * Test setQNm on test document (finds no corresponding path summary after rename -- after
   * references dropped to 0).
   */
  @Test
  public void testSetQNmThird() {
    wtx.moveTo(9);
    wtx.setName(new QNm("d"));
    wtx.moveTo(5);
    wtx.setName(new QNm("t"));
    PathSummaryReader pathSummary = wtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testSetQNmThirdHelper(pathSummary);
    wtx.commit();
    wtx.close();
    pathSummary = holder.getResourceManager().openPathSummary();
    testSetQNmThirdHelper(pathSummary);
    pathSummary.close();
  }

  private void testSetQNmThirdHelper(final PathSummaryReader summaryReader) {
    final Axis axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(7L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "a"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(4, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(7L, summary.getNodeKey());
    assertEquals(9L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(4L, summary.getRightSiblingKey());
    assertEquals(new QNm("d"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(9L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(8L, summary.getRightSiblingKey());
    assertEquals(new QNm("c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(8L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(9L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "", "x"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(7L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(5L, summary.getFirstChildKey());
    assertEquals(new QNm("t"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("i"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("ns", "p", ""), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());

    summary = next(axis);
    assertNull(summary);
  }

  /**
   * Test setQNm on test document (finds no corresponding path summary after rename -- after
   * references dropped to 0).
   */
  @Test
  public void testSetQNmFourth() {
    wtx.moveTo(1);
    wtx.insertElementAsFirstChild(new QNm("b"));
    wtx.moveTo(5);
    wtx.setName(new QNm("d"));
    PathSummaryReader pathSummary = wtx.getPathSummary();
    pathSummary.moveToDocumentRoot();
    testSetQNmFourthHelper(pathSummary);
    wtx.commit();
    wtx.close();
    pathSummary = holder.getResourceManager().openPathSummary();
    testSetQNmFourthHelper(pathSummary);
    pathSummary.close();
  }

  private void testSetQNmFourthHelper(final PathSummaryReader summaryReader) {
    final Axis axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(7L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "a"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(4, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(7L, summary.getNodeKey());
    assertEquals(8L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(4L, summary.getRightSiblingKey());
    assertEquals(new QNm("d"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(8L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(7L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(6L, summary.getFirstChildKey());
    assertEquals(new QNm("b"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(6L, summary.getNodeKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(5L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("ns", "", "x"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(6L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("i"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(new QNm("ns", "p", ""), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    summary = next(axis);
    assertNull(summary);
  }

  @Test
  public void testFirstMoveToFirstChild() {
    wtx.moveTo(5);
    wtx.moveSubtreeToFirstChild(9);

    testFirstMoveToFirstChildBeforeMoveHelper(wtx.getPathSummary());

    wtx.commit();
    wtx.close();

    // System.out.println("nodes");
    //
    // OutputStream out = new ByteArrayOutputStream();
    // XMLSerializer serializer =
    // new XMLSerializerBuilder(holder.getResourceManager(), out).prettyPrint().build();
    // serializer.call();
    // System.out.println(out.toString());
    //
    // System.out.println("summary");
    //
    // PathSummaryReader pathSummary = wtx.getPathSummary();
    // Axis pathSummaryAxis = new DescendantAxis(pathSummary);
    //
    // while (pathSummaryAxis.hasNext()) {
    // pathSummaryAxis.next();
    //
    // System.out.println("nodeKey: " + pathSummary.getNodeKey());
    // System.out.println("path: " + pathSummary.getPath());
    // System.out.println("references: " + pathSummary.getReferences());
    // System.out.println("level: " + pathSummary.getLevel());
    // }

    try (final PathSummaryReader pathSummary = holder.getResourceManager().openPathSummary()) {
      testFirstMoveToFirstChildBeforeMoveHelper(holder.getResourceManager().openPathSummary());
    }
  }

  private void testFirstMoveToFirstChildBeforeMoveHelper(final PathSummaryReader summaryReader) {
    summaryReader.moveToDocumentRoot();

    final Axis axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(4L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "a"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(7L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "b"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(7L, summary.getNodeKey());
    assertEquals(9L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(5L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "b"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(2, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/b", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(9L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(8L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "c"), axis.asPathSummary().getName());
    assertEquals(4, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/b/c", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(8L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(9L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "x"), axis.asPathSummary().getName());
    assertEquals(4, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/b/@p:x", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(7L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/c", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "i"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/@i", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", ""), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/@p:", summary.getPath().toString());

    summary = next(axis);
    assertNull(summary);
  }

  @Test
  public void testSecondMoveToFirstChild() {
    wtx.moveTo(9);
    wtx.insertElementAsFirstChild(new QNm("foo"));
    wtx.insertElementAsFirstChild(new QNm("bar"));
    wtx.moveTo(5);
    wtx.insertElementAsFirstChild(new QNm("b"));
    wtx.insertElementAsFirstChild(new QNm("foo"));
    wtx.insertElementAsFirstChild(new QNm("bar"));
    wtx.commit();

    System.out.println("nodes");

    OutputStream out = new ByteArrayOutputStream();
    XmlSerializer serializer = new XmlSerializerBuilder(holder.getResourceManager(), out).prettyPrint().build();
    serializer.call();
    System.out.println(out.toString());

    System.out.println("summary");

    PathSummaryReader pathSummary = wtx.getPathSummary();
    Axis pathSummaryAxis = new DescendantAxis(pathSummary);

    while (pathSummaryAxis.hasNext()) {
      pathSummaryAxis.next();

      System.out.println("nodeKey: " + pathSummary.getNodeKey());
      System.out.println("path: " + pathSummary.getPath());
      System.out.println("references: " + pathSummary.getReferences());
      System.out.println("level: " + pathSummary.getLevel());
    }

    testSecondMoveToFirstChildBeforeMoveHelper(wtx.getPathSummary());

    wtx.moveToParent().get().moveToParent();
    wtx.moveSubtreeToRightSibling(9);
    wtx.commit();

    testSecondMoveToFirstChildAfterMoveHelper(wtx.getPathSummary());

    wtx.close();

    // System.out.println("nodes");
    //
    // out = new ByteArrayOutputStream();
    // serializer = new XMLSerializerBuilder(holder.getResourceManager(),
    // out).prettyPrint().build();
    // serializer.call();
    // System.out.println(out.toString());
    //
    // System.out.println("summary");
    //
    // pathSummary = holder.getResourceManager().openPathSummary();
    // pathSummaryAxis = new DescendantAxis(pathSummary);
    //
    // while (pathSummaryAxis.hasNext()) {
    // pathSummaryAxis.next();
    //
    // System.out.println("nodeKey: " + pathSummary.getNodeKey());
    // System.out.println("path: " + pathSummary.getPath());
    // System.out.println("references: " + pathSummary.getReferences());
    // System.out.println("level: " + pathSummary.getLevel());
    // }
  }

  private void testSecondMoveToFirstChildBeforeMoveHelper(final PathSummaryReader summaryReader) {
    summaryReader.moveToDocumentRoot();

    final Axis axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(4L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "a"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(9L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "b"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(4, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    assertEquals("/p:a/b", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(9L, summary.getNodeKey());
    assertEquals(10L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(7L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "b"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/b", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(10L, summary.getNodeKey());
    assertEquals(11L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "foo"), axis.asPathSummary().getName());
    assertEquals(4, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/b/foo", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(11L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "bar"), axis.asPathSummary().getName());
    assertEquals(5, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/b/foo/bar", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(7L, summary.getNodeKey());
    assertEquals(8L, summary.getFirstChildKey());
    assertEquals(9L, summary.getLeftSiblingKey());
    assertEquals(6L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "foo"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/foo", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(8L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "bar"), axis.asPathSummary().getName());
    assertEquals(4, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/foo/bar", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(6L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(7L, summary.getLeftSiblingKey());
    assertEquals(5L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "x"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/@p:x", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(6L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    assertEquals("/p:a/b/c", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "i"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/@i", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", ""), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/@p:", summary.getPath().toString());

    summary = next(axis);
    assertNull(summary);
  }

  private void testSecondMoveToFirstChildAfterMoveHelper(final PathSummaryReader summaryReader) {
    final Axis axis = new DescendantAxis(summaryReader);
    PathSummaryReader summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(1L, summary.getNodeKey());
    assertEquals(4L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "a"), axis.asPathSummary().getName());
    assertEquals(1, summary.getLevel());
    assertEquals(3, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(4L, summary.getNodeKey());
    assertEquals(13L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(3L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "b"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(4, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(13L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(12L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/c", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(12L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(13L, summary.getLeftSiblingKey());
    assertEquals(9L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", "x"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/@p:x", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(9L, summary.getNodeKey());
    assertEquals(10L, summary.getFirstChildKey());
    assertEquals(12L, summary.getLeftSiblingKey());
    assertEquals(5L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "b"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    assertEquals("/p:a/b/b", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(10L, summary.getNodeKey());
    assertEquals(11L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "foo"), axis.asPathSummary().getName());
    assertEquals(4, summary.getLevel());
    assertEquals(1, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    assertEquals("/p:a/b/b/foo", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(11L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(-1L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "bar"), axis.asPathSummary().getName());
    assertEquals(5, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(2, summary.getReferences());
    assertEquals("/p:a/b/b/foo/bar", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ELEMENT, summary.getPathKind());
    assertEquals(5L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(9L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "c"), axis.asPathSummary().getName());
    assertEquals(3, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/b/c", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.ATTRIBUTE, summary.getPathKind());
    assertEquals(3L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(4L, summary.getLeftSiblingKey());
    assertEquals(2L, summary.getRightSiblingKey());
    assertEquals(new QNm("", "", "i"), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/@i", summary.getPath().toString());

    summary = next(axis);
    assertNotNull(summary);
    assertEquals(Kind.NAMESPACE, summary.getPathKind());
    assertEquals(2L, summary.getNodeKey());
    assertEquals(-1L, summary.getFirstChildKey());
    assertEquals(3L, summary.getLeftSiblingKey());
    assertEquals(-1L, summary.getRightSiblingKey());
    assertEquals(new QNm("ns", "p", ""), axis.asPathSummary().getName());
    assertEquals(2, summary.getLevel());
    assertEquals(0, summary.getChildCount());
    assertEquals(1, summary.getReferences());
    assertEquals("/p:a/@p:", summary.getPath().toString());

    summary = next(axis);
    assertNull(summary);
  }

  /**
   * Get the next summary.
   *
   * @param axis the axis to use
   * @return the next path summary
   */
  private PathSummaryReader next(final Axis axis) {
    if (axis.hasNext()) {
      axis.next();
      return (PathSummaryReader) axis.getCursor();
    }
    return null;
  }
}
