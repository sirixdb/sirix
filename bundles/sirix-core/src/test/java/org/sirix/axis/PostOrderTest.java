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

package org.sirix.axis;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import javax.xml.stream.XMLStreamException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XdmTestHelper;
import org.sirix.api.xdm.XdmNodeReadOnlyTrx;
import org.sirix.api.xdm.XdmNodeTrx;
import org.sirix.exception.SirixException;
import org.sirix.service.xml.shredder.XMLShredder;
import org.sirix.utils.XdmDocumentCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link PostOrderAxis}.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public class PostOrderTest {

  private static final int ITERATIONS = 5;

  /** {@link Holder} reference. */
  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XdmTestHelper.deleteEverything();
    XdmTestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XdmTestHelper.closeEverything();
  }

  @Test
  public void testIterateWhole() throws SirixException {
    final XdmNodeReadOnlyTrx rtx = holder.getNodeReadTrx();

    rtx.moveToDocumentRoot();
    AbsAxisTest.testIAxisConventions(
        new PostOrderAxis(rtx), new long[] {4L, 6L, 7L, 5L, 8L, 11L, 12L, 9L, 13L, 1L, 0L});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(4L, 6L, 7L, 5L, 8L, 11L, 12L, 9L, 13L, 1L, 0L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XdmNodeReadOnlyTrx rtx = holder.getNodeReadTrx();
        rtx.moveToDocumentRoot();
        return new PostOrderAxis(rtx);
      }
    }.test();
  }

  @Test
  public void testIterateFirstSubtree() throws SirixException {
    final XdmNodeReadOnlyTrx rtx = holder.getNodeReadTrx();

    rtx.moveTo(5L);
    AbsAxisTest.testIAxisConventions(new PostOrderAxis(rtx), new long[] {6L, 7L});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(6L, 7L),
        null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XdmNodeReadOnlyTrx rtx = holder.getNodeReadTrx();
        rtx.moveTo(5L);
        return new PostOrderAxis(rtx);
      }
    }.test();
  }

  @Test
  public void testIterateZero() throws SirixException {
    final XdmNodeReadOnlyTrx rtx = holder.getNodeReadTrx();

    rtx.moveTo(8L);
    AbsAxisTest.testIAxisConventions(new PostOrderAxis(rtx), new long[] {});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE, Collections.emptyList(),
        null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XdmNodeReadOnlyTrx rtx = holder.getNodeReadTrx();
        rtx.moveTo(8L);
        return new PostOrderAxis(rtx);
      }
    }.test();
  }

  @Test
  public void testIterateDocumentFirst() throws SirixException, IOException, XMLStreamException {
    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeWriteTrx()) {
      wtx.moveTo(9);
      wtx.insertSubtreeAsFirstChild(
          XMLShredder.createStringReader(XdmDocumentCreator.XML_WITHOUT_XMLDECL));
      wtx.commit();
      final long key = wtx.getNodeKey();
      AbsAxisTest.testIAxisConventions(
          new PostOrderAxis(wtx), new long[] {17L, 19L, 20L, 18L, 21L, 24L, 25L, 22L, 26L});
      new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(17L, 19L, 20L, 18L, 21L, 24L, 25L, 22L, 26L), null) {
        @Override
        protected Iterator<Long> newTargetIterator() {
          wtx.moveTo(key);
          return new PostOrderAxis(wtx);
        }
      }.test();
      wtx.moveTo(14L);
      AbsAxisTest.testIAxisConventions(
          new PostOrderAxis(wtx, IncludeSelf.YES),
          new long[] {17L, 19L, 20L, 18L, 21L, 24L, 25L, 22L, 26L, 14L});
      new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(17L, 19L, 20L, 18L, 21L, 24L, 25L, 22L, 26L), null) {
        @Override
        protected Iterator<Long> newTargetIterator() {
          wtx.moveTo(14L);
          return new PostOrderAxis(wtx, IncludeSelf.YES);
        }
      }.test();
      wtx.moveToDocumentRoot();
      AbsAxisTest.testIAxisConventions(
          new PostOrderAxis(wtx), new long[] {4L, 6L, 7L, 5L, 8L, 17L, 19L, 20L, 18L, 21L, 24L, 25L,
              22L, 26L, 14L, 11L, 12L, 9L, 13L, 1L});
      new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(
              4L, 6L, 7L, 5L, 8L, 17L, 19L, 20L, 18L, 21L, 24L, 25L, 22L, 26L, 14L, 11L, 12L, 9L,
              13L, 1L),
          null) {
        @Override
        protected Iterator<Long> newTargetIterator() {
          wtx.moveToDocumentRoot();
          return new PostOrderAxis(wtx);
        }
      }.test();
      wtx.moveToDocumentRoot();
      AbsAxisTest.testIAxisConventions(
          new PostOrderAxis(wtx, IncludeSelf.YES), new long[] {4L, 6L, 7L, 5L, 8L, 17L, 19L, 20L,
              18L, 21L, 24L, 25L, 22L, 26L, 14L, 11L, 12L, 9L, 13L, 1L, 0L});
      new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(
              4L, 6L, 7L, 5L, 8L, 17L, 19L, 20L, 18L, 21L, 24L, 25L, 22L, 26L, 14L, 11L, 12L, 9L,
              13L, 1L, 0L),
          null) {
        @Override
        protected Iterator<Long> newTargetIterator() {
          wtx.moveToDocumentRoot();
          return new PostOrderAxis(wtx, IncludeSelf.YES);
        }
      }.test();
    }
  }

  @Test
  public void testIterateDocumentSecond() throws SirixException, IOException, XMLStreamException {
    try (final XdmNodeTrx wtx = holder.getResourceManager().beginNodeWriteTrx()) {
      wtx.moveTo(11);
      wtx.insertSubtreeAsFirstChild(
          XMLShredder.createStringReader(XdmDocumentCreator.XML_WITHOUT_XMLDECL));
      wtx.commit();
      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      final long key = wtx.getNodeKey();
      AbsAxisTest.testIAxisConventions(
          new PostOrderAxis(wtx, IncludeSelf.YES), new long[] {4L, 6L, 7L, 5L, 8L, 17L, 19L, 20L,
              18L, 21L, 24L, 25L, 22L, 26L, 14L, 11L, 12L, 9L, 13L, 1L});
      new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(
              4L, 6L, 7L, 5L, 8L, 17L, 19L, 20L, 18L, 21L, 24L, 25L, 22L, 26L, 14L, 11L, 12L, 9L,
              13L, 1L),
          null) {
        @Override
        protected Iterator<Long> newTargetIterator() {
          wtx.moveTo(key);
          return new PostOrderAxis(wtx, IncludeSelf.YES);
        }
      }.test();

      wtx.moveToDocumentRoot();
      wtx.moveToFirstChild();
      final long secondKey = wtx.getNodeKey();
      AbsAxisTest.testIAxisConventions(
          new PostOrderAxis(wtx), new long[] {4L, 6L, 7L, 5L, 8L, 17L, 19L, 20L, 18L, 21L, 24L, 25L,
              22L, 26L, 14L, 11L, 12L, 9L, 13L});
      new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(
              4L, 6L, 7L, 5L, 8L, 17L, 19L, 20L, 18L, 21L, 24L, 25L, 22L, 26L, 14L, 11L, 12L, 9L,
              13L),
          null) {
        @Override
        protected Iterator<Long> newTargetIterator() {
          wtx.moveTo(secondKey);
          return new PostOrderAxis(wtx);
        }
      }.test();
    }
  }
}
