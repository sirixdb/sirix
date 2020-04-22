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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.sirix.Holder;
import org.sirix.XmlTestHelper;
import org.sirix.api.NodeCursor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.settings.Fixed;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Iterator;

public class CoroutineDescendantAxisTest {

  private static final int ITERATIONS = 5;

  private Holder holder;

  @Before
  public void setUp() throws SirixException {
    XmlTestHelper.deleteEverything();
    XmlTestHelper.createTestDocument();
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() throws SirixException {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testIterate() throws SirixException {
    final XmlResourceManager rm = holder.getResourceManager();
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();

    rtx.moveToDocumentRoot();

    AbsAxisTest.testIAxisConventions(
        new CoroutineDescendantAxis<>(rm), new long[] {1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        return new CoroutineDescendantAxis<>(rm);
      }
    }.test();

    rtx.moveTo(1L);
    AbsAxisTest.testIAxisConventions(
        new CoroutineDescendantAxis<>(rm, IncludeSelf.NO, rtx), new long[] {4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveTo(1L);
        return new CoroutineDescendantAxis<>(rm, IncludeSelf.NO, rtx);
      }
    }.test();

    rtx.moveTo(9L);
    AbsAxisTest.testIAxisConventions(new CoroutineDescendantAxis<>(rm, IncludeSelf.NO, rtx), new long[] {11L, 12L});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(11L, 12L),
            null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveTo(9L);
        return new CoroutineDescendantAxis<>(rm, IncludeSelf.NO, rtx);
      }
    }.test();

    rtx.moveTo(13L);
    AbsAxisTest.testIAxisConventions(new CoroutineDescendantAxis<>(rm, IncludeSelf.NO, rtx), new long[] {});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
            Collections.<Long>emptyList(), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveTo(13L);
        return new CoroutineDescendantAxis<>(rm, IncludeSelf.NO, rtx);
      }
    }.test();
  }

  @Test
  public void testIterateIncludingSelf() throws SirixException {
    final XmlResourceManager rm = holder.getResourceManager();
    final NodeCursor rtx = rm.beginNodeReadOnlyTrx();

    AbsAxisTest.testIAxisConventions(
        new CoroutineDescendantAxis<>(rm, IncludeSelf.YES),
        new long[] {Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), 1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L,
            12L, 13L});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(
                    Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), 1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L,
                    13L),
            null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        return new CoroutineDescendantAxis<>(rm, IncludeSelf.YES);
      }
    }.test();

    rtx.moveTo(1L);
    AbsAxisTest.testIAxisConventions(
        new CoroutineDescendantAxis<>(rm, IncludeSelf.YES, rtx),
        new long[] {1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final NodeCursor rtx = rm.beginNodeReadOnlyTrx();
        rtx.moveTo(1L);
        return new CoroutineDescendantAxis<>(rm, IncludeSelf.YES, rtx);
      }
    }.test();

    rtx.moveTo(9L);
    AbsAxisTest.testIAxisConventions(
        new CoroutineDescendantAxis<>(rm, IncludeSelf.YES, rtx), new long[] {9L, 11L, 12L});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(9L, 11L, 12L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final NodeCursor rtx = rm.beginNodeReadOnlyTrx();
        rtx.moveTo(9L);
        return new CoroutineDescendantAxis<>(rm, IncludeSelf.YES, rtx);
      }
    }.test();

    rtx.moveTo(13L);
    AbsAxisTest.testIAxisConventions(new CoroutineDescendantAxis<>(rm, IncludeSelf.YES, rtx), new long[] {13L});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(13L),
            null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final NodeCursor rtx = rm.beginNodeReadOnlyTrx();
        rtx.moveTo(13L);
        return new CoroutineDescendantAxis<>(rm, IncludeSelf.YES, rtx);
      }
    }.test();
  }

  @Test
  public void testIterationTime() throws SirixException {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
    final XmlResourceManager rm = holder.getResourceManager();

    rtx.moveToDocumentRoot();

    LocalDateTime time1 = LocalDateTime.now();

    AbsAxisTest.testIAxisConventions(
            new CoroutineDescendantAxis<>(rm), new long[] {1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        return new CoroutineDescendantAxis<>(rm);
      }
    }.test();

    LocalDateTime time2 = LocalDateTime.now();

    AbsAxisTest.testIAxisConventions(
            new DescendantAxis(rtx), new long[] {1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L});
    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
            ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveToDocumentRoot();
        return new DescendantAxis(rtx);
      }
    }.test();

    LocalDateTime time3 = LocalDateTime.now();

    System.out.println("CoroutineDescendantAxis -> " + Duration.between(time1, time2).toMillis());
    System.out.println("DescendantAxis -> " + Duration.between(time2, time3).toMillis());
  }
}
