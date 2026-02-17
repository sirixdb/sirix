package io.sirix.axis;

import java.util.Collections;
import java.util.Iterator;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.axis.visitor.VisitorDescendantAxis;
import io.sirix.exception.SirixException;
import io.sirix.settings.Fixed;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

public class VisitorDescendantAxisTest {

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
  public void testIterateVisitor() throws SirixException {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();

    rtx.moveToDocumentRoot();
    AbsAxisTest.testAxisConventions(VisitorDescendantAxis.newBuilder(rtx).build(),
        new long[] {1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveToDocumentRoot();
        return VisitorDescendantAxis.newBuilder(rtx).build();
      }
    }.test();

    rtx.moveTo(1L);
    AbsAxisTest.testAxisConventions(VisitorDescendantAxis.newBuilder(rtx).build(),
        new long[] {4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveTo(1L);
        return VisitorDescendantAxis.newBuilder(rtx).build();
      }
    }.test();

    rtx.moveTo(9L);
    AbsAxisTest.testAxisConventions(VisitorDescendantAxis.newBuilder(rtx).build(), new long[] {11L, 12L});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(11L, 12L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveTo(9L);
        return VisitorDescendantAxis.newBuilder(rtx).build();
      }
    }.test();

    rtx.moveTo(13L);
    AbsAxisTest.testAxisConventions(VisitorDescendantAxis.newBuilder(rtx).build(), new long[] {});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE, Collections.emptyList(), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveTo(13L);
        return VisitorDescendantAxis.newBuilder(rtx).build();
      }
    }.test();
  }

  @Test
  public void testIterateIncludingSelfVisitor() throws SirixException {
    final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
    rtx.moveToDocumentRoot();
    AbsAxisTest.testAxisConventions(VisitorDescendantAxis.newBuilder(rtx).includeSelf().build(),
        new long[] {Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), 1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(Fixed.DOCUMENT_NODE_KEY.getStandardProperty(), 1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L),
        null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveToDocumentRoot();
        return VisitorDescendantAxis.newBuilder(rtx).includeSelf().build();
      }
    }.test();

    rtx.moveTo(1L);
    AbsAxisTest.testAxisConventions(new VisitorDescendantAxis.Builder(rtx).includeSelf().build(),
        new long[] {1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
        ImmutableList.of(1L, 4L, 5L, 6L, 7L, 8L, 9L, 11L, 12L, 13L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveTo(1L);
        return VisitorDescendantAxis.newBuilder(rtx).includeSelf().build();
      }
    }.test();

    rtx.moveTo(9L);
    AbsAxisTest.testAxisConventions(new VisitorDescendantAxis.Builder(rtx).includeSelf().build(),
        new long[] {9L, 11L, 12L});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(9L, 11L, 12L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveTo(9L);
        return VisitorDescendantAxis.newBuilder(rtx).includeSelf().build();
      }
    }.test();

    rtx.moveTo(13L);
    AbsAxisTest.testAxisConventions(new VisitorDescendantAxis.Builder(rtx).includeSelf().build(), new long[] {13L});
    new IteratorTester<Long>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(13L), null) {
      @Override
      protected Iterator<Long> newTargetIterator() {
        final XmlNodeReadOnlyTrx rtx = holder.getXmlNodeReadTrx();
        rtx.moveTo(13L);
        return VisitorDescendantAxis.newBuilder(rtx).includeSelf().build();
      }
    }.test();
  }
}
