package io.sirix.axis.temporal;

import java.util.Iterator;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.IncludeSelf;
import io.sirix.XmlTestHelper;
import io.sirix.utils.XmlDocumentCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link FutureAxis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class FutureAxisTest {

  /** Number of iterations. */
  private static final int ITERATIONS = 5;

  /** The {@link Holder} instance. */
  private Holder holder;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    try (final XmlNodeTrx wtx = Holder.generateWtx().getXmlNodeTrx()) {
      XmlDocumentCreator.createVersioned(wtx);
    }
    holder = Holder.generateRtx();
  }

  @After
  public void tearDown() {
    holder.close();
    XmlTestHelper.closeEverything();
  }

  @Test
  public void testFutureOrSelfAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceSession().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceSession().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXmlNodeReadTrx();

    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(firstRtx, secondRtx, thirdRtx),
        null) {
      @Override
      protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
        return new FutureAxis<>(firstRtx.getResourceSession(), firstRtx, IncludeSelf.YES);
      }
    }.test();
  }

  @Test
  public void testFutureAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceSession().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceSession().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXmlNodeReadTrx();

    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(secondRtx, thirdRtx), null) {
      @Override
      protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
        return new FutureAxis<>(firstRtx.getResourceSession(), firstRtx);
      }
    }.test();
  }

  @Test
  public void testAxisWithDeletedNode() {
    try (final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx()) {
      wtx.moveTo(4);
      wtx.insertCommentAsRightSibling("foooooo");

      // Revision 4.
      wtx.commit();

      wtx.moveTo(4);
      wtx.remove();

      // Revision 5.
      wtx.commit();
    }

    try (final XmlNodeReadOnlyTrx thirdReader = holder.getResourceSession().beginNodeReadOnlyTrx(3);
        final XmlNodeReadOnlyTrx fourthReader = holder.getResourceSession().beginNodeReadOnlyTrx(4)) {
      thirdReader.moveTo(4);
      fourthReader.moveTo(4);

      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(thirdReader, fourthReader),
          null) {
        @Override
        protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
          return new FutureAxis<>(thirdReader.getResourceSession(), thirdReader, IncludeSelf.YES);
        }
      }.test();
    }
  }
}
