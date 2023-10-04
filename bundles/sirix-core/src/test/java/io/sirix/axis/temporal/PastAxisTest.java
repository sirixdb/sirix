package io.sirix.axis.temporal;

import java.util.Iterator;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.IncludeSelf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.utils.XmlDocumentCreator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.testing.IteratorFeature;
import com.google.common.collect.testing.IteratorTester;

/**
 * Test {@link PastAxis}.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class PastAxisTest {

  /** Number of iterations. */
  private static final int ITERATIONS = 5;

  /** The {@link Holder} instance. */
  private Holder holder;

  @Before
  public void setUp() {
    XmlTestHelper.deleteEverything();
    try (final XmlNodeTrx wtx = Holder.generateWtx().getXdmNodeWriteTrx()) {
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
  public void testPastOrSelfAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceManager().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXmlNodeReadTrx();

    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(thirdRtx, secondRtx, firstRtx),
        null) {
      @Override
      protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
        return new PastAxis<>(thirdRtx.getResourceSession(), thirdRtx, IncludeSelf.YES);
      }
    }.test();
  }

  @Test
  public void testPastAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceManager().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceManager().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXmlNodeReadTrx();

    new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE, ImmutableList.of(secondRtx, firstRtx), null) {
      @Override
      protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
        return new PastAxis<>(thirdRtx.getResourceSession(), thirdRtx);
      }
    }.test();
  }

  @Test
  public void testPastAxisWithRemovedNode() {
    try (final XmlNodeTrx wtx = holder.getResourceManager().beginNodeTrx()) {
      // Revision 4.
      wtx.commit();

      // Revision 5.
      wtx.commit();
    }

    try (final XmlNodeReadOnlyTrx thirdReader = holder.getResourceManager().beginNodeReadOnlyTrx(3);
        final XmlNodeReadOnlyTrx fourthReader = holder.getResourceManager().beginNodeReadOnlyTrx(4);
        final XmlNodeReadOnlyTrx fifthReader = holder.getResourceManager().beginNodeReadOnlyTrx(5)) {
      thirdReader.moveTo(16);
      fourthReader.moveTo(16);
      fifthReader.moveTo(16);

      new IteratorTester<>(ITERATIONS, IteratorFeature.UNMODIFIABLE,
          ImmutableList.of(fifthReader, fourthReader, thirdReader), null) {
        @Override
        protected Iterator<XmlNodeReadOnlyTrx> newTargetIterator() {
          return new PastAxis<>(fifthReader.getResourceSession(), fifthReader, IncludeSelf.YES);
        }
      }.test();
    }
  }
}
