package io.sirix.axis.temporal;

import java.util.List;

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.IncludeSelf;
import io.sirix.test.IteratorTester;
import io.sirix.utils.XmlDocumentCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Verifies that {@link PrefetchedPastAxis} yields the same sequence as the
 * sequential {@link PastAxis} — pinning correctness across the look-ahead /
 * virtual-thread pipeline.
 */
public final class PrefetchedPastAxisTest {

  private static final int ITERATIONS = 5;

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
  public void testPastOrSelfAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceSession().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceSession().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXmlNodeReadTrx();

    new IteratorTester<>(ITERATIONS, List.of(thirdRtx, secondRtx, firstRtx), () ->
        new PrefetchedPastAxis<>(thirdRtx.getResourceSession(), thirdRtx, IncludeSelf.YES)
    ).test();
  }

  @Test
  public void testPastAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceSession().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceSession().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXmlNodeReadTrx();

    new IteratorTester<>(ITERATIONS, List.of(secondRtx, firstRtx), () ->
        new PrefetchedPastAxis<>(thirdRtx.getResourceSession(), thirdRtx)
    ).test();
  }

  @Test
  public void testPastAxisWithRemovedNode() {
    try (final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx()) {
      // Revision 4.
      wtx.commit();
      // Revision 5.
      wtx.commit();
    }

    try (final XmlNodeReadOnlyTrx thirdReader = holder.getResourceSession().beginNodeReadOnlyTrx(3);
        final XmlNodeReadOnlyTrx fourthReader = holder.getResourceSession().beginNodeReadOnlyTrx(4);
        final XmlNodeReadOnlyTrx fifthReader = holder.getResourceSession().beginNodeReadOnlyTrx(5)) {
      thirdReader.moveTo(16);
      fourthReader.moveTo(16);
      fifthReader.moveTo(16);

      new IteratorTester<>(ITERATIONS, List.of(fifthReader, fourthReader, thirdReader), () ->
          new PrefetchedPastAxis<>(fifthReader.getResourceSession(), fifthReader, IncludeSelf.YES)
      ).test();
    }
  }
}
