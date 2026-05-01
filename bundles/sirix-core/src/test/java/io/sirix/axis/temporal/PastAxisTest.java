package io.sirix.axis.temporal;

import java.util.List;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.IncludeSelf;
import io.sirix.test.IteratorTester;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.utils.XmlDocumentCreator;

import static org.junit.Assert.assertEquals;

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
        new PastAxis<>(thirdRtx.getResourceSession(), thirdRtx, IncludeSelf.YES)
    ).test();
  }

  @Test
  public void testPastAxis() {
    final XmlNodeReadOnlyTrx firstRtx = holder.getResourceSession().beginNodeReadOnlyTrx(1);
    final XmlNodeReadOnlyTrx secondRtx = holder.getResourceSession().beginNodeReadOnlyTrx(2);
    final XmlNodeReadOnlyTrx thirdRtx = holder.getXmlNodeReadTrx();

    new IteratorTester<>(ITERATIONS, List.of(secondRtx, firstRtx), () ->
        new PastAxis<>(thirdRtx.getResourceSession(), thirdRtx)
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
          new PastAxis<>(fifthReader.getResourceSession(), fifthReader, IncludeSelf.YES)
      ).test();
    }
  }

  /**
   * Regression guard against the {@link AllTimeAxis}-style prefix-rtx leak. PastAxis
   * already closes on the no-match branch (this test would have passed pre-fix) but the
   * assertion locks in the contract for any future axis edit: every rtx the axis opens
   * is either yielded to the consumer (who closes it) or closed by the axis itself.
   */
  @Test
  public void pastAxis_noLeakAfterFullIteration() {
    final int baseline = holder.getResourceSession().activeTrxCount();

    final XmlNodeReadOnlyTrx pivot = holder.getXmlNodeReadTrx();
    final var axis = new PastAxis<>(pivot.getResourceSession(), pivot);
    while (axis.hasNext()) {
      axis.next().close();
    }
    axis.close();

    assertEquals("PastAxis must not leak rtxs after full iteration",
        baseline, holder.getResourceSession().activeTrxCount());
  }
}
