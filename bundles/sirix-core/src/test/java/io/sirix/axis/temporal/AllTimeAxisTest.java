package io.sirix.axis.temporal;

import java.util.List;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.test.IteratorTester;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.utils.XmlDocumentCreator;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link AllTimeAxis}.
 *
 * @author Johannes Lichtenberger
 */
public final class AllTimeAxisTest {

  /**
   * Number of iterations.
   */
  private static final int ITERATIONS = 5;

  /**
   * The {@link Holder} instance.
   */
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
  public void testAxis() {
    try (final XmlNodeReadOnlyTrx firstReader = holder.getResourceSession().beginNodeReadOnlyTrx(1);
        final XmlNodeReadOnlyTrx secondReader = holder.getResourceSession().beginNodeReadOnlyTrx(2);
        final XmlNodeReadOnlyTrx thirdReader = holder.getXmlNodeReadTrx()) {
      new IteratorTester<>(ITERATIONS, List.of(firstReader, secondReader, thirdReader), () ->
          new AllTimeAxis<>(holder.getResourceSession(), holder.getXmlNodeReadTrx())
      ).test();
    }
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

    try (final XmlNodeReadOnlyTrx firstReader = holder.getResourceSession().beginNodeReadOnlyTrx(1);
        final XmlNodeReadOnlyTrx secondReader = holder.getResourceSession().beginNodeReadOnlyTrx(2);
        final XmlNodeReadOnlyTrx thirdReader = holder.getResourceSession().beginNodeReadOnlyTrx(3);
        final XmlNodeReadOnlyTrx fourthReader = holder.getResourceSession().beginNodeReadOnlyTrx(4)) {

      firstReader.moveTo(4);
      secondReader.moveTo(4);
      thirdReader.moveTo(4);
      fourthReader.moveTo(4);

      new IteratorTester<>(ITERATIONS, List.of(firstReader, secondReader, thirdReader, fourthReader), () ->
          new AllTimeAxis<>(fourthReader.getResourceSession(), fourthReader)
      ).test();
    }
  }

  /**
   * Regression: a node that exists only from revision K onwards used to leak the rtxs
   * opened for revisions 1..K-1 (the prefix where {@code moveTo} fails). After the fix,
   * those rtxs are closed before {@code computeNext} loops to the next revision —
   * the session's active-trx count returns to its baseline.
   */
  @Test
  public void prefixRtxIsClosed_noLeak() {
    // Insert a brand-new comment node in revision 4 — it does not exist in revisions 1-3.
    final long lateNodeKey;
    try (final XmlNodeTrx wtx = holder.getResourceSession().beginNodeTrx()) {
      wtx.moveTo(4);
      wtx.insertCommentAsRightSibling("comment-only-in-rev-4-and-later");
      lateNodeKey = wtx.getNodeKey();
      wtx.commit(); // revision 4
    }

    // Snapshot the session's open-trx count BEFORE the axis runs.
    final int baseline = holder.getResourceSession().activeTrxCount();

    // Open at the latest revision (which now is 4), navigate to the late node, and
    // walk the AllTimeAxis. The axis must close every rtx whose moveTo failed
    // (revisions 1-3) before yielding revision 4.
    int yielded = 0;
    try (final XmlNodeReadOnlyTrx pivot = holder.getResourceSession().beginNodeReadOnlyTrx()) {
      pivot.moveTo(lateNodeKey);
      try (final AllTimeAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
               new AllTimeAxis<>(pivot.getResourceSession(), pivot)) {
        while (axis.hasNext()) {
          final XmlNodeReadOnlyTrx yieldedRtx = axis.next();
          yielded++;
          // The consumer is responsible for closing yielded rtxs; do so immediately so
          // the leak we're hunting is the prefix one, not the consumer-yielded one.
          yieldedRtx.close();
        }
      }
    }

    // The pivot rtx was closed by try-with-resources too. Net: active-trx count must
    // match the baseline. Pre-fix this would be off by 3 (revisions 1, 2, 3 leaked).
    assertEquals("AllTimeAxis must not leak rtxs for prefix revisions where the node "
        + "did not yet exist (yielded=" + yielded + ")",
        baseline, holder.getResourceSession().activeTrxCount());
  }
}
