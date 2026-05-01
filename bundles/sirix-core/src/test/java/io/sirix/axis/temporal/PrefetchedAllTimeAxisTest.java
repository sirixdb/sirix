package io.sirix.axis.temporal;

import java.util.List;

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.test.IteratorTester;
import io.sirix.utils.XmlDocumentCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Verifies that {@link PrefetchedAllTimeAxis} yields the same sequence as the
 * sequential {@link AllTimeAxis} — pinning correctness across the look-ahead /
 * virtual-thread pipeline introduced by the prefetcher.
 */
public final class PrefetchedAllTimeAxisTest {

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
  public void testAxis() {
    try (final XmlNodeReadOnlyTrx firstReader = holder.getResourceSession().beginNodeReadOnlyTrx(1);
        final XmlNodeReadOnlyTrx secondReader = holder.getResourceSession().beginNodeReadOnlyTrx(2);
        final XmlNodeReadOnlyTrx thirdReader = holder.getXmlNodeReadTrx()) {
      new IteratorTester<>(ITERATIONS, List.of(firstReader, secondReader, thirdReader), () ->
          new PrefetchedAllTimeAxis<>(holder.getResourceSession(), holder.getXmlNodeReadTrx())
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
          new PrefetchedAllTimeAxis<>(fourthReader.getResourceSession(), fourthReader)
      ).test();
    }
  }

  /**
   * Three concurrent contracts of the prefetcher must hold:
   *
   * <ul>
   *   <li><b>Full iteration:</b> every rtx the axis opens is either yielded (and closed
   *       by the consumer) or closed by the axis. activeTrxCount returns to baseline.</li>
   *   <li><b>Constructor-without-iterate:</b> the lazy ramp-up means constructing the
   *       axis must not open any rtx; close() then is a no-op.</li>
   *   <li><b>Mid-iteration abandon:</b> consumer pulls one rtx, calls close(); the
   *       prefetcher must cancel pending tasks and ensure any rtx already opened by an
   *       in-flight task is released. activeTrxCount returns to baseline (allowing for
   *       tasks that may still be in flight when close() returns — the cooperative
   *       cancellation closes them inline).</li>
   * </ul>
   */
  @Test
  public void prefetchedAllTimeAxis_noLeakUnderFullIteration() {
    final int baseline = holder.getResourceSession().activeTrxCount();

    final XmlNodeReadOnlyTrx pivot = holder.getXmlNodeReadTrx();
    final PrefetchedAllTimeAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
        new PrefetchedAllTimeAxis<>(pivot.getResourceSession(), pivot);
    while (axis.hasNext()) {
      axis.next().close();
    }
    axis.close();

    waitForCleanerOrTimeout(baseline);
    assertEquals("PrefetchedAllTimeAxis must not leak rtxs after full iteration",
        baseline, holder.getResourceSession().activeTrxCount());
  }

  @Test
  public void prefetchedAllTimeAxis_constructorWithoutIterate_isLazyAndLeakFree() {
    final int baseline = holder.getResourceSession().activeTrxCount();

    final XmlNodeReadOnlyTrx pivot = holder.getXmlNodeReadTrx();
    final PrefetchedAllTimeAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
        new PrefetchedAllTimeAxis<>(pivot.getResourceSession(), pivot);
    // Don't iterate. The lazy contract says no opens until first poll; closing
    // immediately must keep activeTrxCount at baseline.
    axis.close();

    waitForCleanerOrTimeout(baseline);
    assertEquals(
        "PrefetchedAllTimeAxis constructed without iteration must open no rtx",
        baseline, holder.getResourceSession().activeTrxCount());
  }

  @Test
  public void prefetchedAllTimeAxis_abandonAfterOneItem_releasesPrefetched() {
    final int baseline = holder.getResourceSession().activeTrxCount();

    final XmlNodeReadOnlyTrx pivot = holder.getXmlNodeReadTrx();
    final PrefetchedAllTimeAxis<XmlNodeReadOnlyTrx, XmlNodeTrx> axis =
        new PrefetchedAllTimeAxis<>(pivot.getResourceSession(), pivot);
    if (axis.hasNext()) {
      axis.next().close();
    }
    axis.close();

    waitForCleanerOrTimeout(baseline);
    assertEquals(
        "PrefetchedAllTimeAxis must release prefetched rtxs when consumer abandons mid-iteration",
        baseline, holder.getResourceSession().activeTrxCount());
  }

  /**
   * Wait briefly for in-flight virtual threads doing prefetcher cancellation to drop
   * their rtxs. Cooperative cancellation may complete a few microseconds after close()
   * returns; without a wait this test would race the Cleaner thread.
   */
  private void waitForCleanerOrTimeout(final int targetCount) {
    final long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
    while (holder.getResourceSession().activeTrxCount() != targetCount
        && System.nanoTime() < deadline) {
      try {
        Thread.sleep(2);
      } catch (final InterruptedException ie) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }
}
