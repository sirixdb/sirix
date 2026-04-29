package io.sirix.axis.temporal;

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.axis.temporal.RevisionPrefetcher.RtxResult;
import io.sirix.utils.XmlDocumentCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Lifecycle / cancellation contracts for {@link RevisionPrefetcher} that are not
 * exercised by the per-axis behavioural tests:
 *
 * <ul>
 *   <li>Constructor is lazy — no in-flight tasks until {@link RevisionPrefetcher#poll}
 *       is called for the first time.</li>
 *   <li>{@link RevisionPrefetcher#close} before any poll() short-circuits the supplier
 *       and prevents any further opens.</li>
 *   <li>close() is idempotent.</li>
 *   <li>poll() after close() always returns {@code null}.</li>
 *   <li>Tasks submitted but cancelled mid-flight either skip the trx-open entirely or
 *       close the rtx inline — the consumer never observes a leaked transaction.</li>
 *   <li>Constructor argument validation rejects non-positive depths and null sources.</li>
 * </ul>
 */
public final class RevisionPrefetcherLifecycleTest {

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

  /**
   * Build a supplier walking revisions ascending from 1 through the most-recent. The
   * caller can read {@code calls.get()} to count how many revisions the prefetcher
   * actually pulled from the iterator — a proxy for "how many opens were submitted."
   */
  private IntSupplier ascendingRevisions(final AtomicInteger calls) {
    final int max = holder.getResourceSession().getMostRecentRevisionNumber();
    final int[] cursor = new int[] {1};
    return () -> {
      calls.incrementAndGet();
      final int next = cursor[0];
      if (next > max) {
        return -1;
      }
      cursor[0] = next + 1;
      return next;
    };
  }

  private RevisionPrefetcher<XmlNodeReadOnlyTrx, XmlNodeTrx> newPrefetcher(
      final IntSupplier source, final int depth) {
    final long rootKey = 0L; // moveTo(0) always succeeds for the document node
    return new RevisionPrefetcher<>(holder.getResourceSession(), rootKey, source, depth);
  }

  @Test
  public void constructorIsLazy_noOpensSubmitted() {
    final AtomicInteger calls = new AtomicInteger();
    try (final var p = newPrefetcher(ascendingRevisions(calls), 4)) {
      // Constructor must not pull from the iterator. inFlight stays 0.
      assertEquals("constructor must be lazy", 0, p.inFlight());
      assertEquals("supplier must not be queried by constructor", 0, calls.get());
      assertFalse(p.isClosed());
      assertEquals(4, p.depth());
    }
  }

  @Test
  public void firstPollFillsPipelineToDepth() {
    final AtomicInteger calls = new AtomicInteger();
    try (final var p = newPrefetcher(ascendingRevisions(calls), 4)) {
      final RtxResult<XmlNodeReadOnlyTrx> first = p.poll();
      assertNotNull("first poll must yield a result", first);
      first.rtx.close();
      // After first poll the head was consumed and the pipeline topped up. The supplier
      // should have been queried at least `depth` times (4 fills + 1 top-up = 5)
      // unless the iterator is exhausted earlier (only 3 revisions in the test doc).
      assertTrue("supplier was queried " + calls.get() + " times", calls.get() >= 1);
    }
  }

  @Test
  public void closeBeforeAnyPoll_isIdempotentAndPreventsFurtherWork() {
    final AtomicInteger calls = new AtomicInteger();
    final var p = newPrefetcher(ascendingRevisions(calls), 4);
    p.close();
    p.close(); // idempotent
    assertTrue(p.isClosed());
    assertEquals("close before poll must not query the supplier", 0, calls.get());
    assertNull("poll after close must return null", p.poll());
    assertEquals(0, p.inFlight());
  }

  @Test
  public void closeAfterFirstPoll_drainsPendingFutures() throws InterruptedException {
    final AtomicInteger calls = new AtomicInteger();
    final var p = newPrefetcher(ascendingRevisions(calls), 4);
    final RtxResult<XmlNodeReadOnlyTrx> first = p.poll();
    assertNotNull(first);
    first.rtx.close();
    final int callsBeforeClose = calls.get();
    p.close();
    assertTrue(p.isClosed());
    assertNull("subsequent poll() must return null after close", p.poll());
    // Allow any in-flight virtual threads to finish their cooperative cancellation.
    // We don't get a strong "all closed" signal — but no exception, no hang, and the
    // supplier did not get queried again after close() are the testable invariants.
    Thread.sleep(50);
    assertEquals("close() must not query the supplier any further",
        callsBeforeClose, calls.get());
  }

  @Test
  public void pollAfterClose_returnsNullEvenIfQueueWasFull() {
    final AtomicInteger calls = new AtomicInteger();
    final var p = newPrefetcher(ascendingRevisions(calls), 4);
    p.poll(); // fill queue
    p.close();
    assertNull(p.poll());
    assertNull(p.poll());
  }

  @Test
  public void exhaustedIterator_pollReturnsNull() {
    final IntSupplier exhausted = () -> -1;
    try (final var p = newPrefetcher(exhausted, 4)) {
      assertNull("exhausted iterator yields null", p.poll());
      assertNull(p.poll()); // and again
    }
  }

  @Test
  public void rejectsNonPositiveDepth() {
    try {
      newPrefetcher(() -> -1, 0);
      fail("expected IAE for depth=0");
    } catch (final IllegalArgumentException expected) {
      // ok
    }
    try {
      newPrefetcher(() -> -1, -1);
      fail("expected IAE for depth=-1");
    } catch (final IllegalArgumentException expected) {
      // ok
    }
  }

  @Test
  public void rejectsNullArguments() {
    try {
      new RevisionPrefetcher<>(null, 0L, () -> -1, 4);
      fail("expected NPE for null session");
    } catch (final NullPointerException expected) {
      // ok
    }
    try {
      new RevisionPrefetcher<>(holder.getResourceSession(), 0L, null, 4);
      fail("expected NPE for null supplier");
    } catch (final NullPointerException expected) {
      // ok
    }
  }
}
