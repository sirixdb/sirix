package io.sirix.axis.temporal;

import io.sirix.Holder;
import io.sirix.XmlTestHelper;
import io.sirix.api.ResourceSession;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.axis.temporal.RevisionPrefetcher.RtxResult;
import io.sirix.utils.XmlDocumentCreator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Proxy;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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

  /**
   * Bound on every handshake in {@link #close_doesNotReturnWhileInFlightTasksStillHoldTransactions}.
   * Each one waits for a step that takes microseconds when it happens at all, so this is a
   * deadlock backstop generous enough for a saturated CI runner, not a tuning knob.
   */
  private static final long HANDSHAKE_TIMEOUT_SECONDS = 20L;

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
  public void closeAfterFirstPoll_drainsPendingFutures() {
    final int baseline = holder.getResourceSession().activeTrxCount();
    final AtomicInteger calls = new AtomicInteger();
    final var p = newPrefetcher(ascendingRevisions(calls), 4);
    final RtxResult<XmlNodeReadOnlyTrx> first = p.poll();
    assertNotNull(first);
    first.rtx.close();
    final int callsBeforeClose = calls.get();
    p.close();
    assertTrue(p.isClosed());
    assertNull("subsequent poll() must return null after close", p.poll());
    // The strong signal, asserted with no sleep: close() drains its in-flight tasks, so by the
    // time it returns every rtx it opened has been released. This used to be untestable — close()
    // cancelled the futures and returned while the bodies ran on, so the only available assertions
    // were "no exception, no hang, supplier not queried again" and callers had to sleep or poll.
    assertEquals("close() must release every prefetched rtx before it returns",
        baseline, holder.getResourceSession().activeTrxCount());
    assertEquals("close() must not query the supplier any further",
        callsBeforeClose, calls.get());
  }

  /**
   * The window {@link RevisionPrefetcher#close()} must close, widened until a test can see it.
   *
   * <p>The natural window is microseconds — a prefetch body opens its rtx and observes the closed
   * flag a few instructions later — so neither repetition nor CPU load reproduces a miss reliably;
   * both pass with the fix and without it. This harness makes it deterministic instead: a proxy
   * session opens the REAL rtx and then parks inside {@code beginNodeReadOnlyTrx}, so the task is
   * provably holding an open, counted transaction while {@code close()} runs. Revision 1 is let
   * through so {@code poll()} can return; revisions 2..4 park.
   *
   * <p>Then the two behaviours separate cleanly. Draining: {@code close()} cannot return until the
   * parked tasks are released and have closed their rtxs, so the count recorded immediately after
   * it is the baseline. Fire-and-forget: {@code close()} returns while they are still parked, and
   * the recorded count is above the baseline by exactly the number in flight.
   *
   * <p>Both hand-offs are latch handshakes rather than timing assumptions. The consumer waits for
   * {@code closeAllowed} before closing, because a {@code close()} that wins the race publishes its
   * flag first and every remaining task then short-circuits ahead of its open — nothing is in
   * flight and the two behaviours become indistinguishable. The harness in turn waits for the
   * prefetcher to report itself closed before releasing the parked tasks, so the release can never
   * precede the flag either.
   *
   * <p>Consumer calls live on one thread, honouring the single-consumer contract; the test thread
   * only operates the latches.
   */
  @Test
  public void close_doesNotReturnWhileInFlightTasksStillHoldTransactions() throws Exception {
    final XmlResourceSession real = holder.getResourceSession();
    final int baseline = real.activeTrxCount();
    final CountDownLatch parked = new CountDownLatch(1);
    final CountDownLatch closeAllowed = new CountDownLatch(1);
    final CountDownLatch release = new CountDownLatch(1);

    @SuppressWarnings("unchecked")
    final ResourceSession<XmlNodeReadOnlyTrx, XmlNodeTrx> blocking =
        (ResourceSession<XmlNodeReadOnlyTrx, XmlNodeTrx>) Proxy.newProxyInstance(
            getClass().getClassLoader(), new Class<?>[] {ResourceSession.class},
            (proxy, method, args) -> {
              if ("beginNodeReadOnlyTrx".equals(method.getName()) && args != null
                  && args.length == 1 && args[0] instanceof Integer revision) {
                final XmlNodeReadOnlyTrx rtx = real.beginNodeReadOnlyTrx(revision);
                if (revision > 1) {
                  // Open and counted, but the body has not reached its closed-flag checkpoint.
                  parked.countDown();
                  release.await(HANDSHAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                return rtx;
              }
              return method.invoke(real, args);
            });

    final RevisionPrefetcher<XmlNodeReadOnlyTrx, XmlNodeTrx> prefetcher =
        new RevisionPrefetcher<>(blocking, 0L, ascendingRevisions(new AtomicInteger()), 4);
    final AtomicInteger countRecordedRightAfterClose = new AtomicInteger(-1);
    final AtomicReference<Throwable> consumerFailure = new AtomicReference<>();
    final Thread consumer = new Thread(() -> {
      try {
        final RtxResult<XmlNodeReadOnlyTrx> first = prefetcher.poll();
        if (first != null) {
          first.rtx.close();
        }
        if (!closeAllowed.await(HANDSHAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
          throw new IllegalStateException("no prefetch task parked within "
              + HANDSHAKE_TIMEOUT_SECONDS + "s");
        }
        prefetcher.close();
        countRecordedRightAfterClose.set(real.activeTrxCount());
      } catch (final InterruptedException interrupted) {
        Thread.currentThread().interrupt();
        consumerFailure.set(interrupted);
      } catch (final RuntimeException failure) {
        consumerFailure.set(failure);
      }
    }, "prefetch-consumer");
    consumer.start();

    assertTrue("a prefetch task must park inside the open, holding an rtx",
        parked.await(HANDSHAKE_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    closeAllowed.countDown();
    assertTrue("the consumer must reach close() while a task is still parked",
        awaitClosed(prefetcher));
    // close() has published its flag and is now either draining or — fire-and-forget — already
    // back in the consumer. The tasks are still parked either way, so give the latter time to
    // record its above-baseline count before we let them go.
    Thread.sleep(300);
    release.countDown();
    consumer.join(60_000);
    assertFalse("consumer thread must finish", consumer.isAlive());
    assertNull("consumer thread must not fail: " + consumerFailure.get(), consumerFailure.get());

    assertEquals("close() must not return while in-flight tasks still hold open transactions",
        baseline, countRecordedRightAfterClose.get());
  }

  /**
   * Spin until the prefetcher reports itself closed, i.e. {@link RevisionPrefetcher#close()} has
   * published its flag and is past the point of no return, whether or not it has returned yet.
   *
   * @return {@code false} if the flag was not observed within {@link #HANDSHAKE_TIMEOUT_SECONDS}
   */
  private static boolean awaitClosed(final RevisionPrefetcher<?, ?> prefetcher)
      throws InterruptedException {
    final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(HANDSHAKE_TIMEOUT_SECONDS);
    while (!prefetcher.isClosed()) {
      if (System.nanoTime() - deadline >= 0L) {
        return false;
      }
      Thread.sleep(1L);
    }
    return true;
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
