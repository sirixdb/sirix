package io.sirix.axis.temporal;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;

import java.util.ArrayDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.IntSupplier;

import io.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;

/**
 * Look-ahead prefetcher for the multi-revision temporal axes
 * ({@link PrefetchedAllTimeAxis}, {@link PrefetchedPastAxis}, {@link PrefetchedFutureAxis}).
 *
 * <p>Each yielded revision costs a {@link ResourceSession#beginNodeReadOnlyTrx(int)} plus
 * a {@link io.sirix.api.NodeCursor#moveTo(long)}. The prefetcher overlaps those steps
 * with consumer-side work using a fixed-depth queue of {@link CompletableFuture}s, each
 * spawning a {@link Thread#startVirtualThread virtual thread} that opens the trx and
 * walks to the target node.
 *
 * <h2>What runs in parallel</h2>
 * <ul>
 *   <li>{@code beginNodeReadOnlyTrx} is not synchronized on the resource session, so its body
 *       (RevisionRoot load, document-node fetch, trx-map registration) runs concurrently across
 *       in-flight tasks. After a warm cache this is a small fraction of the per-yield cost.</li>
 *   <li>The subsequent {@code rtx.moveTo(nodeKey)} runs on per-trx state with the shared
 *       page cache and traverses the indirect-page index for the target node. Multiple
 *       in-flight tasks walk different revisions of that index in parallel — that is
 *       where the depth-{@code N} pipeline pays off, especially for deep histories.</li>
 * </ul>
 *
 * <h2>Lifecycle</h2>
 * <ul>
 *   <li>Lazy: the constructor submits nothing. The first call to {@link #poll()} fills
 *       the pipeline up to {@code depth}. A consumer that constructs the axis but never
 *       iterates pays no I/O cost.</li>
 *   <li>Bounded: at most {@code depth} rtx in flight + the one being yielded.</li>
 *   <li>{@link #close()} flips a flag observed by every supplier — pending tasks short-circuit
 *       before opening a trx, in-flight tasks close their rtx inline once the open returns, and
 *       futures already completed have their rtx closed via a {@code whenComplete} callback — and
 *       then WAITS for those tasks, so every rtx it owns is released by the time it returns.
 *       Idempotent.</li>
 * </ul>
 *
 * <p>Single-consumer by contract: {@link #poll()} and {@link #close()} must be called by
 * the same thread (typically the axis's iterator thread). The supplier bodies run on
 * their own virtual threads and read the {@link #closed} flag through a {@code volatile}
 * publish.
 */
final class RevisionPrefetcher<R extends NodeReadOnlyTrx & NodeCursor,
    W extends NodeTrx & NodeCursor> implements AutoCloseable {

  /** Default look-ahead depth. */
  static final int DEFAULT_DEPTH = 4;

  /** Carry the rtx and the moveTo result so the consumer can branch without reopening. */
  static final class RtxResult<R extends NodeReadOnlyTrx & NodeCursor> {
    final R rtx;
    final boolean nodeFound;

    RtxResult(final R rtx, final boolean nodeFound) {
      this.rtx = rtx;
      this.nodeFound = nodeFound;
    }
  }

  private static final Executor VIRTUAL_THREAD_EXECUTOR = Thread::startVirtualThread;

  private static final LogWrapper LOGGER =
      new LogWrapper(LoggerFactory.getLogger(RevisionPrefetcher.class));

  /**
   * Upper bound on how long {@link #close()} waits for in-flight tasks. A task that has observed
   * {@link #closed} does at most one trx-open plus a {@code moveTo} before finishing, so this is a
   * safety net against a pathological stall, not a tuning knob — hitting it means something else
   * is badly wrong.
   */
  private static final long CLOSE_DRAIN_TIMEOUT_SECONDS = 30L;

  /**
   * Hoisted, capture-free close-on-complete callback. Used by {@link #close()} for every
   * pending future so that an rtx already opened by a finished supplier is still
   * released even though the consumer abandoned the axis. Static + capture-free →
   * single instance for the JVM lifetime → zero allocation per close().
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private static final BiConsumer<RtxResult, Throwable> CLOSE_RESULT_RTX = (result, ex) -> {
    if (result != null && result.rtx != null) {
      result.rtx.close();
    }
  };

  private final ResourceSession<R, W> resourceSession;
  private final long nodeKey;
  private final IntSupplier nextRevision;
  private final int depth;
  private final ArrayDeque<CompletableFuture<RtxResult<R>>> queue;

  /** Set by {@link #close()}; suppliers observe this and abort or close-inline. */
  private volatile boolean closed;

  /**
   * @param resourceSession the session each prefetch task opens an rtx on
   * @param nodeKey         the record to {@code moveTo} after each open
   * @param nextRevision    yields the next revision number to fetch, or a negative value
   *                        once the iteration is exhausted
   * @param depth           look-ahead window, must be {@code > 0}
   */
  RevisionPrefetcher(final ResourceSession<R, W> resourceSession, final long nodeKey,
      final IntSupplier nextRevision, final int depth) {
    this.resourceSession = requireNonNull(resourceSession, "resourceSession");
    this.nextRevision = requireNonNull(nextRevision, "nextRevision");
    if (depth <= 0) {
      throw new IllegalArgumentException("depth must be > 0, got " + depth);
    }
    this.nodeKey = nodeKey;
    this.depth = depth;
    this.queue = new ArrayDeque<>(depth);
  }

  /**
   * Top up the in-flight queue to {@link #depth}. No-op once {@link #closed} is set or
   * the revision iterator is exhausted.
   */
  private void fillToDepth() {
    if (closed) {
      return;
    }
    while (queue.size() < depth && submitNext()) {
      // keep filling
    }
  }

  /** Submit one more open task. Returns {@code false} if the iterator is exhausted. */
  private boolean submitNext() {
    final int rev = nextRevision.getAsInt();
    if (rev < 0) {
      return false;
    }
    queue.offer(CompletableFuture.supplyAsync(() -> {
      // Cooperative cancellation point #1: skip the trx-open entirely if close() has
      // already been called. No rtx allocated → nothing to leak.
      // (CompletableFuture.cancel(true) does NOT interrupt this body — Java contract
      //  explicitly. So we don't bother reading the thread interrupt flag.)
      if (closed) {
        return null;
      }
      final R rtx = resourceSession.beginNodeReadOnlyTrx(rev);
      // Cooperative cancellation point #2: close() ran while we held the session monitor
      // or did the trx-open. The rtx is solely ours — close it inline rather than handing
      // a phantom to a consumer that has already abandoned the axis.
      if (closed) {
        rtx.close();
        return null;
      }
      final boolean ok = rtx.moveTo(nodeKey);
      return new RtxResult<>(rtx, ok);
    }, VIRTUAL_THREAD_EXECUTOR));
    return true;
  }

  /**
   * Block on the head future and return its result; returns {@code null} when no more
   * results will be produced (iterator exhausted or {@link #close()} called).
   */
  RtxResult<R> poll() {
    if (closed) {
      return null;
    }
    fillToDepth();
    final CompletableFuture<RtxResult<R>> head = queue.poll();
    if (head == null) {
      return null;
    }
    final RtxResult<R> result;
    try {
      result = head.join();
    } catch (final CancellationException | CompletionException ex) {
      // Either close() raced us (cancelled) or the supplier threw — the supplier's own
      // close-inline path released any rtx it managed to open.
      return null;
    }
    fillToDepth();
    return result;
  }

  /**
   * Prevents future {@link #poll()} calls from producing results and releases every rtx this
   * prefetcher owns — <b>synchronously</b>: when this returns, no in-flight task is still holding
   * one. Idempotent.
   *
   * <p><b>Why it waits, and why it must not cancel.</b> {@code cancel(true)} completes the future
   * at once but does NOT interrupt the {@code supplyAsync} body (Java's documented contract), so
   * the body runs on, opens its rtx, and only then observes {@link #closed} and closes it. The
   * {@code whenComplete} callback meanwhile fires immediately with a {@link CancellationException}
   * and no result, releasing nothing. Release was therefore asynchronous and unobservable to the
   * caller: {@code close()} returned while rtxs it owned were still open, which is why callers
   * had to poll {@code activeTrxCount()} and why doing so flaked under load. Not cancelling lets
   * each future complete naturally — the bodies short-circuit on the flag we just published — and
   * joining them makes the release part of {@code close()}'s contract.
   *
   * <p>Blocking the consumer thread on these tasks is not a new hazard: {@link #poll()} already
   * does exactly that via {@code head.join()}.
   *
   * <p>The wait is bounded by {@link #CLOSE_DRAIN_TIMEOUT_SECONDS} so a pathologically stalled
   * task degrades to the previous behaviour — the flag is set, so the body still closes its own
   * rtx — rather than hanging the consumer forever.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true; // publish BEFORE draining, so every body short-circuits at its next checkpoint
    if (queue.isEmpty()) {
      return;
    }
    // whenComplete returns a future that completes only AFTER the callback has run, so joining
    // these is what guarantees CLOSE_RESULT_RTX actually executed for every task.
    final CompletableFuture<?>[] draining = new CompletableFuture<?>[queue.size()];
    int i = 0;
    for (final CompletableFuture<RtxResult<R>> pending : queue) {
      draining[i++] = pending.whenComplete((BiConsumer) CLOSE_RESULT_RTX);
    }
    queue.clear();
    try {
      CompletableFuture.allOf(draining).get(CLOSE_DRAIN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final InterruptedException interrupted) {
      Thread.currentThread().interrupt();
    } catch (final ExecutionException | CancellationException taskFailed) {
      // A body threw, or something else cancelled it; its own close-inline path released the rtx.
    } catch (final TimeoutException stalled) {
      LOGGER.warn("Prefetch tasks did not drain within {}s of close(); their rtxs will be released"
          + " asynchronously by the tasks themselves.", CLOSE_DRAIN_TIMEOUT_SECONDS);
    }
  }

  /** Diagnostic accessor — current number of in-flight prefetches. */
  int inFlight() {
    return queue.size();
  }

  /** Diagnostic accessor — configured look-ahead depth. */
  int depth() {
    return depth;
  }

  /** Diagnostic accessor — whether {@link #close()} has been called. */
  boolean isClosed() {
    return closed;
  }
}
