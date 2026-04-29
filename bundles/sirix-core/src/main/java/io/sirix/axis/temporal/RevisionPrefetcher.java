package io.sirix.axis.temporal;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;

import java.util.ArrayDeque;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.IntSupplier;

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
 * <h2>What runs in parallel — and what doesn't</h2>
 * <ul>
 *   <li>{@code beginNodeReadOnlyTrx} is {@code synchronized} on the resource session, so
 *       its body (RevisionRoot load, document-node fetch, trx-map registration) executes
 *       one task at a time. After a warm cache this is a small fraction of the per-yield
 *       cost.</li>
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
 *   <li>{@link #close()} flips a flag observed by every supplier; pending tasks
 *       short-circuit before opening a trx, in-flight tasks close their rtx inline once
 *       the open returns, and futures already completed have their rtx closed via a
 *       {@code whenComplete} callback. Idempotent.</li>
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
   * Cancels every pending task, closes any rtx that already completed but was not
   * yielded, and prevents future {@link #poll()} calls from producing results. Tasks
   * that finish after this returns observe the {@link #closed} flag and close their own
   * rtx. Idempotent.
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  public void close() {
    if (closed) {
      return;
    }
    closed = true;
    while (!queue.isEmpty()) {
      final CompletableFuture<RtxResult<R>> f = queue.poll();
      // cancel(true) on a CompletableFuture only flips its state — it does NOT interrupt
      // the supplyAsync body. The body cooperates by reading `closed` and self-closing
      // any rtx it opened. whenComplete catches the case where the future already
      // completed normally before we got here, so its rtx still gets released.
      f.cancel(true);
      f.whenComplete((BiConsumer) CLOSE_RESULT_RTX);
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
