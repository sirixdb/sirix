package io.sirix.axis.temporal;

import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.IntSupplier;

/**
 * Look-ahead prefetcher for the multi-revision temporal axes
 * ({@link PrefetchedAllTimeAxis}, {@link PrefetchedPastAxis}, {@link PrefetchedFutureAxis}).
 *
 * <p>The naive temporal axes pay one full {@link ResourceSession#beginNodeReadOnlyTrx(int)}
 * per yielded revision — UberPage navigation, RevisionRootPage load, indirect-page
 * traversal — sequentially. While the consumer processes the rtx for revision N, the
 * cores spent on opening N+1 are otherwise idle. The prefetcher overlaps those opens
 * with consumer work using a fixed-depth queue of {@link CompletableFuture}s, each
 * spawning a {@link Thread#startVirtualThread virtual thread} to do its trx open.
 *
 * <h2>HFT properties</h2>
 * <ul>
 *   <li>Bounded memory: at most {@code depth} rtx in flight + the one being yielded.</li>
 *   <li>No long-lived executor — each task starts a virtual thread directly. No
 *       per-axis pool, no shutdown coordination.</li>
 *   <li>Backpressure-free reads: poll() blocks on the head future via {@code .join()};
 *       the consumer's processing time gates the prefetch rate naturally.</li>
 *   <li>Leak-free shutdown: pending futures attach a {@code thenAccept} that closes
 *       their rtx as soon as the open completes, even after the axis is abandoned.</li>
 * </ul>
 *
 * <p>Not thread-safe: an axis is single-consumer by contract.
 */
final class RevisionPrefetcher<R extends NodeReadOnlyTrx & NodeCursor,
    W extends NodeTrx & NodeCursor> implements AutoCloseable {

  /** Default look-ahead depth — overlaps roughly four trx opens behind the consumer. */
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

  private final ResourceSession<R, W> resourceSession;
  private final long nodeKey;
  private final IntSupplier nextRevision;
  private final int depth;
  private final ArrayDeque<CompletableFuture<RtxResult<R>>> queue;

  /**
   * @param resourceSession the session each prefetch task opens an rtx on
   * @param nodeKey         the record to {@code moveTo} after each open
   * @param nextRevision    yields the next revision number to fetch, or {@code -1}
   *                        once the iteration is exhausted
   * @param depth           look-ahead window — bounded number of in-flight opens
   */
  RevisionPrefetcher(final ResourceSession<R, W> resourceSession, final long nodeKey,
      final IntSupplier nextRevision, final int depth) {
    this.resourceSession = resourceSession;
    this.nodeKey = nodeKey;
    this.nextRevision = nextRevision;
    this.depth = depth;
    this.queue = new ArrayDeque<>(depth);
    for (int i = 0; i < depth; i++) {
      if (!submitNext()) {
        break;
      }
    }
  }

  /**
   * Submit one more open task to the back of the queue, drawn from {@link #nextRevision}.
   * Returns {@code false} when the iterator is exhausted (no more revisions to fetch).
   */
  private boolean submitNext() {
    final int rev = nextRevision.getAsInt();
    if (rev < 0) {
      return false;
    }
    queue.offer(CompletableFuture.supplyAsync(() -> {
      final R rtx = resourceSession.beginNodeReadOnlyTrx(rev);
      final boolean ok = rtx.moveTo(nodeKey);
      return new RtxResult<>(rtx, ok);
    }, VIRTUAL_THREAD_EXECUTOR));
    return true;
  }

  /**
   * Block on the head future, return its result, then top up the pipeline so the
   * window stays full. Returns {@code null} when no more results will be produced.
   */
  RtxResult<R> poll() {
    final CompletableFuture<RtxResult<R>> head = queue.poll();
    if (head == null) {
      return null;
    }
    final RtxResult<R> result = head.join();
    submitNext();
    return result;
  }

  /**
   * Releases all in-flight prefetched rtx — those completed get closed inline; those
   * still in flight have a continuation attached that closes them on completion. Safe
   * to call multiple times.
   */
  @Override
  public void close() {
    while (!queue.isEmpty()) {
      final CompletableFuture<RtxResult<R>> f = queue.poll();
      f.thenAccept(result -> {
        if (result != null && result.rtx != null) {
          result.rtx.close();
        }
      }).exceptionally(ex -> null);
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
}
