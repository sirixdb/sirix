package io.sirix.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/** Ownership fences for executors whose workers borrow caller-owned state. */
public final class WorkerTermination {
  private WorkerTermination() {}

  /**
   * Stop accepting work and wait for every worker to exit, even if the owner is interrupted.
   * Cancellation is only a request: neither a cancelled future nor a timeout permits the owner to
   * reuse or free worker state. Restore interruption only after termination is established.
   */
  public static void shutdownAndAwait(final ExecutorService executor, final boolean cancel) {
    requireNonNull(executor);
    boolean interrupted = Thread.interrupted();
    try {
      if (cancel || interrupted) {
        executor.shutdownNow();
      } else {
        executor.shutdown();
      }
      while (!executor.isTerminated()) {
        try {
          executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
        } catch (final InterruptedException _) {
          interrupted = true;
          executor.shutdownNow();
        }
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
