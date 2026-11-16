package io.sirix.api;

/**
 * Post commit hook.
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionalInterface
public interface PostCommitHook {

  /**
   * Called with a dispatcher-owned read-only cursor pinned to exactly the revision just committed.
   * Every hook gets a fresh cursor at the document root, closed on return; do not retain it.
   * The view is never the mutable writer. Delivery follows outermost execution and scheduled task
   * completion. Async delivery follows hardening without blocking successor mutations.
   * Lifecycle calls on the committing transaction/session reject before locking. RuntimeException
   * failures are logged; Error is retained and rethrown by writer close after independent delivery
   * and owner release. Neither can veto durable publication.
   *
   * @param committedRevision read-only committed revision; valid only during this invocation
   */
  void postCommit(NodeReadOnlyTrx committedRevision);
}
