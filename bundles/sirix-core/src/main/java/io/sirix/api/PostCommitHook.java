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
   * Every hook gets a fresh cursor at the document root, closed on return; do not retain it. The view
   * is never the mutable writer. Delivery follows outermost execution and scheduled task completion.
   * Async delivery follows hardening without blocking successor mutations. Do not navigate a captured
   * mutable writer. KEEP_OPEN retains its lifetime write permit. Commit, rollback/abort, revert,
   * truncate and writer/session close on this resource reject with IllegalStateException before
   * locking, including calls through another user session. RuntimeException failures are logged;
   * Error is retained and rethrown by writer close after independent delivery and owner release.
   * Neither can veto durable publication.
   *
   * @param committedRevision read-only committed revision; valid only during this invocation
   */
  void postCommit(NodeReadOnlyTrx committedRevision);
}
