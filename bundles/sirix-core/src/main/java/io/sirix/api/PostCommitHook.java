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
   * Delivered asynchronously after durable publication by the database notification dispatcher,
   * ordered per resource. Every invocation owns a fresh read-only cursor pinned to exactly the
   * committed revision at the document root. The dispatcher closes it on return; do not retain it or
   * use a captured mutable writer. Commit and successor mutations never wait for delivery.
   * Same-database lifecycle calls, including through another handle or session, reject before locks
   * with IllegalStateException. Independent read-only reader open/close remains allowed. Delivery is
   * best effort: capacity overflow drops new jobs without delaying or vetoing commits. Handlers
   * obtain the committed revision number from this view. Resume/replay is not provided. Close drains
   * only up to its configured deadline and may drop queued notifications. A still-running callback
   * retains its readers/storage and causes an explicit close timeout for retry. Callback failures are
   * reported by the dispatcher and cannot veto durable publication.
   *
   * @param committedRevision read-only committed revision; valid only during this invocation
   */
  void postCommit(NodeReadOnlyTrx committedRevision);
}
