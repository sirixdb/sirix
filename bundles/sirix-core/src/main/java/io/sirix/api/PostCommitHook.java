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
   * Called after durable publication and the outermost transaction execution/scheduled task
   * boundary, with execution locks released. KEEP_OPEN retains its lifetime write permit.
   * Lifecycle calls on the same transaction/session reject with IllegalStateException before
   * locking. Notification failures are logged; they cannot veto publication. The borrowed current
   * transaction view may include a subsequent mutation completed by the enclosing operation.
   *
   * @param rtx borrowed Sirix transaction view; do not retain after return
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  void postCommit(NodeReadOnlyTrx rtx);
}
