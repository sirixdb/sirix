package io.sirix.api;

/**
 * Pre commit hook.
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionalInterface
public interface PreCommitHook {
  /**
   * Called before publication; an exception vetoes the commit. Lifecycle calls (commit,
   * rollback/abort, transaction close or session close) on the same transaction/session are
   * forbidden inside every pre/post hook and revision listener, and reject with
   * IllegalStateException before locking. Ordinary read-only work remains allowed.
   *
   * @param rtx borrowed Sirix transaction view
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  void preCommit(NodeReadOnlyTrx rtx);
}
