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
   * Runs synchronously in the writer scope before publication; an exception or Error vetoes the
   * commit with its original identity. The hook must not commit, roll back, close or truncate any
   * handle, session or writer transaction of the same database. Such lifecycle calls reject with
   * IllegalStateException before locks or waits, including through nested callbacks. Ordinary
   * independent read-only work remains allowed. Concurrent retirement may reject hook admission
   * before publication; the writer remains available for retry. External close waits only up to the
   * configured callback deadline and retains active reader/writer ownership on timeout.
   *
   * @param rtx borrowed Sirix transaction view
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  void preCommit(NodeReadOnlyTrx rtx);
}
