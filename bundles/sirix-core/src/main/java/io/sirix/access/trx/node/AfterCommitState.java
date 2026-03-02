package io.sirix.access.trx.node;

public enum AfterCommitState {
  KEEP_OPEN,

  /**
   * Async TIL rotation mode. Intermediate auto-commits rotate the TIL via O(1) array swap
   * and flush KVL pages to disk in the background. No intermediate revisions are created —
   * only the final explicit {@code commit()} creates a revision.
   *
   * <p>Requirements:
   * <ul>
   *   <li>Backend must be FILE_CHANNEL (MMFileWriter deferred to v2)</li>
   *   <li>Only threshold-based auto-commit supported (timed auto-commit not allowed)</li>
   * </ul>
   */
  KEEP_OPEN_ASYNC,

  CLOSE
}
