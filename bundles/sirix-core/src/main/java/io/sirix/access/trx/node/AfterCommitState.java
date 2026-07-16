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
   *   <li>Backend must be FILE_CHANNEL (the MEMORY_MAPPED backend also writes through
   *       {@code FileChannelWriter}, but only FILE_CHANNEL is validated for async rotation)</li>
   *   <li>Only threshold-based auto-commit supported (timed auto-commit not allowed)</li>
   * </ul>
   */
  KEEP_OPEN_ASYNC_FLUSH,

  /**
   * Asynchronous durable commits. Each threshold crossing creates a REAL revision (durable,
   * queryable, with its own commit record), but the durability barriers — index-catalogue fsync,
   * buffered-tail flush, data force, uber-beacon writes — run on a background thread while the
   * transaction continues inserting into the next epoch. The writer thread pays only page
   * serialization (phase 1); readers see a new revision exactly when it hardens
   * (durable-before-visible). Depth-1 pipeline: the next epoch's phase 1 waits for the previous
   * epoch's hardening. A hardening failure poisons the transaction terminally.
   *
   * <p>See {@code docs/ASYNC_COMMIT_DESIGN.md}. Requirements (as for
   * {@link #KEEP_OPEN_ASYNC_FLUSH}): FILE_CHANNEL backend, count-based auto-commit only.</p>
   */
  KEEP_OPEN_ASYNC_COMMIT,

  CLOSE
}
