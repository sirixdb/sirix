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
   *   <li>Backend must be FILE_CHANNEL or MEMORY_MAPPED. Both append through
   *       {@code FileChannelWriter}, and a write transaction's internal storage reads go
   *       through {@code FileChannelReader} on both backends, so the background page flush
   *       is backend-agnostic; memory-mapped read segments (read-only sessions) only ever
   *       resolve pages referenced by a published revision, which the final synchronous
   *       commit orders after all background writes.</li>
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
   * <p>See {@code docs/ASYNC_COMMIT_DESIGN.md}. Requirements: FILE_CHANNEL backend only
   * (stricter than {@link #KEEP_OPEN_ASYNC_FLUSH}, which also allows MEMORY_MAPPED —
   * mid-transaction revision publication is not yet validated against concurrently
   * remapping memory-mapped readers), count-based auto-commit only.</p>
   */
  KEEP_OPEN_ASYNC_COMMIT,

  CLOSE
}
