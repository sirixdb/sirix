package io.sirix.index;

import io.sirix.access.trx.node.IndexController;
import io.sirix.node.interfaces.immutable.ImmutableNode;

public interface ChangeListener {
  void listen(IndexController.ChangeType type, ImmutableNode node, long pathNodeKey);

  /**
   * Commit-time lifecycle hook, called once per commit by the owning
   * {@link IndexController} after the pre-commit hooks and BEFORE page
   * serialization, so index writes issued here still ride the committing
   * transaction.
   *
   * <p>Listeners whose index entry maps 1:1 onto a change notification
   * (PATH/CAS/NAME/valid-time) maintain their index eagerly inside
   * {@link #listen} and keep the default no-op. Listeners whose index unit
   * aggregates MULTIPLE notifications — a projection row spans every field
   * of a record, and a value replace alone arrives as a DELETE/INSERT pair —
   * buffer the affected keys in {@code listen} and apply the batched
   * maintenance here, when the transaction's final state is known
   * (mirroring {@code PathSummaryWriter}'s deferred statistics, flushed at
   * the same commit point).
   */
  default void beforeCommit() {
  }

  /**
   * Same hook, told whether this is the transaction's FINAL commit rather than one of the
   * intermediate auto-commits a bulk insert fires every {@code -Dsirix.autoCommit.nodes} nodes.
   *
   * <p>The distinction only matters to a listener that builds an index ACROSS commits: a load-time
   * projection build streams full leaves into every intermediate commit but can only write its
   * dictionaries, fingerprint blocks and metadata once no more records are coming, and the final
   * commit is the only signal the transaction gives for that. Every other listener ignores the flag —
   * the default forwards to {@link #beforeCommit()} so nothing else has to change.
   */
  default void beforeCommit(final boolean finalCommit) {
    beforeCommit();
  }

  /**
   * The transaction is about to write its pages out WITHOUT committing — the asynchronous pre-flush a
   * bulk import uses instead of intermediate commits.
   *
   * <p>Only matters to a listener that maintains its index by re-reading the changed records, which
   * the projection listener does: extraction reads the record's whole subtree back. Those records are
   * reachable while their pages sit in the transaction's log, and unreachable once the flush has
   * written them out into a revision that is not committed yet — so anything deferred past this point
   * is deferred past the last moment it could be read.
   */
  default void beforePageFlush() {
  }

  /**
   * The owning write transaction is discarding its current lineage rather than committing it.
   *
   * <p>Ordinary entry-at-a-time listeners keep no state beyond the transaction's page log and use
   * this no-op. A listener whose work deliberately spans successful intermediate commits must retire
   * that external state here: rollback, revert and a clean close all sever the lineage that made the
   * accumulated state valid. This hook is intentionally separate from listener rebinding because a
   * successful intermediate commit also rebinds listeners and must preserve that state.</p>
   */
  default void transactionAborted() {
  }

  /**
   * Structural lifecycle hook: the transaction performed subtree surgery —
   * currently a MOVE — whose per-node notifications cannot express the
   * change completely (moved plain containers and value elements fire no
   * per-node events, and a moved record continues to exist outside its old
   * record set). Entry-level indexes (PATH/CAS/NAME/valid-time) are
   * maintained by the move's per-node DELETE/INSERT pairs where those exist
   * and keep the default no-op.
   */
  default void structuralChange() {
  }

  default void beforeStructuralChange(final long movedNodeKey) {
    structuralChange();
  }

  default void afterStructuralChange(final long movedNodeKey) {
  }

  default void structuralChangeAborted(final long movedNodeKey) {
  }
}
