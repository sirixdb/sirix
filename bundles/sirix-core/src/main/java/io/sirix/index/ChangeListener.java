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
}
