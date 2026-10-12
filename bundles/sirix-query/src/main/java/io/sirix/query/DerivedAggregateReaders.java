package io.sirix.query;

import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.api.ResourceSession;
import io.sirix.page.UberPage;
import org.jspecify.annotations.Nullable;

/**
 * Exact derived snapshots owned by a query context's coordinating thread. A borrowed reader is
 * valid through the current aggregate evaluation; the next borrow may evict it. Other threads use
 * their source reader's own snapshot instead, so mutable storage readers are never shared.
 */
final class DerivedAggregateReaders implements AutoCloseable {
  private static final int CAPACITY = 64;
  private final Thread owner = Thread.currentThread();
  private final Snapshot[] snapshots = new Snapshot[CAPACITY];
  private volatile boolean closed;

  private record Snapshot(ResourceSession<?, ?> session, UberPage publication, int revision,
      StorageEngineReader reader) {
  }

  @Nullable
  StorageEngineReader borrow(final StorageEngineReader source) {
    if (closed) {
      throw new IllegalStateException("Query aggregate readers are closed");
    }
    if (source instanceof StorageEngineWriter || source.hasTrxIntentLog()) {
      return null;
    }
    if (Thread.currentThread() != owner) {
      return source.getDerivedIndexReader();
    }
    final int slot =
        (System.identityHashCode(source.getResourceSession()) + source.getRevisionNumber()) & (CAPACITY - 1);
    final Snapshot snapshot = snapshots[slot];
    // The session publishes a fresh UberPage object on commit, truncate and reopen. Its identity
    // distinguishes histories even when truncate reissues the same revision AND timestamp. Never
    // use revision/timestamp equality alone to bypass the persisted slab-identity validation.
    if (snapshot != null && !snapshot.reader().isClosed() && snapshot.session() == source.getResourceSession()
        && snapshot.revision() == source.getRevisionNumber() && snapshot.publication() == source.getUberPage()) {
      return snapshot.reader();
    }
    snapshots[slot] = null;
    if (snapshot != null) {
      snapshot.reader().close();
    }
    final StorageEngineReader opened = source.getResourceSession().openDerivedIndexReader(source);
    // Lag is retryable: the maintainer may publish a mapping without another source commit.
    if (opened != null) {
      snapshots[slot] =
          new Snapshot(source.getResourceSession(), source.getUberPage(), source.getRevisionNumber(), opened);
    }
    return opened;
  }

  /** The owning context must quiesce evaluations before closing, as for its stores. */
  @Override
  public void close() {
    closed = true;
    RuntimeException failure = null;
    for (int slot = 0; slot < CAPACITY; slot++) {
      final Snapshot snapshot = snapshots[slot];
      snapshots[slot] = null;
      if (snapshot != null) {
        try {
          snapshot.reader().close();
        } catch (final RuntimeException closeFailure) {
          if (failure == null) {
            failure = closeFailure;
          } else {
            failure.addSuppressed(closeFailure);
          }
        }
      }
    }
    if (failure != null) {
      throw failure;
    }
  }
}
