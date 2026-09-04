package io.sirix.query.json;

import io.sirix.access.trx.node.json.ForwardingJsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.SirixDeweyID;

import java.time.Instant;
import java.util.Objects;

/**
 * Thread-safe proxy for {@link JsonNodeReadOnlyTrx} that transparently delegates to a per-thread
 * transaction obtained from the resource session's shared pool.
 *
 * <p>
 * Extends {@link ForwardingJsonNodeReadOnlyTrx} — all methods delegate via
 * {@link #nodeReadOnlyTrxDelegate()}, which returns the owner trx on the creating thread (~1-2 ns
 * fast-path) or a per-thread trx from the session pool on worker threads (~20 ns after first
 * access).
 *
 * <p>
 * Only {@code close()}, {@code isClosed()}, and session/revision metadata are overridden. A cursor
 * used to construct escaping lazy items can be {@link #detachOwner() detached} after construction.
 * Such a proxy keeps its immutable revision metadata, closes the construction cursor immediately,
 * and obtains a cursor from a caller-supplied bounded provider when the item is consumed later. A
 * detached item may outlive the executor that materialized it, but it may not be consumed
 * concurrently with terminal close of the provider's owning chain/store: the item itself is not a
 * lifetime lease, and putting a lease around every forwarded node operation would add
 * synchronization to the navigation hot path.
 */
public final class ThreadSafeJsonReadOnlyTrx implements ForwardingJsonNodeReadOnlyTrx {

  /**
   * Supplies a cursor for a detached proxy on the calling thread.
   *
   * <p>
   * The provider owns the returned cursor. In particular, closing one proxy must not close a provider
   * shared by other lazy items. The caller must quiesce detached proxy use before closing the
   * provider; a returned raw cursor is not guaranteed to survive concurrent provider close.
   */
  public interface DetachedCursorProvider {
    JsonNodeReadOnlyTrx cursor(JsonResourceSession session, int revision);

    boolean isClosed();
  }

  /** Cleared when detach publishes the replacement route; never retained by an escaped item. */
  private JsonNodeReadOnlyTrx ownerTrx;
  private final long ownerThreadId;
  private final DetachedCursorProvider detachedCursorProvider;
  /** Published together with the cleared owner by the volatile {@link #ownerDetached} write. */
  private JsonResourceSession detachedResourceSession;
  private int detachedRevision;
  private int detachedId;
  private volatile boolean ownerDetached;
  private volatile boolean closed;

  public ThreadSafeJsonReadOnlyTrx(final JsonNodeReadOnlyTrx ownerTrx) {
    this(ownerTrx, null);
  }

  /**
   * Create a proxy whose construction cursor can later be detached in favour of {@code provider}.
   */
  public ThreadSafeJsonReadOnlyTrx(final JsonNodeReadOnlyTrx ownerTrx, final DetachedCursorProvider provider) {
    this.ownerTrx = Objects.requireNonNull(ownerTrx, "ownerTrx must not be null");
    this.ownerThreadId = Thread.currentThread().threadId();
    detachedCursorProvider = provider;
  }

  @Override
  public JsonNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
    if (closed) {
      throw new IllegalStateException("Read-only transaction proxy is closed");
    }
    if (ownerDetached) {
      return detachedDelegate();
    }
    final JsonNodeReadOnlyTrx owner = ownerTrx;
    if (owner == null) {
      awaitDetachPublication();
      return detachedDelegate();
    }
    if (Thread.currentThread().threadId() == ownerThreadId) {
      return owner;
    }
    final JsonResourceSession session = owner.getResourceSession();
    final int revision = owner.getRevisionNumber();
    return detachedCursorProvider == null
        ? session.getOrCreateSharedReadOnlyTrx(revision)
        : detachedCursorProvider.cursor(session, revision);
  }

  private JsonNodeReadOnlyTrx detachedDelegate() {
    return detachedCursorProvider == null
        ? detachedResourceSession.getOrCreateSharedReadOnlyTrx(detachedRevision)
        : detachedCursorProvider.cursor(detachedResourceSession, detachedRevision);
  }

  /**
   * The owner was observed {@code null} while {@link #ownerDetached} still read {@code false}: a
   * concurrent {@link #detachOwner()}/{@link #close()} sits between its two writes. Only the volatile
   * {@code ownerDetached} store publishes the replacement metadata, so spin for it — the window is a
   * handful of instructions inside a {@code finally}, never blocking work. Without this, a reader in
   * that window dereferenced the cleared owner and the typed "proxy is closed" contract degraded to a
   * raw {@code NullPointerException}.
   */
  private void awaitDetachPublication() {
    while (!ownerDetached) {
      Thread.onSpinWait();
    }
  }

  // -- Immutable revision metadata survives detaching/closing the construction cursor. --

  @Override
  public JsonResourceSession getResourceSession() {
    if (ownerDetached) {
      return detachedResourceSession;
    }
    final JsonNodeReadOnlyTrx owner = ownerTrx;
    if (owner != null) {
      return owner.getResourceSession();
    }
    awaitDetachPublication();
    return detachedResourceSession;
  }

  @Override
  public int getRevisionNumber() {
    if (ownerDetached) {
      return detachedRevision;
    }
    final JsonNodeReadOnlyTrx owner = ownerTrx;
    if (owner != null) {
      return owner.getRevisionNumber();
    }
    awaitDetachPublication();
    return detachedRevision;
  }

  @Override
  public Instant getRevisionTimestamp() {
    if (!ownerDetached) {
      final JsonNodeReadOnlyTrx owner = ownerTrx;
      if (owner != null) {
        return owner.getRevisionTimestamp();
      }
      awaitDetachPublication();
    }
    return nodeReadOnlyTrxDelegate().getRevisionTimestamp();
  }

  @Override
  public long getMaxNodeKey() {
    if (!ownerDetached) {
      final JsonNodeReadOnlyTrx owner = ownerTrx;
      if (owner != null) {
        return owner.getMaxNodeKey();
      }
      awaitDetachPublication();
    }
    return nodeReadOnlyTrxDelegate().getMaxNodeKey();
  }

  @Override
  public int getId() {
    if (ownerDetached) {
      return detachedId;
    }
    final JsonNodeReadOnlyTrx owner = ownerTrx;
    if (owner != null) {
      return owner.getId();
    }
    awaitDetachPublication();
    return detachedId;
  }

  // -- Lifecycle --

  /**
   * Stop retaining the registered cursor used to construct a lazy item.
   *
   * <p>
   * Call only after construction has stopped using the owner cursor and before publishing the item.
   * Subsequent reads transparently use the detached cursor provider. Idempotent.
   */
  public synchronized void detachOwner() {
    if (closed || ownerDetached) {
      return;
    }
    final JsonNodeReadOnlyTrx owner = ownerTrx;
    // These three values are cheap immutable metadata and are required to locate a replacement
    // cursor after owner close. Timestamp/max-node-key stay lazy and come from that replacement.
    captureDetachedMetadata(owner);
    try {
      owner.close();
    } finally {
      // detachOwner is called before the item is published. This volatile release publishes both
      // the immutable replacement metadata and the cleared owner to every later consumer.
      ownerTrx = null;
      ownerDetached = true;
    }
  }

  @Override
  public boolean isClosed() {
    if (closed) {
      return true;
    }
    if (!ownerDetached) {
      final JsonNodeReadOnlyTrx owner = ownerTrx;
      if (owner != null) {
        return owner.isClosed();
      }
      awaitDetachPublication();
    }
    return detachedCursorProvider != null && detachedCursorProvider.isClosed();
  }

  @Override
  public synchronized void close() {
    if (closed) {
      return;
    }
    closed = true;
    // The legacy provider is the session-wide (thread, revision) cache and retains the old close
    // contract. A supplied detached provider is shared by many lazy items and owns its own lifetime.
    if (detachedCursorProvider == null) {
      final JsonResourceSession session = ownerDetached
          ? detachedResourceSession
          : ownerTrx.getResourceSession();
      final int revision = ownerDetached
          ? detachedRevision
          : ownerTrx.getRevisionNumber();
      session.closeSharedReadOnlyTrxs(revision);
    }
    if (!ownerDetached) {
      final JsonNodeReadOnlyTrx owner = ownerTrx;
      captureDetachedMetadata(owner);
      try {
        owner.close();
      } finally {
        ownerTrx = null;
        ownerDetached = true;
      }
    }
  }

  private void captureDetachedMetadata(final JsonNodeReadOnlyTrx owner) {
    detachedResourceSession = owner.getResourceSession();
    detachedRevision = owner.getRevisionNumber();
    detachedId = owner.getId();
  }

  // -- getDeweyID: not in ForwardingJsonNodeReadOnlyTrx defaults --

  @Override
  public SirixDeweyID getDeweyID() {
    return nodeReadOnlyTrxDelegate().getDeweyID();
  }
}
