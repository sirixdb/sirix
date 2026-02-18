package io.sirix.query.json;

import io.sirix.access.trx.node.json.ForwardingJsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.node.SirixDeweyID;

import java.time.Instant;

/**
 * Thread-safe proxy for {@link JsonNodeReadOnlyTrx} that transparently delegates
 * to a per-thread transaction obtained from the resource session's shared pool.
 *
 * <p>Extends {@link ForwardingJsonNodeReadOnlyTrx} — all methods delegate via
 * {@link #nodeReadOnlyTrxDelegate()}, which returns the owner trx on the creating
 * thread (~1-2 ns fast-path) or a per-thread trx from the session pool on worker
 * threads (~20 ns after first access).
 *
 * <p>Only {@code close()}, {@code isClosed()}, and session/revision metadata are
 * overridden to always use the owner trx directly.
 */
public final class ThreadSafeJsonReadOnlyTrx implements ForwardingJsonNodeReadOnlyTrx {

  private final JsonNodeReadOnlyTrx ownerTrx;
  private final long ownerThreadId;

  public ThreadSafeJsonReadOnlyTrx(final JsonNodeReadOnlyTrx ownerTrx) {
    this.ownerTrx = ownerTrx;
    this.ownerThreadId = Thread.currentThread().threadId();
  }

  @Override
  public JsonNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
    final long tid = Thread.currentThread().threadId();
    if (tid == ownerThreadId) {
      return ownerTrx;
    }
    return ownerTrx.getResourceSession().getOrCreateSharedReadOnlyTrx(ownerTrx.getRevisionNumber());
  }

  // -- Shared metadata: always route to ownerTrx --

  @Override
  public JsonResourceSession getResourceSession() {
    return ownerTrx.getResourceSession();
  }

  @Override
  public int getRevisionNumber() {
    return ownerTrx.getRevisionNumber();
  }

  @Override
  public Instant getRevisionTimestamp() {
    return ownerTrx.getRevisionTimestamp();
  }

  @Override
  public long getMaxNodeKey() {
    return ownerTrx.getMaxNodeKey();
  }

  @Override
  public int getId() {
    return ownerTrx.getId();
  }

  // -- Lifecycle: only close ownerTrx --

  @Override
  public boolean isClosed() {
    return ownerTrx.isClosed();
  }

  @Override
  public void close() {
    ownerTrx.getResourceSession().closeSharedReadOnlyTrxs(ownerTrx.getRevisionNumber());
    ownerTrx.close();
  }

  // -- getDeweyID: not in ForwardingJsonNodeReadOnlyTrx defaults --

  @Override
  public SirixDeweyID getDeweyID() {
    return nodeReadOnlyTrxDelegate().getDeweyID();
  }
}
