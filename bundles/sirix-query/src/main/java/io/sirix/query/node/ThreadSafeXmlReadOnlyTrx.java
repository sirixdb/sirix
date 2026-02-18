package io.sirix.query.node;

import io.sirix.access.trx.node.xml.ForwardingXmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.node.SirixDeweyID;

import java.time.Instant;

/**
 * Thread-safe proxy for {@link XmlNodeReadOnlyTrx} that transparently delegates
 * to a per-thread transaction obtained from the resource session's shared pool.
 *
 * <p>Extends {@link ForwardingXmlNodeReadOnlyTrx} — all methods delegate via
 * {@link #nodeReadOnlyTrxDelegate()}, which returns the owner trx on the creating
 * thread (~1-2 ns fast-path) or a per-thread trx from the session pool on worker
 * threads (~20 ns after first access).
 *
 * <p>Only {@code close()}, {@code isClosed()}, and session/revision metadata are
 * overridden to always use the owner trx directly.
 */
public final class ThreadSafeXmlReadOnlyTrx implements ForwardingXmlNodeReadOnlyTrx {

  private final XmlNodeReadOnlyTrx ownerTrx;
  private final long ownerThreadId;

  public ThreadSafeXmlReadOnlyTrx(final XmlNodeReadOnlyTrx ownerTrx) {
    this.ownerTrx = ownerTrx;
    this.ownerThreadId = Thread.currentThread().threadId();
  }

  @Override
  public XmlNodeReadOnlyTrx nodeReadOnlyTrxDelegate() {
    final long tid = Thread.currentThread().threadId();
    if (tid == ownerThreadId) {
      return ownerTrx;
    }
    return ownerTrx.getResourceSession().getOrCreateSharedReadOnlyTrx(ownerTrx.getRevisionNumber());
  }

  // -- Shared metadata: always route to ownerTrx --

  @Override
  public XmlResourceSession getResourceSession() {
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

  // -- getDeweyID: not in ForwardingXmlNodeReadOnlyTrx defaults --

  @Override
  public SirixDeweyID getDeweyID() {
    return nodeReadOnlyTrxDelegate().getDeweyID();
  }
}
