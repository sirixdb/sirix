package org.sirix.index.path.summary;

import java.math.BigInteger;
import java.time.Instant;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.Move;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.ResourceManager;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import com.google.common.collect.ForwardingObject;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractForwardingPathSummaryReader extends ForwardingObject
    implements NodeReadOnlyTrx, NodeCursor {

  /** Constructor for use by subclasses. */
  protected AbstractForwardingPathSummaryReader() {}

  @Override
  protected abstract PathSummaryReader delegate();

  @Override
  public CommitCredentials getCommitCredentials() {
    return delegate().getCommitCredentials();
  }

  @Override
  public long getMaxNodeKey() {
    return delegate().getMaxNodeKey();
  }

  @Override
  public void close() {
    delegate().close();
  }

  @Override
  public BigInteger getHash() {
    return delegate().getHash();
  }

  @Override
  public String getValue() {
    return delegate().getValue();
  }

  @Override
  public PageReadOnlyTrx getPageTrx() {
    return delegate().getPageTrx();
  }

  @Override
  public QNm getName() {
    return delegate().getName();
  }

  @Override
  public int getRevisionNumber() {
    return delegate().getRevisionNumber();
  }

  @Override
  public Instant getRevisionTimestamp() {
    return delegate().getRevisionTimestamp();
  }

  @Override
  public ResourceManager<? extends NodeReadOnlyTrx, ? extends NodeTrx> getResourceManager() {
    return delegate().getResourceManager();
  }

  @Override
  public long getId() {
    return delegate().getId();
  }

  @Override
  public boolean isClosed() {
    return delegate().isClosed();
  }

  @Override
  public int keyForName(final String name) {
    return delegate().keyForName(name);
  }

  @Override
  public Move<? extends PathSummaryReader> moveTo(final long key) {
    return delegate().moveTo(key);
  }

  @Override
  public Move<? extends PathSummaryReader> moveToDocumentRoot() {
    return delegate().moveToDocumentRoot();
  }

  @Override
  public Move<? extends PathSummaryReader> moveToFirstChild() {
    return delegate().moveToFirstChild();
  }

  @Override
  public Move<? extends PathSummaryReader> moveToLeftSibling() {
    return delegate().moveToLeftSibling();
  }

  @Override
  public Move<? extends PathSummaryReader> moveToNextFollowing() {
    return delegate().moveToNextFollowing();
  }

  @Override
  public Move<? extends PathSummaryReader> moveToParent() {
    return delegate().moveToParent();
  }

  @Override
  public Move<? extends PathSummaryReader> moveToRightSibling() {
    return delegate().moveToRightSibling();
  }

  @Override
  public Move<? extends PathSummaryReader> moveToLastChild() {
    return delegate().moveToLastChild();
  }

  @Override
  public Move<? extends PathSummaryReader> moveToPrevious() {
    return delegate().moveToPrevious();
  }

  @Override
  public Move<? extends PathSummaryReader> moveToNext() {
    return delegate().moveToNext();
  }

  @Override
  public String nameForKey(final int pKey) {
    return delegate().nameForKey(pKey);
  }

  @Override
  public long getNodeKey() {
    return delegate().getNodeKey();
  }

  @Override
  public NodeKind getKind() {
    return delegate().getKind();
  }

  @Override
  public boolean hasFirstChild() {
    return delegate().hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    return delegate().hasLastChild();
  }

  @Override
  public boolean hasLeftSibling() {
    return delegate().hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    return delegate().hasRightSibling();
  }

  @Override
  public boolean hasParent() {
    return delegate().hasParent();
  }

  @Override
  public boolean hasNode(final long pKey) {
    return delegate().hasNode(pKey);
  }

  @Override
  public long getFirstChildKey() {
    return delegate().getFirstChildKey();
  }

  @Override
  public long getLastChildKey() {
    return delegate().getLastChildKey();
  }

  @Override
  public long getLeftSiblingKey() {
    return delegate().getLeftSiblingKey();
  }

  @Override
  public long getParentKey() {
    return delegate().getParentKey();
  }

  @Override
  public NodeKind getPathKind() {
    return delegate().getPathKind();
  }

  @Override
  public long getPathNodeKey() {
    return delegate().getPathNodeKey();
  }

  @Override
  public long getRightSiblingKey() {
    return delegate().getRightSiblingKey();
  }

  @Override
  public boolean hasChildren() {
    return delegate().hasChildren();
  }

  @Override
  public ImmutableNode getNode() {
    return delegate().getNode();
  }

  @Override
  public long getChildCount() {
    return delegate().getChildCount();
  }

  @Override
  public long getDescendantCount() {
    return delegate().getDescendantCount();
  }

  @Override
  public NodeKind getFirstChildKind() {
    return delegate().getFirstChildKind();
  }

  @Override
  public NodeKind getLastChildKind() {
    return delegate().getLastChildKind();
  }

  @Override
  public NodeKind getLeftSiblingKind() {
    return delegate().getLeftSiblingKind();
  }

  @Override
  public NodeKind getRightSiblingKind() {
    return delegate().getRightSiblingKind();
  }

  @Override
  public NodeKind getParentKind() {
    return delegate().getParentKind();
  }

  @Override
  public boolean isDocumentRoot() {
    return delegate().isDocumentRoot();
  }
}
