package io.sirix.access.trx.node;

import io.sirix.api.NodeCursor;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.immutable.ImmutableNode;
import com.google.common.collect.ForwardingObject;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractForwardingNodeCursor extends ForwardingObject implements NodeCursor {

  /** Constructor for use by subclasses. */
  protected AbstractForwardingNodeCursor() {}

  @Override
  protected abstract NodeCursor delegate();

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
  public long getRightSiblingKey() {
    return delegate().getRightSiblingKey();
  }

  @Override
  public boolean moveToNextFollowing() {
    return delegate().moveToNextFollowing();
  }

  @Override
  public NodeKind getFirstChildKind() {
    return delegate().getFirstChildKind();
  }

  @Override
  public NodeKind getKind() {
    return delegate().getKind();
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
  public ImmutableNode getNode() {
    return delegate().getNode();
  }

  @Override
  public long getNodeKey() {
    return delegate().getNodeKey();
  }

  @Override
  public NodeKind getParentKind() {
    return delegate().getParentKind();
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
  public boolean hasNode(final long key) {
    return delegate().hasNode(key);
  }

  @Override
  public boolean hasParent() {
    return delegate().hasParent();
  }

  @Override
  public boolean hasRightSibling() {
    return delegate().hasRightSibling();
  }

  @Override
  public boolean moveTo(final long key) {
    return delegate().moveTo(key);
  }

  @Override
  public boolean moveToDocumentRoot() {
    return delegate().moveToDocumentRoot();
  }

  @Override
  public boolean moveToFirstChild() {
    return delegate().moveToFirstChild();
  }

  @Override
  public boolean moveToLastChild() {
    return delegate().moveToLastChild();
  }

  @Override
  public boolean moveToLeftSibling() {
    return delegate().moveToLeftSibling();
  }

  @Override
  public boolean moveToNext() {
    return delegate().moveToNext();
  }

  @Override
  public boolean moveToParent() {
    return delegate().moveToParent();
  }

  @Override
  public boolean moveToPrevious() {
    return delegate().moveToParent();
  }

  @Override
  public boolean moveToRightSibling() {
    return delegate().moveToRightSibling();
  }
}
