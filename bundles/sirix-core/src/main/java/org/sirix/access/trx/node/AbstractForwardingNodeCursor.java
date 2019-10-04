package org.sirix.access.trx.node;

import com.google.common.collect.ForwardingObject;
import org.sirix.api.Move;
import org.sirix.api.NodeCursor;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.immutable.ImmutableNode;

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
  public Move<? extends NodeCursor> moveToNextFollowing() {
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
  public Move<? extends NodeCursor> moveTo(final long key) {
    return delegate().moveTo(key);
  }

  @Override
  public Move<? extends NodeCursor> moveToDocumentRoot() {
    return delegate().moveToDocumentRoot();
  }

  @Override
  public Move<? extends NodeCursor> moveToFirstChild() {
    return delegate().moveToFirstChild();
  }

  @Override
  public Move<? extends NodeCursor> moveToLastChild() {
    return delegate().moveToLastChild();
  }

  @Override
  public Move<? extends NodeCursor> moveToLeftSibling() {
    return delegate().moveToLeftSibling();
  }

  @Override
  public Move<? extends NodeCursor> moveToNext() {
    return delegate().moveToNext();
  }

  @Override
  public Move<? extends NodeCursor> moveToParent() {
    return delegate().moveToParent();
  }

  @Override
  public Move<? extends NodeCursor> moveToPrevious() {
    return delegate().moveToParent();
  }

  @Override
  public Move<? extends NodeCursor> moveToRightSibling() {
    return delegate().moveToRightSibling();
  }
}
