package org.sirix.access.trx.node.json;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.Move;
import org.sirix.api.NodeReadTrx;
import org.sirix.api.NodeWriteTrx;
import org.sirix.api.PageReadTrx;
import org.sirix.api.ResourceManager;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import com.google.common.collect.ForwardingObject;

public abstract class AbstractForwardingJsonNodeReadOnlyTrx extends ForwardingObject implements JsonNodeReadOnlyTrx {
  @Override
  protected abstract JsonNodeReadOnlyTrx delegate();

  @Override
  public CommitCredentials getCommitCredentials() {
    return delegate().getCommitCredentials();
  }

  @Override
  public void close() {
    delegate().close();
  }

  @Override
  public long getFirstChildKey() {
    return delegate().getFirstChildKey();
  }

  @Override
  public boolean isClosed() {
    return delegate().isClosed();
  }

  @Override
  public boolean isDocumentRoot() {
    return delegate().isDocumentRoot();
  }

  @Override
  public boolean hasChildren() {
    return delegate().hasChildren();
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
  public QNm getName() {
    return delegate().getName();
  }

  @Override
  public Kind getPathKind() {
    return delegate().getPathKind();
  }

  @Override
  public long getId() {
    return delegate().getId();
  }

  @Override
  public Kind getFirstChildKind() {
    return delegate().getFirstChildKind();
  }

  @Override
  public Kind getKind() {
    return delegate().getKind();
  }

  @Override
  public long getLastChildKey() {
    return delegate().getLastChildKey();
  }

  @Override
  public Kind getLastChildKind() {
    return delegate().getLastChildKind();
  }

  @Override
  public long getPathNodeKey() {
    return delegate().getPathNodeKey();
  }

  @Override
  public int keyForName(String name) {
    return delegate().keyForName(name);
  }

  @Override
  public String nameForKey(int key) {
    return delegate().nameForKey(key);
  }

  @Override
  public long getLeftSiblingKey() {
    return delegate().getLeftSiblingKey();
  }

  @Override
  public Kind getLeftSiblingKind() {
    return delegate().getLeftSiblingKind();
  }

  @Override
  public long getMaxNodeKey() {
    return delegate().getMaxNodeKey();
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
  public PageReadTrx getPageTrx() {
    return delegate().getPageTrx();
  }

  @Override
  public long getParentKey() {
    return delegate().getParentKey();
  }

  @Override
  public Kind getParentKind() {
    return delegate().getParentKind();
  }

  @Override
  public ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx> getResourceManager() {
    return delegate().getResourceManager();
  }

  @Override
  public int getRevisionNumber() {
    return delegate().getRevisionNumber();
  }

  @Override
  public long getRevisionTimestamp() {
    return delegate().getRevisionTimestamp();
  }

  @Override
  public long getRightSiblingKey() {
    return delegate().getRightSiblingKey();
  }

  @Override
  public Kind getRightSiblingKind() {
    return delegate().getRightSiblingKind();
  }

  @Override
  public String getValue() {
    return delegate().getValue();
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
  public boolean hasNode(long key) {
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
  public boolean isArray() {
    return delegate().isArray();
  }

  @Override
  public boolean isNullValue() {
    return delegate().isNullValue();
  }

  @Override
  public boolean isNumberValue() {
    return delegate().isNumberValue();
  }

  @Override
  public boolean isObject() {
    return delegate().isObject();
  }

  @Override
  public boolean isObjectKey() {
    return delegate().isObjectKey();
  }

  @Override
  public boolean isStringValue() {
    return delegate().isStringValue();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveTo(long key) {
    return delegate().moveTo(key);
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToDocumentRoot() {
    return delegate().moveToDocumentRoot();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToFirstChild() {
    return delegate().moveToFirstChild();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToLastChild() {
    return delegate().moveToLastChild();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToLeftSibling() {
    return delegate().moveToLeftSibling();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToNext() {
    return delegate().moveToNext();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToNextFollowing() {
    return delegate().moveToNextFollowing();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToParent() {
    return delegate().moveToParent();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToPrevious() {
    return delegate().moveToPrevious();
  }

  @Override
  public Move<? extends JsonNodeReadOnlyTrx> moveToRightSibling() {
    return delegate().moveToRightSibling();
  }
}
