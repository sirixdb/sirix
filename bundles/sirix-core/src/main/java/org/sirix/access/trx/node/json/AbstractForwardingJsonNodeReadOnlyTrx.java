package org.sirix.access.trx.node.json;

import com.google.common.collect.ForwardingObject;
import com.google.gson.JsonObject;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.User;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.Move;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.diff.DiffTuple;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public abstract class AbstractForwardingJsonNodeReadOnlyTrx extends ForwardingObject implements JsonNodeReadOnlyTrx {
  @Override
  protected abstract JsonNodeReadOnlyTrx delegate();

  @Override
  public List<JsonObject> getUpdateOperations() {
    return delegate().getUpdateOperations();
  }

  @Override
  public List<JsonObject> getUpdateOperationsInSubtreeOfNode(SirixDeweyID deweyID, long maxDepth) {
    return delegate().getUpdateOperationsInSubtreeOfNode(deweyID, maxDepth);
  }

  @Override
  public boolean storeDeweyIDs() {
    return delegate().storeDeweyIDs();
  }

  @Override
  public Optional<User> getUser() {
    return delegate().getUser();
  }

  @Override
  public int getNameKey() {
    return delegate().getNameKey();
  }

  @Override
  public BigInteger getHash() {
    return delegate().getHash();
  }

  @Override
  public boolean getBooleanValue() {
    return delegate().getBooleanValue();
  }

  @Override
  public Number getNumberValue() {
    return delegate().getNumberValue();
  }

  @Override
  public CommitCredentials getCommitCredentials() {
    return delegate().getCommitCredentials();
  }

  @Override
  public VisitResult acceptVisitor(JsonNodeVisitor visitor) {
    return delegate().acceptVisitor(visitor);
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
  public NodeKind getPathKind() {
    return delegate().getPathKind();
  }

  @Override
  public long getId() {
    return delegate().getId();
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
  public long getLastChildKey() {
    return delegate().getLastChildKey();
  }

  @Override
  public NodeKind getLastChildKind() {
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
  public NodeKind getLeftSiblingKind() {
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
  public PageReadOnlyTrx getPageTrx() {
    return delegate().getPageTrx();
  }

  @Override
  public long getParentKey() {
    return delegate().getParentKey();
  }

  @Override
  public NodeKind getParentKind() {
    return delegate().getParentKind();
  }

  @Override
  public JsonResourceManager getResourceManager() {
    return delegate().getResourceManager();
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
  public long getRightSiblingKey() {
    return delegate().getRightSiblingKey();
  }

  @Override
  public NodeKind getRightSiblingKind() {
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
  public boolean isBooleanValue() {
    return delegate().isBooleanValue();
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
