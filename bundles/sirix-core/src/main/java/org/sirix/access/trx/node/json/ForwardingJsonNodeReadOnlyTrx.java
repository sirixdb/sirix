package org.sirix.access.trx.node.json;

import com.google.gson.JsonObject;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.User;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.immutable.ImmutableNode;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * A forwarding {@link JsonNodeReadOnlyTrx} based on the decorator pattern.
 *
 * @author Joao Sousa
 */
public interface ForwardingJsonNodeReadOnlyTrx extends JsonNodeReadOnlyTrx {
  
  JsonNodeReadOnlyTrx nodeReadOnlyTrxDelegate();

  @Override
  default List<JsonObject> getUpdateOperations() {
    return nodeReadOnlyTrxDelegate().getUpdateOperations();
  }

  @Override
  default List<JsonObject> getUpdateOperationsInSubtreeOfNode(SirixDeweyID deweyID, long maxDepth) {
    return nodeReadOnlyTrxDelegate().getUpdateOperationsInSubtreeOfNode(deweyID, maxDepth);
  }

  @Override
  default boolean storeDeweyIDs() {
    return nodeReadOnlyTrxDelegate().storeDeweyIDs();
  }

  @Override
  default Optional<User> getUser() {
    return nodeReadOnlyTrxDelegate().getUser();
  }

  @Override
  default int getNameKey() {
    return nodeReadOnlyTrxDelegate().getNameKey();
  }

  @Override
  default BigInteger getHash() {
    return nodeReadOnlyTrxDelegate().getHash();
  }

  @Override
  default boolean getBooleanValue() {
    return nodeReadOnlyTrxDelegate().getBooleanValue();
  }

  @Override
  default Number getNumberValue() {
    return nodeReadOnlyTrxDelegate().getNumberValue();
  }

  @Override
  default CommitCredentials getCommitCredentials() {
    return nodeReadOnlyTrxDelegate().getCommitCredentials();
  }

  @Override
  default VisitResult acceptVisitor(JsonNodeVisitor visitor) {
    return nodeReadOnlyTrxDelegate().acceptVisitor(visitor);
  }

  @Override
  default void close() {
    nodeReadOnlyTrxDelegate().close();
  }

  @Override
  default long getFirstChildKey() {
    return nodeReadOnlyTrxDelegate().getFirstChildKey();
  }

  @Override
  default boolean isClosed() {
    return nodeReadOnlyTrxDelegate().isClosed();
  }

  @Override
  default boolean isDocumentRoot() {
    return nodeReadOnlyTrxDelegate().isDocumentRoot();
  }

  @Override
  default boolean hasChildren() {
    return nodeReadOnlyTrxDelegate().hasChildren();
  }

  @Override
  default long getChildCount() {
    return nodeReadOnlyTrxDelegate().getChildCount();
  }

  @Override
  default long getDescendantCount() {
    return nodeReadOnlyTrxDelegate().getDescendantCount();
  }

  @Override
  default QNm getName() {
    return nodeReadOnlyTrxDelegate().getName();
  }

  @Override
  default NodeKind getPathKind() {
    return nodeReadOnlyTrxDelegate().getPathKind();
  }

  @Override
  default long getId() {
    return nodeReadOnlyTrxDelegate().getId();
  }

  @Override
  default NodeKind getFirstChildKind() {
    return nodeReadOnlyTrxDelegate().getFirstChildKind();
  }

  @Override
  default NodeKind getKind() {
    return nodeReadOnlyTrxDelegate().getKind();
  }

  @Override
  default long getLastChildKey() {
    return nodeReadOnlyTrxDelegate().getLastChildKey();
  }

  @Override
  default NodeKind getLastChildKind() {
    return nodeReadOnlyTrxDelegate().getLastChildKind();
  }

  @Override
  default long getPathNodeKey() {
    return nodeReadOnlyTrxDelegate().getPathNodeKey();
  }

  @Override
  default int keyForName(String name) {
    return nodeReadOnlyTrxDelegate().keyForName(name);
  }

  @Override
  default String nameForKey(int key) {
    return nodeReadOnlyTrxDelegate().nameForKey(key);
  }

  @Override
  default long getLeftSiblingKey() {
    return nodeReadOnlyTrxDelegate().getLeftSiblingKey();
  }

  @Override
  default NodeKind getLeftSiblingKind() {
    return nodeReadOnlyTrxDelegate().getLeftSiblingKind();
  }

  @Override
  default long getMaxNodeKey() {
    return nodeReadOnlyTrxDelegate().getMaxNodeKey();
  }

  @Override
  default ImmutableNode getNode() {
    return nodeReadOnlyTrxDelegate().getNode();
  }

  @Override
  default long getNodeKey() {
    return nodeReadOnlyTrxDelegate().getNodeKey();
  }

  @Override
  default PageReadOnlyTrx getPageTrx() {
    return nodeReadOnlyTrxDelegate().getPageTrx();
  }

  @Override
  default long getParentKey() {
    return nodeReadOnlyTrxDelegate().getParentKey();
  }

  @Override
  default NodeKind getParentKind() {
    return nodeReadOnlyTrxDelegate().getParentKind();
  }

  @Override
  default JsonResourceSession getResourceSession() {
    return nodeReadOnlyTrxDelegate().getResourceSession();
  }

  @Override
  default int getRevisionNumber() {
    return nodeReadOnlyTrxDelegate().getRevisionNumber();
  }

  @Override
  default Instant getRevisionTimestamp() {
    return nodeReadOnlyTrxDelegate().getRevisionTimestamp();
  }

  @Override
  default long getRightSiblingKey() {
    return nodeReadOnlyTrxDelegate().getRightSiblingKey();
  }

  @Override
  default NodeKind getRightSiblingKind() {
    return nodeReadOnlyTrxDelegate().getRightSiblingKind();
  }

  @Override
  default String getValue() {
    return nodeReadOnlyTrxDelegate().getValue();
  }

  @Override
  default boolean hasFirstChild() {
    return nodeReadOnlyTrxDelegate().hasFirstChild();
  }

  @Override
  default boolean hasLastChild() {
    return nodeReadOnlyTrxDelegate().hasLastChild();
  }

  @Override
  default boolean hasLeftSibling() {
    return nodeReadOnlyTrxDelegate().hasLeftSibling();
  }

  @Override
  default boolean hasNode(long key) {
    return nodeReadOnlyTrxDelegate().hasNode(key);
  }

  @Override
  default boolean hasParent() {
    return nodeReadOnlyTrxDelegate().hasParent();
  }

  @Override
  default boolean hasRightSibling() {
    return nodeReadOnlyTrxDelegate().hasRightSibling();
  }

  @Override
  default boolean isArray() {
    return nodeReadOnlyTrxDelegate().isArray();
  }

  @Override
  default boolean isNullValue() {
    return nodeReadOnlyTrxDelegate().isNullValue();
  }

  @Override
  default boolean isNumberValue() {
    return nodeReadOnlyTrxDelegate().isNumberValue();
  }

  @Override
  default boolean isObject() {
    return nodeReadOnlyTrxDelegate().isObject();
  }

  @Override
  default boolean isObjectKey() {
    return nodeReadOnlyTrxDelegate().isObjectKey();
  }

  @Override
  default boolean isStringValue() {
    return nodeReadOnlyTrxDelegate().isStringValue();
  }

  @Override
  default boolean isBooleanValue() {
    return nodeReadOnlyTrxDelegate().isBooleanValue();
  }

  @Override
  default boolean moveTo(long key) {
    return nodeReadOnlyTrxDelegate().moveTo(key);
  }

  @Override
  default boolean moveToDocumentRoot() {
    return nodeReadOnlyTrxDelegate().moveToDocumentRoot();
  }

  @Override
  default boolean moveToFirstChild() {
    return nodeReadOnlyTrxDelegate().moveToFirstChild();
  }

  @Override
  default boolean moveToLastChild() {
    return nodeReadOnlyTrxDelegate().moveToLastChild();
  }

  @Override
  default boolean moveToLeftSibling() {
    return nodeReadOnlyTrxDelegate().moveToLeftSibling();
  }

  @Override
  default boolean moveToNext() {
    return nodeReadOnlyTrxDelegate().moveToNext();
  }

  @Override
  default boolean moveToNextFollowing() {
    return nodeReadOnlyTrxDelegate().moveToNextFollowing();
  }

  @Override
  default boolean moveToParent() {
    return nodeReadOnlyTrxDelegate().moveToParent();
  }

  @Override
  default boolean moveToPrevious() {
    return nodeReadOnlyTrxDelegate().moveToPrevious();
  }

  @Override
  default boolean moveToRightSibling() {
    return nodeReadOnlyTrxDelegate().moveToRightSibling();
  }
}
