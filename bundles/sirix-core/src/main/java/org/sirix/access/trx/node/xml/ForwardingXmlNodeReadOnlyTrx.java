package org.sirix.access.trx.node.xml;

import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.User;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.ItemList;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.service.xml.xpath.AtomicValue;

import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Forwards all methods to the {@link #nodeReadOnlyTrxDelegate}.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public interface ForwardingXmlNodeReadOnlyTrx extends XmlNodeReadOnlyTrx {

  XmlNodeReadOnlyTrx nodeReadOnlyTrxDelegate();

  @Override
  default boolean storeDeweyIDs() {
    return nodeReadOnlyTrxDelegate().storeDeweyIDs();
  }

  @Override
  default Optional<User> getUser() {
    return nodeReadOnlyTrxDelegate().getUser();
  }

  @Override
  default ItemList<AtomicValue> getItemList() {
    return nodeReadOnlyTrxDelegate().getItemList();
  }

  @Override
  default CommitCredentials getCommitCredentials() {
    return nodeReadOnlyTrxDelegate().getCommitCredentials();
  }

  @Override
  default long getMaxNodeKey() {
    return nodeReadOnlyTrxDelegate().getMaxNodeKey();
  }

  @Override
  default void close() {
    nodeReadOnlyTrxDelegate().close();
  }

  @Override
  default String getNamespaceURI() {
    return nodeReadOnlyTrxDelegate().getNamespaceURI();
  }

  @Override
  default PageReadOnlyTrx getPageTrx() {
    return nodeReadOnlyTrxDelegate().getPageTrx();
  }

  @Override
  default QNm getName() {
    return nodeReadOnlyTrxDelegate().getName();
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
  default XmlResourceSession getResourceSession() {
    return nodeReadOnlyTrxDelegate().getResourceSession();
  }

  @Override
  default long getId() {
    return nodeReadOnlyTrxDelegate().getId();
  }

  @Override
  default String getType() {
    return nodeReadOnlyTrxDelegate().getType();
  }

  @Override
  default String getValue() {
    return nodeReadOnlyTrxDelegate().getValue();
  }

  @Override
  default boolean isClosed() {
    return nodeReadOnlyTrxDelegate().isClosed();
  }

  @Override
  default int keyForName(final String name) {
    return nodeReadOnlyTrxDelegate().keyForName(name);
  }

  @Override
  default boolean moveTo(final long key) {
    return nodeReadOnlyTrxDelegate().moveTo(key);
  }

  @Override
  default boolean moveToAttribute(final @NonNegative int index) {
    return nodeReadOnlyTrxDelegate().moveToAttribute(index);
  }

  @Override
  default boolean moveToAttributeByName(final QNm name) {
    return nodeReadOnlyTrxDelegate().moveToAttributeByName(name);
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
  default boolean moveToLeftSibling() {
    return nodeReadOnlyTrxDelegate().moveToLeftSibling();
  }

  @Override
  default boolean moveToNamespace(@NonNegative final int index) {
    return nodeReadOnlyTrxDelegate().moveToNamespace(index);
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
  default boolean moveToRightSibling() {
    return nodeReadOnlyTrxDelegate().moveToRightSibling();
  }

  @Override
  default boolean moveToLastChild() {
    return nodeReadOnlyTrxDelegate().moveToLastChild();
  }

  @Override
  default boolean moveToPrevious() {
    return nodeReadOnlyTrxDelegate().moveToPrevious();
  }

  @Override
  default boolean moveToNext() {
    return nodeReadOnlyTrxDelegate().moveToNext();
  }

  @Override
  default String nameForKey(final int pKey) {
    return nodeReadOnlyTrxDelegate().nameForKey(pKey);
  }

  @Override
  default byte[] rawNameForKey(final int pKey) {
    return nodeReadOnlyTrxDelegate().rawNameForKey(pKey);
  }

  @Override
  default int getNameCount(final String name, @NonNull final NodeKind kind) {
    return nodeReadOnlyTrxDelegate().getNameCount(name, kind);
  }

  @Override
  default ImmutableNameNode getNameNode() {
    return nodeReadOnlyTrxDelegate().getNameNode();
  }

  @Override
  default ImmutableValueNode getValueNode() {
    return nodeReadOnlyTrxDelegate().getValueNode();
  }

  @Override
  default int getAttributeCount() {
    return nodeReadOnlyTrxDelegate().getAttributeCount();
  }

  @Override
  default long getNodeKey() {
    return nodeReadOnlyTrxDelegate().getNodeKey();
  }

  @Override
  default int getNamespaceCount() {
    return nodeReadOnlyTrxDelegate().getNamespaceCount();
  }

  @Override
  default NodeKind getKind() {
    return nodeReadOnlyTrxDelegate().getKind();
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
  default boolean hasRightSibling() {
    return nodeReadOnlyTrxDelegate().hasRightSibling();
  }

  @Override
  default boolean hasParent() {
    return nodeReadOnlyTrxDelegate().hasParent();
  }

  @Override
  default boolean hasNode(final long pKey) {
    return nodeReadOnlyTrxDelegate().hasNode(pKey);
  }

  @Override
  default long getAttributeKey(@NonNegative final int index) {
    return nodeReadOnlyTrxDelegate().getAttributeKey(index);
  }

  @Override
  default long getFirstChildKey() {
    return nodeReadOnlyTrxDelegate().getFirstChildKey();
  }

  @Override
  default long getLastChildKey() {
    return nodeReadOnlyTrxDelegate().getLastChildKey();
  }

  @Override
  default long getLeftSiblingKey() {
    return nodeReadOnlyTrxDelegate().getLeftSiblingKey();
  }

  @Override
  default int getPrefixKey() {
    return nodeReadOnlyTrxDelegate().getPrefixKey();
  }

  @Override
  default int getLocalNameKey() {
    return nodeReadOnlyTrxDelegate().getLocalNameKey();
  }

  @Override
  default long getParentKey() {
    return nodeReadOnlyTrxDelegate().getParentKey();
  }

  @Override
  default boolean isNameNode() {
    return nodeReadOnlyTrxDelegate().isNameNode();
  }

  @Override
  default NodeKind getPathKind() {
    return nodeReadOnlyTrxDelegate().getPathKind();
  }

  @Override
  default long getPathNodeKey() {
    return nodeReadOnlyTrxDelegate().getPathNodeKey();
  }

  @Override
  default long getRightSiblingKey() {
    return nodeReadOnlyTrxDelegate().getRightSiblingKey();
  }

  @Override
  default int getTypeKey() {
    return nodeReadOnlyTrxDelegate().getTypeKey();
  }

  @Override
  default VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return nodeReadOnlyTrxDelegate().acceptVisitor(visitor);
  }

  @Override
  default boolean isValueNode() {
    return nodeReadOnlyTrxDelegate().isValueNode();
  }

  @Override
  default boolean hasAttributes() {
    return nodeReadOnlyTrxDelegate().hasAttributes();
  }

  @Override
  default boolean hasNamespaces() {
    return nodeReadOnlyTrxDelegate().hasNamespaces();
  }

  @Override
  default boolean hasChildren() {
    return nodeReadOnlyTrxDelegate().hasChildren();
  }

  @Override
  default ImmutableXmlNode getNode() {
    return nodeReadOnlyTrxDelegate().getNode();
  }

  @Override
  default int getURIKey() {
    return nodeReadOnlyTrxDelegate().getURIKey();
  }

  @Override
  default boolean isStructuralNode() {
    return nodeReadOnlyTrxDelegate().isStructuralNode();
  }

  @Override
  default List<Long> getAttributeKeys() {
    return nodeReadOnlyTrxDelegate().getAttributeKeys();
  }

  @Override
  default BigInteger getHash() {
    return nodeReadOnlyTrxDelegate().getHash();
  }

  @Override
  default List<Long> getNamespaceKeys() {
    return nodeReadOnlyTrxDelegate().getNamespaceKeys();
  }

  @Override
  default byte[] getRawValue() {
    return nodeReadOnlyTrxDelegate().getRawValue();
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
  default NodeKind getFirstChildKind() {
    return nodeReadOnlyTrxDelegate().getFirstChildKind();
  }

  @Override
  default NodeKind getLastChildKind() {
    return nodeReadOnlyTrxDelegate().getLastChildKind();
  }

  @Override
  default NodeKind getLeftSiblingKind() {
    return nodeReadOnlyTrxDelegate().getLeftSiblingKind();
  }

  @Override
  default NodeKind getRightSiblingKind() {
    return nodeReadOnlyTrxDelegate().getRightSiblingKind();
  }

  @Override
  default NodeKind getParentKind() {
    return nodeReadOnlyTrxDelegate().getParentKind();
  }

  @Override
  default boolean isAttribute() {
    return nodeReadOnlyTrxDelegate().isAttribute();
  }

  @Override
  default boolean isComment() {
    return nodeReadOnlyTrxDelegate().isComment();
  }

  @Override
  default boolean isDocumentRoot() {
    return nodeReadOnlyTrxDelegate().isDocumentRoot();
  }

  @Override
  default boolean isElement() {
    return nodeReadOnlyTrxDelegate().isElement();
  }

  @Override
  default boolean isNamespace() {
    return nodeReadOnlyTrxDelegate().isNamespace();
  }

  @Override
  default boolean isPI() {
    return nodeReadOnlyTrxDelegate().isPI();
  }

  @Override
  default boolean isText() {
    return nodeReadOnlyTrxDelegate().isText();
  }

  @Override
  default SirixDeweyID getDeweyID() {
    return nodeReadOnlyTrxDelegate().getDeweyID();
  }

  @Override
  default SirixDeweyID getFirstChildDeweyID() {
    return nodeReadOnlyTrxDelegate().getFirstChildDeweyID();
  }

  @Override
  default SirixDeweyID getLeftSiblingDeweyID() {
    return nodeReadOnlyTrxDelegate().getLeftSiblingDeweyID();
  }

  @Override
  default SirixDeweyID getParentDeweyID() {
    return nodeReadOnlyTrxDelegate().getParentDeweyID();
  }

  @Override
  default SirixDeweyID getRightSiblingDeweyID() {
    return nodeReadOnlyTrxDelegate().getRightSiblingDeweyID();
  }

  @Override
  default int getPreviousRevisionNumber() {
    return nodeReadOnlyTrxDelegate().getPreviousRevisionNumber();
  }
}
