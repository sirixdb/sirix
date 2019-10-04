package org.sirix.access.trx.node.xml;

import com.google.common.collect.ForwardingObject;
import org.brackit.xquery.atomic.QNm;
import org.sirix.access.User;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.api.ItemList;
import org.sirix.api.Move;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.service.xml.xpath.AtomicValue;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.math.BigInteger;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * Forwards all methods to the delegate.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public abstract class AbstractForwardingXmlNodeReadOnlyTrx extends ForwardingObject implements XmlNodeReadOnlyTrx {

  /** Constructor for use by subclasses. */
  protected AbstractForwardingXmlNodeReadOnlyTrx() {}

  @Override
  protected abstract XmlNodeReadOnlyTrx delegate();

  @Override
  public Optional<User> getUser() {
    return delegate().getUser();
  }

  @Override
  public ItemList<AtomicValue> getItemList() {
    return delegate().getItemList();
  }

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
  public String getNamespaceURI() {
    return delegate().getNamespaceURI();
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
  public XmlResourceManager getResourceManager() {
    return delegate().getResourceManager();
  }

  @Override
  public long getId() {
    return delegate().getId();
  }

  @Override
  public String getType() {
    return delegate().getType();
  }

  @Override
  public String getValue() {
    return delegate().getValue();
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
  public Move<? extends XmlNodeReadOnlyTrx> moveTo(final long key) {
    return delegate().moveTo(key);
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToAttribute(final @Nonnegative int index) {
    return delegate().moveToAttribute(index);
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToAttributeByName(final QNm name) {
    return delegate().moveToAttributeByName(name);
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToDocumentRoot() {
    return delegate().moveToDocumentRoot();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToFirstChild() {
    return delegate().moveToFirstChild();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToLeftSibling() {
    return delegate().moveToLeftSibling();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToNamespace(@Nonnegative final int index) {
    return delegate().moveToNamespace(index);
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToNextFollowing() {
    return delegate().moveToNextFollowing();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToParent() {
    return delegate().moveToParent();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToRightSibling() {
    return delegate().moveToRightSibling();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToLastChild() {
    return delegate().moveToLastChild();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToPrevious() {
    return delegate().moveToPrevious();
  }

  @Override
  public Move<? extends XmlNodeReadOnlyTrx> moveToNext() {
    return delegate().moveToNext();
  }

  @Override
  public String nameForKey(final int pKey) {
    return delegate().nameForKey(pKey);
  }

  @Override
  public byte[] rawNameForKey(final int pKey) {
    return delegate().rawNameForKey(pKey);
  }

  @Override
  public int getNameCount(final String name, @Nonnull final NodeKind kind) {
    return delegate().getNameCount(name, kind);
  }

  @Override
  public ImmutableNameNode getNameNode() {
    return delegate().getNameNode();
  }

  @Override
  public ImmutableValueNode getValueNode() {
    return delegate().getValueNode();
  }

  @Override
  public int getAttributeCount() {
    return delegate().getAttributeCount();
  }

  @Override
  public long getNodeKey() {
    return delegate().getNodeKey();
  }

  @Override
  public int getNamespaceCount() {
    return delegate().getNamespaceCount();
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
  public long getAttributeKey(@Nonnegative final int index) {
    return delegate().getAttributeKey(index);
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
  public int getPrefixKey() {
    return delegate().getPrefixKey();
  }

  @Override
  public int getLocalNameKey() {
    return delegate().getLocalNameKey();
  }

  @Override
  public long getParentKey() {
    return delegate().getParentKey();
  }

  @Override
  public boolean isNameNode() {
    return delegate().isNameNode();
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
  public int getTypeKey() {
    return delegate().getTypeKey();
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return delegate().acceptVisitor(visitor);
  }

  @Override
  public boolean isValueNode() {
    return delegate().isValueNode();
  }

  @Override
  public boolean hasAttributes() {
    return delegate().hasAttributes();
  }

  @Override
  public boolean hasNamespaces() {
    return delegate().hasNamespaces();
  }

  @Override
  public boolean hasChildren() {
    return delegate().hasChildren();
  }

  @Override
  public ImmutableXmlNode getNode() {
    return delegate().getNode();
  }

  @Override
  public int getURIKey() {
    return delegate().getURIKey();
  }

  @Override
  public boolean isStructuralNode() {
    return delegate().isStructuralNode();
  }

  @Override
  public List<Long> getAttributeKeys() {
    return delegate().getAttributeKeys();
  }

  @Override
  public BigInteger getHash() {
    return delegate().getHash();
  }

  @Override
  public List<Long> getNamespaceKeys() {
    return delegate().getNamespaceKeys();
  }

  @Override
  public byte[] getRawValue() {
    return delegate().getRawValue();
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
  public boolean isAttribute() {
    return delegate().isAttribute();
  }

  @Override
  public boolean isComment() {
    return delegate().isComment();
  }

  @Override
  public boolean isDocumentRoot() {
    return delegate().isDocumentRoot();
  }

  @Override
  public boolean isElement() {
    return delegate().isElement();
  }

  @Override
  public boolean isNamespace() {
    return delegate().isNamespace();
  }

  @Override
  public boolean isPI() {
    return delegate().isPI();
  }

  @Override
  public boolean isText() {
    return delegate().isText();
  }

  @Override
  public Optional<SirixDeweyID> getDeweyID() {
    return delegate().getDeweyID();
  }

  @Override
  public Optional<SirixDeweyID> getFirstChildDeweyID() {
    return delegate().getFirstChildDeweyID();
  }

  @Override
  public Optional<SirixDeweyID> getLeftSiblingDeweyID() {
    return delegate().getLeftSiblingDeweyID();
  }

  @Override
  public Optional<SirixDeweyID> getParentDeweyID() {
    return delegate().getParentDeweyID();
  }

  @Override
  public Optional<SirixDeweyID> getRightSiblingDeweyID() {
    return delegate().getRightSiblingDeweyID();
  }
}
