package org.sirix.node.immutable.xml;

import static com.google.common.base.Preconditions.checkNotNull;
import java.math.BigInteger;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.XmlDocumentRootNode;
import org.sirix.settings.Fixed;

/**
 * Immutable document root node wrapper.
 *
 * @author Johannes Lichtenberger
 */
public class ImmutableXmlDocumentRootNode implements ImmutableStructNode, ImmutableXmlNode {

  /** Mutable {@link XmlDocumentRootNode} instance. */
  private final XmlDocumentRootNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link XmlDocumentRootNode}
   */
  private ImmutableXmlDocumentRootNode(final XmlDocumentRootNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable document root node instance.
   *
   * @param node the mutable {@link XmlDocumentRootNode} to wrap
   * @return immutable document root node instance
   */
  public static ImmutableXmlDocumentRootNode of(final XmlDocumentRootNode node) {
    return new ImmutableXmlDocumentRootNode(node);
  }

  @Override
  public int getTypeKey() {
    return mNode.getTypeKey();
  }

  @Override
  public boolean isSameItem(Node other) {
    return false;
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public BigInteger getHash() {
    return mNode.getHash();
  }

  @Override
  public long getParentKey() {
    return mNode.getParentKey();
  }

  @Override
  public boolean hasParent() {
    return mNode.hasParent();
  }

  @Override
  public long getNodeKey() {
    return mNode.getNodeKey();
  }

  @Override
  public NodeKind getKind() {
    return mNode.getKind();
  }

  @Override
  public long getRevision() {
    return mNode.getRevision();
  }

  @Override
  public boolean hasFirstChild() {
    return mNode.hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    return false;
  }

  @Override
  public boolean hasLeftSibling() {
    return false;
  }

  @Override
  public boolean hasRightSibling() {
    return false;
  }

  @Override
  public long getChildCount() {
    return mNode.getChildCount();
  }

  @Override
  public long getDescendantCount() {
    return mNode.getDescendantCount();
  }

  @Override
  public long getFirstChildKey() {
    return mNode.getFirstChildKey();
  }

  @Override
  public long getLastChildKey() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getLeftSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getRightSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return mNode.getDeweyID();
  }

  @Override
  public boolean equals(Object obj) {
    return mNode.equals(obj);
  }

  @Override
  public int hashCode() {
    return mNode.hashCode();
  }

  @Override
  public String toString() {
    return mNode.toString();
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
  }
}
