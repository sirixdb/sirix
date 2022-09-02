package org.sirix.node.immutable.xml;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.XmlDocumentRootNode;
import org.sirix.settings.Fixed;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable document root node wrapper.
 *
 * @author Johannes Lichtenberger
 */
public class ImmutableXmlDocumentRootNode implements ImmutableStructNode, ImmutableXmlNode {

  /** Mutable {@link XmlDocumentRootNode} instance. */
  private final XmlDocumentRootNode node;

  /**
   * Private constructor.
   *
   * @param node mutable {@link XmlDocumentRootNode}
   */
  private ImmutableXmlDocumentRootNode(final XmlDocumentRootNode node) {
    this.node = checkNotNull(node);
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
    return node.getTypeKey();
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
    return node.getHash();
  }

  @Override
  public long getParentKey() {
    return node.getParentKey();
  }

  @Override
  public boolean hasParent() {
    return node.hasParent();
  }

  @Override
  public long getNodeKey() {
    return node.getNodeKey();
  }

  @Override
  public NodeKind getKind() {
    return node.getKind();
  }

  @Override
  public int getPreviousRevisionNumber() {
    return node.getPreviousRevisionNumber();
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return node.getLastModifiedRevisionNumber();
  }

  @Override
  public boolean hasFirstChild() {
    return node.hasFirstChild();
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
    return node.getChildCount();
  }

  @Override
  public long getDescendantCount() {
    return node.getDescendantCount();
  }

  @Override
  public long getFirstChildKey() {
    return node.getFirstChildKey();
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
    return node.getDeweyID();
  }

  @Override
  public boolean equals(Object obj) {
    return node.equals(obj);
  }

  @Override
  public int hashCode() {
    return node.hashCode();
  }

  @Override
  public String toString() {
    return node.toString();
  }

  @Override
  public BigInteger computeHash() {
    return node.computeHash();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return node.getDeweyIDAsBytes();
  }
}
