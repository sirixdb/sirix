package org.sirix.node.immutable.xml;

import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.ElementNode;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable element wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public class ImmutableElement implements ImmutableNameNode, ImmutableStructNode, ImmutableXmlNode {

  /** Mutable {@link ElementNode}. */
  private final ElementNode node;

  /**
   * Private constructor.
   *
   * @param node mutable {@link ElementNode}
   */
  private ImmutableElement(final ElementNode node) {
    this.node = checkNotNull(node);
  }

  /**
   * Get an immutable element node instance.
   *
   * @param node the mutable {@link ElementNode} to wrap
   * @return immutable element instance
   */
  public static ImmutableElement of(final ElementNode node) {
    return new ImmutableElement(node);
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
    return node.hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    return node.hasRightSibling();
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
    return node.getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    return node.getRightSiblingKey();
  }

  @Override
  public int getTypeKey() {
    return node.getTypeKey();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return node.isSameItem(other);
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
  public long getRevision() {
    return node.getRevision();
  }

  @Override
  public int getLocalNameKey() {
    return node.getLocalNameKey();
  }

  @Override
  public int getPrefixKey() {
    return node.getPrefixKey();
  }

  @Override
  public int getURIKey() {
    return node.getURIKey();
  }

  @Override
  public long getPathNodeKey() {
    return node.getPathNodeKey();
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

  /**
   * Get the namespace count.
   *
   * @return namespace count
   */
  public int getNamespaceCount() {
    return node.getNamespaceCount();
  }

  /**
   * Get the attribute count.
   *
   * @return attribute count
   */
  public int getAttributeCount() {
    return node.getAttributeCount();
  }

  @Override
  public QNm getName() {
    return node.getName();
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
