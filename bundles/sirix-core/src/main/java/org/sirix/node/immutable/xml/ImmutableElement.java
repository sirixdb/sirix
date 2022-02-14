package org.sirix.node.immutable.xml;

import static com.google.common.base.Preconditions.checkNotNull;
import java.math.BigInteger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.ElementNode;

/**
 * Immutable element wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public class ImmutableElement implements ImmutableNameNode, ImmutableStructNode, ImmutableXmlNode {

  /** Mutable {@link ElementNode}. */
  private final ElementNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link ElementNode}
   */
  private ImmutableElement(final ElementNode node) {
    mNode = checkNotNull(node);
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
    return mNode.hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    return false;
  }

  @Override
  public boolean hasLeftSibling() {
    return mNode.hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    return mNode.hasRightSibling();
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
    return mNode.getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    return mNode.getRightSiblingKey();
  }

  @Override
  public int getTypeKey() {
    return mNode.getTypeKey();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return mNode.isSameItem(other);
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
  public int getLocalNameKey() {
    return mNode.getLocalNameKey();
  }

  @Override
  public int getPrefixKey() {
    return mNode.getPrefixKey();
  }

  @Override
  public int getURIKey() {
    return mNode.getURIKey();
  }

  @Override
  public long getPathNodeKey() {
    return mNode.getPathNodeKey();
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

  /**
   * Get the namespace count.
   *
   * @return namespace count
   */
  public int getNamespaceCount() {
    return mNode.getNamespaceCount();
  }

  /**
   * Get the attribute count.
   *
   * @return attribute count
   */
  public int getAttributeCount() {
    return mNode.getAttributeCount();
  }

  @Override
  public QNm getName() {
    return mNode.getName();
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
  }
}
