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
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.NamespaceNode;

/**
 * Immutable namespace node wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public class ImmutableNamespace implements ImmutableNameNode, ImmutableXmlNode {

  /** Mutable {@link NamespaceNode}. */
  private final NamespaceNode mNode;

  /**
   * Private constructor.
   *
   * @param node {@link NamespaceNode} to wrap
   */
  private ImmutableNamespace(final NamespaceNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable namespace node instance.
   *
   * @param node the mutable {@link NamespaceNode} to wrap
   * @return immutable namespace node instance
   */
  public static ImmutableNamespace of(final NamespaceNode node) {
    return new ImmutableNamespace(node);
  }

  @Override
  public int getTypeKey() {
    return mNode.getTypeKey();
  }

  @Override
  public boolean isSameItem(final @Nullable Node pOther) {
    return mNode.isSameItem(pOther);
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor pVisitor) {
    return pVisitor.visit(this);
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

  @Override
  public QNm getName() {
    return mNode.getName();
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
  }
}
