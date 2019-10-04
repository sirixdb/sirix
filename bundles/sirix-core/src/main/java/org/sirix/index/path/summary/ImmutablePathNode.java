package org.sirix.index.path.summary;

import org.brackit.xquery.atomic.QNm;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

import javax.annotation.Nullable;
import java.math.BigInteger;

/**
 * Wraps a {@link PathNode} to provide immutability.
 *
 * @author Johannes Lichtenberger
 *
 */
public class ImmutablePathNode implements ImmutableNameNode, ImmutableStructNode {

  /** {@link PathNode} instance. */
  private final PathNode mNode;

  /**
   * Private constructor.
   *
   * @param node the mutable path node
   */
  public ImmutablePathNode(final PathNode node) {
    mNode = node;
  }

  /**
   * Get an immutable path node instance.
   *
   * @param node the mutable {@link PathNode} to wrap
   * @return immutable path node instance
   */
  public static ImmutableNode of(final PathNode node) {
    return new ImmutablePathNode(node);
  }

  @Override
  public NodeKind getKind() {
    return mNode.getKind();
  }

  @Override
  public boolean isSameItem(@Nullable Node other) {
    return mNode.isSameItem(other);
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
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
  public boolean hasFirstChild() {
    return mNode.hasFirstChild();
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
  public long getLeftSiblingKey() {
    return mNode.getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    return mNode.getRightSiblingKey();
  }

  @Override
  public QNm getName() {
    return mNode.getName();
  }
}
