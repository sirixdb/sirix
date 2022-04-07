package org.sirix.index.path.summary;

import org.brackit.xquery.atomic.QNm;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableNameNode;
import org.sirix.node.interfaces.immutable.ImmutableNode;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;

import org.checkerframework.checker.nullness.qual.Nullable;
import java.math.BigInteger;

/**
 * Wraps a {@link PathNode} to provide immutability.
 *
 * @author Johannes Lichtenberger
 *
 */
public class ImmutablePathNode implements ImmutableNameNode, ImmutableStructNode {

  /** {@link PathNode} instance. */
  private final PathNode node;

  /**
   * Private constructor.
   *
   * @param node the mutable path node
   */
  public ImmutablePathNode(final PathNode node) {
    this.node = node;
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
    return node.getKind();
  }

  @Override
  public boolean isSameItem(@Nullable Node other) {
    return node.isSameItem(other);
  }

  @Override
  public BigInteger computeHash() {
    return node.computeHash();
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
  public boolean hasFirstChild() {
    return node.hasFirstChild();
  }

  @Override
  public boolean hasLastChild() {
    return node.hasLastChild();
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
    return node.getLastChildKey();
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
  public QNm getName() {
    return node.getName();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return node.getDeweyID();
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return node.getDeweyIDAsBytes();
  }
}
