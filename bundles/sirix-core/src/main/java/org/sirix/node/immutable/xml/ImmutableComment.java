package org.sirix.node.immutable.xml;

import static com.google.common.base.Preconditions.checkNotNull;
import java.math.BigInteger;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.immutable.ImmutableStructNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.node.interfaces.immutable.ImmutableXmlNode;
import org.sirix.node.xml.CommentNode;

/**
 * Immutable comment node wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableComment implements ImmutableValueNode, ImmutableStructNode, ImmutableXmlNode {

  /** Mutable {@link CommentNode}. */
  private final CommentNode mNode;

  /**
   * Constructor.
   *
   * @param node mutable {@link CommentNode}
   */
  private ImmutableComment(final CommentNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable comment node instance.
   *
   * @param node the mutable {@link CommentNode} to wrap
   * @return immutable comment node instance
   */
  public static ImmutableComment of(final CommentNode node) {
    return new ImmutableComment(node);
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
  public byte[] getRawValue() {
    return mNode.getRawValue();
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
  public String getValue() {
    return mNode.getValue();
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
  }
}
