package org.sirix.node.immutable.json;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.node.json.StringNode;
import org.sirix.node.xml.TextNode;

import javax.annotation.Nullable;
import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable JSONValueString wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableStringNode extends AbstractImmutableJsonStructuralNode implements ImmutableValueNode {
  /** Mutable {@link StringNode}. */
  private final StringNode mNode;

  /**
   * Private constructor.
   *
   * @param node {@link StringNode} to wrap
   */
  private ImmutableStringNode(final StringNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable text node instance.
   *
   * @param node the mutable {@link TextNode} to wrap
   * @return immutable text node instance
   */
  public static ImmutableStringNode of(final StringNode node) {
    return new ImmutableStringNode(node);
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return mNode.isSameItem(other);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
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
  public byte[] getRawValue() {
    return mNode.getRawValue();
  }

  @Override
  public String getValue() {
    return mNode.getValue();
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
  public StructNode structDelegate() {
    return mNode.getStructNodeDelegate();
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
  }
}
