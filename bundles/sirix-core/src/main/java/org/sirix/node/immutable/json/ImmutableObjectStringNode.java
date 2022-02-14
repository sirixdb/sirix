package org.sirix.node.immutable.json;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.interfaces.immutable.ImmutableValueNode;
import org.sirix.node.json.ObjectStringNode;

import org.checkerframework.checker.nullness.qual.Nullable;
import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable JSONValueString wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectStringNode extends AbstractImmutableJsonStructuralNode implements ImmutableValueNode {
  /** Mutable {@link ObjectStringNode}. */
  private final ObjectStringNode node;

  /**
   * Private constructor.
   *
   * @param node {@link ObjectStringNode} to wrap
   */
  private ImmutableObjectStringNode(final ObjectStringNode node) {
    this.node = checkNotNull(node);
  }

  /**
   * Get an immutable text node instance.
   *
   * @param node the mutable {@link ObjectStringNode} to wrap
   * @return immutable text node instance
   */
  public static ImmutableObjectStringNode of(final ObjectStringNode node) {
    return new ImmutableObjectStringNode(node);
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    return node.isSameItem(other);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
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
  public byte[] getRawValue() {
    return node.getRawValue();
  }

  @Override
  public String getValue() {
    return node.getValue();
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
  public StructNode structDelegate() {
    return node.getStructNodeDelegate();
  }

  @Override
  public BigInteger computeHash() {
    return node.computeHash();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return node.getDeweyID();
  }
}
