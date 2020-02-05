package org.sirix.node.immutable.json;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.NumberNode;
import org.sirix.node.json.ObjectNumberNode;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable NumberNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectNumberNode extends AbstractImmutableJsonStructuralNode {
  /** Mutable {@link ObjectNumberNode}. */
  private final ObjectNumberNode mNode;

  /**
   * Private constructor.
   *
   * @param node {@link ObjectNumberNode} to wrap
   */
  private ImmutableObjectNumberNode(final ObjectNumberNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable text node instance.
   *
   * @param node the mutable {@link NumberNode} to wrap
   * @return immutable text node instance
   */
  public static ImmutableObjectNumberNode of(final ObjectNumberNode node) {
    return new ImmutableObjectNumberNode(node);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public StructNode structDelegate() {
    return mNode.getStructNodeDelegate();
  }

  public Number getValue() {
    return mNode.getValue();
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.BOOLEAN_VALUE;
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
  }
}
