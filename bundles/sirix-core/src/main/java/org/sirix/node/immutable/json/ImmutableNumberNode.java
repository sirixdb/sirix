package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.NumberNode;

/**
 * Immutable NumberNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableNumberNode extends AbstractImmutableJsonStructuralNode {
  /** Mutable {@link NumberNode}. */
  private final NumberNode mNode;

  /**
   * Private constructor.
   *
   * @param node {@link NumberNode} to wrap
   */
  private ImmutableNumberNode(final NumberNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable text node instance.
   *
   * @param node the mutable {@link NumberNode} to wrap
   * @return immutable text node instance
   */
  public static ImmutableNumberNode of(final NumberNode node) {
    return new ImmutableNumberNode(node);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public StructNode structDelegate() {
    return mNode.getStructNodeDelegate();
  }

  public double getValue() {
    return mNode.getValue();
  }

  @Override
  public Kind getKind() {
    return Kind.JSON_BOOLEAN_VALUE;
  }
}
