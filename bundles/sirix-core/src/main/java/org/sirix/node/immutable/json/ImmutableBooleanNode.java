package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.BooleanNode;
import org.sirix.node.json.StringNode;
import org.sirix.node.xdm.TextNode;

/**
 * Immutable JSONBooleanNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableBooleanNode extends AbstractImmutableJsonStructuralNode {
  /** Mutable {@link BooleanNode}. */
  private final BooleanNode mNode;

  /**
   * Private constructor.
   *
   * @param node {@link StringNode} to wrap
   */
  private ImmutableBooleanNode(final BooleanNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable text node instance.
   *
   * @param node the mutable {@link TextNode} to wrap
   * @return immutable text node instance
   */
  public static ImmutableBooleanNode of(final BooleanNode node) {
    return new ImmutableBooleanNode(node);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public StructNode structDelegate() {
    return mNode.getStructNodeDelegate();
  }

  public boolean getValue() {
    return mNode.getValue();
  }

  @Override
  public Kind getKind() {
    return Kind.JSON_BOOLEAN_VALUE;
  }
}
