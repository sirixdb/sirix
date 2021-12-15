package org.sirix.node.immutable.json;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.BooleanNode;
import org.sirix.node.json.StringNode;
import org.sirix.node.xml.TextNode;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable JSONBooleanNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableBooleanNode extends AbstractImmutableJsonStructuralNode {
  /** Mutable {@link BooleanNode}. */
  private final BooleanNode node;

  /**
   * Private constructor.
   *
   * @param node {@link StringNode} to wrap
   */
  private ImmutableBooleanNode(final BooleanNode node) {
    this.node = checkNotNull(node);
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
    return node.getStructNodeDelegate();
  }

  public boolean getValue() {
    return node.getValue();
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.BOOLEAN_VALUE;
  }

  @Override
  public BigInteger computeHash() {
    return node.computeHash();
  }

}
