package io.sirix.node.immutable.json;

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.BytesOut;
import io.sirix.node.json.ObjectBooleanNode;
import io.sirix.node.json.StringNode;
import io.sirix.node.xml.TextNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable JSONBooleanNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectBooleanNode extends AbstractImmutableJsonStructuralNode {
  /** Mutable {@link ObjectBooleanNode}. */
  private final ObjectBooleanNode node;

  /**
   * Private constructor.
   *
   * @param node {@link StringNode} to wrap
   */
  private ImmutableObjectBooleanNode(final ObjectBooleanNode node) {
    this.node = requireNonNull(node);
  }

  /**
   * Get an immutable text node instance.
   *
   * @param node the mutable {@link TextNode} to wrap
   * @return immutable text node instance
   */
  public static ImmutableObjectBooleanNode of(final ObjectBooleanNode node) {
    return new ImmutableObjectBooleanNode(node);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public StructNode structDelegate() {
    return node;
  }

  public boolean getValue() {
    return node.getValue();
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.BOOLEAN_VALUE;
  }

  @Override
  public long computeHash(BytesOut<?> bytes) {
    return node.computeHash(bytes);
  }
}
