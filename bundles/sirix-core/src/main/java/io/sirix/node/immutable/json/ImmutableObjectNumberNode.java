package io.sirix.node.immutable.json;

import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.NodeKind;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.json.NumberNode;
import io.sirix.node.json.ObjectNumberNode;
import net.openhft.chronicle.bytes.Bytes;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable NumberNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectNumberNode extends AbstractImmutableJsonStructuralNode {
  /** Mutable {@link ObjectNumberNode}. */
  private final ObjectNumberNode node;

  /**
   * Private constructor.
   *
   * @param node {@link ObjectNumberNode} to wrap
   */
  private ImmutableObjectNumberNode(final ObjectNumberNode node) {
    this.node = requireNonNull(node);
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

  public ImmutableObjectNumberNode clone() {
    return new ImmutableObjectNumberNode(node.clone());
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public StructNode structDelegate() {
    return node.getStructNodeDelegate();
  }

  public Number getValue() {
    return node.getValue();
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.BOOLEAN_VALUE;
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    return node.computeHash(bytes);
  }
}
