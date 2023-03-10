package org.sirix.node.immutable.json;

import net.openhft.chronicle.bytes.Bytes;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.NumberNode;
import org.sirix.node.json.ObjectNumberNode;

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
