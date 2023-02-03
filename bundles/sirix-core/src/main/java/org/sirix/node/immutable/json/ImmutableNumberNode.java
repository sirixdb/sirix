package org.sirix.node.immutable.json;

import net.openhft.chronicle.bytes.Bytes;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.NumberNode;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable NumberNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableNumberNode extends AbstractImmutableJsonStructuralNode {
  /** Mutable {@link NumberNode}. */
  private final NumberNode node;

  /**
   * Private constructor.
   *
   * @param node {@link NumberNode} to wrap
   */
  private ImmutableNumberNode(final NumberNode node) {
    this.node = checkNotNull(node);
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
