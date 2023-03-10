package org.sirix.node.immutable.json;

import net.openhft.chronicle.bytes.Bytes;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.ObjectNode;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

/**
 * Immutable JSONObject wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectNode extends AbstractImmutableJsonStructuralNode {

  /** Mutable {@link ObjectNode}. */
  private final ObjectNode node;

  /**
   * Private constructor.
   *
   * @param node mutable {@link ObjectNode}
   */
  private ImmutableObjectNode(final ObjectNode node) {
    this.node = requireNonNull(node);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableObjectNode} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableObjectNode of(final ObjectNode node) {
    return new ImmutableObjectNode(node);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
  }

  @Override
  public StructNode structDelegate() {
    return node;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ARRAY;
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    return node.computeHash(bytes);
  }
}
