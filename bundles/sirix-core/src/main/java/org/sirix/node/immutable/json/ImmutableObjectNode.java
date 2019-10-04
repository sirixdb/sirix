package org.sirix.node.immutable.json;

import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.ObjectNode;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable JSONObject wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectNode extends AbstractImmutableJsonStructuralNode {

  /** Mutable {@link ObjectNode}. */
  private final ObjectNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link ObjectNode}
   */
  private ImmutableObjectNode(final ObjectNode node) {
    mNode = checkNotNull(node);
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
    return mNode;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ARRAY;
  }

  @Override
  public BigInteger computeHash() {
    return mNode.computeHash();
  }
}
