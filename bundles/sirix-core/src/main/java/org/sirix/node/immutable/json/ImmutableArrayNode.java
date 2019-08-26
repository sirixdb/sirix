package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import java.math.BigInteger;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.NodeKind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.ArrayNode;
import org.sirix.node.xdm.ElementNode;

/**
 * Immutable array node wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableArrayNode extends AbstractImmutableJsonStructuralNode {

  /** Mutable {@link ArrayNode}. */
  private final ArrayNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link ElementNode}
   */
  private ImmutableArrayNode(final ArrayNode node) {
    mNode = checkNotNull(node);
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(this);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableArrayNode} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableArrayNode of(final ArrayNode node) {
    return new ImmutableArrayNode(node);
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
