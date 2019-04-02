package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResult;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.ObjectKeyNode;
import org.sirix.node.json.ObjectNode;

/**
 * Immutable JSONObject wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableObjectKeyNode extends AbstractImmutableJsonStructuralNode {

  /** Mutable {@link ObjectNode}. */
  private final ObjectKeyNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link ObjectNode}
   */
  private ImmutableObjectKeyNode(final ObjectKeyNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableObjectKeyNode} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableObjectKeyNode of(final ObjectKeyNode node) {
    return new ImmutableObjectKeyNode(node);
  }

  /**
   * Get a path node key.
   *
   * @return path node key
   */
  public long getPathNodeKey() {
    return mNode.getPathNodeKey();
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
  public Kind getKind() {
    return Kind.JSON_ARRAY;
  }

  public String getName() {
    return mNode.getName();
  }
}
