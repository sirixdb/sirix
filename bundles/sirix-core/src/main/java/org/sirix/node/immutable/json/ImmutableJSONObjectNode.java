package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.JsonObjectNode;

/**
 * Immutable JSONObject wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableJSONObjectNode extends AbstractImmutableJSONStructuralNode {

  /** Mutable {@link JsonObjectNode}. */
  private final JsonObjectNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link JsonObjectNode}
   */
  private ImmutableJSONObjectNode(final JsonObjectNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableJSONObjectNode} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableJSONObjectNode of(final JsonObjectNode node) {
    return new ImmutableJSONObjectNode(node);
  }

  @Override
  public StructNode structDelegate() {
    return mNode;
  }

  @Override
  public Kind getKind() {
    return Kind.JSON_ARRAY;
  }
}
