package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.JsonObjectNode;
import org.sirix.node.json.JsonObjectKeyNode;

/**
 * Immutable JSONObject wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableJSONObjectKeyNode extends AbstractImmutableJSONStructuralNode {

  /** Mutable {@link JsonObjectNode}. */
  private final JsonObjectKeyNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link JsonObjectNode}
   */
  private ImmutableJSONObjectKeyNode(final JsonObjectKeyNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableJSONObjectKeyNode} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableJSONObjectKeyNode of(final JsonObjectKeyNode node) {
    return new ImmutableJSONObjectKeyNode(node);
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
