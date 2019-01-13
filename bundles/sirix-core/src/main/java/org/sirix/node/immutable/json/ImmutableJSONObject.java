package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.JSONObjectNode;

/**
 * Immutable JSONObject wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableJSONObject extends AbstractImmutableJSONStructuralNode {

  /** Mutable {@link JSONObjectNode}. */
  private final JSONObjectNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link JSONObjectNode}
   */
  private ImmutableJSONObject(final JSONObjectNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableJSONObject} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableJSONObject of(final JSONObjectNode node) {
    return new ImmutableJSONObject(node);
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
