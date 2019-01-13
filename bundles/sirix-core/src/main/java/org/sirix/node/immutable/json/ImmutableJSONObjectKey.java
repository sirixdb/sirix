package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.JSONObjectNode;
import org.sirix.node.json.JSONObjectKeyNode;

/**
 * Immutable JSONObject wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableJSONObjectKey extends AbstractImmutableJSONStructuralNode {

  /** Mutable {@link JSONObjectNode}. */
  private final JSONObjectKeyNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link JSONObjectNode}
   */
  private ImmutableJSONObjectKey(final JSONObjectKeyNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableJSONObjectKey} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableJSONObjectKey of(final JSONObjectKeyNode node) {
    return new ImmutableJSONObjectKey(node);
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
