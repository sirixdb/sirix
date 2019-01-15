package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.JSONArrayNode;
import org.sirix.node.xdm.ElementNode;

/**
 * Immutable element wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableJSONArrayNode extends AbstractImmutableJSONStructuralNode {

  /** Mutable {@link JSONArrayNode}. */
  private final JSONArrayNode mNode;

  /**
   * Private constructor.
   *
   * @param node mutable {@link ElementNode}
   */
  private ImmutableJSONArrayNode(final JSONArrayNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable JSON-array node instance.
   *
   * @param node the mutable {@link ImmutableJSONArrayNode} to wrap
   * @return immutable JSON-array node instance
   */
  public static ImmutableJSONArrayNode of(final JSONArrayNode node) {
    return new ImmutableJSONArrayNode(node);
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
