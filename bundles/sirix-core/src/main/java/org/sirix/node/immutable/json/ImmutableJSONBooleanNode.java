package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.JsonBooleanNode;
import org.sirix.node.json.JsonStringNode;
import org.sirix.node.xdm.TextNode;

/**
 * Immutable JSONBooleanNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableJSONBooleanNode extends AbstractImmutableJSONStructuralNode {
  /** Mutable {@link JsonStringNode}. */
  private final JsonBooleanNode mNode;

  /**
   * Private constructor.
   *
   * @param node {@link JsonStringNode} to wrap
   */
  private ImmutableJSONBooleanNode(final JsonBooleanNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable text node instance.
   *
   * @param node the mutable {@link TextNode} to wrap
   * @return immutable text node instance
   */
  public static ImmutableJSONBooleanNode of(final JsonBooleanNode node) {
    return new ImmutableJSONBooleanNode(node);
  }

  @Override
  public StructNode structDelegate() {
    return mNode.getStructNodeDelegate();
  }

  public boolean getValue() {
    return mNode.getValue();
  }

  @Override
  public Kind getKind() {
    return Kind.JSON_BOOLEAN_VALUE;
  }
}
