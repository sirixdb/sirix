package org.sirix.node.immutable.json;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.StructNode;
import org.sirix.node.json.JSONBooleanNode;
import org.sirix.node.json.JSONStringNode;
import org.sirix.node.xdm.TextNode;

/**
 * Immutable JSONBooleanNode wrapper.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class ImmutableJSONBooleanNode extends AbstractImmutableJSONStructuralNode {
  /** Mutable {@link JSONStringNode}. */
  private final JSONBooleanNode mNode;

  /**
   * Private constructor.
   *
   * @param node {@link JSONStringNode} to wrap
   */
  private ImmutableJSONBooleanNode(final JSONBooleanNode node) {
    mNode = checkNotNull(node);
  }

  /**
   * Get an immutable text node instance.
   *
   * @param node the mutable {@link TextNode} to wrap
   * @return immutable text node instance
   */
  public static ImmutableJSONBooleanNode of(final JSONBooleanNode node) {
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
