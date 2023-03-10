package org.sirix.node;

import org.jetbrains.annotations.NotNull;
import org.sirix.node.delegates.NodeDelegate;

import static java.util.Objects.requireNonNull;

/**
 * If a node is deleted, it will be encapsulated over this class.
 *
 * @author Sebastian Graf
 *
 */
public final class DeweyIDMappingNode extends AbstractForwardingNode {

  /**
   * Delegate for common data.
   */
  private final NodeDelegate mDelegate;

  /**
   * Constructor.
   *
   * @param nodeDelegate node delegate
   */
  public DeweyIDMappingNode(final NodeDelegate nodeDelegate) {
    mDelegate = requireNonNull(nodeDelegate);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.DEWEYIDMAPPING;
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return mDelegate;
  }
}
