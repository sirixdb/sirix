package org.sirix.index.redblacktree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.sirix.index.redblacktree.interfaces.MutableRBNodeValue;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.settings.Fixed;

import static java.util.Objects.requireNonNull;

/**
 * Red-black tree node which is mutable.
 *
 * @author Johannes Lichtenberger
 */
public final class RBNodeValue<V> extends AbstractForwardingNode
    implements MutableRBNodeValue<V> {
  /** The value. */
  private V value;

  /** Reference to the left node. */
  private static final long LEFT = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** Reference to the right node. */
  private static final long RIGHT = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** {@link NodeDelegate} reference. */
  private NodeDelegate nodeDelegate;

  /**
   * Constructor.
   *
   * @param value the value
   * @param nodeDelegate the used node delegate
   */
  public RBNodeValue(final V value, final NodeDelegate nodeDelegate) {
    this.value = requireNonNull(value);
    this.nodeDelegate = requireNonNull(nodeDelegate);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.RB_NODE_VALUE;
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return nodeDelegate;
  }

  @Override
  public V getValue() {
    return value;
  }

  @Override
  public boolean hasLeftChild() {
    return LEFT != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasRightChild() {
    return RIGHT != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLeftChildKey() {
    return LEFT;
  }

  @Override
  public long getRightChildKey() {
    return RIGHT;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeDelegate.getNodeKey());
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof RBNodeValue) {
      @SuppressWarnings("unchecked")
      final RBNodeValue<V> other = (RBNodeValue<V>) obj;
      return this.nodeDelegate.getNodeKey() == other.nodeDelegate.getNodeKey();
    }
    return false;
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", nodeDelegate)
                      .add("value", value)
                      .toString();
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return null;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    return null;
  }

  @Override
  public int getPreviousRevisionNumber() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setValue(V value) {
    this.value = value;
  }
}
