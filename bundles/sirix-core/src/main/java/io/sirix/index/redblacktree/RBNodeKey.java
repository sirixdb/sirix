package io.sirix.index.redblacktree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.index.redblacktree.interfaces.MutableRBNodeKey;
import io.sirix.node.AbstractForwardingNode;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import io.sirix.index.redblacktree.keyvalue.CASValue;
import io.sirix.node.delegates.NodeDelegate;
import io.sirix.settings.Fixed;

import static java.util.Objects.requireNonNull;

/**
 * Red-black tree node which is mutable.
 *
 * @author Johannes Lichtenberger
 */
public final class RBNodeKey<K extends Comparable<? super K>> extends AbstractForwardingNode
    implements MutableRBNodeKey<K> {
  /** Key token. */
  private K key;

  /** Value. */
  private long valueNodeKey;

  /** Reference to the left node. */
  private long left = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** Reference to the right node. */
  private long right = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** 'changed' status of tree node. */
  private boolean isChanged;

  /** {@link NodeDelegate} reference. */
  private final NodeDelegate nodeDelegate;

  private RBNodeKey<K> parent;

  private RBNodeKey<K> leftChild;

  private RBNodeKey<K> rightChild;

  /**
   * Constructor.
   *
   * @param key the key
   * @param valueNodeKey the node key of the value node
   * @param nodeDelegate the used node delegate
   */
  public RBNodeKey(final K key, final long valueNodeKey, final NodeDelegate nodeDelegate) {
    this.key = requireNonNull(key);
    this.valueNodeKey = valueNodeKey;
    this.nodeDelegate = requireNonNull(nodeDelegate);
  }

  @Override
  public NodeKind getKind() {
    if (key instanceof Long) {
      return NodeKind.PATHRB;
    }
    if (key instanceof CASValue) {
      return NodeKind.CASRB;
    }
    if (key instanceof QNm) {
      return NodeKind.NAMERB;
    }
    return NodeKind.UNKNOWN;
  }

  @Override
  protected @NotNull NodeDelegate delegate() {
    return nodeDelegate;
  }

  @Override
  public K getKey() {
    return key;
  }

  @Override
  public long getValueNodeKey() {
    return valueNodeKey;
  }

  /**
   * Flag which determines if node is changed.
   *
   * @return {@code true} if it has been changed in memory, {@code false} otherwise
   */
  @Override
  public boolean isChanged() {
    return isChanged;
  }

  @Override
  public void setChanged(final boolean changed) {
    isChanged = changed;
  }

  @Override
  public boolean hasLeftChild() {
    return left != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasRightChild() {
    return right != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLeftChildKey() {
    return left;
  }

  @Override
  public long getRightChildKey() {
    return right;
  }

  public RBNodeKey<K> getParent() {
    return parent;
  }

  public RBNodeKey<K> getLeftChild() {
    return leftChild;
  }

  public RBNodeKey<K> getRightChild() {
    return rightChild;
  }

  public void setLeftChild(RBNodeKey<K> leftChild) {
    this.leftChild = leftChild;
  }

  public void setRightChild(RBNodeKey<K> rightChild) {
    this.rightChild = rightChild;
  }

  public void setParent(RBNodeKey<K> parent) {
    this.parent = parent;
  }

  @Override
  public void setLeftChildKey(final long left) {
    this.left = left;
  }

  @Override
  public void setRightChildKey(final long right) {
    this.right = right;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeDelegate.getNodeKey());
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof RBNodeKey) {
      @SuppressWarnings("unchecked")
      final RBNodeKey<K> other = (RBNodeKey<K>) obj;
      return this.nodeDelegate.getNodeKey() == other.nodeDelegate.getNodeKey();
    }
    return false;
  }

  @Override
  public @NotNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", nodeDelegate)
                      .add("left child", left)
                      .add("right child", right)
                      .add("changed", isChanged)
                      .add("key", key)
                      .add("valueNodeKey", valueNodeKey)
                      .toString();
  }

  @Override
  public void setKey(final K key) {
    this.key = requireNonNull(key);
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
}
