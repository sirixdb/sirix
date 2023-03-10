package org.sirix.index.redblacktree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.brackit.xquery.atomic.QNm;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.sirix.index.redblacktree.interfaces.MutableRBNode;
import org.sirix.index.redblacktree.keyvalue.CASValue;
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
public final class RBNode<K extends Comparable<? super K>, V> extends AbstractForwardingNode
    implements MutableRBNode<K, V> {
  /** Key token. */
  private K key;

  /** Value. */
  private V value;

  /** Reference to the left node. */
  private long left = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** Reference to the right node. */
  private long right = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** 'changed' status of tree node. */
  private boolean isChanged;

  /** {@link NodeDelegate} reference. */
  private NodeDelegate nodeDelegate;

  private RBNode<K,V> parent;

  private RBNode<K,V> leftChild;

  private RBNode<K,V> rightChild;

  /**
   * Constructor.
   *
   * @param key the key
   * @param value the value
   * @param nodeDelegate the used node delegate
   */
  public RBNode(final K key, final V value, final NodeDelegate nodeDelegate) {
    this.key = requireNonNull(key);
    this.value = requireNonNull(value);
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
  public V getValue() {
    return value;
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

  public RBNode<K, V> getParent() {
    return parent;
  }

  public RBNode<K, V> getLeftChild() {
    return leftChild;
  }

  public RBNode<K, V> getRightChild() {
    return rightChild;
  }

  public void setLeftChild(RBNode<K, V> leftChild) {
    this.leftChild = leftChild;
  }

  public void setRightChild(RBNode<K, V> rightChild) {
    this.rightChild = rightChild;
  }

  public void setParent(RBNode<K, V> parent) {
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
    if (obj instanceof RBNode) {
      @SuppressWarnings("unchecked")
      final RBNode<K, V> other = (RBNode<K, V>) obj;
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
                      .add("value", value)
                      .toString();
  }

  @Override
  public void setKey(final K key) {
    this.key = requireNonNull(key);
  }

  @Override
  public void setValue(final V value) {
    this.value = requireNonNull(value);
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
