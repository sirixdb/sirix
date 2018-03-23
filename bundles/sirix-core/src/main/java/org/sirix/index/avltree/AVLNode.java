package org.sirix.index.avltree;

import static com.google.common.base.Preconditions.checkNotNull;
import javax.annotation.Nullable;
import org.brackit.xquery.atomic.QNm;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.index.avltree.interfaces.MutableAVLNode;
import org.sirix.index.avltree.keyvalue.CASValue;
import org.sirix.node.AbstractForwardingNode;
import org.sirix.node.Kind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.settings.Fixed;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * AVLNode which is mutable.
 *
 * @author Johannes Lichtenberger
 */
public final class AVLNode<K extends Comparable<? super K>, V> extends AbstractForwardingNode
    implements MutableAVLNode<K, V> {
  /** Key token. */
  private K mKey;

  /** Value. */
  private V mValue;

  /** Reference to the left node. */
  private long mLeft = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** Reference to the right node. */
  private long mRight = Fixed.NULL_NODE_KEY.getStandardProperty();

  /** 'changed' status of tree node. */
  private boolean mChanged;

  /** {@link NodeDelegate} reference. */
  private NodeDelegate mNodeDelegate;

  /**
   * Constructor.
   *
   * @param pToken token
   * @param pParent id of the parent node
   */
  public AVLNode(final K key, final V value, final NodeDelegate delegate) {
    mKey = checkNotNull(key);
    mValue = checkNotNull(value);
    mNodeDelegate = checkNotNull(delegate);
  }

  @Override
  public Kind getKind() {
    if (mKey instanceof Long) {
      return Kind.PATHAVL;
    }
    if (mKey instanceof CASValue) {
      return Kind.CASAVL;
    }
    if (mKey instanceof QNm) {
      return Kind.NAMEAVL;
    }
    return Kind.UNKNOWN;
  }

  @Override
  protected NodeDelegate delegate() {
    return mNodeDelegate;
  }

  @Override
  public K getKey() {
    return mKey;
  }

  @Override
  public V getValue() {
    return mValue;
  }

  /**
   * Flag which determines if node is changed.
   *
   * @return {@code true} if it has been changed in memory, {@code false} otherwise
   */
  @Override
  public boolean isChanged() {
    return mChanged;
  }

  @Override
  public void setChanged(final boolean changed) {
    mChanged = changed;
  }

  @Override
  public boolean hasLeftChild() {
    return mLeft != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasRightChild() {
    return mRight != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLeftChildKey() {
    return mLeft;
  }

  @Override
  public long getRightChildKey() {
    return mRight;
  }

  @Override
  public void setLeftChildKey(final long left) {
    mLeft = left;
  }

  @Override
  public void setRightChildKey(final long right) {
    mRight = right;
  }

  @Override
  public VisitResultType acceptVisitor(final Visitor visitor) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodeDelegate.getNodeKey());
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof AVLNode) {
      @SuppressWarnings("unchecked")
      final AVLNode<K, V> other = (AVLNode<K, V>) obj;
      return this.mNodeDelegate.getNodeKey() == other.mNodeDelegate.getNodeKey();
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node delegate", mNodeDelegate)
                      .add("left child", mLeft)
                      .add("right child", mRight)
                      .add("changed", mChanged)
                      .add("key", mKey)
                      .add("value", mValue)
                      .toString();
  }

  @Override
  public void setKey(final K key) {
    mKey = checkNotNull(key);
  }

  @Override
  public void setValue(final V value) {
    mValue = checkNotNull(value);
  }
}
