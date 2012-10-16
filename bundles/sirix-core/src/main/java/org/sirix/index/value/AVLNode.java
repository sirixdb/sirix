package org.sirix.index.value;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;

import javax.annotation.Nonnull;

import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.AbsForwardingNode;
import org.sirix.node.Kind;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.settings.EFixed;

/**
 * AVLNode.
 */
public class AVLNode<K extends Comparable<? super K>, V> extends
  AbsForwardingNode {
  /** Key token. */
  private K mKey;

  /** Value. */
  private V mValue;

  /** Reference to the left node. */
  private long mLeft;

  /** Reference to the right node. */
  private long mRight;

  /** 'changed' status of tree node. */
  private boolean mChanged;

  /** {@link NodeDelegate} reference. */
  private NodeDelegate mNodeDelegate;

  /**
   * Constructor.
   * 
   * @param pToken
   *          token
   * @param pParent
   *          id of the parent node
   */
  public AVLNode(final @Nonnull K pKey, final @Nonnull V pValue,
    final @Nonnull NodeDelegate pDelegate) {
    mKey = checkNotNull(pKey);
    mValue = checkNotNull(pValue);
    mNodeDelegate = checkNotNull(pDelegate);
  }

  @Override
  public Kind getKind() {
    return Kind.AVL;
  }

  @Override
  protected NodeDelegate delegate() {
    return mNodeDelegate;
  }

  /**
   * Key to be indexed.
   * 
   * @return key reference
   */
  public K getKey() {
    return mKey;
  }

  /**
   * Value to be indexed.
   * 
   * @return key reference
   */
  public V getValue() {
    return mValue;
  }

  /**
   * Flag which determines if node is changed.
   * 
   * @return {@code true} if it has been changed in memory, {@code false} otherwise
   */
  public boolean isChanged() {
    return mChanged;
  }

  /**
   * Flag which determines if node is changed.
   * 
   * @param pChanged
   *          flag which indicates if node is changed or not
   */
  public void setChanged(final boolean pChanged) {
    mChanged = pChanged;
  }

  public boolean hasLeftChild() {
    return mLeft != EFixed.NULL_NODE_KEY.getStandardProperty();
  }

  public boolean hasRightChild() {
    return mRight != EFixed.NULL_NODE_KEY.getStandardProperty();
  }

  /**
   * Get left child.
   * 
   * @return left child pointer
   */
  public long getLeftChildKey() {
    return mLeft;
  }

  /**
   * Get right child.
   * 
   * @return right child pointer
   */
  public long getRightChildKey() {
    return mRight;
  }

  /**
   * Set left child.
   * 
   * @param left
   *          child pointer
   */
  public void setLeftChildKey(final long pLeft) {
    mLeft = pLeft;
  }

  /**
   * Set right child.
   * 
   * @param right
   *          child pointer
   */
  public void setRightChildKey(final long pRight) {
    mLeft = pRight;
  }

  @Override
  public VisitResultType acceptVisitor(final @Nonnull IVisitor pVisitor) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("node delegate", mNodeDelegate)
      .add("left child", mLeft).add("right child", mRight).add("changed",
        mChanged).add("key", mKey).add("value", mValue).toString();
  }
}
