package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;

import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.interfaces.IStructNode;

/**
 * Skeletal implementation of {@link IStructNode} interface.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
abstract class AbsStructNode extends AbsNode implements IStructNode {

  /** Delegate for struct node information. */
  private final StructNodeDelegate mStructDel;

  /**
   * Constructor.
   * 
   * @param pNodeDelegate
   *          {@link NodeDelegate} reference
   * @param pStructNodeDelegate
   *          {@link StructNodeDelegate} reference
   */
  public AbsStructNode(final NodeDelegate pNodeDelegate, final StructNodeDelegate pStructNodeDelegate) {
    super(pNodeDelegate);
    mStructDel = checkNotNull(pStructNodeDelegate);
  }

  /**
   * Getting the inlying {@link NodeDelegate}.
   * 
   * @return the inlying {@link NodeDelegate} instance
   */
  StructNodeDelegate getStructNodeDelegate() {
    return mStructDel;
  }

  @Override
  public boolean hasFirstChild() {
    return mStructDel.hasFirstChild();
  }

  @Override
  public boolean hasLeftSibling() {
    return mStructDel.hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    return mStructDel.hasRightSibling();
  }

  @Override
  public long getChildCount() {
    return mStructDel.getChildCount();
  }

  @Override
  public long getFirstChildKey() {
    return mStructDel.getFirstChildKey();
  }

  @Override
  public long getLeftSiblingKey() {
    return mStructDel.getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    return mStructDel.getRightSiblingKey();
  }

  @Override
  public void setRightSiblingKey(final long pKey) {
    mStructDel.setRightSiblingKey(pKey);
  }

  @Override
  public void setLeftSiblingKey(final long pKey) {
    mStructDel.setLeftSiblingKey(pKey);
  }

  @Override
  public void setFirstChildKey(final long pKey) {
    mStructDel.setFirstChildKey(pKey);
  }

  @Override
  public void decrementChildCount() {
    mStructDel.decrementChildCount();
  }

  @Override
  public void incrementChildCount() {
    mStructDel.incrementChildCount();
  }

  @Override
  public long getDescendantCount() {
    return mStructDel.getDescendantCount();
  }

  @Override
  public void decrementDescendantCount() {
    mStructDel.decrementDescendantCount();

  }

  @Override
  public void incrementDescendantCount() {
    mStructDel.incrementDescendantCount();
  }

  @Override
  public void setDescendantCount(final long pDescendantCount) {
    mStructDel.setDescendantCount(pDescendantCount);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("nodeDelegate", super.toString()).add("structDelegate",
      mStructDel.toString()).toString();
  }

}
