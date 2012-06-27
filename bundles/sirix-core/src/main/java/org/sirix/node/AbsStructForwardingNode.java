package org.sirix.node;

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
public abstract class AbsStructForwardingNode extends AbsForwardingNode implements IStructNode {

  /** Constructor for use by subclasses. */
  protected AbsStructForwardingNode() {
  }

  /** {@link StructNodeDelegate} instance. */
  protected abstract StructNodeDelegate structDelegate();

  /**
   * Getting the inlying {@link NodeDelegate}.
   * 
   * @return the inlying {@link NodeDelegate} instance
   */
  public StructNodeDelegate getStructNodeDelegate() {
    return new StructNodeDelegate(structDelegate());
  }

  @Override
  public boolean hasFirstChild() {
    return structDelegate().hasFirstChild();
  }

  @Override
  public boolean hasLeftSibling() {
    return structDelegate().hasLeftSibling();
  }

  @Override
  public boolean hasRightSibling() {
    return structDelegate().hasRightSibling();
  }

  @Override
  public long getChildCount() {
    return structDelegate().getChildCount();
  }

  @Override
  public long getFirstChildKey() {
    return structDelegate().getFirstChildKey();
  }

  @Override
  public long getLeftSiblingKey() {
    return structDelegate().getLeftSiblingKey();
  }

  @Override
  public long getRightSiblingKey() {
    return structDelegate().getRightSiblingKey();
  }

  @Override
  public void setRightSiblingKey(final long pKey) {
    structDelegate().setRightSiblingKey(pKey);
  }

  @Override
  public void setLeftSiblingKey(final long pKey) {
    structDelegate().setLeftSiblingKey(pKey);
  }

  @Override
  public void setFirstChildKey(final long pKey) {
    structDelegate().setFirstChildKey(pKey);
  }

  @Override
  public void decrementChildCount() {
    structDelegate().decrementChildCount();
  }

  @Override
  public void incrementChildCount() {
    structDelegate().incrementChildCount();
  }

  @Override
  public long getDescendantCount() {
    return structDelegate().getDescendantCount();
  }

  @Override
  public void decrementDescendantCount() {
    structDelegate().decrementDescendantCount();
  }

  @Override
  public void incrementDescendantCount() {
    structDelegate().incrementDescendantCount();
  }

  @Override
  public void setDescendantCount(final long pDescendantCount) {
    structDelegate().setDescendantCount(pDescendantCount);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("nodeDelegate", super.toString()).add("structDelegate",
      structDelegate().toString()).toString();
  }

}
