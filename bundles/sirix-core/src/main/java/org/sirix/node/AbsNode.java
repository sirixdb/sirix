package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.INode;

/**
 * Skeletal implementation of {@link INode} interface.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
abstract class AbsNode implements INode {

  /** Delegate for common node information. */
  private final NodeDelegate mNodeDel;

  /**
   * Constructor.
   * 
   * @param pNodeDelegate
   *          {@link NodeDelegate} reference
   * 
   */
  public AbsNode(final NodeDelegate pNodeDelegate) {
    mNodeDel = checkNotNull(pNodeDelegate);
  }

  /**
   * Getting the inlying {@link NodeDelegate}.
   * 
   * @return the inlying {@link NodeDelegate} instance
   */
  NodeDelegate getNodeDelegate() {
    return mNodeDel;
  }

  @Override
  public int getTypeKey() {
    return mNodeDel.getTypeKey();
  }

  @Override
  public void setTypeKey(final int pTypeKey) {
    mNodeDel.setTypeKey(pTypeKey);
  }

  @Override
  public boolean hasParent() {
    return mNodeDel.hasParent();
  }

  @Override
  public long getNodeKey() {
    return mNodeDel.getNodeKey();
  }

  @Override
  public void setNodeKey(final long pNodeKey) {
    mNodeDel.setNodeKey(pNodeKey);
  }

  @Override
  public long getParentKey() {
    return mNodeDel.getParentKey();
  }

  @Override
  public void setParentKey(final long pParentKey) {
    mNodeDel.setParentKey(pParentKey);
  }

  @Override
  public long getHash() {
    return mNodeDel.getHash();
  }

  @Override
  public void setHash(final long pHash) {
    mNodeDel.setHash(pHash);
  }

  @Override
  public String toString() {
    return mNodeDel.toString();
  }
  
  @Override
  public boolean isSameItem(final INode pOther) {
    return mNodeDel.isSameItem(pOther);
  }
}
