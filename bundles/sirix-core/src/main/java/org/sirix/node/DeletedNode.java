/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.node;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.interfaces.INode;

/**
 * If a node is deleted, it will be encapsulated over this class.
 * 
 * @author Sebastian Graf
 * 
 */
public final class DeletedNode implements INode {

  /**
   * Delegate for common data.
   */
  private final NodeDelegate mDel;

  /**
   * Constructor.
   * 
   * @param paramNode
   *          nodekey to be replaced with a deletednode
   * @param paramParent
   *          parent of this key.
   */
  public DeletedNode(@Nonnull final NodeDelegate pDel) {
    mDel = checkNotNull(pDel);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ENode getKind() {
    return ENode.DELETE_KIND;
  }

  /**
   * Delegate method for getKey.
   * 
   * @return
   * @see org.sirix.node.delegates.NodeDelegate#getNodeKey()
   */
  @Override
  public long getNodeKey() {
    return mDel.getNodeKey();
  }

  /**
   * Delegate method for getParentKey.
   * 
   * @return
   * @see org.sirix.node.delegates.NodeDelegate#getParentKey()
   */
  @Override
  public long getParentKey() {
    return mDel.getParentKey();
  }

  /**
   * Delegate method for setParentKey.
   * 
   * @param pParentKey
   * @see org.sirix.node.delegates.NodeDelegate#setParentKey(long)
   */
  @Override
  public void setParentKey(final long pParentKey) {
    mDel.setParentKey(pParentKey);
  }

  /**
   * Delegate method for getHash.
   * 
   * @return
   * @see org.sirix.node.delegates.NodeDelegate#getHash()
   */
  @Override
  public long getHash() {
    return mDel.getHash();
  }

  /**
   * Delegate method for setHash.
   * 
   * @param pHash
   * @see org.sirix.node.delegates.NodeDelegate#setHash(long)
   */
  @Override
  public void setHash(final long pHash) {
    mDel.setHash(pHash);
  }

  /**
   * Delegate method for acceptVisitor.
   * 
   * @param pVisitor
   * @see org.sirix.node.delegates.NodeDelegate#acceptVisitor(org.sirix.api.visitor.IVisitor)
   */
  @Override
  public EVisitResult acceptVisitor(final IVisitor pVisitor) {
    return mDel.acceptVisitor(pVisitor);
  }

  /**
   * Delegate method for getTypeKey.
   * 
   * @return
   * @see org.sirix.node.delegates.NodeDelegate#getTypeKey()
   */
  @Override
  public int getTypeKey() {
    return mDel.getTypeKey();
  }

  /**
   * Delegate method for setTypeKey.
   * 
   * @param pTypeKey
   * @see org.sirix.node.delegates.NodeDelegate#setTypeKey(int)
   */
  @Override
  public void setTypeKey(final int pTypeKey) {
    mDel.setTypeKey(pTypeKey);
  }

  /**
   * Delegate method for hasParent.
   * 
   * @return
   * @see org.sirix.node.delegates.NodeDelegate#hasParent()
   */
  @Override
  public boolean hasParent() {
    return mDel.hasParent();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mDel == null) ? 0 : mDel.hashCode());
    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(final Object pObj) {
    if (this == pObj)
      return true;
    if (pObj == null)
      return false;
    if (getClass() != pObj.getClass())
      return false;
    DeletedNode other = (DeletedNode)pObj;
    if (mDel == null) {
      if (other.mDel != null)
        return false;
    } else if (!mDel.equals(other.mDel))
      return false;
    return true;
  }

  /**
   * Delegate method for toString.
   * 
   * @return
   * @see org.sirix.node.delegates.NodeDelegate#toString()
   */
  @Override
  public String toString() {
    return mDel.toString();
  }

  /**
   * Getting the inlying {@link NodeDelegate}.
   * 
   * @return
   */
  NodeDelegate getNodeDelegate() {
    return mDel;
  }
  
  @Override
  public boolean isSameItem(final INode pOther) {
    return mDel.isSameItem(pOther);
  }

}
