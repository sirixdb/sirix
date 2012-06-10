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

package org.treetank.node;

import org.treetank.api.visitor.EVisitResult;
import org.treetank.api.visitor.IVisitor;
import org.treetank.node.delegates.NameNodeDelegate;
import org.treetank.node.delegates.NodeDelegate;
import org.treetank.node.interfaces.INameNode;

/**
 * <h1>NamespaceNode</h1>
 * 
 * <p>
 * Node representing a namespace.
 * </p>
 */
public final class NamespaceNode extends AbsNode implements INameNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate mNameDel;

  /**
   * Constructor.
   * 
   * @param pDel
   * @param mIntBuilder
   *          building int data
   */
  public NamespaceNode(final NodeDelegate pDel, final NameNodeDelegate pNameDel) {
    super(pDel);
    mNameDel = pNameDel;
  }

  @Override
  public ENode getKind() {
    return ENode.NAMESPACE_KIND;
  }

  @Override
  public int getNameKey() {
    return mNameDel.getNameKey();
  }

  @Override
  public void setNameKey(final int pNameKey) {
    mNameDel.setNameKey(pNameKey);
  }

  @Override
  public int getURIKey() {
    return mNameDel.getURIKey();
  }

  @Override
  public void setURIKey(final int pUriKey) {
    mNameDel.setURIKey(pUriKey);
  }

  @Override
  public EVisitResult acceptVisitor(final IVisitor pVisitor) {
    return pVisitor.visit(this);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getNodeDelegate() == null) ? 0 : getNodeDelegate().hashCode());
    result = prime * result + ((mNameDel == null) ? 0 : mNameDel.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object pObj) {
    if (this == pObj)
      return true;
    if (pObj == null)
      return false;
    if (getClass() != pObj.getClass())
      return false;
    NamespaceNode other = (NamespaceNode)pObj;
    final NodeDelegate del = getNodeDelegate();
    if (del == null) {
      if (other.getNodeDelegate() != null)
        return false;
    } else if (!del.equals(other.getNodeDelegate()))
      return false;
    if (mNameDel == null) {
      if (other.mNameDel != null)
        return false;
    } else if (!mNameDel.equals(other.mNameDel))
      return false;
    return true;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder(super.toString());
    builder.append("\n");
    builder.append(mNameDel.toString());
    return builder.toString();
  }

  /**
   * Getting the inlying {@link NameNodeDelegate}.
   * 
   * @return {@link NameNodeDelegate} instance
   */
  NameNodeDelegate getNameNodeDelegate() {
    return mNameDel;
  }
}
