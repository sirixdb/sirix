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
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IStructNode;
import org.sirix.node.interfaces.IValNode;
import org.sirix.settings.EFixed;

/**
 * <h1>TextNode</h1>
 * 
 * <p>
 * Node representing a text node.
 * </p>
 */
public final class TextNode extends AbsStructForwardingNode implements IValNode {

  /** Delegate for common value node information. */
  private final ValNodeDelegate mValDel;
  
  /** {@link NodeDelegate} reference. */
  private final NodeDelegate mNodeDel;

  /** {@link StructNodeDelegate} reference. */
  private final StructNodeDelegate mStructNodeDel;
  
  /** Value of the node. */
  private byte[] mValue;

  /**
   * Constructor for TextNode.
   * 
   * @param pDel
   *          delegate for {@link INode} implementation
   * @param pValDel
   *          delegate for {@link IValNode} implementation
   * @param pStructDel
   *          delegate for {@link IStructNode} implementation
   */
  public TextNode(@Nonnull final NodeDelegate pDel, @Nonnull final ValNodeDelegate pValDel,
    @Nonnull final StructNodeDelegate pStructDel) {
    mNodeDel = checkNotNull(pDel);
    mStructNodeDel = checkNotNull(pStructDel);
    mValDel = checkNotNull(pValDel);
  }

  @Override
  public ENode getKind() {
    return ENode.TEXT_KIND;
  }

  @Override
  public byte[] getRawValue() {
    if (mValue == null) {
      mValue = mValDel.getRawValue();
    }
    return mValue;
  }

  @Override
  public void setValue(@Nonnull final byte[] pVal) {
    mValue = null;
    mValDel.setValue(pVal);
  }

  @Override
  public long getFirstChildKey() {
    return EFixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public EVisitResult acceptVisitor(@Nonnull final IVisitor pVisitor) {
    return pVisitor.visit(this);
  }

  @Override
  public void decrementChildCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void incrementChildCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getDescendantCount() {
    return 0;
  }

  @Override
  public void decrementDescendantCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void incrementDescendantCount() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDescendantCount(final long pDescendantCount) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((getNodeDelegate() == null) ? 0 : getNodeDelegate().hashCode());
    result = prime * result + ((getStructNodeDelegate() == null) ? 0 : getStructNodeDelegate().hashCode());
    result = prime * result + ((mValDel == null) ? 0 : mValDel.hashCode());
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
    TextNode other = (TextNode)pObj;
    final NodeDelegate del = getNodeDelegate();
    final NodeDelegate otherDel = other.getNodeDelegate();
    if (del == null) {
      if (otherDel != null)
        return false;
    } else if (!del.equals(otherDel))
      return false;
    final StructNodeDelegate structDel = getStructNodeDelegate();
    final StructNodeDelegate otherStructDel = other.getStructNodeDelegate();
    if (structDel == null) {
      if (otherStructDel != null)
        return false;
    } else if (!structDel.equals(otherStructDel))
      return false;
    if (mValDel == null) {
      if (other.mValDel != null)
        return false;
    } else if (!mValDel.equals(other.mValDel))
      return false;
    return true;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder(super.toString());
    builder.append("\n");
    builder.append(mValDel.toString());
    builder.append("\n");
    return builder.toString();
  }

  /**
   * Getting the inlying {@link ValNodeDelegate}.
   * 
   * @return
   */
  ValNodeDelegate getValNodeDelegate() {
    return mValDel;
  }
  
  @Override
  protected NodeDelegate delegate() {
    return mNodeDel;
  }

  @Override
  protected StructNodeDelegate structDelegate() {
    return mStructNodeDel;
  }
}
