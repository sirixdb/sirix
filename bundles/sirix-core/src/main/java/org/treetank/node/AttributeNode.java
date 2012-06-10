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

import com.google.common.base.Objects;
import org.treetank.api.visitor.EVisitResult;
import org.treetank.api.visitor.IVisitor;
import org.treetank.node.delegates.NameNodeDelegate;
import org.treetank.node.delegates.NodeDelegate;
import org.treetank.node.delegates.StructNodeDelegate;
import org.treetank.node.delegates.ValNodeDelegate;
import org.treetank.node.interfaces.INameNode;
import org.treetank.node.interfaces.IValNode;

/**
 * <h1>AttributeNode</h1>
 * 
 * <p>
 * Node representing an attribute.
 * </p>
 */
public final class AttributeNode extends AbsNode implements IValNode, INameNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate mNameDel;

  /** Delegate for val node information. */
  private final ValNodeDelegate mValDel;

  /**
   * Creating an attribute.
   * 
   * @param pDel
   *          {@link NodeDelegate} to be set
   * @param pDel
   *          {@link StructNodeDelegate} to be set
   * @param pValDel
   *          {@link ValNodeDelegate} to be set
   * 
   */
  public AttributeNode(final NodeDelegate pDel, final NameNodeDelegate pNameDel, final ValNodeDelegate pValDel) {
    super(pDel);
    mNameDel = pNameDel;
    mValDel = pValDel;
  }

  @Override
  public ENode getKind() {
    return ENode.ATTRIBUTE_KIND;
  }

  @Override
  public EVisitResult acceptVisitor(final IVisitor pVisitor) {
    return pVisitor.visit(this);
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder(super.toString());
    builder.append("\n");
    builder.append(mNameDel.toString());
    builder.append("\n");
    builder.append(mValDel.toString());
    return builder.toString();
  }

  /**
   * Delegate method for getNameKey.
   * 
   * @return
   * @see org.treetank.node.delegates.NameNodeDelegate#getNameKey()
   */
  @Override
  public int getNameKey() {
    return mNameDel.getNameKey();
  }

  /**
   * Delegate method for getURIKey.
   * 
   * @return
   * @see org.treetank.node.delegates.NameNodeDelegate#getURIKey()
   */
  @Override
  public int getURIKey() {
    return mNameDel.getURIKey();
  }

  /**
   * Delegate method for setNameKey.
   * 
   * @param pNameKey
   * @see org.treetank.node.delegates.NameNodeDelegate#setNameKey(int)
   */
  @Override
  public void setNameKey(final int pNameKey) {
    mNameDel.setNameKey(pNameKey);
  }

  /**
   * Delegate method for setURIKey.
   * 
   * @param pUriKey
   * @see org.treetank.node.delegates.NameNodeDelegate#setURIKey(int)
   */
  @Override
  public void setURIKey(final int pUriKey) {
    mNameDel.setURIKey(pUriKey);
  }

  /**
   * Delegate method for getRawValue.
   * 
   * @return
   * @see org.treetank.node.delegates.ValNodeDelegate#getRawValue()
   */
  @Override
  public byte[] getRawValue() {
    return mValDel.getRawValue();
  }

  /**
   * Delegate method for setValue.
   * 
   * @param pVal
   * @see org.treetank.node.delegates.ValNodeDelegate#setValue(byte[])
   */
  @Override
  public void setValue(final byte[] pVal) {
    mValDel.setValue(pVal);
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((mNameDel == null) ? 0 : mNameDel.hashCode());
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
    AttributeNode other = (AttributeNode)pObj;
    return Objects.equal(mNameDel, other.mNameDel) && Objects.equal(mValDel, other.mValDel);
  }

  // /**
  // * Getting the inlying {@link NodeDelegate}.
  // *
  // * @return the {@link NodeDelegate} instance
  // */
  // NodeDelegate getNodeDelegate() {
  // return mDelegate;
  // }

  /**
   * Getting the inlying {@link NameNodeDelegate}.
   * 
   * @return the {@link NameNodeDelegate} instance
   */
  NameNodeDelegate getNameNodeDelegate() {
    return mNameDel;
  }

  /**
   * Getting the inlying {@link ValNodeDelegate}.
   * 
   * @return the {@link ValNodeDelegate} instance
   */
  ValNodeDelegate getValNodeDelegate() {
    return mValDel;
  }

}
