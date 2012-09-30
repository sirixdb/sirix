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
import com.google.common.base.Objects;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.delegates.StructNodeDelegate;
import org.sirix.node.delegates.ValNodeDelegate;
import org.sirix.node.interfaces.INameNode;
import org.sirix.node.interfaces.IValNode;

/**
 * <h1>AttributeNode</h1>
 * 
 * <p>
 * Node representing an attribute.
 * </p>
 */
public final class AttributeNode extends AbsForwardingNode implements IValNode,
  INameNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate mNameDel;

  /** Delegate for val node information. */
  private final ValNodeDelegate mValDel;

  /** Node delegate. */
  private final NodeDelegate mDel;

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
  public AttributeNode(@Nonnull final NodeDelegate pDel,
    @Nonnull final NameNodeDelegate pNameDel,
    @Nonnull final ValNodeDelegate pValDel) {
    mDel = checkNotNull(pDel);
    mNameDel = checkNotNull(pNameDel);
    mValDel = pValDel;
  }

  @Override
  public EKind getKind() {
    return EKind.ATTRIBUTE;
  }

  @Override
  public EVisitResult acceptVisitor(@Nonnull final IVisitor pVisitor) {
    return pVisitor.visit(this);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("nameDel", mNameDel).add("valDel",
      mValDel).toString();
  }

  @Override
  public int getNameKey() {
    return mNameDel.getNameKey();
  }

  @Override
  public int getURIKey() {
    return mNameDel.getURIKey();
  }

  @Override
  public void setNameKey(final int pNameKey) {
    mNameDel.setNameKey(pNameKey);
  }

  @Override
  public void setURIKey(final int pUriKey) {
    mNameDel.setURIKey(pUriKey);
  }

  @Override
  public byte[] getRawValue() {
    return mValDel.getRawValue();
  }

  @Override
  public void setValue(@Nonnull final byte[] pVal) {
    mValDel.setValue(pVal);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNameDel, mValDel);
  }

  @Override
  public boolean equals(@Nullable final Object pObj) {
    boolean retVal = false;
    if (pObj instanceof AttributeNode) {
      final AttributeNode other = (AttributeNode)pObj;
      retVal =
        Objects.equal(mNameDel, other.mNameDel)
          && Objects.equal(mValDel, other.mValDel);
    }
    return retVal;
  }

  @Override
  public void setPathNodeKey(@Nonnegative final long pPathNodeKey) {
    mNameDel.setPathNodeKey(pPathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return mNameDel.getPathNodeKey();
  }

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

  @Override
  protected NodeDelegate delegate() {
    return mDel;
  }
}
