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

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.visitor.VisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.delegates.NameNodeDelegate;
import org.sirix.node.delegates.NodeDelegate;
import org.sirix.node.immutable.ImmutableNamespace;
import org.sirix.node.interfaces.NameNode;

import com.google.common.base.Objects;

/**
 * <h1>NamespaceNode</h1>
 * 
 * <p>
 * Node representing a namespace.
 * </p>
 */
public final class NamespaceNode extends AbsForwardingNode implements NameNode {

  /** Delegate for name node information. */
  private final NameNodeDelegate mNameDel;

  /** {@link NodeDelegate} reference. */
  private final NodeDelegate mNodeDel;

  /**
   * Constructor.
   * 
   * @param pDel
   *          {@link NodeDelegate} reference
   * @param pNameDel
   *          {@link NameNodeDelegate} reference
   */
  public NamespaceNode(@Nonnull final NodeDelegate pDel,
    @Nonnull final NameNodeDelegate pNameDel) {
    mNodeDel = checkNotNull(pDel);
    mNameDel = checkNotNull(pNameDel);
  }

  @Override
  public Kind getKind() {
    return Kind.NAMESPACE;
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
  public VisitResult acceptVisitor(@Nonnull final IVisitor pVisitor) {
  	return pVisitor.visit(ImmutableNamespace.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodeDel, mNameDel);
  }

  @Override
  public boolean equals(final Object pObj) {
    if (pObj instanceof NamespaceNode) {
      final NamespaceNode other = (NamespaceNode)pObj;
      return Objects.equal(mNodeDel, other.mNodeDel)
        && Objects.equal(mNameDel, other.mNameDel);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("nodeDel", mNodeDel).add("nameDel",
      mNameDel).toString();
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
   * @return {@link NameNodeDelegate} instance
   */
  NameNodeDelegate getNameNodeDelegate() {
    return mNameDel;
  }

  @Override
  protected NodeDelegate delegate() {
    return mNodeDel;
  }
}
