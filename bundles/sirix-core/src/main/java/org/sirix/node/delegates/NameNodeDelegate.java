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
/**
 * 
 */
package org.sirix.node.delegates;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.AbsForwardingNode;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INameNode;

/**
 * Delegate method for all nodes containing \"naming\"-data. That means that
 * different fixed defined names are represented by the nodes delegating the
 * calls of the interface {@link INameNode} to this class. Mainly, keys are
 * stored referencing later on to the string stored in dedicated pages.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class NameNodeDelegate extends AbsForwardingNode implements INameNode {

  /** Node delegate, containing basic node information. */
  private final NodeDelegate mDelegate;

  /** Key of the name. The name contains the prefix as well. */
  private int mNameKey;

  /** URI of the related namespace. */
  private int mUriKey;

  /**
   * Constructor.
   * 
   * @param pDel
   *          page delegator
   * @param pNameKey
   *          namekey to be stored
   * @param pUriKey
   *          urikey to be stored
   */
  public NameNodeDelegate(@Nonnull final NodeDelegate pDel, final int pNameKey,
    final int pUriKey) {
    mDelegate = checkNotNull(pDel);
    mNameKey = pNameKey;
    mUriKey = pUriKey;
  }

  /**
   * Copy constructor.
   * 
   * @param pNameDel
   *          old name node delegate
   */
  public NameNodeDelegate(@Nonnull NameNodeDelegate pNameDel) {
    mDelegate = pNameDel.mDelegate;
    mNameKey = pNameDel.mNameKey;
    mUriKey = pNameDel.mUriKey;
  }

  @Override
  public EKind getKind() {
    return EKind.UNKOWN;
  }

  @Override
  public EVisitResult acceptVisitor(@Nonnull final IVisitor pVisitor) {
    return mDelegate.acceptVisitor(pVisitor);
  }

  @Override
  public int getNameKey() {
    return mNameKey;
  }

  @Override
  public int getURIKey() {
    return mUriKey;
  }

  @Override
  public void setNameKey(final int pNameKey) {
    mNameKey = pNameKey;
  }

  @Override
  public void setURIKey(final int pUriKey) {
    mUriKey = pUriKey;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNameKey, mUriKey);
  }

  @Override
  public boolean equals(@Nullable final Object pObj) {
    if (pObj instanceof NameNodeDelegate) {
      final NameNodeDelegate other = (NameNodeDelegate)pObj;
      return Objects.equal(mNameKey, other.mNameKey)
        && Objects.equal(mUriKey, other.mNameKey);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("node delegate", mDelegate).add(
      "uriKey", mUriKey).add("nameKey", mNameKey).toString();
  }

  @Override
  protected NodeDelegate delegate() {
    return mDelegate;
  }
}
