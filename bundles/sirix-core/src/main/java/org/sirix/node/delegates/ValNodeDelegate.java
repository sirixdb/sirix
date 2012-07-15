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
package org.sirix.node.delegates;

import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;

import java.util.zip.Deflater;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.AbsForwardingNode;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IValNode;
import org.sirix.utils.Compression;

/**
 * Delegate method for all nodes containing \"value\"-data. That means that
 * independent values are stored by the nodes delegating the calls of the
 * interface {@link IValNode} to this class.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class ValNodeDelegate extends AbsForwardingNode implements IValNode {

  /** Delegate for common node information. */
  private NodeDelegate mDelegate;

  /** Storing the value. */
  private byte[] mVal;

  /** Determines if input has been compressed. */
  private boolean mCompressed;

  /**
   * Constructor
   * 
   * @param pNodeDel
   *          the common data.
   * @param pVal
   *          the own value.
   */
  public ValNodeDelegate(@Nonnull final NodeDelegate pNodeDel,
    @Nonnull final byte[] pVal, final boolean pCompressed) {
    mDelegate = checkNotNull(pNodeDel);
    mVal = checkNotNull(pVal);
    mCompressed = pCompressed;
  }

  @Override
  public EVisitResult acceptVisitor(@Nonnull final IVisitor pVisitor) {
    return mDelegate.acceptVisitor(pVisitor);
  }

  @Override
  public byte[] getRawValue() {
    return mCompressed ? Compression.decompress(mVal) : mVal;
  }

  /**
   * Get value which might be compressed.
   * 
   * @return {@code value} which might be compressed
   */
  public byte[] getCompressed() {
    return mVal;
  }

  @Override
  public void setValue(@Nonnull final byte[] pVal) {
    mCompressed = new String(pVal).length() > 10 ? true : false;
    mVal =
      mCompressed ? Compression.compress(pVal, Deflater.DEFAULT_COMPRESSION)
        : pVal;
  }

  /**
   * Determine if input value has been compressed.
   * 
   * @return {@code true}, if it has been compressed, {@code false} otherwise
   */
  public boolean isCompressed() {
    return mCompressed;
  }

  /**
   * Set compression.
   * 
   * @param pCompressed
   *          determines if value is compressed or not
   */
  public void setCompressed(final boolean pCompressed) {
    mCompressed = pCompressed;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mDelegate, mVal);
  }

  @Override
  public boolean equals(@Nullable final Object pObj) {
    if (pObj instanceof ValNodeDelegate) {
      final ValNodeDelegate other = (ValNodeDelegate)pObj;
      return Objects.equal(mDelegate, other.mDelegate)
        && Objects.equal(mVal, other.mVal);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this).add("value", mVal).toString();
  }

  @Override
  public boolean isSameItem(@Nullable final INode pOther) {
    return mDelegate.isSameItem(pOther);
  }

  @Override
  public EKind getKind() {
    return EKind.UNKOWN;
  }

  @Override
  protected NodeDelegate delegate() {
    return mDelegate;
  }
}
