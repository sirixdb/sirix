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
package org.treetank.node.delegates;

import java.util.Arrays;
import java.util.zip.Deflater;

import org.treetank.api.visitor.EVisitResult;
import org.treetank.api.visitor.IVisitor;
import org.treetank.node.ENode;
import org.treetank.node.interfaces.INode;
import org.treetank.node.interfaces.IValNode;
import org.treetank.utils.Compression;

/**
 * Delegate method for all nodes containing \"value\"-data. That means that
 * independent values are stored by the nodes delegating the calls of the
 * interface {@link IValNode} to this class.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class ValNodeDelegate implements IValNode {

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
  public ValNodeDelegate(final NodeDelegate pNodeDel, final byte[] pVal, final boolean pCompressed) {
    mDelegate = pNodeDel;
    mVal = pVal;
    mCompressed = pCompressed;
  }

  /**
   * Delegate method for getKind.
   * 
   * @return
   * @see org.treetank.node.delegates.NodeDelegate#getKind()
   */
  @Override
  public ENode getKind() {
    return mDelegate.getKind();
  }

  /**
   * Delegate method for getKey.
   * 
   * @return
   * @see org.treetank.node.delegates.NodeDelegate#getNodeKey()
   */
  @Override
  public long getNodeKey() {
    return mDelegate.getNodeKey();
  }

  /**
   * Delegate method for setKey.
   * 
   * @param pNodeKey
   * @see org.treetank.node.delegates.NodeDelegate#setNodeKey(long)
   */
  @Override
  public void setNodeKey(long pNodeKey) {
    mDelegate.setNodeKey(pNodeKey);
  }

  /**
   * Delegate method for getParentKey.
   * 
   * @return
   * @see org.treetank.node.delegates.NodeDelegate#getParentKey()
   */
  @Override
  public long getParentKey() {
    return mDelegate.getParentKey();
  }

  /**
   * Delegate method for setParentKey.
   * 
   * @param pParentKey
   * @see org.treetank.node.delegates.NodeDelegate#setParentKey(long)
   */
  @Override
  public void setParentKey(long pParentKey) {
    mDelegate.setParentKey(pParentKey);
  }

  /**
   * Delegate method for getHash.
   * 
   * @return
   * @see org.treetank.node.delegates.NodeDelegate#getHash()
   */
  @Override
  public long getHash() {
    return mDelegate.getHash();
  }

  /**
   * Delegate method for setHash.
   * 
   * @param pHash
   * @see org.treetank.node.delegates.NodeDelegate#setHash(long)
   */
  @Override
  public void setHash(long pHash) {
    mDelegate.setHash(pHash);
  }

  /**
   * Delegate method for acceptVisitor.
   * 
   * @param pVisitor
   * @see org.treetank.node.delegates.NodeDelegate#acceptVisitor(org.treetank.api.visitor.IVisitor)
   */
  @Override
  public EVisitResult acceptVisitor(IVisitor pVisitor) {
    return mDelegate.acceptVisitor(pVisitor);
  }

  /**
   * Delegate method for getTypeKey.
   * 
   * @return
   * @see org.treetank.node.delegates.NodeDelegate#getTypeKey()
   */
  @Override
  public int getTypeKey() {
    return mDelegate.getTypeKey();
  }

  /**
   * Delegate method for setTypeKey.
   * 
   * @param pTypeKey
   * @see org.treetank.node.delegates.NodeDelegate#setTypeKey(int)
   */
  @Override
  public void setTypeKey(int pTypeKey) {
    mDelegate.setTypeKey(pTypeKey);
  }

  /**
   * Delegate method for hasParent.
   * 
   * @return
   * @see org.treetank.node.delegates.NodeDelegate#hasParent()
   */
  @Override
  public boolean hasParent() {
    return mDelegate.hasParent();
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
  public void setValue(final byte[] pVal) {
    mCompressed = new String(pVal).length() > 10 ? true : false;
    mVal = mCompressed ? Compression.compress(pVal, Deflater.DEFAULT_COMPRESSION) : pVal;
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
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(mVal);
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    ValNodeDelegate other = (ValNodeDelegate)obj;
    if (!Arrays.equals(mVal, other.mVal))
      return false;
    return true;
  }

  /**
   * Delegate method for toString.
   * 
   * @return
   * @see org.treetank.node.delegates.NodeDelegate#toString()
   */
  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("value: ");
    builder.append(new String(mVal));
    return builder.toString();
  }

  @Override
  public boolean isSameItem(final INode pOther) {
    return mDelegate.isSameItem(pOther);
  }
}
