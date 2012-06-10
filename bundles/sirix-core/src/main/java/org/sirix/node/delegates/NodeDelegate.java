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

import static com.google.common.base.Preconditions.checkArgument;
import org.sirix.api.visitor.EVisitResult;
import org.sirix.api.visitor.IVisitor;
import org.sirix.node.ENode;
import org.sirix.node.interfaces.INode;
import org.sirix.settings.EFixed;
import org.sirix.utils.NamePageHash;

/**
 * Delegate method for all nodes. That means that all nodes stored in sirix
 * are represented by an instance of the interface {@link INode} namely
 * containing the position in the tree related to a parent-node, the related
 * type and the corresponding hash recursivly computed.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public class NodeDelegate implements INode {

  /** Untyped type. */
  private static final int TYPE_KEY = NamePageHash.generateHashForString("xs:untyped");

  /** Key of the current node. Must be unique for all nodes. */
  private long mNodeKey;

  /** Key of the parent node. */
  private long mParentKey;

  /** Hash of the parent node. */
  private long mHash;

  /**
   * TypeKey of the parent node. Can be referenced later on over special
   * pages.
   */
  private int mTypeKey;

  /**
   * Constructor.
   * 
   * @param pNodeKey
   *          to be represented by this delegate.
   * @param pParentKey
   *          to be represented by this delegate
   * @param pHash
   *          to be represented by this delegate
   */
  public NodeDelegate(final long pNodeKey, final long pParentKey, final long pHash) {
    mNodeKey = pNodeKey;
    mParentKey = pParentKey;
    mHash = pHash;
    mTypeKey = TYPE_KEY;
  }

  @Override
  public ENode getKind() {
    return ENode.UNKOWN_KIND;
  }

  @Override
  public long getNodeKey() {
    return mNodeKey;
  }

  @Override
  public void setNodeKey(final long pKey) {
    checkArgument(pKey >= 0, "pKey must be >= 0!");
    mNodeKey = pKey;
  }

  @Override
  public long getParentKey() {
    return mParentKey;
  }

  @Override
  public void setParentKey(final long pParentKey) {
    checkArgument(pParentKey >= EFixed.NULL_NODE_KEY.getStandardProperty());
    mParentKey = pParentKey;
  }

  @Override
  public long getHash() {
    return mHash;
  }

  @Override
  public void setHash(final long pHash) {
    mHash = pHash;
  }

  @Override
  public EVisitResult acceptVisitor(final IVisitor pVisitor) {
    return EVisitResult.CONTINUE;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int)(getHash() ^ (getHash() >>> 32));
    result = prime * result + (int)(getNodeKey() ^ (getNodeKey() >>> 32));
    result = prime * result + (int)(getParentKey() ^ (getParentKey() >>> 32));
    result = prime * result + getTypeKey();
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
    NodeDelegate other = (NodeDelegate)pObj;
    if (getHash() != other.getHash())
      return false;
    if (getNodeKey() != other.getNodeKey())
      return false;
    if (getParentKey() != other.getParentKey())
      return false;
    if (getTypeKey() != other.getTypeKey())
      return false;
    return true;
  }

  @Override
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("node key: ");
    builder.append(getNodeKey());
    builder.append("\nparent key: ");
    builder.append(getParentKey());
    builder.append("\ntype key: ");
    builder.append(getTypeKey());
    builder.append("\nhash: ");
    builder.append(getHash());
    return builder.toString();
  }

  @Override
  public int getTypeKey() {
    return mTypeKey;
  }

  @Override
  public void setTypeKey(int pTypeKey) {
    mTypeKey = pTypeKey;
  }

  @Override
  public boolean hasParent() {
    return mParentKey != EFixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean isSameItem(final INode pOther) {
    if (pOther == null) {
      return false;
    }
    if (pOther == this) {
      return true;
    }
    return pOther.getNodeKey() == this.getNodeKey();
  }
}
