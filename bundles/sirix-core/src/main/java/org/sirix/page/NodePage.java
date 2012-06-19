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
package org.sirix.page;

import static com.google.common.base.Preconditions.checkArgument;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

import java.util.Arrays;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.sirix.api.IPageWriteTrx;
import org.sirix.exception.AbsTTException;
import org.sirix.io.ITTSink;
import org.sirix.io.ITTSource;
import org.sirix.node.ENode;
import org.sirix.node.interfaces.INode;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.IPage;
import org.sirix.utils.IConstants;

/**
 * <h1>NodePage</h1>
 * 
 * <p>
 * A node page stores a set of nodes.
 * </p>
 */
public class NodePage implements IPage {

  /** Key of node page. This is the base key of all contained nodes. */
  private final long mNodePageKey;

  /** Array of nodes. This can have null nodes that were removed. */
  private final INode[] mNodes;

  /** {@link PageDelegate} reference. */
  private final long mRevision;

  /**
   * Create node page.
   * 
   * @param pNodePageKey
   *          base key assigned to this node page
   * @param pRevision
   *          revision the page belongs to
   */
  public NodePage(@Nonnegative final long pNodePageKey, @Nonnegative final long pRevision) {
    checkArgument(pNodePageKey >= 0, "pNodePageKey must not be negative!");
    checkArgument(pRevision >= 0, "pRevision must not be negative!");
    mRevision = pRevision;
    mNodePageKey = pNodePageKey;
    mNodes = new INode[IConstants.NDP_NODE_COUNT];
  }

  /**
   * Read node page.
   * 
   * @param pIn
   *          input bytes to read page from
   */
  protected NodePage(@Nonnull final ITTSource pIn) {
    mRevision = pIn.readLong();
    mNodePageKey = pIn.readLong();
    mNodes = new INode[IConstants.NDP_NODE_COUNT];
    for (int offset = 0; offset < mNodes.length; offset++) {
      final byte id = pIn.readByte();
      final ENode enumKind = ENode.getKind(id);
      if (enumKind != ENode.UNKOWN_KIND) {
        mNodes[offset] = enumKind.deserialize(pIn);
      }
    }
  }

  /**
   * Get key of node page.
   * 
   * @return Node page key.
   */
  public final long getNodePageKey() {
    return mNodePageKey;
  }

  /**
   * Get node at a given offset.
   * 
   * @param pOffset
   *          offset of node within local node page
   * @return node at given offset
   */
  public INode getNode(@Nonnegative final int pOffset) {
    checkArgument(pOffset >= 0 && pOffset < IConstants.NDP_NODE_COUNT,
      "offset must not be negative and less than the max. nodes per node page!");
    if (pOffset < mNodes.length) {
      return mNodes[pOffset];
    } else {
      return null;
    }
  }

  /**
   * Overwrite a single node at a given offset.
   * 
   * @param pOffset
   *          offset of node to overwrite in this node page
   * @param pNode
   *          node to store at given nodeOffset
   */
  public void setNode(@Nonnegative final int pOffset, @Nullable final INode pNode) {
    checkArgument(pOffset >= 0, "pOffset may not be negative!");
    mNodes[pOffset] = pNode;
  }

  @Override
  public void serialize(@Nonnull final ITTSink pOut) {
    pOut.writeLong(mNodePageKey);
    for (final INode node : mNodes) {
      if (node == null) {
        pOut.writeByte(getLastByte(ENode.UNKOWN_KIND.getId()));
      } else {
        final byte id = node.getKind().getId();
        pOut.writeByte(id);
        ENode.getKind(node.getClass()).serialize(pOut, node);
      }
    }
  }

  /**
   * Get last byte of integer.
   * 
   * @param pVal
   *          integer value
   * @return last byte
   */
  private byte getLastByte(final int pVal) {
    byte val = (byte)(pVal >>> 24);
    val = (byte)(pVal >>> 16);
    val = (byte)(pVal >>> 8);
    val = (byte)pVal;
    return val;
  }

  @Override
  public final String toString() {
    final ToStringHelper helper =
      Objects.toStringHelper(this).add("pagekey", mNodePageKey).add("nodes", mNodes.toString());
    for (int i = 0; i < mNodes.length; i++) {
      final INode node = mNodes[i];
      helper.add(String.valueOf(i), node == null ? "" : node.getNodeKey());
    }
    return helper.toString();
  }

  /**
   * Get all nodes from this node page.
   * 
   * @return the nodes
   */
  public final INode[] getNodes() {
    return mNodes;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodePageKey, mNodes);
  }

  @Override
  public boolean equals(final Object mObj) {
    if (this == mObj) {
      return true;
    }

    if (mObj == null) {
      return false;
    }

    if (getClass() != mObj.getClass()) {
      return false;
    }

    final NodePage mOther = (NodePage)mObj;
    if (mNodePageKey != mOther.mNodePageKey) {
      return false;
    }

    if (!Arrays.equals(mNodes, mOther.mNodes)) {
      return false;
    }

    return true;
  }

  @Override
  public long getRevision() {
    return mRevision;
  }

  @Override
  public PageReference[] getReferences() {
    return null;
  }

  @Override
  public void commit(@Nonnull final IPageWriteTrx pPageWriteTrx) throws AbsTTException {
  }

}
