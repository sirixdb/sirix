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
package org.treetank.page;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.treetank.encryption.EncryptionController;
import org.treetank.encryption.database.model.KeySelector;
import org.treetank.encryption.utils.NodeEncryption;
import org.treetank.exception.TTUsageException;
import org.treetank.io.ITTSink;
import org.treetank.io.ITTSource;
import org.treetank.node.ENode;
import org.treetank.node.interfaces.INode;
import org.treetank.node.io.NodeInputSource;
import org.treetank.node.io.NodeOutputSink;
import org.treetank.page.delegates.PageDelegate;
import org.treetank.page.interfaces.IPage;
import org.treetank.utils.IConstants;

/**
 * <h1>NodePage</h1>
 * 
 * <p>
 * A node page stores a set of nodes.
 * </p>
 */
public class NodePage extends AbsForwardingPage {

  /** Key of node page. This is the base key of all contained nodes. */
  private final long mNodePageKey;

  /** Array of nodes. This can have null nodes that were removed. */
  private final INode[] mNodes;

  /** {@link PageDelegate} reference. */
  private final PageDelegate mDelegate;

  // private boolean mHasDeleted;

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
    mDelegate = new PageDelegate(0, pRevision);
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
    mDelegate = new PageDelegate(0, pIn.readLong());
    mDelegate.initialize(pIn);

    mNodePageKey = pIn.readLong();
    mNodes = new INode[IConstants.NDP_NODE_COUNT];

    final EncryptionController enController = EncryptionController.getInstance();

    if (enController.checkEncryption()) {
      for (int i = 0; i < mNodes.length; i++) {
        final long mRightKey = getRightKey(pIn);

        final List<Long> mUserKeys = enController.getKeyCache().get(enController.getUser());
        byte[] mSecretKey = null;

        if (mUserKeys.contains(mRightKey) || mRightKey == -1) {
          final byte mElementKind = pIn.readByte();

          if (mRightKey != -1) {

            // get secret key
            mSecretKey = enController.getSelDb().getEntry(mRightKey).getSecretKey();

            final int mNodeBytes = pIn.readInt();
            final int mPointerBytes = pIn.readInt();

            final byte[] mDecryptedNode;

            if (mPointerBytes == 0) {

              final byte[] mEncryptedNode = new byte[mNodeBytes];

              for (int j = 0; j < mNodeBytes; j++) {
                mEncryptedNode[j] = pIn.readByte();
              }

              mDecryptedNode = NodeEncryption.decrypt(mEncryptedNode, mSecretKey);

            } else {

              final byte[] mEncryptedPointer = new byte[mPointerBytes];
              for (int j = 0; j < mPointerBytes; j++) {
                mEncryptedPointer[j] = pIn.readByte();
              }

              final int mDataBytes = mNodeBytes - mPointerBytes;
              final byte[] mEncryptedData = new byte[mDataBytes];
              for (int j = 0; j < mDataBytes; j++) {
                mEncryptedData[j] = pIn.readByte();
              }

              final byte[] mDecryptedPointer = NodeEncryption.decrypt(mEncryptedPointer, mSecretKey);

              final byte[] mDecryptedData = NodeEncryption.decrypt(mEncryptedData, mSecretKey);

              mDecryptedNode = new byte[mDecryptedPointer.length + mDecryptedData.length];

              int mCounter = 0;
              for (int j = 0; j < mDecryptedPointer.length; j++) {
                mDecryptedNode[mCounter] = mDecryptedPointer[j];
                mCounter++;
              }
              for (int j = 0; j < mDecryptedData.length; j++) {
                mDecryptedNode[mCounter] = mDecryptedData[j];
                mCounter++;
              }

            }

            final NodeInputSource mNodeInput = new NodeInputSource(mDecryptedNode);

            final ENode mEnumKind = ENode.getKind(mElementKind);

            if (mEnumKind != ENode.UNKOWN_KIND) {
              getNodes()[i] = mEnumKind.deserialize(mNodeInput);
            }
          }

        } else {
          try {
            throw new TTUsageException("User has no permission to access the node");

          } catch (final TTUsageException mExp) {
            mExp.printStackTrace();
          }
        }
      }
    } else {
      // mHasDeleted = pIn.readByte() == (byte)1 ? true : false;
      for (int offset = 0; offset < mNodes.length; offset++) {
        final byte id = pIn.readByte();
        final ENode enumKind = ENode.getKind(id);
        if (enumKind != ENode.UNKOWN_KIND) {
          mNodes[offset] = enumKind.deserialize(pIn);
        }
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

  private long getRightKey(@Nonnull final ITTSource pIn) {
    final long rightKey = pIn.readLong();
    pIn.readInt();
    pIn.readInt();
    return rightKey;
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
    // if (mHasDeleted) {
    // final int offset =
    // mNodes.length < IConstants.NDP_NODE_COUNT ? (pOffset >> (IConstants.NDP_NODE_COUNT - 1))
    // * (mNodes.length - 1) : pOffset;
    // return mNodes[offset];
    // } else {
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
  public void setNode(@Nonnegative final int pOffset, @Nonnull final INode pNode) {
    checkArgument(pOffset >= 0, "pOffset may not be negative!");
    // int offset = pOffset;
    // if (mHasDeleted) {
    // offset =
    // mNodes.length < IConstants.NDP_NODE_COUNT ? (pOffset >> (IConstants.NDP_NODE_COUNT - 1))
    // * (mNodes.length - 1) : pOffset;
    // }
    mNodes[pOffset] = checkNotNull(pNode);
  }

  @Override
  public void serialize(final ITTSink pOut) {
    mDelegate.serialize(checkNotNull(pOut));
    pOut.writeLong(mNodePageKey);

    final EncryptionController enController = EncryptionController.getInstance();

    if (enController.checkEncryption()) {
      NodeOutputSink mNodeOut = null;
      for (final INode node : mNodes) {
        if (node != null) {
          mNodeOut = new NodeOutputSink();

          final long mDek = enController.getDataEncryptionKey();

          final KeySelector mKeySel = enController.getSelDb().getEntry(mDek);
          final byte[] mSecretKey = mKeySel.getSecretKey();

          pOut.writeLong(mKeySel.getPrimaryKey());
          pOut.writeInt(mKeySel.getRevision());
          pOut.writeInt(mKeySel.getVersion());
          final byte kind = node.getKind().getId();
          pOut.writeByte(kind);
          ENode.getKind(kind).serialize(pOut, node);

          final byte[] mStream = ((ByteArrayOutputStream)mNodeOut.getOutputStream()).toByteArray();

          byte[] mEncrypted = null;
          final int pointerEnSize;

          if (mStream.length > 0) {

            final byte[] mPointer = new byte[mStream.length];

            for (int i = 0; i < mPointer.length; i++) {
              mPointer[i] = mStream[i];
            }

            final byte[] mData = new byte[mStream.length - mPointer.length];
            for (int i = 0; i < mData.length; i++) {
              mData[i] = mStream[mPointer.length + i];
            }

            final byte[] mEnPointer = NodeEncryption.encrypt(mPointer, mSecretKey);
            pointerEnSize = mEnPointer.length;
            final byte[] mEnData = NodeEncryption.encrypt(mData, mSecretKey);

            mEncrypted = new byte[mEnPointer.length + mEnData.length];

            int mCounter = 0;
            for (int i = 0; i < mEnPointer.length; i++) {
              mEncrypted[mCounter] = mEnPointer[i];
              mCounter++;
            }
            for (int i = 0; i < mEnData.length; i++) {
              mEncrypted[mCounter] = mEnData[i];
              mCounter++;
            }

          } else {
            pointerEnSize = 0;
            mEncrypted = NodeEncryption.encrypt(mStream, mSecretKey);
          }

          pOut.writeInt(mEncrypted.length);
          pOut.writeInt(pointerEnSize);

          for (byte aByte : mEncrypted) {
            pOut.writeByte(aByte);
          }

        } else {
          pOut.writeLong(-1);
          pOut.writeInt(-1);
          pOut.writeInt(-1);
          pOut.writeInt(ENode.UNKOWN_KIND.getId());
        }
      }
    } else {
      // int length = 0;
      // // boolean hasDeleted = false;
      // for (int i = 0; i < mNodes.length; i++) {
      // if (mNodes[i] != null) {
      // length = i + 1;
      // }
      // }
      // // if (mNodes[i].getKind() == ENode.DELETE_KIND) {
      // // hasDeleted = true;
      // // }
      // // length++;
      // // }
      // // }
      // pOut.writeInt(IConstants.NDP_NODE_COUNT);
      // pOut.writeByte(hasDeleted ? (byte)1 : (byte)0);
      // for (int i = 0; i < IConstants.NDP_NODE_COUNT; i++) {
      for (final INode node : mNodes) {
        // final INode node = mNodes.get(i);
        if (node == null) {
          pOut.writeByte(getLastByte(ENode.UNKOWN_KIND.getId()));
        } else {
          final byte id = node.getKind().getId();
          pOut.writeByte(id);
          ENode.getKind(node.getClass()).serialize(pOut, node);
        }
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
  protected IPage delegate() {
    return mDelegate;
  }
}
