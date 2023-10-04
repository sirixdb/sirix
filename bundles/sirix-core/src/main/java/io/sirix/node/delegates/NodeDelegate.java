/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package io.sirix.node.delegates;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.Node;
import io.sirix.settings.Fixed;
import io.sirix.utils.NamePageHash;
import net.openhft.chronicle.bytes.Bytes;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.nio.ByteBuffer;

/**
 * Delegate method for all nodes. That means that all nodes stored in Sirix are represented by an
 * instance of the interface {@link Node} namely containing the position in the tree related to a
 * parent-node, the related type and the corresponding hash recursively computed.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public class NodeDelegate implements Node {

  /**
   * Untyped type.
   */
  private static final int TYPE_KEY = NamePageHash.generateHashForString("xs:untyped");

  /**
   * Key of the current node. Must be unique for all nodes.
   */
  private final long nodeKey;

  /**
   * The DeweyID data.
   */
  private byte[] deweyIDData;

  /**
   * Key of the parent node.
   */
  private long parentKey;

  /**
   * TypeKey of the parent node. Can be referenced later on over special pages.
   */
  private int typeKey;

  /**
   * Previous revision.
   */
  private int previousRevision;

  /**
   * Revision, when a node has been last modified.
   */
  private int lastModifiedRevision;

  /**
   * {@link SirixDeweyID} (needs to be deserialized).
   */
  private SirixDeweyID sirixDeweyID;

  /**
   * The hash function.
   */
  private final LongHashFunction hashFunction;

  /**
   * Constructor.
   *
   * @param nodeKey              node key
   * @param parentKey            parent node key
   * @param hashFunction         the hash function used to compute hash codes
   * @param previousRevision     previous revision, when the node has changed
   * @param lastModifiedRevision the revision, when the node has been last modified
   * @param deweyID              optional DeweyID
   */
  public NodeDelegate(final @NonNegative long nodeKey, final long parentKey, final LongHashFunction hashFunction,
      final int previousRevision, final int lastModifiedRevision, final SirixDeweyID deweyID) {
    assert parentKey >= Fixed.NULL_NODE_KEY.getStandardProperty();
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.hashFunction = hashFunction;
    this.lastModifiedRevision = lastModifiedRevision;
    this.previousRevision = previousRevision;
    typeKey = TYPE_KEY;
    this.sirixDeweyID = deweyID;
  }

  /**
   * Constructor.
   *
   * @param nodeKey              node key
   * @param parentKey            parent node key
   * @param hashFunction         the hash function used to compute hash codes
   * @param previousRevision     previous previousRevision, when the node has changed
   * @param lastModifiedRevision the previousRevision, when the node has been last modified
   * @param deweyID              optional DeweyID
   */
  public NodeDelegate(final @NonNegative long nodeKey, final long parentKey, final LongHashFunction hashFunction,
      final int previousRevision, final int lastModifiedRevision, final byte[] deweyID) {
    assert parentKey >= Fixed.NULL_NODE_KEY.getStandardProperty();
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.hashFunction = hashFunction;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    typeKey = TYPE_KEY;
    deweyIDData = deweyID;
  }

  public LongHashFunction getHashFunction() {
    return hashFunction;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.UNKNOWN;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    return parentKey;
  }

  @Override
  public void setParentKey(final long parentKey) {
    assert parentKey >= Fixed.NULL_NODE_KEY.getStandardProperty();
    this.parentKey = parentKey;
  }

  @Override
  public long computeHash(Bytes<ByteBuffer> bytes) {
    throw new UnsupportedOperationException();
  }

  @Override
  public long getHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHash(final long hash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, typeKey, parentKey);
  }

  @Override
  public boolean equals(final Object otherObj) {
    if (!(otherObj instanceof final NodeDelegate other))
      return false;

    return Objects.equal(nodeKey, other.nodeKey) && Objects.equal(typeKey, other.typeKey) && Objects.equal(parentKey,
                                                                                                           other.parentKey);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node key", nodeKey)
                      .add("parent key", parentKey)
                      .add("type key", typeKey)
                      .add("deweyID", sirixDeweyID)
                      .toString();
  }

  public int getTypeKey() {
    return typeKey;
  }

  @Override
  public void setTypeKey(final int typeKey) {
    this.typeKey = typeKey;
  }

  @Override
  public boolean hasParent() {
    return parentKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean isSameItem(final @Nullable Node other) {
    if (other == null) {
      return false;
    }
    return other.getNodeKey() == this.getNodeKey();
  }

  @Override
  public int getPreviousRevisionNumber() {
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return lastModifiedRevision;
  }

  @Override
  public void setPreviousRevision(int previousRevision) {
    this.previousRevision = previousRevision;
  }

  @Override
  public void setLastModifiedRevision(int lastModifiedRevision) {
    this.lastModifiedRevision = lastModifiedRevision;
  }

  @Override
  public void setDeweyID(final SirixDeweyID id) {
    sirixDeweyID = id;
  }

  @Override
  public synchronized SirixDeweyID getDeweyID() {
    if (sirixDeweyID == null && deweyIDData != null) {
      sirixDeweyID = new SirixDeweyID(deweyIDData);
    }
    return sirixDeweyID;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    if (deweyIDData != null) {
      return deweyIDData;
    } else if (sirixDeweyID != null) {
      deweyIDData = sirixDeweyID.toBytes();
      return deweyIDData;
    } else {
      return null;
    }
  }
}
