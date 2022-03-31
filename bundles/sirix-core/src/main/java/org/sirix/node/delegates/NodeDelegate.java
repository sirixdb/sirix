/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package org.sirix.node.delegates;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.PrimitiveSink;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.math.BigInteger;

/**
 * Delegate method for all nodes. That means that all nodes stored in Sirix are represented by an
 * instance of the interface {@link Node} namely containing the position in the tree related to a
 * parent-node, the related type and the corresponding hash recursively computed.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public class NodeDelegate implements Node {

  /** Untyped type. */
  private static final int TYPE_KEY = NamePageHash.generateHashForString("xs:untyped");

  /** Key of the current node. Must be unique for all nodes. */
  private final long nodeKey;

  /** Key of the parent node. */
  private long parentKey;

  /** Hash of the parent node. */
  private final BigInteger hashCode;

  /**
   * TypeKey of the parent node. Can be referenced later on over special pages.
   */
  private int typeKey;

  /** Revision this node was added. */
  private final long revision;

  /** {@link SirixDeweyID} reference. */
  private SirixDeweyID sirixDeweyID;

  /** The hash function. */
  private final HashFunction hashFunction;

  /**
   * Constructor.
   *
   * @param nodeKey node key
   * @param parentKey parent node key
   * @param hashCode hash code of the node
   * @param hashFunction the hash function used to compute hash codes
   * @param revision revision this node was added
   * @param deweyID optional DeweyID
   */
  public NodeDelegate(final @NonNegative long nodeKey, final long parentKey, final HashFunction hashFunction,
      final BigInteger hashCode, final @NonNegative long revision, final SirixDeweyID deweyID) {
    assert nodeKey >= 0 : "nodeKey must be >= 0!";
    assert parentKey >= Fixed.NULL_NODE_KEY.getStandardProperty();
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.hashFunction = hashFunction;
    this.hashCode = hashCode;
    this.revision = revision;
    typeKey = TYPE_KEY;
    sirixDeweyID = deweyID;
  }

  public HashFunction getHashFunction() {
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
  public BigInteger computeHash() {
    final Funnel<Node> nodeFunnel = (Node node, PrimitiveSink into) -> into.putLong(node.getNodeKey()).putLong(node.getParentKey()).putByte(node.getKind().getId());

    return Node.to128BitsAtMaximumBigInteger(new BigInteger(1, hashFunction.hashObject(this, nodeFunnel).asBytes()));
  }

  @Override
  public BigInteger getHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHash(final BigInteger hash) {
    throw new UnsupportedOperationException();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, typeKey, hashCode, parentKey);
  }

  @Override
  public boolean equals(final Object otherObj) {
    if (!(otherObj instanceof NodeDelegate))
      return false;

    final NodeDelegate other = (NodeDelegate) otherObj;

    return Objects.equal(nodeKey, other.nodeKey) && Objects.equal(typeKey, other.typeKey)
        && Objects.equal(hashCode, other.hashCode) && Objects.equal(parentKey, other.parentKey);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node key", nodeKey)
                      .add("parent key", parentKey)
                      .add("type key", typeKey)
                      .add("hash", hashCode)
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
  public long getRevision() {
    return revision;
  }

  @Override
  public void setDeweyID(final SirixDeweyID id) {
    sirixDeweyID = id;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    return sirixDeweyID;
  }
}
