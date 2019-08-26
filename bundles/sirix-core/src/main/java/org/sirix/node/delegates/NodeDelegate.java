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

import java.math.BigInteger;
import java.util.Optional;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.sirix.api.visitor.JsonNodeVisitor;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.XmlNodeVisitor;
import org.sirix.node.NodeKind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.hash.Funnel;
import com.google.common.hash.HashFunction;
import com.google.common.hash.PrimitiveSink;

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
  private long mNodeKey;

  /** Key of the parent node. */
  private long mParentKey;

  /** Hash of the parent node. */
  private BigInteger mHashCode;

  /**
   * TypeKey of the parent node. Can be referenced later on over special pages.
   */
  private int mTypeKey;

  /** Revision this node was added. */
  private final long mRevision;

  /** {@link SirixDeweyID} reference. */
  private SirixDeweyID mID;

  /** The hash function. */
  private final HashFunction mHashFunction;

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
  public NodeDelegate(final @Nonnegative long nodeKey, final long parentKey, final HashFunction hashFunction,
      final BigInteger hashCode, final @Nonnegative long revision, final SirixDeweyID deweyID) {
    assert nodeKey >= 0 : "nodeKey must be >= 0!";
    assert parentKey >= Fixed.NULL_NODE_KEY.getStandardProperty();
    mNodeKey = nodeKey;
    mParentKey = parentKey;
    mHashFunction = hashFunction;
    mHashCode = hashCode;
    mRevision = revision;
    mTypeKey = TYPE_KEY;
    mID = deweyID;
  }

  public HashFunction getHashFunction() {
    return mHashFunction;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.UNKNOWN;
  }

  @Override
  public long getNodeKey() {
    return mNodeKey;
  }

  @Override
  public long getParentKey() {
    return mParentKey;
  }

  @Override
  public void setParentKey(final long parentKey) {
    assert parentKey >= Fixed.NULL_NODE_KEY.getStandardProperty();
    mParentKey = parentKey;
  }

  @Override
  public BigInteger computeHash() {
    final Funnel<Node> nodeFunnel = (Node node, PrimitiveSink into) -> {
      into.putLong(node.getNodeKey()).putLong(node.getParentKey()).putByte(node.getKind().getId());
    };

    return Node.to128BitsAtMaximumBigInteger(new BigInteger(1, mHashFunction.hashObject(this, nodeFunnel).asBytes()));
  }

  @Override
  public BigInteger getHash() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setHash(final BigInteger hash) {
    throw new UnsupportedOperationException();
  }

  public VisitResultType acceptVisitor(final XmlNodeVisitor pVisitor) {
    return VisitResultType.CONTINUE;
  }

  public VisitResultType acceptVisitor(final JsonNodeVisitor pVisitor) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodeKey, mTypeKey, mHashCode, mParentKey);
  }

  @Override
  public boolean equals(final Object otherObj) {
    if (!(otherObj instanceof NodeDelegate))
      return false;

    final NodeDelegate other = (NodeDelegate) otherObj;

    return Objects.equal(mNodeKey, other.mNodeKey) && Objects.equal(mTypeKey, other.mTypeKey)
        && Objects.equal(mHashCode, other.mHashCode) && Objects.equal(mParentKey, other.mParentKey);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node key", mNodeKey)
                      .add("parent key", mParentKey)
                      .add("type key", mTypeKey)
                      .add("hash", mHashCode)
                      .add("deweyID", mID)
                      .toString();
  }

  public int getTypeKey() {
    return mTypeKey;
  }

  @Override
  public void setTypeKey(final int typeKey) {
    mTypeKey = typeKey;
  }

  @Override
  public boolean hasParent() {
    return mParentKey != Fixed.NULL_NODE_KEY.getStandardProperty();
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
    return mRevision;
  }

  @Override
  public void setDeweyID(final SirixDeweyID id) {
    mID = id;
  }

  public Optional<SirixDeweyID> getDeweyID() {
    return Optional.ofNullable(mID);
  }
}
