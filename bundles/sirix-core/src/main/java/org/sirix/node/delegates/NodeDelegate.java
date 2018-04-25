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

import java.util.Optional;
import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.sirix.api.visitor.VisitResultType;
import org.sirix.api.visitor.Visitor;
import org.sirix.node.Kind;
import org.sirix.node.SirixDeweyID;
import org.sirix.node.interfaces.Node;
import org.sirix.settings.Fixed;
import org.sirix.utils.NamePageHash;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

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
  private long mHash;

  /**
   * TypeKey of the parent node. Can be referenced later on over special pages.
   */
  private int mTypeKey;

  /** Revision this node was added. */
  private final long mRevision;

  /** {@link SirixDeweyID} reference. */
  private Optional<SirixDeweyID> mID;

  /**
   * Constructor.
   *
   * @param nodeKey node key
   * @param parentKey parent node key
   * @param hash hash of the node
   * @param revision revision this node was added
   */
  public NodeDelegate(final @Nonnegative long nodeKey, final long parentKey, final long hash,
      final @Nonnegative long revision, final Optional<SirixDeweyID> deweyID) {
    assert nodeKey >= 0 : "nodeKey must be >= 0!";
    assert parentKey >= Fixed.NULL_NODE_KEY.getStandardProperty();
    assert deweyID != null : "deweyID must not be null!";
    mNodeKey = nodeKey;
    mParentKey = parentKey;
    mHash = hash;
    mRevision = revision;
    mTypeKey = TYPE_KEY;
    mID = deweyID;
  }

  @Override
  public Kind getKind() {
    return Kind.UNKNOWN;
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
  public void setParentKey(final long pParentKey) {
    assert pParentKey >= Fixed.NULL_NODE_KEY.getStandardProperty();
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
  public VisitResultType acceptVisitor(final Visitor pVisitor) {
    return VisitResultType.CONTINUE;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mNodeKey, mTypeKey, mHash, mParentKey);
  }

  @Override
  public boolean equals(final Object otherObj) {
    if (!(otherObj instanceof NodeDelegate))
      return false;

    final NodeDelegate other = (NodeDelegate) otherObj;

    return Objects.equal(mNodeKey, other.mNodeKey) && Objects.equal(mTypeKey, other.mTypeKey)
        && Objects.equal(mHash, other.mHash) && Objects.equal(mParentKey, other.mParentKey);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("node key", mNodeKey)
                      .add("parent key", mParentKey)
                      .add("type key", mTypeKey)
                      .add("hash", mHash)
                      .add("deweyID", mID)
                      .toString();
  }

  @Override
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
  public void setDeweyID(final Optional<SirixDeweyID> id) {
    assert id != null : "id must be != null!";
    mID = id;
  }

  @Override
  public Optional<SirixDeweyID> getDeweyID() {
    return mID;
  }
}
