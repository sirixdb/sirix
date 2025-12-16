/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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

package io.sirix.node.json;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.json.ImmutableNullNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * JSON Null node.
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class NullNode implements StructNode, ImmutableJsonNode {

  // Immutable node identity
  private final long nodeKey;
  
  // Mutable structural fields
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  
  // Mutable revision tracking
  private int previousRevision;
  private int lastModifiedRevision;
  
  // Mutable hash (computed on demand for value nodes)
  private long hash;
  
  // Hash function for computing node hashes
  private final LongHashFunction hashFunction;
  
  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  /**
   * Primary constructor with all primitive fields.
   */
  public NullNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long hash,
      LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.hash = hash;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public NullNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long hash,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.hash = hash;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.NULL_VALUE;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    return parentKey;
  }
  
  public void setParentKey(final long parentKey) {
    this.parentKey = parentKey;
  }

  @Override
  public boolean hasParent() {
    return parentKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean isSameItem(@Nullable Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  @Override
  public void setTypeKey(final int typeKey) {
    // Not supported for JSON nodes
  }

  @Override
  public void setDeweyID(final SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = null;
  }

  @Override
  public void setPreviousRevision(final int revision) {
    this.previousRevision = revision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    return hash;
  }

  @Override
  public void setHash(final long hash) {
    this.hash = hash;
  }

  @Override
  public long computeHash(final BytesOut<?> bytes) {
    bytes.clear();
    bytes.writeLong(getNodeKey())
         .writeLong(getParentKey())
         .writeByte(getKind().getId());

    bytes.writeLong(getChildCount())
         .writeLong(getDescendantCount())
         .writeLong(getLeftSiblingKey())
         .writeLong(getRightSiblingKey())
         .writeLong(getFirstChildKey());

    if (getLastChildKey() != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      bytes.writeLong(getLastChildKey());
    }

    return hashFunction.hashBytes(bytes.toByteArray());
  }

  @Override
  public long getRightSiblingKey() {
    return rightSiblingKey;
  }
  
  public void setRightSiblingKey(final long rightSibling) {
    this.rightSiblingKey = rightSibling;
  }

  @Override
  public long getLeftSiblingKey() {
    return leftSiblingKey;
  }
  
  public void setLeftSiblingKey(final long leftSibling) {
    this.leftSiblingKey = leftSibling;
  }

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setFirstChildKey(final long firstChild) {
    // Value nodes are leaf nodes - no-op
  }

  @Override
  public long getLastChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setLastChildKey(final long lastChild) {
    // Value nodes are leaf nodes - no-op
  }

  @Override
  public long getChildCount() {
    return 0;
  }
  
  public void setChildCount(final long childCount) {
    // Value nodes are leaf nodes - no-op
  }

  @Override
  public long getDescendantCount() {
    return 0;
  }
  
  public void setDescendantCount(final long descendantCount) {
    // Value nodes are leaf nodes - no-op
  }

  @Override
  public boolean hasFirstChild() {
    return false;
  }

  @Override
  public boolean hasLastChild() {
    return false;
  }

  @Override
  public void incrementChildCount() {
    // No-op
  }

  @Override
  public void decrementChildCount() {
    // No-op
  }

  @Override
  public void incrementDescendantCount() {
    // No-op
  }

  @Override
  public void decrementDescendantCount() {
    // No-op
  }

  @Override
  public boolean hasLeftSibling() {
    return leftSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasRightSibling() {
    return rightSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public int getPreviousRevisionNumber() {
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return lastModifiedRevision;
  }

  public LongHashFunction getHashFunction() {
    return hashFunction;
  }

  @Override
  public SirixDeweyID getDeweyID() {
    if (deweyIDBytes != null && sirixDeweyID == null) {
      sirixDeweyID = new SirixDeweyID(deweyIDBytes);
    }
    return sirixDeweyID;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    if (deweyIDBytes == null && sirixDeweyID != null) {
      deweyIDBytes = sirixDeweyID.toBytes();
    }
    return deweyIDBytes;
  }

  @Override
  public VisitResult acceptVisitor(final JsonNodeVisitor visitor) {
    return visitor.visit(ImmutableNullNode.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("parentKey", parentKey)
                      .add("previousRevision", previousRevision)
                      .add("lastModifiedRevision", lastModifiedRevision)
                      .add("rightSibling", rightSiblingKey)
                      .add("leftSibling", leftSiblingKey)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final NullNode other))
      return false;

    return nodeKey == other.nodeKey
        && parentKey == other.parentKey;
  }
}
