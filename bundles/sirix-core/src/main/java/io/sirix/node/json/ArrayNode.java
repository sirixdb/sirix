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
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.json.ImmutableArrayNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * JSON Array node.
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.
 * This eliminates MemorySegment/VarHandle overhead and enables compact serialization.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class ArrayNode implements StructNode, ImmutableJsonNode {

  // Node identity (mutable for singleton reuse)
  private long nodeKey;
  
  // Mutable structural fields (updated during tree modifications)
  private long parentKey;
  private long pathNodeKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private long firstChildKey;
  private long lastChildKey;
  
  // Mutable revision tracking
  private int previousRevision;
  private int lastModifiedRevision;
  
  // Mutable counters
  private long childCount;
  private long descendantCount;
  
  // Mutable hash
  private long hash;
  
  // Hash function for computing node hashes (mutable for singleton reuse)
  private LongHashFunction hashFunction;
  
  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  /**
   * Primary constructor with all primitive fields.
   * Used by deserialization (NodeKind.ARRAY.deserialize).
   *
   * @param nodeKey              the node key (record ID)
   * @param parentKey            the parent node key
   * @param pathNodeKey          the path node key for path summary
   * @param previousRevision     the previous revision number
   * @param lastModifiedRevision the last modified revision number
   * @param rightSiblingKey      the right sibling key
   * @param leftSiblingKey       the left sibling key
   * @param firstChildKey        the first child key
   * @param lastChildKey         the last child key
   * @param childCount           the child count
   * @param descendantCount      the descendant count
   * @param hash                 the hash value
   * @param hashFunction         the hash function for computing hashes
   * @param deweyID              the DeweyID as byte array (can be null)
   */
  public ArrayNode(long nodeKey, long parentKey, long pathNodeKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long firstChildKey,
      long lastChildKey, long childCount, long descendantCount, long hash,
      LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   * Used by factory methods when creating new nodes.
   */
  public ArrayNode(long nodeKey, long parentKey, long pathNodeKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long firstChildKey,
      long lastChildKey, long childCount, long descendantCount, long hash,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ARRAY;
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

    bytes.writeLong(getPathNodeKey());

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
    return firstChildKey;
  }
  
  public void setFirstChildKey(final long firstChild) {
    this.firstChildKey = firstChild;
  }

  @Override
  public long getLastChildKey() {
    return lastChildKey;
  }
  
  public void setLastChildKey(final long lastChild) {
    this.lastChildKey = lastChild;
  }

  @Override
  public long getChildCount() {
    return childCount;
  }
  
  public void setChildCount(final long childCount) {
    this.childCount = childCount;
  }

  @Override
  public long getDescendantCount() {
    return descendantCount;
  }
  
  public void setDescendantCount(final long descendantCount) {
    this.descendantCount = descendantCount;
  }

  public long getPathNodeKey() {
    return pathNodeKey;
  }

  public ArrayNode setPathNodeKey(final @NonNegative long pathNodeKey) {
    this.pathNodeKey = pathNodeKey;
    return this;
  }

  @Override
  public boolean hasFirstChild() {
    return firstChildKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasLastChild() {
    return lastChildKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public void incrementChildCount() {
    childCount++;
  }

  @Override
  public void decrementChildCount() {
    childCount--;
  }

  @Override
  public void incrementDescendantCount() {
    descendantCount++;
  }

  @Override
  public void decrementDescendantCount() {
    descendantCount--;
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
  public void setNodeKey(final long nodeKey) {
    this.nodeKey = nodeKey;
  }

  /**
   * Populate this node from a BytesIn source for singleton reuse.
   * Mirrors the deserialization logic in NodeKind.ARRAY.deserialize().
   *
   * @param source       the BytesIn source positioned after the NodeKind byte
   * @param nodeKey      the node key (record ID)
   * @param deweyId      the DeweyID bytes (may be null)
   * @param hashFunction the hash function for computing hashes
   * @param config       the resource configuration
   */
  public void readFrom(final BytesIn<?> source, final long nodeKey, final byte[] deweyId,
                       final LongHashFunction hashFunction, final ResourceConfiguration config) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.previousRevision = DeltaVarIntCodec.decodeSigned(source);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.lastChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.childCount = config.storeChildCount() ? DeltaVarIntCodec.decodeSigned(source) : 0;
    if (config.hashType != HashType.NONE) {
      this.hash = source.readLong();
      this.descendantCount = DeltaVarIntCodec.decodeSigned(source);
    } else {
      this.hash = 0;
      this.descendantCount = 0;
    }
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
  }

  /**
   * Create a deep copy snapshot of this node.
   *
   * @return a new ArrayNode with all values copied (byte arrays cloned)
   */
  public ArrayNode toSnapshot() {
    return new ArrayNode(nodeKey, parentKey, pathNodeKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, firstChildKey, lastChildKey, childCount,
        descendantCount, hash, hashFunction,
        deweyIDBytes != null ? deweyIDBytes.clone() : null);
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
    return visitor.visit(ImmutableArrayNode.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("pathNodeKey", pathNodeKey)
                      .add("parentKey", parentKey)
                      .add("previousRevision", previousRevision)
                      .add("lastModifiedRevision", lastModifiedRevision)
                      .add("rightSibling", rightSiblingKey)
                      .add("leftSibling", leftSiblingKey)
                      .add("firstChild", firstChildKey)
                      .add("lastChild", lastChildKey)
                      .add("childCount", childCount)
                      .add("hash", hash)
                      .add("descendantCount", descendantCount)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ArrayNode other))
      return false;

    return nodeKey == other.nodeKey
        && parentKey == other.parentKey;
  }
}
