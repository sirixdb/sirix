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

package io.sirix.node.xml;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.brackit.query.atomic.QNm;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.xml.ImmutablePI;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.NamePageHash;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Processing Instruction node using primitive fields.
 *
 * @author Johannes Lichtenberger
 */
public final class PINode implements StructNode, NameNode, ValueNode, ImmutableXmlNode {

  // === IMMEDIATE STRUCTURAL FIELDS ===
  private long nodeKey;
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private long firstChildKey;
  private long lastChildKey;

  // === NAME NODE FIELDS ===
  private long pathNodeKey;
  private int prefixKey;
  private int localNameKey;
  private int uriKey;

  // === METADATA FIELDS ===
  private int previousRevision;
  private int lastModifiedRevision;
  private long childCount;
  private long descendantCount;
  private long hash;

  // === VALUE FIELD ===
  private byte[] value;
  private boolean isCompressed;

  // === NON-SERIALIZED FIELDS ===
  private LongHashFunction hashFunction;
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;
  private QNm qNm;

  /**
   * Primary constructor with all primitive fields.
   */
  public PINode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision,
      long rightSiblingKey, long leftSiblingKey, long firstChildKey, long lastChildKey,
      long childCount, long descendantCount, long hash, long pathNodeKey,
      int prefixKey, int localNameKey, int uriKey, byte[] value, boolean isCompressed,
      LongHashFunction hashFunction, byte[] deweyID, QNm qNm) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.pathNodeKey = pathNodeKey;
    this.prefixKey = prefixKey;
    this.localNameKey = localNameKey;
    this.uriKey = uriKey;
    this.value = value;
    this.isCompressed = isCompressed;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.qNm = qNm;
  }

  /**
   * Constructor with SirixDeweyID.
   */
  public PINode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision,
      long rightSiblingKey, long leftSiblingKey, long firstChildKey, long lastChildKey,
      long childCount, long descendantCount, long hash, long pathNodeKey,
      int prefixKey, int localNameKey, int uriKey, byte[] value, boolean isCompressed,
      LongHashFunction hashFunction, SirixDeweyID deweyID, QNm qNm) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.pathNodeKey = pathNodeKey;
    this.prefixKey = prefixKey;
    this.localNameKey = localNameKey;
    this.uriKey = uriKey;
    this.value = value;
    this.isCompressed = isCompressed;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.qNm = qNm;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PROCESSING_INSTRUCTION;
  }

  @Override
  public long getNodeKey() { return nodeKey; }

  @Override
  public void setNodeKey(long nodeKey) { this.nodeKey = nodeKey; }

  @Override
  public long getParentKey() { return parentKey; }

  public void setParentKey(long parentKey) { this.parentKey = parentKey; }

  @Override
  public boolean hasParent() { return parentKey != Fixed.NULL_NODE_KEY.getStandardProperty(); }

  @Override
  public long getRightSiblingKey() { return rightSiblingKey; }

  public void setRightSiblingKey(long key) { this.rightSiblingKey = key; }

  @Override
  public boolean hasRightSibling() { return rightSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty(); }

  @Override
  public long getLeftSiblingKey() { return leftSiblingKey; }

  public void setLeftSiblingKey(long key) { this.leftSiblingKey = key; }

  @Override
  public boolean hasLeftSibling() { return leftSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty(); }

  @Override
  public long getFirstChildKey() { return firstChildKey; }

  public void setFirstChildKey(long key) { this.firstChildKey = key; }

  @Override
  public boolean hasFirstChild() { return firstChildKey != Fixed.NULL_NODE_KEY.getStandardProperty(); }

  @Override
  public long getLastChildKey() { return lastChildKey; }

  public void setLastChildKey(long key) { this.lastChildKey = key; }

  @Override
  public boolean hasLastChild() { return lastChildKey != Fixed.NULL_NODE_KEY.getStandardProperty(); }

  @Override
  public long getChildCount() { return childCount; }

  public void setChildCount(long childCount) { this.childCount = childCount; }

  @Override
  public void incrementChildCount() { childCount++; }

  @Override
  public void decrementChildCount() { childCount--; }

  @Override
  public long getDescendantCount() { return descendantCount; }

  @Override
  public void setDescendantCount(long descendantCount) { this.descendantCount = descendantCount; }

  @Override
  public void incrementDescendantCount() { descendantCount++; }

  @Override
  public void decrementDescendantCount() { descendantCount--; }

  @Override
  public long getPathNodeKey() { return pathNodeKey; }

  @Override
  public void setPathNodeKey(@NonNegative long pathNodeKey) { this.pathNodeKey = pathNodeKey; }

  @Override
  public int getPrefixKey() { return prefixKey; }

  @Override
  public void setPrefixKey(int prefixKey) { this.prefixKey = prefixKey; }

  @Override
  public int getLocalNameKey() { return localNameKey; }

  @Override
  public void setLocalNameKey(int localNameKey) { this.localNameKey = localNameKey; }

  @Override
  public int getURIKey() { return uriKey; }

  @Override
  public void setURIKey(int uriKey) { this.uriKey = uriKey; }

  @Override
  public int getPreviousRevisionNumber() { return previousRevision; }

  @Override
  public void setPreviousRevision(int revision) { this.previousRevision = revision; }

  @Override
  public int getLastModifiedRevisionNumber() { return lastModifiedRevision; }

  @Override
  public void setLastModifiedRevision(int revision) { this.lastModifiedRevision = revision; }

  @Override
  public long getHash() {
    if (hash == 0L && hashFunction != null) {
      hash = computeHash(Bytes.elasticOffHeapByteBuffer());
    }
    return hash;
  }

  @Override
  public void setHash(long hash) { this.hash = hash; }

  @Override
  public byte[] getRawValue() { return value; }

  @Override
  public void setRawValue(byte[] value) { this.value = value; this.hash = 0L; }

  @Override
  public String getValue() { return value != null ? new String(value, Constants.DEFAULT_ENCODING) : ""; }

  @Override
  public QNm getName() { return qNm; }

  public void setName(QNm name) { this.qNm = name; }

  public boolean isCompressed() { return isCompressed; }

  @Override
  public boolean isSameItem(@Nullable Node other) { return other != null && other.getNodeKey() == nodeKey; }

  @Override
  public int getTypeKey() { return NamePageHash.generateHashForString("xs:untyped"); }

  @Override
  public void setTypeKey(int typeKey) { }

  @Override
  public void setDeweyID(SirixDeweyID id) { this.sirixDeweyID = id; this.deweyIDBytes = null; }

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

  public LongHashFunction getHashFunction() { return hashFunction; }

  @Override
  public long computeHash(BytesOut<?> bytes) {
    if (hashFunction == null) return 0L;
    bytes.clear();
    bytes.writeLong(nodeKey).writeLong(parentKey).writeByte(getKind().getId());
    bytes.writeLong(childCount).writeLong(descendantCount)
         .writeLong(leftSiblingKey).writeLong(rightSiblingKey)
         .writeLong(firstChildKey);
    if (lastChildKey != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      bytes.writeLong(lastChildKey);
    }
    bytes.writeInt(prefixKey).writeInt(localNameKey).writeInt(uriKey);
    if (value != null) bytes.write(value);
    return bytes.hashDirect(hashFunction);
  }

  public PINode toSnapshot() {
    return new PINode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, firstChildKey, lastChildKey,
        childCount, descendantCount, hash, pathNodeKey, prefixKey, localNameKey, uriKey,
        value != null ? value.clone() : null, isCompressed, hashFunction,
        deweyIDBytes != null ? deweyIDBytes.clone() : null, qNm);
  }

  @Override
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) { return visitor.visit(ImmutablePI.of(this)); }

  @Override
  public int hashCode() { return Objects.hashCode(nodeKey, parentKey, prefixKey, localNameKey, uriKey); }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof PINode other)) return false;
    return nodeKey == other.nodeKey && parentKey == other.parentKey
        && prefixKey == other.prefixKey && localNameKey == other.localNameKey && uriKey == other.uriKey;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
        .add("nodeKey", nodeKey).add("parentKey", parentKey)
        .add("qNm", qNm).add("value", getValue())
        .toString();
  }
}
