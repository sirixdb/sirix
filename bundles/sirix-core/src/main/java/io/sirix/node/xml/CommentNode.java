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
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.xml.ImmutableComment;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.ReusableNodeProxy;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.Compression;
import io.sirix.utils.NamePageHash;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;

/**
 * Comment node implementation using primitive fields.
 *
 * <p>Uses primitive fields for efficient storage.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class CommentNode implements StructNode, ValueNode, ImmutableXmlNode, ReusableNodeProxy {

  // === IMMEDIATE STRUCTURAL FIELDS ===
  private long nodeKey;
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;

  // === METADATA FIELDS ===
  private int previousRevision;
  private int lastModifiedRevision;
  private long hash;

  // === VALUE FIELD ===
  private byte[] value;
  private boolean isCompressed;
  private Object lazyValueSource;
  private long lazyValueOffset;
  private int lazyValueLength;
  private boolean lazyValueCompressed;
  private boolean valueParsed = true;

  // === NON-SERIALIZED FIELDS ===
  private LongHashFunction hashFunction;
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  /**
   * Primary constructor with all primitive fields.
   */
  public CommentNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey,
      long hash, byte[] value, boolean isCompressed,
      LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.hash = hash;
    this.value = value;
    this.isCompressed = isCompressed;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public CommentNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey,
      long hash, byte[] value, boolean isCompressed,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.hash = hash;
    this.value = value;
    this.isCompressed = isCompressed;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.COMMENT;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public void setNodeKey(long nodeKey) {
    this.nodeKey = nodeKey;
  }

  @Override
  public long getParentKey() {
    return parentKey;
  }

  public void setParentKey(long parentKey) {
    this.parentKey = parentKey;
  }

  @Override
  public boolean hasParent() {
    return parentKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getRightSiblingKey() {
    return rightSiblingKey;
  }

  public void setRightSiblingKey(long key) {
    this.rightSiblingKey = key;
  }

  @Override
  public boolean hasRightSibling() {
    return rightSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLeftSiblingKey() {
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(long key) {
    this.leftSiblingKey = key;
  }

  @Override
  public boolean hasLeftSibling() {
    return leftSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public int getPreviousRevisionNumber() {
    return previousRevision;
  }

  @Override
  public void setPreviousRevision(int revision) {
    this.previousRevision = revision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return lastModifiedRevision;
  }

  @Override
  public void setLastModifiedRevision(int revision) {
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (hash == 0L && hashFunction != null) {
      hash = computeHash(Bytes.elasticOffHeapByteBuffer());
    }
    return hash;
  }

  @Override
  public void setHash(long hash) {
    this.hash = hash;
  }

  @Override
  public byte[] getRawValue() {
    if (!valueParsed) {
      parseLazyValue();
    }
    if (value != null && isCompressed) {
      value = Compression.decompress(value);
      isCompressed = false;
    }
    return value;
  }

  @Override
  public void setRawValue(byte[] value) {
    this.value = value;
    this.valueParsed = true;
    this.lazyValueSource = null;
    this.lazyValueOffset = 0L;
    this.lazyValueLength = 0;
    this.lazyValueCompressed = false;
    this.isCompressed = false;
    this.hash = 0L;
  }

  public void setLazyRawValue(Object source, long valueOffset, int valueLength, boolean compressed) {
    this.lazyValueSource = source;
    this.lazyValueOffset = valueOffset;
    this.lazyValueLength = valueLength;
    this.lazyValueCompressed = compressed;
    this.value = null;
    this.valueParsed = false;
    this.isCompressed = compressed;
    this.hash = 0L;
  }

  public void setCompressed(boolean compressed) {
    this.isCompressed = compressed;
  }

  @Override
  public String getValue() {
    return new String(getRawValue(), Constants.DEFAULT_ENCODING);
  }

  // === LEAF NODE METHODS ===

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  public void setFirstChildKey(long key) {
    // No-op
  }

  @Override
  public boolean hasFirstChild() {
    return false;
  }

  @Override
  public long getLastChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  public void setLastChildKey(long key) {
    // No-op
  }

  @Override
  public boolean hasLastChild() {
    return false;
  }

  @Override
  public long getChildCount() {
    return 0;
  }

  public void setChildCount(long childCount) {
    // No-op
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
  public long getDescendantCount() {
    return 0;
  }

  @Override
  public void setDescendantCount(long descendantCount) {
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
  public boolean isSameItem(@Nullable Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  @Override
  public int getTypeKey() {
    return NamePageHash.generateHashForString("xs:untyped");
  }

  @Override
  public void setTypeKey(int typeKey) {
    // Not stored
  }

  @Override
  public void setDeweyID(SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = null;
  }

  public void setDeweyIDBytes(byte[] deweyIDBytes) {
    this.deweyIDBytes = deweyIDBytes;
    this.sirixDeweyID = null;
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

  public LongHashFunction getHashFunction() {
    return hashFunction;
  }

  /**
   * Populate this node from a BytesIn source for singleton reuse.
   */
  public void readFrom(BytesIn<?> source, long nodeKey, byte[] deweyId,
      LongHashFunction hashFunction, ResourceConfiguration config) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;

    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.previousRevision = DeltaVarIntCodec.decodeSigned(source);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);

    final boolean compressed = source.readByte() == (byte) 1;
    final int valueLength = DeltaVarIntCodec.decodeSigned(source);
    final long valueOffset = source.position();
    setLazyRawValue(source.getSource(), valueOffset, valueLength, compressed);
    source.position(valueOffset + valueLength);

    // Comment hash is not serialized; keep it invalidated for lazy recompute.
    this.hash = 0L;
  }

  private void parseLazyValue() {
    if (valueParsed) {
      return;
    }

    if (lazyValueSource == null) {
      valueParsed = true;
      return;
    }

    final BytesIn<?> bytesIn = createBytesIn(lazyValueSource, lazyValueOffset);
    final byte[] parsedValue = new byte[lazyValueLength];
    if (lazyValueLength > 0) {
      bytesIn.read(parsedValue, 0, lazyValueLength);
    }
    value = parsedValue;
    isCompressed = lazyValueCompressed;
    valueParsed = true;
    lazyValueSource = null;
  }

  private static BytesIn<?> createBytesIn(Object source, long offset) {
    if (source instanceof MemorySegment segment) {
      final var bytesIn = new MemorySegmentBytesIn(segment);
      bytesIn.position(offset);
      return bytesIn;
    }
    if (source instanceof byte[] bytes) {
      final var bytesIn = new ByteArrayBytesIn(bytes);
      bytesIn.position(offset);
      return bytesIn;
    }
    throw new IllegalStateException("Unknown lazy source type: " + source.getClass());
  }

  public boolean isCompressed() {
    return isCompressed;
  }

  @Override
  public long computeHash(BytesOut<?> bytes) {
    if (hashFunction == null) {
      return 0L;
    }

    bytes.clear();
    bytes.writeLong(nodeKey)
         .writeLong(parentKey)
         .writeByte(NodeKind.COMMENT.getId());

    bytes.writeLong(leftSiblingKey).writeLong(rightSiblingKey);
    bytes.writeUtf8(new String(getRawValue(), Constants.DEFAULT_ENCODING));

    final var buffer = ((java.nio.ByteBuffer) bytes.underlyingObject()).rewind();
    buffer.limit((int) bytes.readLimit());

    return hashFunction.hashBytes(buffer);
  }

  public CommentNode toSnapshot() {
    final byte[] rawValue = getRawValue();
    return new CommentNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, hash,
        rawValue != null ? rawValue.clone() : null, isCompressed,
        hashFunction,
        deweyIDBytes != null ? deweyIDBytes.clone() : null);
  }

  @Override
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableComment.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey, getValue());
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj instanceof CommentNode other) {
      return nodeKey == other.nodeKey
          && parentKey == other.parentKey
          && java.util.Arrays.equals(getRawValue(), other.getRawValue());
    }
    return false;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("parentKey", parentKey)
                      .add("rightSiblingKey", rightSiblingKey)
                      .add("leftSiblingKey", leftSiblingKey)
                      .add("value", getValue())
                      .add("compressed", isCompressed)
                      .toString();
  }
}
