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
import io.sirix.node.immutable.xml.ImmutableText;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.ReusableNodeProxy;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.node.layout.NodeKindLayout;
import io.sirix.node.layout.SlotLayoutAccessors;
import io.sirix.node.layout.StructuralField;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.Compression;
import io.sirix.utils.NamePageHash;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;

/**
 * Text node implementation using primitive fields.
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.
 * Structural fields are parsed immediately; metadata and value are parsed lazily.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class TextNode implements StructNode, ValueNode, ImmutableXmlNode, ReusableNodeProxy {

  // === IMMEDIATE STRUCTURAL FIELDS ===
  private long nodeKey;
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;

  // === LAZY FIELDS (Metadata) ===
  private int previousRevision;
  private int lastModifiedRevision;
  private long hash;

  // === VALUE FIELD (Lazy) ===
  private byte[] value;
  private boolean isCompressed;

  // === NON-SERIALIZED FIELDS ===
  private LongHashFunction hashFunction;
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // === LAZY PARSING STATE ===
  private Object lazySource;
  private long lazyOffset;
  private boolean metadataParsed;
  private boolean valueParsed;
  private boolean hasHash;
  private long valueOffset;
  private boolean fixedValueEncoding;
  private int fixedValueLength;
  private boolean fixedValueCompressed;

  // Fixed-slot lazy metadata support
  private NodeKindLayout fixedSlotLayout;

  /**
   * Primary constructor with all primitive fields.
   */
  public TextNode(long nodeKey, long parentKey, int previousRevision,
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
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public TextNode(long nodeKey, long parentKey, int previousRevision,
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
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.TEXT;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public void setNodeKey(long nodeKey) {
    this.nodeKey = nodeKey;
  }

  // === IMMEDIATE STRUCTURAL GETTERS (no lazy parsing) ===

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

  // === LAZY GETTERS ===

  @Override
  public int getPreviousRevisionNumber() {
    if (!metadataParsed) parseMetadataFields();
    return previousRevision;
  }

  @Override
  public void setPreviousRevision(int revision) {
    this.previousRevision = revision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (!metadataParsed) parseMetadataFields();
    return lastModifiedRevision;
  }

  @Override
  public void setLastModifiedRevision(int revision) {
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (!metadataParsed) parseMetadataFields();
    if (hash == 0L && hashFunction != null) {
      hash = computeHash(Bytes.threadLocalHashBuffer());
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
      parseValuePayload();
    }
    if (value != null && isCompressed) {
      // Lazy decompress
      value = Compression.decompress(value);
      isCompressed = false;
    }
    return value;
  }

  @Override
  public void setRawValue(byte[] value) {
    this.value = value;
    this.fixedValueEncoding = false;
    this.fixedValueLength = 0;
    this.fixedValueCompressed = false;
    this.isCompressed = false;
    this.hash = 0L;
  }

  public void setLazyRawValue(Object source, long valueOffset, int valueLength, boolean compressed) {
    this.lazySource = source;
    this.lazyOffset = valueOffset;
    this.valueOffset = valueOffset;
    this.metadataParsed = true;
    this.valueParsed = false;
    this.hasHash = false;
    this.fixedValueEncoding = true;
    this.fixedValueLength = valueLength;
    this.fixedValueCompressed = compressed;
    this.value = null;
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

  // === LEAF NODE METHODS (no children) ===

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  public void setFirstChildKey(long key) {
    // No-op - text nodes can't have children
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
    // No-op - text nodes can't have children
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

  // === NON-SERIALIZED FIELD ACCESSORS ===

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
   * Returns the raw value bytes without triggering decompression.
   * Used by the fixed-slot projector to preserve the original compressed bytes.
   */
  public byte[] getRawValueWithoutDecompression() {
    if (!valueParsed) {
      parseValuePayload();
    }
    return value;
  }

  public boolean isCompressed() {
    return isCompressed;
  }

  // === HASH COMPUTATION ===

  @Override
  public long computeHash(BytesOut<?> bytes) {
    if (hashFunction == null) {
      return 0L;
    }

    bytes.clear();
    bytes.writeLong(nodeKey)
         .writeLong(parentKey)
         .writeByte(NodeKind.TEXT.getId());

    bytes.writeLong(leftSiblingKey).writeLong(rightSiblingKey);
    bytes.write(getRawValue());

    final var buffer = ((java.nio.ByteBuffer) bytes.underlyingObject()).rewind();
    buffer.limit((int) bytes.readLimit());

    return hashFunction.hashBytes(buffer);
  }

  // === LAZY PARSING ===

  /**
   * Populate this node from a BytesIn source for singleton reuse.
   */
  public void readFrom(BytesIn<?> source, long nodeKey, byte[] deweyId,
      LongHashFunction hashFunction, ResourceConfiguration config) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;

    // IMMEDIATE: Structural fields
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

    // LAZY: Store offset for deferred parsing
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.metadataParsed = false;
    this.valueParsed = false;
    // Text-node hash is not serialized in compact encoding.
    this.hasHash = false;
    this.valueOffset = 0;
    this.fixedValueEncoding = false;
    this.fixedValueLength = 0;
    this.fixedValueCompressed = false;

    // Initialize lazy fields
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
    this.value = null;
    this.isCompressed = false;
  }

  public void bindFixedSlotLazy(final MemorySegment slotData, final NodeKindLayout layout) {
    this.fixedSlotLayout = layout;
    this.metadataParsed = false;
  }

  private void parseMetadataFields() {
    if (metadataParsed) {
      return;
    }

    if (fixedSlotLayout != null) {
      final MemorySegment sd = (MemorySegment) lazySource;
      final NodeKindLayout ly = fixedSlotLayout;
      this.previousRevision = SlotLayoutAccessors.readIntField(sd, ly, StructuralField.PREVIOUS_REVISION);
      this.lastModifiedRevision = SlotLayoutAccessors.readIntField(sd, ly, StructuralField.LAST_MODIFIED_REVISION);
      this.hash = SlotLayoutAccessors.readLongField(sd, ly, StructuralField.HASH);
      this.fixedSlotLayout = null;
      this.metadataParsed = true;
      return;
    }

    if (lazySource == null) {
      metadataParsed = true;
      return;
    }

    BytesIn<?> bytesIn = createBytesIn(lazyOffset);

    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    if (hasHash) {
      this.hash = bytesIn.readLong();
    }
    this.valueOffset = bytesIn.position();
    this.metadataParsed = true;
  }

  private void parseValuePayload() {
    if (valueParsed) {
      return;
    }

    if (!metadataParsed) {
      parseMetadataFields();
    }

    if (lazySource == null) {
      valueParsed = true;
      return;
    }

    if (fixedValueEncoding) {
      BytesIn<?> bytesIn = createBytesIn(valueOffset);
      this.isCompressed = fixedValueCompressed;
      this.value = new byte[fixedValueLength];
      if (fixedValueLength > 0) {
        bytesIn.read(this.value, 0, fixedValueLength);
      }
      this.valueParsed = true;
      return;
    }

    BytesIn<?> bytesIn = createBytesIn(valueOffset);

    this.isCompressed = bytesIn.readBoolean();
    int length = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.value = new byte[length];
    bytesIn.read(this.value);
    this.valueParsed = true;
  }

  private BytesIn<?> createBytesIn(long offset) {
    if (lazySource instanceof MemorySegment segment) {
      var bytesIn = new MemorySegmentBytesIn(segment);
      bytesIn.position(offset);
      return bytesIn;
    } else if (lazySource instanceof byte[] bytes) {
      var bytesIn = new ByteArrayBytesIn(bytes);
      bytesIn.position(offset);
      return bytesIn;
    } else {
      throw new IllegalStateException("Unknown lazy source type: " + lazySource.getClass());
    }
  }

  /**
   * Create a deep copy snapshot of this node.
   */
  public TextNode toSnapshot() {
    if (!metadataParsed) parseMetadataFields();
    if (!valueParsed) parseValuePayload();

    return new TextNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, hash,
        value != null ? value.clone() : null, isCompressed,
        hashFunction,
        deweyIDBytes != null ? deweyIDBytes.clone() : null);
  }

  @Override
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableText.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey, getValue());
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj instanceof TextNode other) {
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
