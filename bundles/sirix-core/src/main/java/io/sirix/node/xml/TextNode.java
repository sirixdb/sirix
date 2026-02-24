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
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.Compression;
import io.sirix.utils.NamePageHash;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Text node implementation using primitive fields.
 *
 * <p>
 * Uses primitive fields for efficient storage with delta+varint encoding. Structural fields are
 * parsed immediately; metadata and value are parsed lazily.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class TextNode implements StructNode, ValueNode, ImmutableXmlNode, FlyweightNode {

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

  // ==================== FLYWEIGHT BINDING (LeanStore page-direct access) ====================
  private MemorySegment page;
  private long recordBase;
  private long dataRegionStart;
  private int slotIndex;
  private static final int FIELD_COUNT = NodeFieldLayout.TEXT_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public TextNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
  }

  /**
   * Primary constructor with all primitive fields.
   */
  public TextNode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision, long rightSiblingKey,
      long leftSiblingKey, long hash, byte[] value, boolean isCompressed, LongHashFunction hashFunction,
      byte[] deweyID) {
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
  public TextNode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision, long rightSiblingKey,
      long leftSiblingKey, long hash, byte[] value, boolean isCompressed, LongHashFunction hashFunction,
      SirixDeweyID deweyID) {
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

  // ==================== FLYWEIGHT BIND/UNBIND ====================

  @Override
  public void bind(final MemorySegment page, final long recordBase, final long nodeKey,
      final int slotIndex) {
    this.page = page;
    this.recordBase = recordBase;
    this.nodeKey = nodeKey;
    this.slotIndex = slotIndex;
    this.dataRegionStart = recordBase + 1 + FIELD_COUNT;
    this.metadataParsed = true;
    this.valueParsed = false; // Payload still needs lazy parsing from page
    this.lazySource = null;
  }

  @Override
  public void unbind() {
    if (page == null) return;
    final long nk = this.nodeKey;
    this.parentKey = readDeltaField(NodeFieldLayout.TEXT_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.TEXT_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.TEXT_LEFT_SIB_KEY, nk);
    this.previousRevision = readSignedField(NodeFieldLayout.TEXT_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.TEXT_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.TEXT_HASH);
    // Payload needs to be read from page before unbinding
    if (!valueParsed) {
      readPayloadFromPage();
    }
    this.page = null;
  }

  @Override
  public boolean isBound() {
    return page != null;
  }

  @Override
  public boolean isBoundTo(final MemorySegment page) {
    return this.page == page;
  }

  @Override
  public int getSlotIndex() {
    return slotIndex;
  }

  @Override
  public int estimateSerializedSize() {
    final int payloadLen = value != null ? value.length : 0;
    return 64 + payloadLen;
  }

  // ==================== FLYWEIGHT FIELD READ HELPERS ====================

  private long readDeltaField(final int fieldIndex, final long baseKey) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.decodeDeltaFromSegment(page, dataRegionStart + fieldOff, baseKey);
  }

  private int readSignedField(final int fieldIndex) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.decodeSignedFromSegment(page, dataRegionStart + fieldOff);
  }

  private long readLongField(final int fieldIndex) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.readLongFromSegment(page, (int) (dataRegionStart + fieldOff));
  }

  private void setDeltaFieldInPlace(final int fieldIndex, final long newKey) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    final long absOff = dataRegionStart + fieldOff;
    final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
    final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(newKey, nodeKey);
    if (newWidth == currentWidth) {
      DeltaVarIntCodec.writeDeltaToSegment(page, absOff, newKey, nodeKey);
    } else {
      unbind();
    }
  }

  private boolean setSignedFieldInPlace(final int fieldIndex, final int newValue) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    final long absOff = dataRegionStart + fieldOff;
    final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
    final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(newValue);
    if (newWidth == currentWidth) {
      DeltaVarIntCodec.writeSignedToSegment(page, absOff, newValue);
      return true;
    }
    unbind();
    return false;
  }

  /**
   * Read the payload (value bytes) directly from page memory when bound.
   */
  private void readPayloadFromPage() {
    final int payloadFieldOff = page.get(ValueLayout.JAVA_BYTE,
        recordBase + 1 + NodeFieldLayout.TEXT_PAYLOAD) & 0xFF;
    final long payloadStart = dataRegionStart + payloadFieldOff;

    // Read isCompressed flag (1 byte)
    this.isCompressed = page.get(ValueLayout.JAVA_BYTE, payloadStart) == 1;

    // Read value length (varint)
    final long lenOffset = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(page, lenOffset);
    final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(page, lenOffset);

    // Read value bytes
    final long dataOffset = lenOffset + lenBytes;
    this.value = new byte[length];
    MemorySegment.copy(page, ValueLayout.JAVA_BYTE, dataOffset, this.value, 0, length);
    this.valueParsed = true;
  }

  // ==================== SERIALIZE TO HEAP ====================

  @Override
  public int serializeToHeap(final MemorySegment target, final long offset) {
    if (!metadataParsed) parseMetadataFields();
    if (!valueParsed) parseValuePayload();

    long pos = offset;
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.TEXT.getId());
    pos++;

    final long offsetTableStart = pos;
    pos += FIELD_COUNT;
    final long dataStart = pos;
    final int[] offsets = new int[FIELD_COUNT];

    // Field 0: parentKey
    offsets[NodeFieldLayout.TEXT_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    // Field 1: rightSiblingKey
    offsets[NodeFieldLayout.TEXT_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSiblingKey, nodeKey);

    // Field 2: leftSiblingKey
    offsets[NodeFieldLayout.TEXT_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSiblingKey, nodeKey);

    // Field 3: prevRevision
    offsets[NodeFieldLayout.TEXT_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, previousRevision);

    // Field 4: lastModRevision
    offsets[NodeFieldLayout.TEXT_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModifiedRevision);

    // Field 5: hash
    offsets[NodeFieldLayout.TEXT_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    // Field 6: payload [isCompressed:1][length:varint][data:bytes]
    offsets[NodeFieldLayout.TEXT_PAYLOAD] = (int) (pos - dataStart);
    target.set(ValueLayout.JAVA_BYTE, pos, isCompressed ? (byte) 1 : (byte) 0);
    pos++;
    final byte[] rawValue = value != null ? value : new byte[0];
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, rawValue.length);
    if (rawValue.length > 0) {
      MemorySegment.copy(rawValue, 0, target, ValueLayout.JAVA_BYTE, pos, rawValue.length);
      pos += rawValue.length;
    }

    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) offsets[i]);
    }

    return (int) (pos - offset);
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

  // === STRUCTURAL GETTERS (dual-mode: flyweight or primitive) ===

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.TEXT_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.TEXT_PARENT_KEY, parentKey);
      if (page != null) return;
    }
    this.parentKey = parentKey;
  }

  @Override
  public boolean hasParent() {
    return getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.TEXT_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long key) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.TEXT_RIGHT_SIB_KEY, key);
      if (page != null) return;
    }
    this.rightSiblingKey = key;
  }

  @Override
  public boolean hasRightSibling() {
    return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.TEXT_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long key) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.TEXT_LEFT_SIB_KEY, key);
      if (page != null) return;
    }
    this.leftSiblingKey = key;
  }

  @Override
  public boolean hasLeftSibling() {
    return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  // === LAZY GETTERS (dual-mode) ===

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.TEXT_PREV_REVISION);
    }
    if (!metadataParsed)
      parseMetadataFields();
    return previousRevision;
  }

  @Override
  public void setPreviousRevision(final int revision) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.TEXT_PREV_REVISION, revision)) return;
    }
    this.previousRevision = revision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.TEXT_LAST_MOD_REVISION);
    }
    if (!metadataParsed)
      parseMetadataFields();
    return lastModifiedRevision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.TEXT_LAST_MOD_REVISION, revision)) return;
    }
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (page != null) {
      // Bound: read hash from MemorySegment. If non-zero (set by rollingAdd/rollingUpdate),
      // return it as-is to preserve rolling hash arithmetic. If zero (never set), compute.
      final long storedHash = readLongField(NodeFieldLayout.TEXT_HASH);
      if (storedHash != 0L) {
        return storedHash;
      }
      if (hashFunction != null) {
        return computeHash(Bytes.threadLocalHashBuffer());
      }
      return 0L;
    }
    // Unbound (in-memory): return stored hash if set by rollingAdd, else compute
    if (!metadataParsed) parseMetadataFields();
    if (hash == 0L && hashFunction != null) {
      hash = computeHash(Bytes.threadLocalHashBuffer());
    }
    return hash;
  }

  @Override
  public void setHash(final long hash) {
    if (page != null) {
      // Hash is ALWAYS in-place (fixed 8 bytes)
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.TEXT_HASH) & 0xFF;
      DeltaVarIntCodec.writeLongToSegment(page, dataRegionStart + fieldOff, hash);
      return;
    }
    this.hash = hash;
  }

  @Override
  public byte[] getRawValue() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    } else if (!valueParsed) {
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
  public void setRawValue(final byte[] value) {
    if (page != null) unbind();
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

  /**
   * Returns the raw value bytes without triggering decompression. Used by the fixed-slot projector to
   * preserve the original compressed bytes.
   */
  public byte[] getRawValueWithoutDecompression() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    } else if (!valueParsed) {
      parseValuePayload();
    }
    return value;
  }

  public boolean isCompressed() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    }
    return isCompressed;
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

  // === HASH COMPUTATION ===

  @Override
  public long computeHash(BytesOut<?> bytes) {
    if (hashFunction == null) {
      return 0L;
    }

    bytes.clear();
    bytes.writeLong(nodeKey).writeLong(getParentKey()).writeByte(NodeKind.TEXT.getId());

    bytes.writeLong(getLeftSiblingKey()).writeLong(getRightSiblingKey());
    bytes.write(getRawValue());

    final var buffer = ((java.nio.ByteBuffer) bytes.underlyingObject()).rewind();
    buffer.limit((int) bytes.readLimit());

    return hashFunction.hashBytes(buffer);
  }

  // === LAZY PARSING ===

  /**
   * Populate this node from a BytesIn source for singleton reuse.
   */
  public void readFrom(BytesIn<?> source, long nodeKey, byte[] deweyId, LongHashFunction hashFunction,
      ResourceConfiguration config) {
    // Unbind flyweight -- ensures getters use Java fields, not stale page reference
    this.page = null;
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

  private void parseMetadataFields() {
    if (metadataParsed) {
      return;
    }

    if (page != null) {
      // When bound to a page, metadata is read directly from MemorySegment via getters
      metadataParsed = true;
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

    if (page != null) {
      readPayloadFromPage();
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
   * Forces parsing of all lazy fields since snapshot must be independent.
   */
  public TextNode toSnapshot() {
    if (page != null) {
      // Bound mode: read all fields from page
      if (!valueParsed) {
        readPayloadFromPage();
      }
      return new TextNode(nodeKey,
          readDeltaField(NodeFieldLayout.TEXT_PARENT_KEY, nodeKey),
          readSignedField(NodeFieldLayout.TEXT_PREV_REVISION),
          readSignedField(NodeFieldLayout.TEXT_LAST_MOD_REVISION),
          readDeltaField(NodeFieldLayout.TEXT_RIGHT_SIB_KEY, nodeKey),
          readDeltaField(NodeFieldLayout.TEXT_LEFT_SIB_KEY, nodeKey),
          readLongField(NodeFieldLayout.TEXT_HASH),
          value != null ? value.clone() : null,
          isCompressed,
          hashFunction,
          getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
    }
    // Force parse all lazy fields for snapshot (must be complete and independent)
    if (!metadataParsed) {
      parseMetadataFields();
    }
    if (!valueParsed) {
      parseValuePayload();
    }
    return new TextNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, hash,
        value != null ? value.clone() : null,
        isCompressed,
        hashFunction,
        getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
  }

  @Override
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableText.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, getParentKey(), getValue());
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj instanceof TextNode other) {
      return nodeKey == other.nodeKey && getParentKey() == other.getParentKey()
          && java.util.Arrays.equals(getRawValue(), other.getRawValue());
    }
    return false;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("parentKey", getParentKey())
                      .add("rightSiblingKey", getRightSiblingKey())
                      .add("leftSiblingKey", getLeftSiblingKey())
                      .add("value", getValue())
                      .add("compressed", isCompressed)
                      .toString();
  }
}
