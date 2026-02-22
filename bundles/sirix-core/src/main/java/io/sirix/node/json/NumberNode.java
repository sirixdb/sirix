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
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import io.sirix.node.immutable.json.ImmutableNumberNode;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.NumericValueNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * JSON Number node.
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class NumberNode implements StructNode, ImmutableJsonNode, NumericValueNode, FlyweightNode {

  // Node identity (mutable for singleton reuse)
  private long nodeKey;
  
  // Mutable structural fields
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  
  // Mutable revision tracking
  private int previousRevision;
  private int lastModifiedRevision;
  
  // Mutable hash (computed on demand for value nodes)
  private long hash;
  
  // Number value
  private Number value;
  
  // Hash function for computing node hashes (mutable for singleton reuse)
  private LongHashFunction hashFunction;
  
  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Lazy parsing state (for singleton reuse optimization)
  // Two-stage lazy parsing: metadata (cheap) vs value (expensive Number allocation)
  private Object lazySource;            // Source for lazy parsing (MemorySegment or byte[])
  private long lazyOffset;              // Offset where lazy metadata fields start
  private boolean metadataParsed;       // Whether prevRev, lastModRev, hash are parsed
  private boolean valueParsed;          // Whether Number value is parsed
  private boolean hasHash;              // Whether hash is stored (from config)
  private long valueOffset;             // Offset where value starts (after metadata)

  // ==================== FLYWEIGHT BINDING (LeanStore page-direct access) ====================
  private MemorySegment page;
  private long recordBase;
  private long dataRegionStart;
  private int slotIndex;
  private static final int FIELD_COUNT = NodeFieldLayout.NUMBER_VALUE_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public NumberNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
  }

  /**
   * Primary constructor with all primitive fields.
   * All fields are already parsed - no lazy loading needed.
   */
  public NumberNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long hash,
      Number value, LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    // Constructed with all values - mark as fully parsed
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   * All fields are already parsed - no lazy loading needed.
   */
  public NumberNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long hash,
      Number value, LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    // Constructed with all values - mark as fully parsed
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  // ==================== FLYWEIGHT BIND/UNBIND ====================

  public void bind(final MemorySegment page, final long recordBase, final long nodeKey,
      final int slotIndex) {
    this.page = page;
    this.recordBase = recordBase;
    this.nodeKey = nodeKey;
    this.slotIndex = slotIndex;
    this.dataRegionStart = recordBase + 1 + FIELD_COUNT;
    this.metadataParsed = true;
    this.valueParsed = false;
    this.lazySource = null;
  }

  public void unbind() {
    if (page == null) return;
    final long nk = this.nodeKey;
    this.parentKey = readDeltaField(NodeFieldLayout.NUMVAL_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.NUMVAL_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.NUMVAL_LEFT_SIB_KEY, nk);
    this.previousRevision = readSignedField(NodeFieldLayout.NUMVAL_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.NUMVAL_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.NUMVAL_HASH);
    if (!valueParsed) {
      readPayloadFromPage();
    }
    this.page = null;
  }

  public boolean isBound() { return page != null; }

  @Override
  public boolean isBoundTo(final MemorySegment page) {
    return this.page == page;
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
   * Read the Number payload from page memory when bound.
   * Uses the flyweight format written by {@link #serializeNumberToSegment}.
   */
  private void readPayloadFromPage() {
    final int payloadFieldOff = page.get(ValueLayout.JAVA_BYTE,
        recordBase + 1 + NodeFieldLayout.NUMVAL_PAYLOAD) & 0xFF;
    final long payloadStart = dataRegionStart + payloadFieldOff;
    this.value = deserializeNumberFromSegment(page, payloadStart);
    this.valueParsed = true;
  }

  /**
   * Deserialize a Number value directly from a MemorySegment.
   * Format must match {@link #serializeNumberToSegment} exactly:
   * <pre>
   *   Type 0 = Integer (zigzag varint)
   *   Type 1 = Long (zigzag varlong)
   *   Type 2 = Float (4 bytes, native endian raw bits)
   *   Type 3 = Double (8 bytes, native endian raw bits)
   *   Type 4 = BigDecimal (varint scale + varint byte-length + bytes)
   *   Type 5 = BigInteger (varint byte-length + bytes)
   * </pre>
   *
   * @param segment the MemorySegment containing the serialized number
   * @param offset  the byte offset of the number type byte
   * @return the deserialized Number
   */
  static Number deserializeNumberFromSegment(final MemorySegment segment, final long offset) {
    final byte valueType = segment.get(ValueLayout.JAVA_BYTE, offset);
    long pos = offset + 1;

    return switch (valueType) {
      case 0 -> // Integer (zigzag varint)
          DeltaVarIntCodec.decodeSignedFromSegment(segment, pos);
      case 1 -> // Long (zigzag varlong)
          DeltaVarIntCodec.decodeSignedLongFromSegment(segment, pos);
      case 2 -> // Float (4 bytes raw bits, native endian)
          Float.intBitsToFloat(segment.get(ValueLayout.JAVA_INT_UNALIGNED, pos));
      case 3 -> // Double (8 bytes raw bits, native endian)
          Double.longBitsToDouble(segment.get(ValueLayout.JAVA_LONG_UNALIGNED, pos));
      case 4 -> { // BigDecimal (varint scale + varint byte-length + bytes)
        final int scale = DeltaVarIntCodec.decodeSignedFromSegment(segment, pos);
        final int scaleWidth = DeltaVarIntCodec.readSignedVarintWidth(segment, pos);
        pos += scaleWidth;
        final int bytesLen = DeltaVarIntCodec.decodeSignedFromSegment(segment, pos);
        final int bytesLenWidth = DeltaVarIntCodec.readSignedVarintWidth(segment, pos);
        pos += bytesLenWidth;
        final byte[] bytes = new byte[bytesLen];
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, pos, bytes, 0, bytesLen);
        yield new BigDecimal(new BigInteger(bytes), scale);
      }
      case 5 -> { // BigInteger (varint byte-length + bytes)
        final int bytesLen = DeltaVarIntCodec.decodeSignedFromSegment(segment, pos);
        final int bytesLenWidth = DeltaVarIntCodec.readSignedVarintWidth(segment, pos);
        pos += bytesLenWidth;
        final byte[] bytes = new byte[bytesLen];
        MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, pos, bytes, 0, bytesLen);
        yield new BigInteger(bytes);
      }
      default -> throw new IllegalStateException("Unknown flyweight number type: " + valueType);
    };
  }

  // ==================== SERIALIZE TO HEAP ====================

  public int serializeToHeap(final MemorySegment target, final long offset) {
    if (!metadataParsed) parseMetadataFields();
    if (!valueParsed) parseValueField();

    long pos = offset;
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.NUMBER_VALUE.getId());
    pos++;

    final long offsetTableStart = pos;
    pos += FIELD_COUNT;
    final long dataStart = pos;
    final int[] offsets = new int[FIELD_COUNT];

    offsets[NodeFieldLayout.NUMVAL_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    offsets[NodeFieldLayout.NUMVAL_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSiblingKey, nodeKey);

    offsets[NodeFieldLayout.NUMVAL_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSiblingKey, nodeKey);

    offsets[NodeFieldLayout.NUMVAL_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, previousRevision);

    offsets[NodeFieldLayout.NUMVAL_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModifiedRevision);

    offsets[NodeFieldLayout.NUMVAL_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    // Payload: serialize Number via type dispatch
    offsets[NodeFieldLayout.NUMVAL_PAYLOAD] = (int) (pos - dataStart);
    pos += serializeNumberToSegment(target, pos, value);

    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) offsets[i]);
    }

    return (int) (pos - offset);
  }

  /**
   * Serialize a Number value directly to a MemorySegment.
   * Format: [numberType:1][numberData:variable]
   */
  private static int serializeNumberToSegment(final MemorySegment target, final long offset,
      final Number number) {
    long pos = offset;
    switch (number) {
      case Integer intVal -> {
        target.set(ValueLayout.JAVA_BYTE, pos, (byte) 0);
        pos++;
        pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, intVal);
      }
      case Long longVal -> {
        target.set(ValueLayout.JAVA_BYTE, pos, (byte) 1);
        pos++;
        pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, longVal);
      }
      case Float floatVal -> {
        target.set(ValueLayout.JAVA_BYTE, pos, (byte) 2);
        pos++;
        target.set(ValueLayout.JAVA_INT_UNALIGNED, pos, Float.floatToRawIntBits(floatVal));
        pos += Float.BYTES;
      }
      case Double doubleVal -> {
        target.set(ValueLayout.JAVA_BYTE, pos, (byte) 3);
        pos++;
        target.set(ValueLayout.JAVA_LONG_UNALIGNED, pos, Double.doubleToRawLongBits(doubleVal));
        pos += Double.BYTES;
      }
      case BigDecimal bigDecimalVal -> {
        target.set(ValueLayout.JAVA_BYTE, pos, (byte) 4);
        pos++;
        final byte[] bytes = bigDecimalVal.unscaledValue().toByteArray();
        pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, bigDecimalVal.scale());
        pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, bytes.length);
        MemorySegment.copy(bytes, 0, target, ValueLayout.JAVA_BYTE, pos, bytes.length);
        pos += bytes.length;
      }
      case BigInteger bigIntegerVal -> {
        target.set(ValueLayout.JAVA_BYTE, pos, (byte) 5);
        pos++;
        final byte[] bytes = bigIntegerVal.toByteArray();
        pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, bytes.length);
        MemorySegment.copy(bytes, 0, target, ValueLayout.JAVA_BYTE, pos, bytes.length);
        pos += bytes.length;
      }
      default -> throw new IllegalStateException("Unexpected number type: " + number.getClass());
    }
    return (int) (pos - offset);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.NUMBER_VALUE;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.NUMVAL_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.NUMVAL_PARENT_KEY, parentKey);
      if (page != null) return;
    }
    this.parentKey = parentKey;
  }

  @Override
  public boolean hasParent() {
    return getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
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
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.NUMVAL_PREV_REVISION, revision)) return;
      unbind();
    }
    this.previousRevision = revision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.NUMVAL_LAST_MOD_REVISION, revision)) return;
      unbind();
    }
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (page != null) {
      return readLongField(NodeFieldLayout.NUMVAL_HASH);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return hash;
  }

  @Override
  public void setHash(final long hash) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.NUMVAL_HASH) & 0xFF;
      DeltaVarIntCodec.writeLongToSegment(page, dataRegionStart + fieldOff, hash);
      return;
    }
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

    final Number number = getValue();
    switch (number) {
      case Float floatVal -> bytes.writeFloat(floatVal);
      case Double doubleVal -> bytes.writeDouble(doubleVal);
      case BigDecimal bigDecimalVal -> bytes.writeBigDecimal(bigDecimalVal);
      case Integer intVal -> bytes.writeInt(intVal);
      case Long longVal -> bytes.writeLong(longVal);
      case BigInteger bigIntegerVal -> bytes.writeBigInteger(bigIntegerVal);
      default -> throw new IllegalStateException("Unexpected value: " + number);
    }

    return hashFunction.hashBytes(bytes.toByteArray());
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.NUMVAL_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long rightSibling) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.NUMVAL_RIGHT_SIB_KEY, rightSibling);
      if (page != null) return;
    }
    this.rightSiblingKey = rightSibling;
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.NUMVAL_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long leftSibling) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.NUMVAL_LEFT_SIB_KEY, leftSibling);
      if (page != null) return;
    }
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

  public Number getValue() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    } else if (!valueParsed) {
      parseValueField();
    }
    return value;
  }

  public void setValue(final Number value) {
    if (page != null) unbind();
    this.value = value;
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
    return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasRightSibling() {
    return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.NUMVAL_PREV_REVISION);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.NUMVAL_LAST_MOD_REVISION);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
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
   * LAZY OPTIMIZATION: Only parses structural fields immediately.
   * Two-stage lazy parsing: metadata (cheap) vs value (expensive Number allocation).
   */
  public void readFrom(final BytesIn<?> source, final long nodeKey, final byte[] deweyId,
                       final LongHashFunction hashFunction, final ResourceConfiguration config) {
    // Unbind flyweight — ensures getters use Java fields, not stale page reference
    this.page = null;
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;

    // STRUCTURAL FIELDS - parse immediately (NEW ORDER)
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

    // Store state for lazy parsing - DON'T parse remaining fields yet
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.metadataParsed = false;
    this.valueParsed = false;
    this.hasHash = config.hashType != HashType.NONE;
    this.valueOffset = 0;
    
    // Initialize lazy fields to defaults (will be populated on demand)
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
    this.value = null;
  }
  
  /**
   * Parse metadata fields on demand (cheap - just varints and optionally a long).
   * Called by getters that access prevRev, lastModRev, or hash.
   */
  private void parseMetadataFields() {
    if (metadataParsed) {
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
  
  /**
   * Parse value field on demand (expensive - may allocate BigDecimal/BigInteger).
   */
  private void parseValueField() {
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
    
    BytesIn<?> bytesIn = createBytesIn(valueOffset);
    this.value = NodeKind.deserializeNumber(bytesIn);
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
  public NumberNode toSnapshot() {
    if (page != null) {
      // Bound mode: read all fields from page
      if (!valueParsed) {
        readPayloadFromPage();
      }
      return new NumberNode(nodeKey,
          readDeltaField(NodeFieldLayout.NUMVAL_PARENT_KEY, nodeKey),
          readSignedField(NodeFieldLayout.NUMVAL_PREV_REVISION),
          readSignedField(NodeFieldLayout.NUMVAL_LAST_MOD_REVISION),
          readDeltaField(NodeFieldLayout.NUMVAL_RIGHT_SIB_KEY, nodeKey),
          readDeltaField(NodeFieldLayout.NUMVAL_LEFT_SIB_KEY, nodeKey),
          readLongField(NodeFieldLayout.NUMVAL_HASH),
          value, hashFunction,
          deweyIDBytes != null ? deweyIDBytes.clone() : null);
    }
    // Force parse all lazy fields for snapshot (must be complete and independent)
    if (!metadataParsed) {
      parseMetadataFields();
    }
    if (!valueParsed) {
      parseValueField();
    }
    return new NumberNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, hash, value, hashFunction,
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
    return visitor.visit(ImmutableNumberNode.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("number", value)
                      .add("parentKey", parentKey)
                      .add("previousRevision", previousRevision)
                      .add("lastModifiedRevision", lastModifiedRevision)
                      .add("rightSibling", rightSiblingKey)
                      .add("leftSibling", leftSiblingKey)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey, value);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final NumberNode other))
      return false;

    return nodeKey == other.nodeKey
        && parentKey == other.parentKey
        && Objects.equal(value, other.value);
  }
}
