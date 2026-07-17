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

import java.math.BigDecimal;
import java.math.BigInteger;

import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.MemorySegmentBytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.NumericValueNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

/**
 * Fused JSON node representing an object key bound to a NUMBER value in a single slot.
 *
 * <p>Replaces the legacy pair {@code OBJECT_KEY + OBJECT_NUMBER_VALUE} for the common
 * {@code {"fieldname": 42}} pattern, eliminating one record per such field.
 *
 * <h2>Wire layout</h2>
 * <pre>
 * [kindByte=45][offsetTable: FIELD_COUNT × 1 byte][data region]
 *
 * Field order (offset table indices):
 *   0 parentKey (delta-varint, base=nodeKey)
 *   1 rightSiblingKey (delta-varint)
 *   2 leftSiblingKey (delta-varint)
 *   3 nameKey (signed varint)
 *   4 pathNodeKey (delta-varint)
 *   5 previousRevision (signed varint)
 *   6 lastModifiedRevision (signed varint)
 *   7 hash (fixed 8 bytes)
 *   8 payload [numberType:1][numberData:variable]
 * </pre>
 *
 * <p>The number payload reuses {@link NodeKind#serializeNumber} /
 * {@link NodeKind#deserializeNumber} so it is identical to the legacy
 * {@code OBJECT_NUMBER_VALUE} body format.
 *
 * <p>HFT contract: primitive fields only, {@code final} where possible, zero-alloc
 * bind/unbind, offset-table lookups in O(1).
 */
public final class ObjectNamedNumberNode
    implements StructNode, NameNode, ImmutableJsonNode, NumericValueNode, FlyweightNode {

  private long nodeKey;
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private int nameKey;
  private long pathNodeKey;
  private int previousRevision;
  private int lastModifiedRevision;
  private long hash;
  private Number value;
  private LongHashFunction hashFunction;

  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Cache for name (not serialized)
  private QNm cachedName;

  // Lazy parsing state
  private Object lazySource;
  private long lazyOffset;
  private boolean metadataParsed;
  private boolean valueParsed;
  private long valueOffset;
  private boolean hasHash;

  // ==================== FLYWEIGHT BINDING ====================

  private MemorySegment page;
  private long recordBase;
  private long dataRegionStart;
  private int slotIndex;
  private boolean writeSingleton;
  private KeyValueLeafPage ownerPage;
  private int[] heapOffsets;

  private static final int FIELD_COUNT = NodeFieldLayout.OBJECT_NAMED_NUMBER_FIELD_COUNT;

  /** Thread-local scratch buffer for serializing number payloads. */
  private static final ThreadLocal<MemorySegmentBytesOut> TL_NUMBER_BUFFER =
      ThreadLocal.withInitial(() -> new MemorySegmentBytesOut(64));

  public ObjectNamedNumberNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
  }

  public ObjectNamedNumberNode(long nodeKey, long parentKey, long rightSiblingKey, long leftSiblingKey,
      int nameKey, long pathNodeKey, int previousRevision, int lastModifiedRevision, long hash, Number value,
      LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.nameKey = nameKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  public ObjectNamedNumberNode(long nodeKey, long parentKey, long rightSiblingKey, long leftSiblingKey,
      int nameKey, long pathNodeKey, int previousRevision, int lastModifiedRevision, long hash, Number value,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.nameKey = nameKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  // ==================== FLYWEIGHT BIND/UNBIND ====================

  public void bind(final MemorySegment page, final long recordBase, final long nodeKey, final int slotIndex) {
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
    if (page == null) {
      return;
    }
    final long nk = this.nodeKey;
    this.parentKey = readDeltaField(NodeFieldLayout.OBJNAMEDNUM_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.OBJNAMEDNUM_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.OBJNAMEDNUM_LEFT_SIB_KEY, nk);
    this.nameKey = readSignedField(NodeFieldLayout.OBJNAMEDNUM_NAME_KEY);
    this.pathNodeKey = readDeltaField(NodeFieldLayout.OBJNAMEDNUM_PATH_NODE_KEY, nk);
    this.previousRevision = readSignedField(NodeFieldLayout.OBJNAMEDNUM_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.OBJNAMEDNUM_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.OBJNAMEDNUM_HASH);
    if (!valueParsed) {
      readPayloadFromPage();
    }
    this.page = null;
    this.ownerPage = null;
  }

  @Override
  public void clearBinding() {
    this.page = null;
    this.ownerPage = null;
  }

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
    // 1 (nodeKind) + 9 (offset table) + ~35 (varint fields avg) + 8 (hash) + ~10 (number payload) = ~63
    return 80;
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

  /**
   * Read the Number payload from page memory when bound.
   */
  private void readPayloadFromPage() {
    final int payloadFieldOff = page.get(ValueLayout.JAVA_BYTE,
        recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PAYLOAD) & 0xFF;
    final long payloadStart = dataRegionStart + payloadFieldOff;
    final MemorySegmentBytesIn bytesIn = new MemorySegmentBytesIn(page);
    bytesIn.position(payloadStart);
    this.value = NodeKind.deserializeNumber(bytesIn);
    this.valueParsed = true;
  }

  // ==================== OWNER PAGE ====================

  @Override
  public KeyValueLeafPage getOwnerPage() {
    return ownerPage;
  }

  @Override
  public void setOwnerPage(final KeyValueLeafPage ownerPage) {
    this.ownerPage = ownerPage;
  }

  // ==================== SERIALIZE TO HEAP ====================

  public static int writeNewRecord(final MemorySegment target, final long offset,
      final int[] heapOffsets, final long nodeKey,
      final long parentKey, final long rightSibKey, final long leftSibKey,
      final int nameKey, final long pathNodeKey,
      final int prevRev, final int lastModRev, final long hash, final Number value) {
    long pos = offset;

    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.OBJECT_NAMED_NUMBER.getId());
    pos++;

    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    final long dataStart = pos;

    heapOffsets[NodeFieldLayout.OBJNAMEDNUM_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDNUM_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSibKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDNUM_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSibKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDNUM_NAME_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, nameKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDNUM_PATH_NODE_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, pathNodeKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDNUM_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prevRev);

    heapOffsets[NodeFieldLayout.OBJNAMEDNUM_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModRev);

    heapOffsets[NodeFieldLayout.OBJNAMEDNUM_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    heapOffsets[NodeFieldLayout.OBJNAMEDNUM_PAYLOAD] = (int) (pos - dataStart);
    final MemorySegmentBytesOut numBuf = TL_NUMBER_BUFFER.get();
    numBuf.clear();
    NodeKind.serializeNumber(value, numBuf);
    final int numBytes = (int) numBuf.position();
    final MemorySegment numSegment = numBuf.getDestination();
    MemorySegment.copy(numSegment, 0, target, pos, numBytes);
    pos += numBytes;

    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) heapOffsets[i]);
    }

    return (int) (pos - offset);
  }

  public int serializeToHeap(final MemorySegment target, final long offset) {
    if (!metadataParsed) {
      parseMetadataFields();
    }
    if (!valueParsed) {
      parseValueField();
    }
    return writeNewRecord(target, offset, getHeapOffsets(), nodeKey,
        parentKey, rightSiblingKey, leftSiblingKey,
        nameKey, pathNodeKey,
        previousRevision, lastModifiedRevision, hash, value);
  }

  public int[] getHeapOffsets() {
    int[] offsets = heapOffsets;
    if (offsets == null) {
      offsets = new int[FIELD_COUNT];
      heapOffsets = offsets;
    }
    return offsets;
  }

  public void setDeweyIDAfterCreation(final SirixDeweyID id, final byte[] bytes) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = bytes;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_NAMED_NUMBER;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDNUM_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PARENT_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(parentKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, parentKey, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDNUM_PARENT_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, parentKey, nodeKey));
      return;
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
  public void setTypeKey(final int typeKey) {}

  @Override
  public void setDeweyID(final SirixDeweyID id) {
    final var owner = this.ownerPage;
    if (owner != null) {
      final long nk = this.nodeKey;
      final int slot = this.slotIndex;
      unbind();
      this.sirixDeweyID = id;
      this.deweyIDBytes = null;
      owner.resizeRecord(this, nk, slot);
      return;
    }
    this.sirixDeweyID = id;
    this.deweyIDBytes = null;
  }

  @Override
  public void setDeweyIDBytes(final byte[] bytes) {
    this.deweyIDBytes = bytes;
    this.sirixDeweyID = null;
  }

  @Override
  public void setPreviousRevision(final int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PREV_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDNUM_PREV_REVISION, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
      return;
    }
    this.previousRevision = revision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_LAST_MOD_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDNUM_LAST_MOD_REVISION, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
      return;
    }
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (page != null) {
      return readLongField(NodeFieldLayout.OBJNAMEDNUM_HASH);
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
          recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_HASH) & 0xFF;
      DeltaVarIntCodec.writeLongToSegment(page, dataRegionStart + fieldOff, hash);
      return;
    }
    this.hash = hash;
  }

  @Override
  public long computeHash(BytesOut<?> bytes) {
    bytes.clear();
    bytes.writeLong(getNodeKey())
         .writeLong(getParentKey())
         .writeByte(getKind().getId())
         .writeLong(getLeftSiblingKey())
         .writeLong(getRightSiblingKey())
         .writeInt(getNameKey());
    // The inline number payload MUST participate in the hash — otherwise hash-based diff
    // cannot distinguish a value change (e.g. 10 → 99) on the same fused record, leading
    // to an empty diff. Sibling node kinds (String/Boolean) already include the payload.
    //
    // Hash the value TYPE-FAITHFULLY (mirroring NumberNode.computeHash). Collapsing to
    // doubleValue() normalized away the type/precision, so the optimized HASHED diff missed
    // real changes such as 10 -> 10.0, or 2^53 -> 2^53+1 (two distinct longs, identical double).
    final Number v = getValue();
    switch (v) {
      case null -> bytes.writeDouble(Double.NaN);
      case Float floatVal -> bytes.writeFloat(floatVal);
      case Double doubleVal -> bytes.writeDouble(doubleVal);
      case BigDecimal bigDecimalVal -> bytes.writeBigDecimal(bigDecimalVal);
      case Integer intVal -> bytes.writeInt(intVal);
      case Long longVal -> bytes.writeLong(longVal);
      case BigInteger bigIntegerVal -> bytes.writeBigInteger(bigIntegerVal);
      default -> throw new IllegalStateException("Unexpected value: " + v);
    }
    return bytes.hashDirect(hashFunction);
  }

  public int getNameKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDNUM_NAME_KEY);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return nameKey;
  }

  public void setNameKey(final int nameKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_NAME_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(nameKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, nameKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDNUM_NAME_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, nameKey));
      return;
    }
    this.nameKey = nameKey;
  }

  public QNm getName() {
    return cachedName;
  }

  public void setName(final QNm name) {
    this.cachedName = name;
  }

  public int getLocalNameKey() {
    return getNameKey();
  }

  public int getPrefixKey() {
    return -1;
  }

  public void setPrefixKey(final int prefixKey) {}

  public int getURIKey() {
    return -1;
  }

  public void setURIKey(final int uriKey) {}

  public void setLocalNameKey(final int localNameKey) {
    setNameKey(localNameKey);
  }

  public long getPathNodeKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDNUM_PATH_NODE_KEY, nodeKey);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return pathNodeKey;
  }

  public void setPathNodeKey(final long pathNodeKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_PATH_NODE_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(pathNodeKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, pathNodeKey, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDNUM_PATH_NODE_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, pathNodeKey, nodeKey));
      return;
    }
    this.pathNodeKey = pathNodeKey;
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDNUM_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long rightSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_RIGHT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(rightSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, rightSibling, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDNUM_RIGHT_SIB_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, rightSibling, nodeKey));
      return;
    }
    this.rightSiblingKey = rightSibling;
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDNUM_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long leftSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDNUM_LEFT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(leftSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, leftSibling, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDNUM_LEFT_SIB_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, leftSibling, nodeKey));
      return;
    }
    this.leftSiblingKey = leftSibling;
  }

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  public void setFirstChildKey(final long firstChild) {}

  @Override
  public long getLastChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  public void setLastChildKey(final long lastChild) {}

  @Override
  public long getChildCount() {
    return 0;
  }

  public void setChildCount(final long childCount) {}

  @Override
  public void incrementChildCount() {}

  @Override
  public void decrementChildCount() {}

  @Override
  public long getDescendantCount() {
    return 0;
  }

  @Override
  public void setDescendantCount(final long descendantCount) {}

  @Override
  public void decrementDescendantCount() {}

  @Override
  public void incrementDescendantCount() {}

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDNUM_PREV_REVISION);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDNUM_LAST_MOD_REVISION);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return lastModifiedRevision;
  }

  public Number getValue() {
    if (page != null) {
      if (!valueParsed) {
        readPayloadFromPage();
      }
      return value;
    }
    if (!valueParsed) {
      parseValueField();
    }
    return value;
  }

  public void setValue(final Number value) {
    final var owner = this.ownerPage;
    if (owner != null) {
      final long nk = this.nodeKey;
      final int slot = this.slotIndex;
      unbind();
      this.value = value;
      this.valueParsed = true;
      owner.resizeRecord(this, nk, slot);
      return;
    }
    if (page != null) unbind();
    this.value = value;
    this.valueParsed = true;
  }

  public int getTypeKey() {
    return -1;
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
  public boolean hasLeftSibling() {
    return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasRightSibling() {
    return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean isWriteSingleton() {
    return writeSingleton;
  }

  @Override
  public void setWriteSingleton(final boolean writeSingleton) {
    this.writeSingleton = writeSingleton;
  }

  @Override
  public @Nullable SirixDeweyID getDeweyID() {
    if (sirixDeweyID == null && deweyIDBytes != null) {
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
    return visitor.visit(this);
  }

  public LongHashFunction getHashFunction() {
    return hashFunction;
  }

  @Override
  public void setNodeKey(final long nodeKey) {
    this.nodeKey = nodeKey;
  }

  public void readFrom(final BytesIn<?> source, final long nodeKey, final byte[] deweyId,
                       final LongHashFunction hashFunction, final ResourceConfiguration config) {
    this.page = null;
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    this.cachedName = null;

    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.metadataParsed = false;
    this.valueParsed = false;
    this.valueOffset = 0;
    this.hasHash = config.hashType != HashType.NONE;

    this.nameKey = 0;
    this.pathNodeKey = 0;
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
    this.value = null;
  }

  private void parseMetadataFields() {
    if (metadataParsed) {
      return;
    }
    if (lazySource == null) {
      metadataParsed = true;
      return;
    }

    BytesIn<?> bytesIn = createBytesIn(lazyOffset);
    this.nameKey = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(bytesIn, nodeKey);
    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    if (hasHash) {
      this.hash = bytesIn.readLong();
    }
    this.valueOffset = bytesIn.position();
    this.metadataParsed = true;
  }

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

  private BytesIn<?> createBytesIn(final long offset) {
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

  public ObjectNamedNumberNode toSnapshot() {
    if (page != null) {
      if (!valueParsed) {
        readPayloadFromPage();
      }
      return new ObjectNamedNumberNode(nodeKey,
          getParentKey(), getRightSiblingKey(), getLeftSiblingKey(),
          getNameKey(), getPathNodeKey(),
          getPreviousRevisionNumber(), getLastModifiedRevisionNumber(), getHash(), value,
          hashFunction,
          getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    if (!valueParsed) {
      parseValueField();
    }
    return new ObjectNamedNumberNode(nodeKey, parentKey, rightSiblingKey, leftSiblingKey,
        nameKey, pathNodeKey, previousRevision, lastModifiedRevision, hash, value, hashFunction,
        getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
  }

  @Override
  public String toString() {
    return "ObjectNamedNumberNode{" +
        "nodeKey=" + nodeKey +
        ", parentKey=" + parentKey +
        ", nameKey=" + nameKey +
        ", value=" + value +
        '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeKey, parentKey, nameKey, value);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ObjectNamedNumberNode other)) {
      return false;
    }
    return nodeKey == other.nodeKey
        && parentKey == other.parentKey
        && nameKey == other.nameKey
        && Objects.equals(value, other.value);
  }
}
