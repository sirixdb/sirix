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
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
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
import io.sirix.node.immutable.xml.ImmutablePI;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.Compression;
import io.sirix.utils.NamePageHash;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * Processing Instruction node using primitive fields.
 *
 * <p>Supports LeanStore-style flyweight binding for zero-copy reads from a slotted page
 * MemorySegment. When bound, all getters/setters operate directly on page memory via
 * the per-record offset table. When unbound, they operate on Java primitive fields.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class PINode implements StructNode, NameNode, ValueNode, ImmutableXmlNode, FlyweightNode {

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
  private Object lazyValueSource;
  private long lazyValueOffset;
  private int lazyValueLength;
  private boolean lazyValueCompressed;
  private boolean valueParsed = true;

  // === NON-SERIALIZED FIELDS ===
  private LongHashFunction hashFunction;
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;
  private QNm qNm;

  // ==================== FLYWEIGHT BINDING (LeanStore page-direct access) ====================

  /** Page MemorySegment when bound (null = primitive mode). */
  private MemorySegment page;

  /** Absolute byte offset of this record in the page (after HEAP_START + heapOffset). */
  private long recordBase;

  /** Absolute byte offset where the data region starts (recordBase + 1 + FIELD_COUNT). */
  private long dataRegionStart;

  /** Slot index in the page directory (for re-serialization). */
  private int slotIndex;

  /** True if this node is a factory-managed write singleton (must not be stored in records[]). */
  private boolean writeSingleton;

  /** Owning page for resize-in-place on varint width changes. */
  private KeyValueLeafPage ownerPage;

  /** Pre-allocated offset array reused across serializations (zero-alloc hot path). */
  private final int[] heapOffsets;

  private static final int FIELD_COUNT = NodeFieldLayout.PI_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public PINode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  /**
   * Primary constructor with all primitive fields.
   */
  public PINode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision, long rightSiblingKey,
      long leftSiblingKey, long firstChildKey, long lastChildKey, long childCount, long descendantCount, long hash,
      long pathNodeKey, int prefixKey, int localNameKey, int uriKey, byte[] value, boolean isCompressed,
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
    this.heapOffsets = new int[FIELD_COUNT];
  }

  /**
   * Constructor with SirixDeweyID.
   */
  public PINode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision, long rightSiblingKey,
      long leftSiblingKey, long firstChildKey, long lastChildKey, long childCount, long descendantCount, long hash,
      long pathNodeKey, int prefixKey, int localNameKey, int uriKey, byte[] value, boolean isCompressed,
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
    this.heapOffsets = new int[FIELD_COUNT];
  }

  // ==================== FLYWEIGHT BIND/UNBIND ====================

  /**
   * Bind this node as a flyweight to a page MemorySegment.
   * When bound, getters/setters read/write directly to page memory via the offset table.
   *
   * @param page       the page MemorySegment
   * @param recordBase absolute byte offset of this record in the page
   * @param nodeKey    the node key (for delta decoding)
   * @param slotIndex  the slot index in the page directory
   */
  public void bind(final MemorySegment page, final long recordBase, final long nodeKey,
      final int slotIndex) {
    this.page = page;
    this.recordBase = recordBase;
    this.nodeKey = nodeKey;
    this.slotIndex = slotIndex;
    this.dataRegionStart = recordBase + 1 + FIELD_COUNT;
    this.valueParsed = false; // Payload still needs lazy parsing from page
    this.lazyValueSource = null;
    this.hash = 0;
  }

  /**
   * Unbind from page memory and materialize all fields into Java primitives.
   * After unbind, the node operates in primitive mode.
   */
  public void unbind() {
    if (page == null) {
      return;
    }
    // Materialize all fields from page to Java primitives
    final long nk = this.nodeKey;
    this.parentKey = readDeltaField(NodeFieldLayout.PI_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.PI_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.PI_LEFT_SIB_KEY, nk);
    this.firstChildKey = readDeltaField(NodeFieldLayout.PI_FIRST_CHILD_KEY, nk);
    this.lastChildKey = readDeltaField(NodeFieldLayout.PI_LAST_CHILD_KEY, nk);
    this.pathNodeKey = readDeltaField(NodeFieldLayout.PI_PATH_NODE_KEY, nk);
    this.prefixKey = readSignedField(NodeFieldLayout.PI_PREFIX_KEY);
    this.localNameKey = readSignedField(NodeFieldLayout.PI_LOCAL_NAME_KEY);
    this.uriKey = readSignedField(NodeFieldLayout.PI_URI_KEY);
    this.previousRevision = readSignedField(NodeFieldLayout.PI_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.PI_LAST_MOD_REVISION);
    this.childCount = readSignedLongField(NodeFieldLayout.PI_CHILD_COUNT);
    this.descendantCount = readSignedLongField(NodeFieldLayout.PI_DESCENDANT_COUNT);
    // Payload needs to be read from page before unbinding
    if (!valueParsed) {
      readPayloadFromPage();
    }
    this.ownerPage = null;
    this.page = null;
  }

  @Override
  public void clearBinding() {
    this.page = null;
    this.ownerPage = null;
  }

  /** Check if this node is bound to a page MemorySegment. */
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
  public boolean isWriteSingleton() { return writeSingleton; }

  @Override
  public void setWriteSingleton(final boolean writeSingleton) { this.writeSingleton = writeSingleton; }

  @Override
  public KeyValueLeafPage getOwnerPage() { return ownerPage; }

  @Override
  public void setOwnerPage(final KeyValueLeafPage ownerPage) { this.ownerPage = ownerPage; }

  @Override
  public int estimateSerializedSize() {
    final int payloadLen = value != null ? value.length : 0;
    return 119 + payloadLen;
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

  private long readSignedLongField(final int fieldIndex) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.decodeSignedLongFromSegment(page, dataRegionStart + fieldOff);
  }

  /**
   * Read the payload (value bytes) directly from page memory when bound.
   */
  private void readPayloadFromPage() {
    final int payloadFieldOff = page.get(ValueLayout.JAVA_BYTE,
        recordBase + 1 + NodeFieldLayout.PI_PAYLOAD) & 0xFF;
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

  /**
   * Encode a PINode record directly to a MemorySegment from parameter values.
   * Static -- reads nothing from any instance. Zero field intermediation.
   *
   * @param target          the target MemorySegment (reinterpreted slotted page)
   * @param offset          absolute byte offset to write at
   * @param heapOffsets     pre-allocated offset array (reused, FIELD_COUNT elements)
   * @param nodeKey         the node key (delta base for structural keys)
   * @param parentKey       the parent node key
   * @param rightSibKey     the right sibling key
   * @param leftSibKey      the left sibling key
   * @param firstChildKey   the first child key
   * @param lastChildKey    the last child key
   * @param pathNodeKey     the path node key
   * @param prefixKey       the prefix key
   * @param localNameKey    the local name key
   * @param uriKey          the URI key
   * @param prevRev         the previous revision number
   * @param lastModRev      the last modified revision number
   * @param childCount      the child count
   * @param descendantCount the descendant count
   * @param rawValue        the raw value bytes (possibly compressed)
   * @param isCompressed    whether the value is compressed
   * @return the total number of bytes written
   */
  public static int writeNewRecord(final MemorySegment target, final long offset,
      final int[] heapOffsets, final long nodeKey,
      final long parentKey, final long rightSibKey, final long leftSibKey,
      final long firstChildKey, final long lastChildKey,
      final long pathNodeKey, final int prefixKey, final int localNameKey, final int uriKey,
      final int prevRev, final int lastModRev,
      final long childCount, final long descendantCount,
      final byte[] rawValue, final boolean isCompressed) {
    long pos = offset;

    // Write nodeKind byte
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.PROCESSING_INSTRUCTION.getId());
    pos++;

    // Reserve space for offset table
    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    // Data region start
    final long dataStart = pos;

    // Field 0: parentKey (delta-varint)
    heapOffsets[NodeFieldLayout.PI_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    // Field 1: rightSiblingKey (delta-varint)
    heapOffsets[NodeFieldLayout.PI_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSibKey, nodeKey);

    // Field 2: leftSiblingKey (delta-varint)
    heapOffsets[NodeFieldLayout.PI_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSibKey, nodeKey);

    // Field 3: firstChildKey (delta-varint)
    heapOffsets[NodeFieldLayout.PI_FIRST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, firstChildKey, nodeKey);

    // Field 4: lastChildKey (delta-varint)
    heapOffsets[NodeFieldLayout.PI_LAST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, lastChildKey, nodeKey);

    // Field 5: pathNodeKey (delta-varint)
    heapOffsets[NodeFieldLayout.PI_PATH_NODE_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, pathNodeKey, nodeKey);

    // Field 6: prefixKey (signed varint)
    heapOffsets[NodeFieldLayout.PI_PREFIX_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prefixKey);

    // Field 7: localNameKey (signed varint)
    heapOffsets[NodeFieldLayout.PI_LOCAL_NAME_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, localNameKey);

    // Field 8: uriKey (signed varint)
    heapOffsets[NodeFieldLayout.PI_URI_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, uriKey);

    // Field 9: previousRevision (signed varint)
    heapOffsets[NodeFieldLayout.PI_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prevRev);

    // Field 10: lastModifiedRevision (signed varint)
    heapOffsets[NodeFieldLayout.PI_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModRev);

    // Field 11: childCount (signed long varint)
    heapOffsets[NodeFieldLayout.PI_CHILD_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, childCount);

    // Field 12: descendantCount (signed long varint)
    heapOffsets[NodeFieldLayout.PI_DESCENDANT_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, descendantCount);

    // Field 13: payload [isCompressed:1][valueLength:varint][value:bytes]
    heapOffsets[NodeFieldLayout.PI_PAYLOAD] = (int) (pos - dataStart);
    target.set(ValueLayout.JAVA_BYTE, pos, isCompressed ? (byte) 1 : (byte) 0);
    pos++;
    final byte[] val = rawValue != null ? rawValue : new byte[0];
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, val.length);
    if (val.length > 0) {
      MemorySegment.copy(val, 0, target, ValueLayout.JAVA_BYTE, pos, val.length);
      pos += val.length;
    }

    // Write offset table
    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) heapOffsets[i]);
    }

    return (int) (pos - offset);
  }

  /**
   * Serialize this node from Java fields. Delegates to static writeNewRecord.
   */
  public int serializeToHeap(final MemorySegment target, final long offset) {
    if (!valueParsed) parseLazyValue();
    return writeNewRecord(target, offset, heapOffsets, nodeKey,
        parentKey, rightSiblingKey, leftSiblingKey,
        firstChildKey, lastChildKey, pathNodeKey,
        prefixKey, localNameKey, uriKey,
        previousRevision, lastModifiedRevision,
        childCount, descendantCount, value, isCompressed);
  }

  /**
   * Get the pre-allocated heap offsets array for use with static writeNewRecord.
   */
  public int[] getHeapOffsets() {
    return heapOffsets;
  }

  /**
   * Set DeweyID fields directly after creation, bypassing write-through.
   * The DeweyID is already in the page trailer -- this just sets the Java cache fields.
   */
  public void setDeweyIDAfterCreation(final SirixDeweyID id, final byte[] bytes) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = bytes;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PROCESSING_INSTRUCTION;
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
    if (page != null) {
      return readDeltaField(NodeFieldLayout.PI_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_PARENT_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(parentKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, parentKey, nodeKey);
        return;
      }
      resizeParentKey(parentKey);
      return;
    }
    this.parentKey = parentKey;
  }

  private void resizeParentKey(final long parentKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_PARENT_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, parentKey, nodeKey));
  }

  @Override
  public boolean hasParent() {
    return getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.PI_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long key) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_RIGHT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(key, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, key, nodeKey);
        return;
      }
      resizeRightSiblingKey(key);
      return;
    }
    this.rightSiblingKey = key;
  }

  private void resizeRightSiblingKey(final long key) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_RIGHT_SIB_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, key, nodeKey));
  }

  @Override
  public boolean hasRightSibling() {
    return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.PI_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long key) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_LEFT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(key, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, key, nodeKey);
        return;
      }
      resizeLeftSiblingKey(key);
      return;
    }
    this.leftSiblingKey = key;
  }

  private void resizeLeftSiblingKey(final long key) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_LEFT_SIB_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, key, nodeKey));
  }

  @Override
  public boolean hasLeftSibling() {
    return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getFirstChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.PI_FIRST_CHILD_KEY, nodeKey);
    }
    return firstChildKey;
  }

  public void setFirstChildKey(final long key) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_FIRST_CHILD_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(key, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, key, nodeKey);
        return;
      }
      resizeFirstChildKey(key);
      return;
    }
    this.firstChildKey = key;
  }

  private void resizeFirstChildKey(final long key) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_FIRST_CHILD_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, key, nodeKey));
  }

  @Override
  public boolean hasFirstChild() {
    return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLastChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.PI_LAST_CHILD_KEY, nodeKey);
    }
    return lastChildKey;
  }

  public void setLastChildKey(final long key) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_LAST_CHILD_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(key, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, key, nodeKey);
        return;
      }
      resizeLastChildKey(key);
      return;
    }
    this.lastChildKey = key;
  }

  private void resizeLastChildKey(final long key) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_LAST_CHILD_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, key, nodeKey));
  }

  @Override
  public boolean hasLastChild() {
    return getLastChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getChildCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.PI_CHILD_COUNT);
    }
    return childCount;
  }

  public void setChildCount(final long childCount) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_CHILD_COUNT) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedLongEncodedWidth(childCount);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedLongToSegment(page, absOff, childCount);
        return;
      }
      resizeChildCount(childCount);
      return;
    }
    this.childCount = childCount;
  }

  private void resizeChildCount(final long childCount) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_CHILD_COUNT, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedLongToSegment(target, off, childCount));
  }

  @Override
  public void incrementChildCount() {
    setChildCount(getChildCount() + 1);
  }

  @Override
  public void decrementChildCount() {
    setChildCount(getChildCount() - 1);
  }

  @Override
  public long getDescendantCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.PI_DESCENDANT_COUNT);
    }
    return descendantCount;
  }

  @Override
  public void setDescendantCount(final long descendantCount) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_DESCENDANT_COUNT) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedLongEncodedWidth(descendantCount);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedLongToSegment(page, absOff, descendantCount);
        return;
      }
      resizeDescendantCount(descendantCount);
      return;
    }
    this.descendantCount = descendantCount;
  }

  private void resizeDescendantCount(final long descendantCount) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_DESCENDANT_COUNT, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedLongToSegment(target, off, descendantCount));
  }

  @Override
  public void incrementDescendantCount() {
    setDescendantCount(getDescendantCount() + 1);
  }

  @Override
  public void decrementDescendantCount() {
    setDescendantCount(getDescendantCount() - 1);
  }

  @Override
  public long getPathNodeKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.PI_PATH_NODE_KEY, nodeKey);
    }
    return pathNodeKey;
  }

  @Override
  public void setPathNodeKey(@NonNegative long pathNodeKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_PATH_NODE_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(pathNodeKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, pathNodeKey, nodeKey);
        return;
      }
      resizePathNodeKey(pathNodeKey);
      return;
    }
    this.pathNodeKey = pathNodeKey;
  }

  private void resizePathNodeKey(final long pathNodeKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_PATH_NODE_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, pathNodeKey, nodeKey));
  }

  @Override
  public int getPrefixKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.PI_PREFIX_KEY);
    }
    return prefixKey;
  }

  @Override
  public void setPrefixKey(int prefixKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_PREFIX_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(prefixKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, prefixKey);
        return;
      }
      resizePrefixKey(prefixKey);
      return;
    }
    this.prefixKey = prefixKey;
  }

  private void resizePrefixKey(final int prefixKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_PREFIX_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, prefixKey));
  }

  @Override
  public int getLocalNameKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.PI_LOCAL_NAME_KEY);
    }
    return localNameKey;
  }

  @Override
  public void setLocalNameKey(int localNameKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_LOCAL_NAME_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(localNameKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, localNameKey);
        return;
      }
      resizeLocalNameKey(localNameKey);
      return;
    }
    this.localNameKey = localNameKey;
  }

  private void resizeLocalNameKey(final int localNameKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_LOCAL_NAME_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, localNameKey));
  }

  @Override
  public int getURIKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.PI_URI_KEY);
    }
    return uriKey;
  }

  @Override
  public void setURIKey(int uriKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_URI_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(uriKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, uriKey);
        return;
      }
      resizeURIKey(uriKey);
      return;
    }
    this.uriKey = uriKey;
  }

  private void resizeURIKey(final int uriKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_URI_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, uriKey));
  }

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.PI_PREV_REVISION);
    }
    return previousRevision;
  }

  @Override
  public void setPreviousRevision(int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_PREV_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      resizePreviousRevision(revision);
      return;
    }
    this.previousRevision = revision;
  }

  private void resizePreviousRevision(final int revision) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_PREV_REVISION, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.PI_LAST_MOD_REVISION);
    }
    return lastModifiedRevision;
  }

  @Override
  public void setLastModifiedRevision(int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.PI_LAST_MOD_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      resizeLastModifiedRevision(revision);
      return;
    }
    this.lastModifiedRevision = revision;
  }

  private void resizeLastModifiedRevision(final int revision) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.PI_LAST_MOD_REVISION, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public long getHash() {
    // Return stored hash if set by rollingAdd/rollingUpdate, else compute on demand
    if (hash != 0L) {
      return hash;
    }
    if (hashFunction != null) {
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
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    } else if (!valueParsed) {
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
    final var owner = this.ownerPage;
    if (owner != null) {
      final long nk = this.nodeKey;
      final int slot = this.slotIndex;
      unbind();
      this.value = value;
      this.valueParsed = true;
      this.lazyValueSource = null;
      this.lazyValueOffset = 0L;
      this.lazyValueLength = 0;
      this.lazyValueCompressed = false;
      this.isCompressed = false;
      this.hash = 0L;
      owner.resizeRecord(this, nk, slot);
      return;
    }
    if (page != null) unbind();
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

  @Override
  public String getValue() {
    final byte[] raw = getRawValue();
    return raw != null
        ? new String(raw, Constants.DEFAULT_ENCODING)
        : "";
  }

  @Override
  public QNm getName() {
    return qNm;
  }

  public void setName(QNm name) {
    this.qNm = name;
  }

  /**
   * Returns the raw value bytes without triggering decompression. Used by the fixed-slot projector to
   * preserve the original compressed bytes.
   */
  public byte[] getRawValueWithoutDecompression() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    } else if (!valueParsed) {
      parseLazyValue();
    }
    return value;
  }

  public boolean isCompressed() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    }
    return isCompressed;
  }

  public void setCompressed(boolean compressed) {
    this.isCompressed = compressed;
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
  public void setTypeKey(int typeKey) {}

  @Override
  public void setDeweyID(SirixDeweyID id) {
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
  public void readFrom(BytesIn<?> source, long nodeKey, byte[] deweyId, LongHashFunction hashFunction,
      ResourceConfiguration config) {
    // Unbind flyweight -- ensures getters use Java fields, not stale page reference
    this.page = null;
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;

    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.lastChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.prefixKey = DeltaVarIntCodec.decodeSigned(source);
    this.localNameKey = DeltaVarIntCodec.decodeSigned(source);
    this.uriKey = DeltaVarIntCodec.decodeSigned(source);
    this.previousRevision = DeltaVarIntCodec.decodeSigned(source);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);

    this.childCount = config.storeChildCount()
        ? DeltaVarIntCodec.decodeSigned(source)
        : 0L;
    if (config.hashType != HashType.NONE) {
      this.hash = source.readLong();
      this.descendantCount = DeltaVarIntCodec.decodeSigned(source);
    } else {
      this.hash = 0L;
      this.descendantCount = 0L;
    }

    final boolean compressed = source.readByte() == (byte) 1;
    final int valueLength = DeltaVarIntCodec.decodeSigned(source);
    final long valueOffset = source.position();
    setLazyRawValue(source.getSource(), valueOffset, valueLength, compressed);
    source.position(valueOffset + valueLength);
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

  @Override
  public long computeHash(BytesOut<?> bytes) {
    if (hashFunction == null)
      return 0L;
    bytes.clear();
    bytes.writeLong(getNodeKey()).writeLong(getParentKey()).writeByte(getKind().getId());
    bytes.writeLong(getChildCount())
         .writeLong(getDescendantCount())
         .writeLong(getLeftSiblingKey())
         .writeLong(getRightSiblingKey())
         .writeLong(getFirstChildKey());
    if (getLastChildKey() != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      bytes.writeLong(getLastChildKey());
    }
    bytes.writeInt(getPrefixKey()).writeInt(getLocalNameKey()).writeInt(getURIKey());
    final byte[] rawValue = getRawValue();
    if (rawValue != null) {
      bytes.write(rawValue);
    }
    return bytes.hashDirect(hashFunction);
  }

  /**
   * Create a deep copy snapshot of this node.
   *
   * @return a new instance with copied values
   */
  public PINode toSnapshot() {
    if (page != null) {
      final long nk = this.nodeKey;
      if (!valueParsed) {
        readPayloadFromPage();
      }
      final PINode snapshot = new PINode(
          nk,
          readDeltaField(NodeFieldLayout.PI_PARENT_KEY, nk),
          readSignedField(NodeFieldLayout.PI_PREV_REVISION),
          readSignedField(NodeFieldLayout.PI_LAST_MOD_REVISION),
          readDeltaField(NodeFieldLayout.PI_RIGHT_SIB_KEY, nk),
          readDeltaField(NodeFieldLayout.PI_LEFT_SIB_KEY, nk),
          readDeltaField(NodeFieldLayout.PI_FIRST_CHILD_KEY, nk),
          readDeltaField(NodeFieldLayout.PI_LAST_CHILD_KEY, nk),
          readSignedLongField(NodeFieldLayout.PI_CHILD_COUNT),
          readSignedLongField(NodeFieldLayout.PI_DESCENDANT_COUNT),
          hash,
          readDeltaField(NodeFieldLayout.PI_PATH_NODE_KEY, nk),
          readSignedField(NodeFieldLayout.PI_PREFIX_KEY),
          readSignedField(NodeFieldLayout.PI_LOCAL_NAME_KEY),
          readSignedField(NodeFieldLayout.PI_URI_KEY),
          value != null ? value.clone() : null,
          isCompressed,
          hashFunction,
          getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null,
          qNm);
      if (sirixDeweyID != null) {
        snapshot.sirixDeweyID = sirixDeweyID;
      }
      return snapshot;
    }
    if (!valueParsed) {
      parseLazyValue();
    }
    final PINode snapshot = new PINode(
        nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, firstChildKey, lastChildKey,
        childCount, descendantCount, hash, pathNodeKey,
        prefixKey, localNameKey, uriKey,
        value != null ? value.clone() : null,
        isCompressed, hashFunction,
        getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null,
        qNm);
    if (sirixDeweyID != null) {
      snapshot.sirixDeweyID = sirixDeweyID;
    }
    return snapshot;
  }

  @Override
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) {
    return visitor.visit(ImmutablePI.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey, prefixKey, localNameKey, uriKey);
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof PINode other))
      return false;
    return nodeKey == other.nodeKey && parentKey == other.parentKey && prefixKey == other.prefixKey
        && localNameKey == other.localNameKey && uriKey == other.uriKey;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("parentKey", parentKey)
                      .add("qNm", qNm)
                      .add("value", getValue())
                      .toString();
  }
}
