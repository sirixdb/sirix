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
import io.sirix.node.immutable.xml.ImmutableElement;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Fixed;
import io.sirix.utils.NamePageHash;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Collections;
import java.util.List;

/**
 * Node representing an XML element.
 *
 * <p>
 * Uses primitive fields for efficient storage with delta+varint encoding.
 * Implements FlyweightNode for LeanStore-style zero-copy page-direct access.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class ElementNode implements StructNode, NameNode, ImmutableXmlNode, FlyweightNode {

  // === IMMEDIATE STRUCTURAL FIELDS ===
  private long nodeKey;
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private long firstChildKey;
  private long lastChildKey;

  // === NAME FIELDS ===
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

  // === NON-SERIALIZED FIELDS ===
  private LongHashFunction hashFunction;
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;
  private LongList attributeKeys;
  private LongList namespaceKeys;
  private QNm qNm;

  // Lazy parsing state
  private Object lazySource;
  private long lazyOffset;
  private boolean lazyFieldsParsed;
  private boolean hasHash;
  private boolean storeChildCount;

  // ==================== FLYWEIGHT BINDING ====================

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

  /** Whether the payload (attributeKeys, namespaceKeys) has been parsed from page memory. */
  private boolean payloadParsed;

  private static final int FIELD_COUNT = NodeFieldLayout.ELEMENT_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public ElementNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.attributeKeys = new LongArrayList();
    this.namespaceKeys = new LongArrayList();
    this.heapOffsets = new int[FIELD_COUNT];
  }

  /**
   * Primary constructor with all primitive fields. Used by deserialization
   * (NodeKind.ELEMENT.deserialize).
   */
  public ElementNode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision, long rightSiblingKey,
      long leftSiblingKey, long firstChildKey, long lastChildKey, long childCount, long descendantCount, long hash,
      long pathNodeKey, int prefixKey, int localNameKey, int uriKey, LongHashFunction hashFunction, byte[] deweyID,
      LongList attributeKeys, LongList namespaceKeys, QNm qNm) {
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
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.attributeKeys = attributeKeys != null
        ? attributeKeys
        : new LongArrayList();
    this.namespaceKeys = namespaceKeys != null
        ? namespaceKeys
        : new LongArrayList();
    this.qNm = qNm;
    this.lazyFieldsParsed = true;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public ElementNode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision, long rightSiblingKey,
      long leftSiblingKey, long firstChildKey, long lastChildKey, long childCount, long descendantCount, long hash,
      long pathNodeKey, int prefixKey, int localNameKey, int uriKey, LongHashFunction hashFunction,
      SirixDeweyID deweyID, LongList attributeKeys, LongList namespaceKeys, QNm qNm) {
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
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.attributeKeys = attributeKeys != null
        ? attributeKeys
        : new LongArrayList();
    this.namespaceKeys = namespaceKeys != null
        ? namespaceKeys
        : new LongArrayList();
    this.qNm = qNm;
    this.lazyFieldsParsed = true;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  // ==================== FLYWEIGHT BIND/UNBIND ====================

  /**
   * Bind this node as a flyweight to a page MemorySegment.
   * When bound, getters/setters read/write directly to page memory via the offset table.
   * Attribute and namespace keys are eagerly read from the payload region because element
   * nodes almost always need their attr/ns keys.
   *
   * @param page       the page MemorySegment
   * @param recordBase absolute byte offset of this record in the page
   * @param nodeKey    the node key (for delta decoding)
   * @param slotIndex  the slot index in the page directory
   */
  @Override
  public void bind(final MemorySegment page, final long recordBase, final long nodeKey,
      final int slotIndex) {
    this.page = page;
    this.recordBase = recordBase;
    this.nodeKey = nodeKey;
    this.slotIndex = slotIndex;
    this.dataRegionStart = recordBase + 1 + FIELD_COUNT;
    this.lazyFieldsParsed = true; // No lazy state when bound
    this.lazySource = null;
    this.payloadParsed = false;
    // Eagerly parse payload (attribute/namespace keys) since element nodes always need them
    ensurePayloadParsed();
  }

  /**
   * Unbind from page memory and materialize all fields into Java primitives.
   * After unbind, the node operates in primitive mode.
   */
  @Override
  public void unbind() {
    if (page == null) {
      return;
    }
    // Materialize all fields from page to Java primitives
    final long nk = this.nodeKey;
    this.parentKey = readDeltaField(NodeFieldLayout.ELEM_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.ELEM_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.ELEM_LEFT_SIB_KEY, nk);
    this.firstChildKey = readDeltaField(NodeFieldLayout.ELEM_FIRST_CHILD_KEY, nk);
    this.lastChildKey = readDeltaField(NodeFieldLayout.ELEM_LAST_CHILD_KEY, nk);
    this.pathNodeKey = readDeltaField(NodeFieldLayout.ELEM_PATH_NODE_KEY, nk);
    this.prefixKey = readSignedField(NodeFieldLayout.ELEM_PREFIX_KEY);
    this.localNameKey = readSignedField(NodeFieldLayout.ELEM_LOCAL_NAME_KEY);
    this.uriKey = readSignedField(NodeFieldLayout.ELEM_URI_KEY);
    this.previousRevision = readSignedField(NodeFieldLayout.ELEM_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.ELEM_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.ELEM_HASH);
    this.childCount = readSignedLongField(NodeFieldLayout.ELEM_CHILD_COUNT);
    this.descendantCount = readSignedLongField(NodeFieldLayout.ELEM_DESCENDANT_COUNT);
    // Parse payload (attributeKeys, namespaceKeys) if not already parsed
    ensurePayloadParsed();
    this.ownerPage = null;
    this.page = null;
  }

  @Override
  public void clearBinding() {
    this.ownerPage = null;
    this.page = null;
  }

  /** Check if this node is bound to a page MemorySegment. */
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
  public boolean isWriteSingleton() {
    return writeSingleton;
  }

  @Override
  public void setWriteSingleton(final boolean writeSingleton) {
    this.writeSingleton = writeSingleton;
  }

  @Override
  public KeyValueLeafPage getOwnerPage() {
    return ownerPage;
  }

  @Override
  public void setOwnerPage(final KeyValueLeafPage ownerPage) {
    this.ownerPage = ownerPage;
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

  private long readLongField(final int fieldIndex) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.readLongFromSegment(page, (int) (dataRegionStart + fieldOff));
  }

  // ==================== PAYLOAD PARSING ====================

  /**
   * Parse the attribute and namespace key lists from the page payload region if not yet parsed.
   * When bound, reads from page memory; when unbound, does nothing (already materialized).
   */
  private void ensurePayloadParsed() {
    if (payloadParsed || page == null) {
      return;
    }
    payloadParsed = true;

    final int payloadFieldOff = page.get(ValueLayout.JAVA_BYTE,
        recordBase + 1 + NodeFieldLayout.ELEM_PAYLOAD) & 0xFF;
    long pos = dataRegionStart + payloadFieldOff;

    // Read attribute count and keys
    final int attrCount = DeltaVarIntCodec.decodeSignedFromSegment(page, pos);
    pos += DeltaVarIntCodec.readSignedVarintWidth(page, pos);
    if (attributeKeys == null) {
      attributeKeys = new LongArrayList(attrCount);
    } else {
      attributeKeys.clear();
    }
    for (int i = 0; i < attrCount; i++) {
      final long attrKey = DeltaVarIntCodec.decodeDeltaFromSegment(page, pos, nodeKey);
      final int width = DeltaVarIntCodec.readDeltaEncodedWidth(page, pos);
      pos += width;
      attributeKeys.add(attrKey);
    }

    // Read namespace count and keys
    final int nsCount = DeltaVarIntCodec.decodeSignedFromSegment(page, pos);
    pos += DeltaVarIntCodec.readSignedVarintWidth(page, pos);
    if (namespaceKeys == null) {
      namespaceKeys = new LongArrayList(nsCount);
    } else {
      namespaceKeys.clear();
    }
    for (int i = 0; i < nsCount; i++) {
      final long nsKey = DeltaVarIntCodec.decodeDeltaFromSegment(page, pos, nodeKey);
      final int width = DeltaVarIntCodec.readDeltaEncodedWidth(page, pos);
      pos += width;
      namespaceKeys.add(nsKey);
    }
  }

  // ==================== SERIALIZE TO HEAP ====================

  /**
   * Serialize this node (from Java fields) into the new slotted page format with offset table.
   * Writes: [nodeKind:1][offsetTable:FIELD_COUNT][data fields].
   *
   * @param target the target MemorySegment
   * @param offset the absolute byte offset to write at
   * @return the total number of bytes written
   */
  @Override
  public int serializeToHeap(final MemorySegment target, final long offset) {
    // Ensure all lazy fields are materialized
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }

    long pos = offset;

    // Write nodeKind byte
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.ELEMENT.getId());
    pos++;

    // Reserve space for offset table (will be written after computing offsets)
    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    // Data region start
    final long dataStart = pos;
    final int[] offsets = this.heapOffsets;

    // Field 0: parentKey (delta-varint)
    offsets[NodeFieldLayout.ELEM_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    // Field 1: rightSiblingKey (delta-varint)
    offsets[NodeFieldLayout.ELEM_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSiblingKey, nodeKey);

    // Field 2: leftSiblingKey (delta-varint)
    offsets[NodeFieldLayout.ELEM_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSiblingKey, nodeKey);

    // Field 3: firstChildKey (delta-varint)
    offsets[NodeFieldLayout.ELEM_FIRST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, firstChildKey, nodeKey);

    // Field 4: lastChildKey (delta-varint)
    offsets[NodeFieldLayout.ELEM_LAST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, lastChildKey, nodeKey);

    // Field 5: pathNodeKey (delta-varint)
    offsets[NodeFieldLayout.ELEM_PATH_NODE_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, pathNodeKey, nodeKey);

    // Field 6: prefixKey (signed varint)
    offsets[NodeFieldLayout.ELEM_PREFIX_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prefixKey);

    // Field 7: localNameKey (signed varint)
    offsets[NodeFieldLayout.ELEM_LOCAL_NAME_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, localNameKey);

    // Field 8: uriKey (signed varint)
    offsets[NodeFieldLayout.ELEM_URI_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, uriKey);

    // Field 9: previousRevision (signed varint)
    offsets[NodeFieldLayout.ELEM_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, previousRevision);

    // Field 10: lastModifiedRevision (signed varint)
    offsets[NodeFieldLayout.ELEM_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModifiedRevision);

    // Field 11: hash (fixed 8 bytes)
    offsets[NodeFieldLayout.ELEM_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    // Field 12: childCount (signed long varint)
    offsets[NodeFieldLayout.ELEM_CHILD_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, childCount);

    // Field 13: descendantCount (signed long varint)
    offsets[NodeFieldLayout.ELEM_DESCENDANT_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, descendantCount);

    // Field 14: PAYLOAD [attrCount:signed varint][attrKeys:delta...][nsCount:signed varint][nsKeys:delta...]
    offsets[NodeFieldLayout.ELEM_PAYLOAD] = (int) (pos - dataStart);
    // Write attribute count and keys
    final int attrCount = attributeKeys != null ? attributeKeys.size() : 0;
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, attrCount);
    for (int i = 0; i < attrCount; i++) {
      pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, attributeKeys.getLong(i), nodeKey);
    }
    // Write namespace count and keys
    final int nsCount = namespaceKeys != null ? namespaceKeys.size() : 0;
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, nsCount);
    for (int i = 0; i < nsCount; i++) {
      pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, namespaceKeys.getLong(i), nodeKey);
    }

    // Write offset table
    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) offsets[i]);
    }

    return (int) (pos - offset);
  }

  @Override
  public int estimateSerializedSize() {
    return 128 + (attributeKeys != null ? attributeKeys.size() * 10 : 0)
        + (namespaceKeys != null ? namespaceKeys.size() * 10 : 0);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ELEMENT;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public void setNodeKey(long nodeKey) {
    this.nodeKey = nodeKey;
  }

  // === IMMEDIATE STRUCTURAL GETTERS (dual-mode: page or Java field) ===

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.ELEM_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_PARENT_KEY) & 0xFF;
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
        NodeFieldLayout.ELEM_PARENT_KEY, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, parentKey, nodeKey));
  }

  @Override
  public boolean hasParent() {
    return getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.ELEM_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long rightSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_RIGHT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(rightSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, rightSibling, nodeKey);
        return;
      }
      resizeRightSiblingKey(rightSibling);
      return;
    }
    this.rightSiblingKey = rightSibling;
  }

  private void resizeRightSiblingKey(final long rightSibling) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ELEM_RIGHT_SIB_KEY, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, rightSibling, nodeKey));
  }

  @Override
  public boolean hasRightSibling() {
    return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.ELEM_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long leftSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_LEFT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(leftSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, leftSibling, nodeKey);
        return;
      }
      resizeLeftSiblingKey(leftSibling);
      return;
    }
    this.leftSiblingKey = leftSibling;
  }

  private void resizeLeftSiblingKey(final long leftSibling) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ELEM_LEFT_SIB_KEY, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, leftSibling, nodeKey));
  }

  @Override
  public boolean hasLeftSibling() {
    return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getFirstChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.ELEM_FIRST_CHILD_KEY, nodeKey);
    }
    return firstChildKey;
  }

  public void setFirstChildKey(final long firstChild) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_FIRST_CHILD_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(firstChild, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, firstChild, nodeKey);
        return;
      }
      resizeFirstChildKey(firstChild);
      return;
    }
    this.firstChildKey = firstChild;
  }

  private void resizeFirstChildKey(final long firstChild) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ELEM_FIRST_CHILD_KEY, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, firstChild, nodeKey));
  }

  @Override
  public boolean hasFirstChild() {
    return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLastChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.ELEM_LAST_CHILD_KEY, nodeKey);
    }
    return lastChildKey;
  }

  public void setLastChildKey(final long lastChild) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_LAST_CHILD_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(lastChild, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, lastChild, nodeKey);
        return;
      }
      resizeLastChildKey(lastChild);
      return;
    }
    this.lastChildKey = lastChild;
  }

  private void resizeLastChildKey(final long lastChild) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ELEM_LAST_CHILD_KEY, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, lastChild, nodeKey));
  }

  @Override
  public boolean hasLastChild() {
    return getLastChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  // === METADATA/NAME GETTERS (dual-mode) ===

  @Override
  public long getPathNodeKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.ELEM_PATH_NODE_KEY, nodeKey);
    }
    return pathNodeKey;
  }

  @Override
  public void setPathNodeKey(@NonNegative long pathNodeKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_PATH_NODE_KEY) & 0xFF;
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
        NodeFieldLayout.ELEM_PATH_NODE_KEY, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, pathNodeKey, nodeKey));
  }

  @Override
  public int getPrefixKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ELEM_PREFIX_KEY);
    }
    return prefixKey;
  }

  @Override
  public void setPrefixKey(int prefixKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_PREFIX_KEY) & 0xFF;
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
        NodeFieldLayout.ELEM_PREFIX_KEY, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, prefixKey));
  }

  @Override
  public int getLocalNameKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ELEM_LOCAL_NAME_KEY);
    }
    return localNameKey;
  }

  @Override
  public void setLocalNameKey(int localNameKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_LOCAL_NAME_KEY) & 0xFF;
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
        NodeFieldLayout.ELEM_LOCAL_NAME_KEY, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, localNameKey));
  }

  @Override
  public int getURIKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ELEM_URI_KEY);
    }
    return uriKey;
  }

  @Override
  public void setURIKey(int uriKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_URI_KEY) & 0xFF;
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
        NodeFieldLayout.ELEM_URI_KEY, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, uriKey));
  }

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ELEM_PREV_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return previousRevision;
  }

  @Override
  public void setPreviousRevision(int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_PREV_REVISION) & 0xFF;
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
        NodeFieldLayout.ELEM_PREV_REVISION, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ELEM_LAST_MOD_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return lastModifiedRevision;
  }

  @Override
  public void setLastModifiedRevision(int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_LAST_MOD_REVISION) & 0xFF;
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
        NodeFieldLayout.ELEM_LAST_MOD_REVISION, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public long getChildCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.ELEM_CHILD_COUNT);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return childCount;
  }

  public void setChildCount(final long childCount) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_CHILD_COUNT) & 0xFF;
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
        NodeFieldLayout.ELEM_CHILD_COUNT, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedLongToSegment(target, off, childCount));
  }

  @Override
  public long getDescendantCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.ELEM_DESCENDANT_COUNT);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return descendantCount;
  }

  @Override
  public void setDescendantCount(long descendantCount) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ELEM_DESCENDANT_COUNT) & 0xFF;
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
        NodeFieldLayout.ELEM_DESCENDANT_COUNT, NodeFieldLayout.ELEMENT_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedLongToSegment(target, off, descendantCount));
  }

  @Override
  public long getHash() {
    if (page != null) {
      final long h = readLongField(NodeFieldLayout.ELEM_HASH);
      if (h != 0L) return h;
      if (hashFunction != null) {
        final long computed = computeHash(Bytes.threadLocalHashBuffer());
        setHash(computed);
        return computed;
      }
      return 0L;
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
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
          recordBase + 1 + NodeFieldLayout.ELEM_HASH) & 0xFF;
      DeltaVarIntCodec.writeLongToSegment(page, dataRegionStart + fieldOff, hash);
      return;
    }
    this.hash = hash;
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
  public void incrementDescendantCount() {
    setDescendantCount(getDescendantCount() + 1);
  }

  @Override
  public void decrementDescendantCount() {
    setDescendantCount(getDescendantCount() - 1);
  }

  // === NON-SERIALIZED FIELD ACCESSORS ===

  @Override
  public QNm getName() {
    return qNm;
  }

  public void setName(QNm name) {
    this.qNm = name;
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
    // Not stored for elements
  }

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

  // === ATTRIBUTE METHODS (dual-mode) ===

  public int getAttributeCount() {
    if (page != null) {
      ensurePayloadParsed();
    }
    return attributeKeys.size();
  }

  public long getAttributeKey(@NonNegative int index) {
    if (page != null) {
      ensurePayloadParsed();
    }
    if (attributeKeys.size() <= index) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return attributeKeys.getLong(index);
  }

  public void insertAttribute(@NonNegative long attrKey) {
    if (page != null) {
      ensurePayloadParsed();
      // Payload changed: must unbind and re-serialize
      unbind();
    }
    attributeKeys.add(attrKey);
  }

  public void removeAttribute(@NonNegative long attrNodeKey) {
    if (page != null) {
      ensurePayloadParsed();
      unbind();
    }
    attributeKeys.removeIf(key -> key == attrNodeKey);
  }

  public void clearAttributeKeys() {
    if (page != null) {
      ensurePayloadParsed();
      unbind();
    }
    attributeKeys.clear();
  }

  public List<Long> getAttributeKeys() {
    if (page != null) {
      ensurePayloadParsed();
    }
    return Collections.unmodifiableList(attributeKeys);
  }

  // === NAMESPACE METHODS (dual-mode) ===

  public int getNamespaceCount() {
    if (page != null) {
      ensurePayloadParsed();
    }
    return namespaceKeys.size();
  }

  public long getNamespaceKey(@NonNegative int namespaceKey) {
    if (page != null) {
      ensurePayloadParsed();
    }
    if (namespaceKeys.size() <= namespaceKey) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return namespaceKeys.getLong(namespaceKey);
  }

  public void insertNamespace(long namespaceKey) {
    if (page != null) {
      ensurePayloadParsed();
      unbind();
    }
    namespaceKeys.add(namespaceKey);
  }

  public void removeNamespace(long namespaceKey) {
    if (page != null) {
      ensurePayloadParsed();
      unbind();
    }
    namespaceKeys.removeIf(key -> key == namespaceKey);
  }

  public void clearNamespaceKeys() {
    if (page != null) {
      ensurePayloadParsed();
      unbind();
    }
    namespaceKeys.clear();
  }

  public List<Long> getNamespaceKeys() {
    if (page != null) {
      ensurePayloadParsed();
    }
    return Collections.unmodifiableList(namespaceKeys);
  }

  // === HASH COMPUTATION ===

  @Override
  public long computeHash(BytesOut<?> bytes) {
    if (hashFunction == null) {
      return 0L;
    }

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

    return bytes.hashDirect(hashFunction);
  }

  /**
   * Populate this node from a BytesIn source for singleton reuse.
   * LAZY OPTIMIZATION: Only parses structural fields immediately (NEW ORDER).
   */
  public void readFrom(BytesIn<?> source, long nodeKey, byte[] deweyId, LongHashFunction hashFunction,
      ResourceConfiguration config) {
    // Unbind flyweight -- ensures getters use Java fields, not stale page reference
    this.page = null;
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    this.payloadParsed = false;
    if (this.attributeKeys == null) {
      this.attributeKeys = new LongArrayList();
    }
    if (this.namespaceKeys == null) {
      this.namespaceKeys = new LongArrayList();
    }
    this.attributeKeys.clear();
    this.namespaceKeys.clear();

    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.lastChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.prefixKey = DeltaVarIntCodec.decodeSigned(source);
    this.localNameKey = DeltaVarIntCodec.decodeSigned(source);
    this.uriKey = DeltaVarIntCodec.decodeSigned(source);

    // Store for lazy parsing of remaining fields
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.lazyFieldsParsed = false;
    this.hasHash = config.hashType != HashType.NONE;
    this.storeChildCount = config.storeChildCount();

    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.childCount = 0;
    this.hash = 0;
    this.descendantCount = 0;
  }

  private void parseLazyFields() {
    if (lazyFieldsParsed) {
      return;
    }

    if (lazySource == null) {
      lazyFieldsParsed = true;
      return;
    }

    BytesIn<?> bytesIn;
    if (lazySource instanceof MemorySegment segment) {
      bytesIn = new MemorySegmentBytesIn(segment);
      bytesIn.position(lazyOffset);
    } else if (lazySource instanceof byte[] bytes) {
      bytesIn = new ByteArrayBytesIn(bytes);
      bytesIn.position(lazyOffset);
    } else {
      throw new IllegalStateException("Unknown lazy source type: " + lazySource.getClass());
    }

    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.childCount = storeChildCount
        ? DeltaVarIntCodec.decodeSigned(bytesIn)
        : 0L;
    if (hasHash) {
      this.hash = bytesIn.readLong();
      this.descendantCount = DeltaVarIntCodec.decodeSigned(bytesIn);
    } else {
      this.hash = 0L;
      this.descendantCount = 0L;
    }

    final int attributeCount = DeltaVarIntCodec.decodeSigned(bytesIn);
    for (int i = 0; i < attributeCount; i++) {
      this.attributeKeys.add(DeltaVarIntCodec.decodeDelta(bytesIn, nodeKey));
    }

    final int namespaceCount = DeltaVarIntCodec.decodeSigned(bytesIn);
    for (int i = 0; i < namespaceCount; i++) {
      this.namespaceKeys.add(DeltaVarIntCodec.decodeDelta(bytesIn, nodeKey));
    }

    this.lazyFieldsParsed = true;
  }

  /**
   * Create a deep copy snapshot of this node.
   *
   * @return a new instance with copied values
   */
  public ElementNode toSnapshot() {
    if (page != null) {
      final long nk = this.nodeKey;
      ensurePayloadParsed();
      final ElementNode snapshot = new ElementNode(
          nk,
          readDeltaField(NodeFieldLayout.ELEM_PARENT_KEY, nk),
          readSignedField(NodeFieldLayout.ELEM_PREV_REVISION),
          readSignedField(NodeFieldLayout.ELEM_LAST_MOD_REVISION),
          readDeltaField(NodeFieldLayout.ELEM_RIGHT_SIB_KEY, nk),
          readDeltaField(NodeFieldLayout.ELEM_LEFT_SIB_KEY, nk),
          readDeltaField(NodeFieldLayout.ELEM_FIRST_CHILD_KEY, nk),
          readDeltaField(NodeFieldLayout.ELEM_LAST_CHILD_KEY, nk),
          readSignedLongField(NodeFieldLayout.ELEM_CHILD_COUNT),
          readSignedLongField(NodeFieldLayout.ELEM_DESCENDANT_COUNT),
          readLongField(NodeFieldLayout.ELEM_HASH),
          readDeltaField(NodeFieldLayout.ELEM_PATH_NODE_KEY, nk),
          readSignedField(NodeFieldLayout.ELEM_PREFIX_KEY),
          readSignedField(NodeFieldLayout.ELEM_LOCAL_NAME_KEY),
          readSignedField(NodeFieldLayout.ELEM_URI_KEY),
          hashFunction,
          getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null,
          new LongArrayList(attributeKeys),
          new LongArrayList(namespaceKeys),
          qNm);
      if (sirixDeweyID != null) {
        snapshot.sirixDeweyID = sirixDeweyID;
      }
      return snapshot;
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    final ElementNode snapshot = new ElementNode(
        nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, firstChildKey, lastChildKey,
        childCount, descendantCount, hash, pathNodeKey,
        prefixKey, localNameKey, uriKey, hashFunction,
        getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null,
        new LongArrayList(attributeKeys),
        new LongArrayList(namespaceKeys),
        qNm);
    if (sirixDeweyID != null) {
      snapshot.sirixDeweyID = sirixDeweyID;
    }
    return snapshot;
  }

  @Override
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableElement.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("qName", qNm)
                      .add("parentKey", parentKey)
                      .add("rightSibling", rightSiblingKey)
                      .add("leftSibling", leftSiblingKey)
                      .add("firstChild", firstChildKey)
                      .add("lastChild", lastChildKey)
                      .add("attributeKeys", attributeKeys)
                      .add("namespaceKeys", namespaceKeys)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey, getPrefixKey(), getLocalNameKey(), getURIKey());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ElementNode other))
      return false;

    return nodeKey == other.nodeKey && parentKey == other.parentKey && getPrefixKey() == other.getPrefixKey()
        && getLocalNameKey() == other.getLocalNameKey() && getURIKey() == other.getURIKey();
  }
}
