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

import io.sirix.node.AbstractFlyweightNode;
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
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
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
 * Fused JSON node representing an object key bound to a nested OBJECT value in a single
 * slot. <b>Phase 1 reservation</b>: the kindId 52 and class plumbing exist, but no
 * factory or shredder path emits this kind yet. The flyweight bind/unbind, getters, and
 * static {@code writeNewRecord} are wired up so subsequent phases can flip emission on
 * without revisiting this file.
 *
 * <p>Conceptually replaces the legacy two-record pair {@code OBJECT_KEY + OBJECT} for
 * fields shaped {@code {"name": { ... }}}, fusing the name lookup with the structural
 * node into a single slot.
 *
 * <h2>Wire layout</h2>
 * <pre>
 * [kindByte=52][offsetTable: 12 × 1 byte][data region]
 *
 * Field order (offset-table indices):
 *   0 parentKey         (delta-varint, base=nodeKey)
 *   1 rightSiblingKey   (delta-varint)
 *   2 leftSiblingKey    (delta-varint)
 *   3 firstChildKey     (delta-varint)
 *   4 lastChildKey      (delta-varint)
 *   5 nameKey           (signed varint)
 *   6 pathNodeKey       (delta-varint)
 *   7 previousRevision  (signed varint)
 *   8 lastModifiedRev   (signed varint)
 *   9 hash              (fixed 8 bytes)
 *  10 childCount        (signed long varint)
 *  11 descendantCount   (signed long varint)
 * </pre>
 *
 * <p>HFT contract: primitive fields only, {@code final} where possible, zero-alloc
 * bind/unbind, offset-table lookups in O(1), no autoboxing in any hot accessor.
 */
public final class ObjectNamedObjectNode extends AbstractFlyweightNode implements StructNode, NameNode, ImmutableJsonNode, FlyweightNode {

  private long nodeKey;
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private long firstChildKey;
  private long lastChildKey;
  private int nameKey;
  private long pathNodeKey;
  private int previousRevision;
  private int lastModifiedRevision;
  private long hash;
  private long childCount;
  private long descendantCount;
  private LongHashFunction hashFunction;

  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Cache for name (not serialized — populated via setName from QNm string)
  private QNm cachedName;

  // Lazy parsing state — defers metadata field parsing until first access.
  private Object lazySource;
  private long lazyOffset;
  private boolean lazyFieldsParsed;
  private boolean hasHash;

  // ==================== FLYWEIGHT BINDING ====================

  private MemorySegment page;
  private long recordBase;
  private long dataRegionStart;
  private int slotIndex;
  private boolean writeSingleton;
  private KeyValueLeafPage ownerPage;
  private static final int FIELD_COUNT = NodeFieldLayout.OBJECT_NAMED_OBJECT_FIELD_COUNT;

  /**
   * Constructor for flyweight binding (factory singleton). All fields except nodeKey and
   * hashFunction will be read from page memory after {@link #bind}.
   */
  public ObjectNamedObjectNode(final long nodeKey, final LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
  }

  /** Primary constructor with all primitive fields. */
  public ObjectNamedObjectNode(final long nodeKey, final long parentKey, final long rightSiblingKey,
      final long leftSiblingKey, final long firstChildKey, final long lastChildKey,
      final int nameKey, final long pathNodeKey, final int previousRevision,
      final int lastModifiedRevision, final long hash, final long childCount,
      final long descendantCount, final LongHashFunction hashFunction, final byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.nameKey = nameKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.lazyFieldsParsed = true;
  }

  /** Constructor with SirixDeweyID instead of byte array. */
  public ObjectNamedObjectNode(final long nodeKey, final long parentKey, final long rightSiblingKey,
      final long leftSiblingKey, final long firstChildKey, final long lastChildKey,
      final int nameKey, final long pathNodeKey, final int previousRevision,
      final int lastModifiedRevision, final long hash, final long childCount,
      final long descendantCount, final LongHashFunction hashFunction, final SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.nameKey = nameKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.lazyFieldsParsed = true;
  }

  // ==================== FLYWEIGHT BIND/UNBIND ====================

  public void bind(final MemorySegment page, final long recordBase, final long nodeKey, final int slotIndex) {
    this.page = page;
    this.recordBase = recordBase;
    this.nodeKey = nodeKey;
    this.slotIndex = slotIndex;
    this.dataRegionStart = recordBase + 1 + FIELD_COUNT;
    this.lazyFieldsParsed = true;
    this.lazySource = null;
  }

  public void unbind() {
    if (page == null) {
      return;
    }
    final long nk = this.nodeKey;
    this.parentKey = readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_LEFT_SIB_KEY, nk);
    this.firstChildKey = readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY, nk);
    this.lastChildKey = readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_LAST_CHILD_KEY, nk);
    this.nameKey = readSignedField(NodeFieldLayout.OBJNAMEDOBJ_NAME_KEY);
    this.pathNodeKey = readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_PATH_NODE_KEY, nk);
    this.previousRevision = readSignedField(NodeFieldLayout.OBJNAMEDOBJ_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.OBJNAMEDOBJ_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.OBJNAMEDOBJ_HASH);
    this.childCount = readSignedLongField(NodeFieldLayout.OBJNAMEDOBJ_CHILD_COUNT);
    this.descendantCount = readSignedLongField(NodeFieldLayout.OBJNAMEDOBJ_DESCENDANT_COUNT);
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
    // 1 (nodeKind) + 12 (offset table) + ~50 (varint fields avg) + 8 (hash) = ~71
    return 96;
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

  /**
   * Encode an OBJECT_NAMED_OBJECT record directly to a MemorySegment from parameter values.
   * Static — reads nothing from any instance. Zero field intermediation, zero allocation.
   *
   * @return the total number of bytes written
   */
  public static int writeNewRecord(final MemorySegment target, final long offset,
      final int[] heapOffsets, final long nodeKey,
      final long parentKey, final long rightSibKey, final long leftSibKey,
      final long firstChildKey, final long lastChildKey,
      final int nameKey, final long pathNodeKey,
      final int prevRev, final int lastModRev, final long hash,
      final long childCount, final long descendantCount) {
    long pos = offset;

    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.OBJECT_NAMED_OBJECT.getId());
    pos++;

    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    final long dataStart = pos;

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSibKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSibKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, firstChildKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_LAST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, lastChildKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_NAME_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, nameKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_PATH_NODE_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, pathNodeKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prevRev);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModRev);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_CHILD_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, childCount);

    heapOffsets[NodeFieldLayout.OBJNAMEDOBJ_DESCENDANT_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, descendantCount);

    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) heapOffsets[i]);
    }

    return (int) (pos - offset);
  }

  public int serializeToHeap(final MemorySegment target, final long offset) {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return writeNewRecord(target, offset, getHeapOffsets(), nodeKey,
        parentKey, rightSiblingKey, leftSiblingKey,
        firstChildKey, lastChildKey,
        nameKey, pathNodeKey,
        previousRevision, lastModifiedRevision, hash,
        childCount, descendantCount);
  }

  @Override
  protected int heapOffsetFieldCount() {
    return FIELD_COUNT;
  }

  public void setDeweyIDAfterCreation(final SirixDeweyID id, final byte[] bytes) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = bytes;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_NAMED_OBJECT;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_PARENT_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(parentKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, parentKey, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_PARENT_KEY, FIELD_COUNT,
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
  public boolean isSameItem(@Nullable final Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  @Override
  public void setTypeKey(final int typeKey) {
    // Not supported for JSON nodes
  }

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
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_PREV_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_PREV_REVISION, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
      return;
    }
    this.previousRevision = revision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_LAST_MOD_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_LAST_MOD_REVISION, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
      return;
    }
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (page != null) {
      return readLongField(NodeFieldLayout.OBJNAMEDOBJ_HASH);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return hash;
  }

  @Override
  public void setHash(final long hash) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_HASH) & 0xFF;
      DeltaVarIntCodec.writeLongToSegment(page, dataRegionStart + fieldOff, hash);
      return;
    }
    this.hash = hash;
  }

  /**
   * Compute the canonical hash of this fused structural node. Mirrors the byte sequence
   * that a legacy {@code ObjectKeyNode + ObjectNode} pair would have produced so that the
   * rolling hash invariant in revision-diff is preserved across fusion. Specifically:
   * <ol>
   *   <li>{@code nodeKey, parentKey, kindByte} (header — same shape as both parents)</li>
   *   <li>{@code childCount, descendantCount} (structural counters from {@link ObjectNode})</li>
   *   <li>{@code leftSib, rightSib, firstChild} (siblings + descent — common to both)</li>
   *   <li>{@code lastChild} (only if not the INVALID sentinel — same conditional as ObjectNode)</li>
   *   <li>{@code nameKey} (suffix — matches the legacy {@code ObjectKeyNode.computeHash}'s nameKey-last)</li>
   * </ol>
   * Note: {@code pathNodeKey} is intentionally excluded — it's a derived index entry, not a
   * structural property of the node, and including it would double-count with the
   * path-summary tree.
   */
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

    bytes.writeInt(getNameKey());

    return bytes.hashDirect(hashFunction);
  }

  public int getNameKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDOBJ_NAME_KEY);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return nameKey;
  }

  public void setNameKey(final int nameKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_NAME_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(nameKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, nameKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_NAME_KEY, FIELD_COUNT,
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

  public void setPrefixKey(final int prefixKey) {
    // Not supported for JSON nodes
  }

  public int getURIKey() {
    return -1;
  }

  public void setURIKey(final int uriKey) {
    // Not supported for JSON nodes
  }

  public void setLocalNameKey(final int localNameKey) {
    setNameKey(localNameKey);
  }

  public long getPathNodeKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_PATH_NODE_KEY, nodeKey);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return pathNodeKey;
  }

  public void setPathNodeKey(final long pathNodeKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_PATH_NODE_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(pathNodeKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, pathNodeKey, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_PATH_NODE_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, pathNodeKey, nodeKey));
      return;
    }
    this.pathNodeKey = pathNodeKey;
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long rightSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_RIGHT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(rightSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, rightSibling, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_RIGHT_SIB_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, rightSibling, nodeKey));
      return;
    }
    this.rightSiblingKey = rightSibling;
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long leftSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_LEFT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(leftSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, leftSibling, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_LEFT_SIB_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, leftSibling, nodeKey));
      return;
    }
    this.leftSiblingKey = leftSibling;
  }

  @Override
  public long getFirstChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY, nodeKey);
    }
    return firstChildKey;
  }

  public void setFirstChildKey(final long firstChild) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(firstChild, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, firstChild, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_FIRST_CHILD_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, firstChild, nodeKey));
      return;
    }
    this.firstChildKey = firstChild;
  }

  @Override
  public long getLastChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDOBJ_LAST_CHILD_KEY, nodeKey);
    }
    return lastChildKey;
  }

  public void setLastChildKey(final long lastChild) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_LAST_CHILD_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(lastChild, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, lastChild, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_LAST_CHILD_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, lastChild, nodeKey));
      return;
    }
    this.lastChildKey = lastChild;
  }

  @Override
  public long getChildCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.OBJNAMEDOBJ_CHILD_COUNT);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return childCount;
  }

  public void setChildCount(final long childCount) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_CHILD_COUNT) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedLongEncodedWidth(childCount);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedLongToSegment(page, absOff, childCount);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_CHILD_COUNT, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedLongToSegment(target, off, childCount));
      return;
    }
    this.childCount = childCount;
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
      return readSignedLongField(NodeFieldLayout.OBJNAMEDOBJ_DESCENDANT_COUNT);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return descendantCount;
  }

  @Override
  public void setDescendantCount(final long descendantCount) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDOBJ_DESCENDANT_COUNT) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedLongEncodedWidth(descendantCount);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedLongToSegment(page, absOff, descendantCount);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDOBJ_DESCENDANT_COUNT, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedLongToSegment(target, off, descendantCount));
      return;
    }
    this.descendantCount = descendantCount;
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
  public boolean hasFirstChild() {
    return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasLastChild() {
    return getLastChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
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
      return readSignedField(NodeFieldLayout.OBJNAMEDOBJ_PREV_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDOBJ_LAST_MOD_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return lastModifiedRevision;
  }

  public int getTypeKey() {
    return -1;
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
    // P2 dispatch — JsonNodeVisitor has a default visit(ObjectNamedObjectNode) returning CONTINUE.
    // Visitors that care about the structural-fused shape can override that method.
    return visitor.visit(this);
  }

  public LongHashFunction getHashFunction() {
    return hashFunction;
  }

  @Override
  public void setNodeKey(final long nodeKey) {
    this.nodeKey = nodeKey;
  }

  /**
   * Populate this node from a BytesIn source for singleton reuse. Mirrors the lazy
   * parsing pattern of {@link ObjectNode#readFrom} extended with the name fields.
   *
   * <p><b>Phase 1</b>: this method exists for completeness and singleton lifecycle
   * symmetry with sibling fused kinds; it is not called by any production code path
   * yet because no NodeKind serialize/deserialize emits 52/53.
   */
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
    this.firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.lastChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.lazyFieldsParsed = false;
    this.hasHash = config.hashType != HashType.NONE;

    this.nameKey = 0;
    this.pathNodeKey = 0;
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
    this.childCount = 0;
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

    final BytesIn<?> bytesIn;
    if (lazySource instanceof MemorySegment segment) {
      bytesIn = new MemorySegmentBytesIn(segment);
      bytesIn.position(lazyOffset);
    } else if (lazySource instanceof byte[] bytesArr) {
      bytesIn = new ByteArrayBytesIn(bytesArr);
      bytesIn.position(lazyOffset);
    } else {
      throw new IllegalStateException("Unknown lazy source type: " + lazySource.getClass());
    }

    this.nameKey = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(bytesIn, nodeKey);
    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    if (hasHash) {
      this.hash = bytesIn.readLong();
    }
    this.childCount = DeltaVarIntCodec.decodeSignedLong(bytesIn);
    this.descendantCount = DeltaVarIntCodec.decodeSignedLong(bytesIn);
    this.lazyFieldsParsed = true;
  }

  public ObjectNamedObjectNode toSnapshot() {
    if (page != null) {
      return new ObjectNamedObjectNode(nodeKey,
          getParentKey(), getRightSiblingKey(), getLeftSiblingKey(),
          getFirstChildKey(), getLastChildKey(),
          getNameKey(), getPathNodeKey(),
          getPreviousRevisionNumber(), getLastModifiedRevisionNumber(), getHash(),
          getChildCount(), getDescendantCount(),
          hashFunction,
          getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return new ObjectNamedObjectNode(nodeKey, parentKey, rightSiblingKey, leftSiblingKey,
        firstChildKey, lastChildKey, nameKey, pathNodeKey,
        previousRevision, lastModifiedRevision, hash, childCount, descendantCount, hashFunction,
        getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
  }

  @Override
  public String toString() {
    return "ObjectNamedObjectNode{" +
        "nodeKey=" + nodeKey +
        ", parentKey=" + parentKey +
        ", nameKey=" + nameKey +
        ", firstChild=" + firstChildKey +
        ", lastChild=" + lastChildKey +
        ", childCount=" + childCount +
        ", descendantCount=" + descendantCount +
        '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeKey, parentKey, nameKey);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ObjectNamedObjectNode other)) {
      return false;
    }
    return nodeKey == other.nodeKey
        && parentKey == other.parentKey
        && nameKey == other.nameKey;
  }
}
