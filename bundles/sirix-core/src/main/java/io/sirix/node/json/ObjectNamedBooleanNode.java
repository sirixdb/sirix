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
 * Fused JSON node representing an object key bound to a BOOLEAN value in a single slot.
 *
 * <p>Replaces the legacy pair {@code OBJECT_KEY + OBJECT_BOOLEAN_VALUE} for the common
 * {@code {"fieldname": true|false}} pattern, eliminating one record per such field.
 *
 * <h2>Wire layout</h2>
 * <pre>
 * [kindByte=44][offsetTable: FIELD_COUNT × 1 byte][data region]
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
 *   8 value (1 byte: 0 = false, 1 = true)
 * </pre>
 *
 * <p>HFT contract: primitive fields only, {@code final} where possible, zero-alloc
 * bind/unbind, offset-table lookups in O(1).
 */
public final class ObjectNamedBooleanNode implements StructNode, NameNode, ImmutableJsonNode, FlyweightNode {

  private long nodeKey;
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private int nameKey;
  private long pathNodeKey;
  private int previousRevision;
  private int lastModifiedRevision;
  private long hash;
  private boolean value;
  private LongHashFunction hashFunction;

  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Cache for name (not serialized)
  private QNm cachedName;

  // Lazy parsing state
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
  private final int[] heapOffsets;

  private static final int FIELD_COUNT = NodeFieldLayout.OBJECT_NAMED_BOOLEAN_FIELD_COUNT;

  public ObjectNamedBooleanNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  public ObjectNamedBooleanNode(long nodeKey, long parentKey, long rightSiblingKey, long leftSiblingKey,
      int nameKey, long pathNodeKey, int previousRevision, int lastModifiedRevision, long hash, boolean value,
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
    this.lazyFieldsParsed = true;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  public ObjectNamedBooleanNode(long nodeKey, long parentKey, long rightSiblingKey, long leftSiblingKey,
      int nameKey, long pathNodeKey, int previousRevision, int lastModifiedRevision, long hash, boolean value,
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
    this.lazyFieldsParsed = true;
    this.heapOffsets = new int[FIELD_COUNT];
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
    this.parentKey = readDeltaField(NodeFieldLayout.OBJNAMEDBOOL_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.OBJNAMEDBOOL_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.OBJNAMEDBOOL_LEFT_SIB_KEY, nk);
    this.nameKey = readSignedField(NodeFieldLayout.OBJNAMEDBOOL_NAME_KEY);
    this.pathNodeKey = readDeltaField(NodeFieldLayout.OBJNAMEDBOOL_PATH_NODE_KEY, nk);
    this.previousRevision = readSignedField(NodeFieldLayout.OBJNAMEDBOOL_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.OBJNAMEDBOOL_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.OBJNAMEDBOOL_HASH);
    this.value = readByteField(NodeFieldLayout.OBJNAMEDBOOL_VALUE) != 0;
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
    // 1 (nodeKind) + 9 (offset table) + ~35 (varint fields avg incl. pathNodeKey & nameKey) + 8 (hash) + 1 (bool payload) = ~54
    return 64;
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

  private byte readByteField(final int fieldIndex) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return page.get(ValueLayout.JAVA_BYTE, dataRegionStart + fieldOff);
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
      final int prevRev, final int lastModRev, final long hash, final boolean value) {
    long pos = offset;

    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.OBJECT_NAMED_BOOLEAN.getId());
    pos++;

    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    final long dataStart = pos;

    heapOffsets[NodeFieldLayout.OBJNAMEDBOOL_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDBOOL_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSibKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDBOOL_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSibKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDBOOL_NAME_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, nameKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDBOOL_PATH_NODE_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, pathNodeKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDBOOL_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prevRev);

    heapOffsets[NodeFieldLayout.OBJNAMEDBOOL_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModRev);

    heapOffsets[NodeFieldLayout.OBJNAMEDBOOL_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    heapOffsets[NodeFieldLayout.OBJNAMEDBOOL_VALUE] = (int) (pos - dataStart);
    target.set(ValueLayout.JAVA_BYTE, pos, (byte) (value ? 1 : 0));
    pos++;

    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) heapOffsets[i]);
    }

    return (int) (pos - offset);
  }

  public int serializeToHeap(final MemorySegment target, final long offset) {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return writeNewRecord(target, offset, heapOffsets, nodeKey,
        parentKey, rightSiblingKey, leftSiblingKey,
        nameKey, pathNodeKey,
        previousRevision, lastModifiedRevision, hash, value);
  }

  public int[] getHeapOffsets() {
    return heapOffsets;
  }

  public void setDeweyIDAfterCreation(final SirixDeweyID id, final byte[] bytes) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = bytes;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_NAMED_BOOLEAN;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDBOOL_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_PARENT_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(parentKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, parentKey, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDBOOL_PARENT_KEY, FIELD_COUNT,
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
          recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_PREV_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDBOOL_PREV_REVISION, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
      return;
    }
    this.previousRevision = revision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_LAST_MOD_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDBOOL_LAST_MOD_REVISION, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
      return;
    }
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (page != null) {
      return readLongField(NodeFieldLayout.OBJNAMEDBOOL_HASH);
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
          recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_HASH) & 0xFF;
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
         .writeInt(getNameKey())
         .writeBoolean(getValue());
    return bytes.hashDirect(hashFunction);
  }

  public int getNameKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDBOOL_NAME_KEY);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return nameKey;
  }

  public void setNameKey(final int nameKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_NAME_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(nameKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, nameKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDBOOL_NAME_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, nameKey));
      return;
    }
    this.nameKey = nameKey;
  }

  public QNm getName() {
    return cachedName;
  }

  public void setName(final String name) {
    this.cachedName = new QNm(name);
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
      return readDeltaField(NodeFieldLayout.OBJNAMEDBOOL_PATH_NODE_KEY, nodeKey);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return pathNodeKey;
  }

  public void setPathNodeKey(final long pathNodeKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_PATH_NODE_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(pathNodeKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, pathNodeKey, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDBOOL_PATH_NODE_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, pathNodeKey, nodeKey));
      return;
    }
    this.pathNodeKey = pathNodeKey;
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDBOOL_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long rightSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_RIGHT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(rightSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, rightSibling, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDBOOL_RIGHT_SIB_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, rightSibling, nodeKey));
      return;
    }
    this.rightSiblingKey = rightSibling;
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDBOOL_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long leftSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_LEFT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(leftSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, leftSibling, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex,
          NodeFieldLayout.OBJNAMEDBOOL_LEFT_SIB_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, leftSibling, nodeKey));
      return;
    }
    this.leftSiblingKey = leftSibling;
  }

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  public void setFirstChildKey(final long firstChild) {
    // leaf: no child
  }

  @Override
  public long getLastChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  public void setLastChildKey(final long lastChild) {
    // leaf: no child
  }

  @Override
  public long getChildCount() {
    return 0;
  }

  public void setChildCount(final long childCount) {
    // leaf: always 0
  }

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
      return readSignedField(NodeFieldLayout.OBJNAMEDBOOL_PREV_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDBOOL_LAST_MOD_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return lastModifiedRevision;
  }

  public boolean getValue() {
    if (page != null) {
      return readByteField(NodeFieldLayout.OBJNAMEDBOOL_VALUE) != 0;
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return value;
  }

  public void setValue(final boolean value) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJNAMEDBOOL_VALUE) & 0xFF;
      page.set(ValueLayout.JAVA_BYTE, dataRegionStart + fieldOff, (byte) (value ? 1 : 0));
      return;
    }
    this.value = value;
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
    this.lazyFieldsParsed = false;
    this.hasHash = config.hashType != HashType.NONE;

    this.nameKey = 0;
    this.pathNodeKey = 0;
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
    this.value = false;
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
    } else if (lazySource instanceof byte[] bytes) {
      bytesIn = new ByteArrayBytesIn(bytes);
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
    this.value = bytesIn.readBoolean();
    this.lazyFieldsParsed = true;
  }

  public ObjectNamedBooleanNode toSnapshot() {
    if (page != null) {
      return new ObjectNamedBooleanNode(nodeKey,
          getParentKey(), getRightSiblingKey(), getLeftSiblingKey(),
          getNameKey(), getPathNodeKey(),
          getPreviousRevisionNumber(), getLastModifiedRevisionNumber(), getHash(), getValue(),
          hashFunction,
          getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return new ObjectNamedBooleanNode(nodeKey, parentKey, rightSiblingKey, leftSiblingKey,
        nameKey, pathNodeKey, previousRevision, lastModifiedRevision, hash, value, hashFunction,
        getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
  }

  @Override
  public String toString() {
    return "ObjectNamedBooleanNode{" +
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
    if (!(obj instanceof final ObjectNamedBooleanNode other)) {
      return false;
    }
    return nodeKey == other.nodeKey
        && parentKey == other.parentKey
        && nameKey == other.nameKey
        && value == other.value;
  }
}
