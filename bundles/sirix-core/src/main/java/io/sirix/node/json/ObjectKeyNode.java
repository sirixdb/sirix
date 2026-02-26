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

import com.google.common.hash.Funnel;
import com.google.common.hash.PrimitiveSink;
import io.brackit.query.atomic.QNm;
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
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.page.PageLayout;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Node representing an object key/field.
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.</p>
 */
public final class ObjectKeyNode implements StructNode, NameNode, ImmutableJsonNode, FlyweightNode {

  // Node identity (mutable for singleton reuse)
  private long nodeKey;
  
  // Mutable structural fields
  private long parentKey;
  private long pathNodeKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private long firstChildKey;
  
  // Name key (hash of the name string)
  private int nameKey;
  
  // Mutable revision tracking
  private int previousRevision;
  private int lastModifiedRevision;
  
  // Mutable hash and descendant count
  private long hash;
  private long descendantCount;
  
  // Hash function for computing node hashes (mutable for singleton reuse)
  private LongHashFunction hashFunction;
  
  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;
  
  // Cache for name (not serialized, only nameKey is)
  private QNm cachedName;

  // Lazy parsing state
  private Object lazySource;
  private long lazyOffset;
  private boolean lazyFieldsParsed;
  private boolean hasHash;

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

  private static final int FIELD_COUNT = NodeFieldLayout.OBJECT_KEY_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public ObjectKeyNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  /**
   * Primary constructor with all primitive fields.
   */
  public ObjectKeyNode(long nodeKey, long parentKey, long pathNodeKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long firstChildKey,
      int nameKey, long descendantCount, long hash,
      LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.nameKey = nameKey;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.lazyFieldsParsed = true;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public ObjectKeyNode(long nodeKey, long parentKey, long pathNodeKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long firstChildKey,
      int nameKey, long descendantCount, long hash,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.nameKey = nameKey;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.lazyFieldsParsed = true;
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
    this.lazyFieldsParsed = true; // No lazy state when bound
    this.lazySource = null;
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
    this.parentKey = readDeltaField(NodeFieldLayout.OBJKEY_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.OBJKEY_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.OBJKEY_LEFT_SIB_KEY, nk);
    this.firstChildKey = readDeltaField(NodeFieldLayout.OBJKEY_FIRST_CHILD_KEY, nk);
    this.nameKey = readSignedField(NodeFieldLayout.OBJKEY_NAME_KEY);
    this.pathNodeKey = readDeltaField(NodeFieldLayout.OBJKEY_PATH_NODE_KEY, nk);
    this.previousRevision = readSignedField(NodeFieldLayout.OBJKEY_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.OBJKEY_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.OBJKEY_HASH);
    this.descendantCount = readSignedLongField(NodeFieldLayout.OBJKEY_DESCENDANT_COUNT);
    this.page = null;
    this.ownerPage = null;
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

  // ==================== OWNER PAGE (for resize-in-place) ====================

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
   * Encode an ObjectKeyNode record directly to a MemorySegment from parameter values.
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
   * @param nameKey         the name key (hash of the name string)
   * @param pathNodeKey     the path node key
   * @param prevRev         the previous revision number
   * @param lastModRev      the last modified revision number
   * @param hash            the hash value
   * @param descendantCount the descendant count
   * @return the total number of bytes written
   */
  public static int writeNewRecord(final MemorySegment target, final long offset,
      final int[] heapOffsets, final long nodeKey,
      final long parentKey, final long rightSibKey, final long leftSibKey,
      final long firstChildKey, final int nameKey, final long pathNodeKey,
      final int prevRev, final int lastModRev, final long hash,
      final long descendantCount) {
    long pos = offset;

    // Write nodeKind byte
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.OBJECT_KEY.getId());
    pos++;

    // Reserve space for offset table
    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    // Data region start
    final long dataStart = pos;

    // Field 0: parentKey (delta-varint)
    heapOffsets[NodeFieldLayout.OBJKEY_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    // Field 1: rightSiblingKey (delta-varint)
    heapOffsets[NodeFieldLayout.OBJKEY_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSibKey, nodeKey);

    // Field 2: leftSiblingKey (delta-varint)
    heapOffsets[NodeFieldLayout.OBJKEY_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSibKey, nodeKey);

    // Field 3: firstChildKey (delta-varint)
    heapOffsets[NodeFieldLayout.OBJKEY_FIRST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, firstChildKey, nodeKey);

    // Field 4: nameKey (signed varint)
    heapOffsets[NodeFieldLayout.OBJKEY_NAME_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, nameKey);

    // Field 5: pathNodeKey (delta-varint)
    heapOffsets[NodeFieldLayout.OBJKEY_PATH_NODE_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, pathNodeKey, nodeKey);

    // Field 6: previousRevision (signed varint)
    heapOffsets[NodeFieldLayout.OBJKEY_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prevRev);

    // Field 7: lastModifiedRevision (signed varint)
    heapOffsets[NodeFieldLayout.OBJKEY_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModRev);

    // Field 8: hash (fixed 8 bytes)
    heapOffsets[NodeFieldLayout.OBJKEY_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    // Field 9: descendantCount (signed long varint)
    heapOffsets[NodeFieldLayout.OBJKEY_DESCENDANT_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, descendantCount);

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
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return writeNewRecord(target, offset, heapOffsets, nodeKey,
        parentKey, rightSiblingKey, leftSiblingKey,
        firstChildKey, nameKey, pathNodeKey,
        previousRevision, lastModifiedRevision,
        hash, descendantCount);
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
    return NodeKind.OBJECT_KEY;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJKEY_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_PARENT_KEY) & 0xFF;
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

  private void resizeParentKey(final long value) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.OBJKEY_PARENT_KEY, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, value, nodeKey));
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
  public void setPreviousRevision(final int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_PREV_REVISION) & 0xFF;
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
        NodeFieldLayout.OBJKEY_PREV_REVISION, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_LAST_MOD_REVISION) & 0xFF;
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
        NodeFieldLayout.OBJKEY_LAST_MOD_REVISION, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public long getHash() {
    if (page != null) {
      return readLongField(NodeFieldLayout.OBJKEY_HASH);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return hash;
  }

  @Override
  public void setHash(final long hash) {
    if (page != null) {
      // Hash is ALWAYS in-place (fixed 8 bytes)
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_HASH) & 0xFF;
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
         .writeByte(getKind().getId());

    bytes.writeLong(getDescendantCount())
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
      return readSignedField(NodeFieldLayout.OBJKEY_NAME_KEY);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return nameKey;
  }

  public void setNameKey(final int nameKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_NAME_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(nameKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, nameKey);
        return;
      }
      resizeNameKey(nameKey);
      return;
    }
    this.nameKey = nameKey;
  }

  private void resizeNameKey(final int nameKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.OBJKEY_NAME_KEY, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, nameKey));
  }

  public QNm getName() {
    return cachedName;
  }

  public void setName(final String name) {
    this.cachedName = new QNm(name);
  }

  // NameNode interface methods
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
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_NAME_KEY) & 0xFF;
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
    this.nameKey = localNameKey;
  }

  private void resizeLocalNameKey(final int localNameKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.OBJKEY_NAME_KEY, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, localNameKey));
  }

  public long getPathNodeKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJKEY_PATH_NODE_KEY, nodeKey);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return pathNodeKey;
  }

  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_PATH_NODE_KEY) & 0xFF;
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

  private void resizePathNodeKey(final long value) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.OBJKEY_PATH_NODE_KEY, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, value, nodeKey));
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJKEY_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long rightSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_RIGHT_SIB_KEY) & 0xFF;
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

  private void resizeRightSiblingKey(final long value) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.OBJKEY_RIGHT_SIB_KEY, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, value, nodeKey));
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJKEY_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long leftSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_LEFT_SIB_KEY) & 0xFF;
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

  private void resizeLeftSiblingKey(final long value) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.OBJKEY_LEFT_SIB_KEY, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, value, nodeKey));
  }

  @Override
  public long getFirstChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJKEY_FIRST_CHILD_KEY, nodeKey);
    }
    return firstChildKey;
  }

  public void setFirstChildKey(final long firstChild) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.OBJKEY_FIRST_CHILD_KEY) & 0xFF;
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

  private void resizeFirstChildKey(final long value) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.OBJKEY_FIRST_CHILD_KEY, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, value, nodeKey));
  }

  @Override
  public long getLastChildKey() {
    // ObjectKeyNode only has one child (the value), so first == last
    return getFirstChildKey();
  }

  public void setLastChildKey(final long lastChild) {
    // Not used for ObjectKeyNode
  }

  @Override
  public long getChildCount() {
    // ObjectKeyNode always has exactly 1 child (the value)
    return 1;
  }

  public void setChildCount(final long childCount) {
    // Not stored for ObjectKeyNode
  }

  @Override
  public void incrementChildCount() {
    // Not supported for ObjectKeyNode - always has 1 child
  }

  @Override
  public void decrementChildCount() {
    // Not supported for ObjectKeyNode - always has 1 child
  }

  @Override
  public long getDescendantCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.OBJKEY_DESCENDANT_COUNT);
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
          recordBase + 1 + NodeFieldLayout.OBJKEY_DESCENDANT_COUNT) & 0xFF;
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

  private void resizeDescendantCount(final long value) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.OBJKEY_DESCENDANT_COUNT, NodeFieldLayout.OBJECT_KEY_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedLongToSegment(target, off, value));
  }

  @Override
  public void decrementDescendantCount() {
    setDescendantCount(getDescendantCount() - 1);
  }

  @Override
  public void incrementDescendantCount() {
    setDescendantCount(getDescendantCount() + 1);
  }

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJKEY_PREV_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJKEY_LAST_MOD_REVISION);
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
  public boolean hasFirstChild() {
    return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasLastChild() {
    return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
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
    return visitor.visit(ImmutableObjectKeyNode.of(this));
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
   * LAZY OPTIMIZATION: Only parses structural fields immediately (NEW ORDER).
   */
  public void readFrom(final BytesIn<?> source, final long nodeKey, final byte[] deweyId,
                       final LongHashFunction hashFunction, final ResourceConfiguration config) {
    // Unbind flyweight — ensures getters use Java fields, not stale page reference
    this.page = null;
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    this.cachedName = null;

    // STRUCTURAL FIELDS - parse immediately (NEW ORDER)
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    
    // Store for lazy parsing
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.lazyFieldsParsed = false;
    this.hasHash = config.hashType != HashType.NONE;
    
    this.nameKey = 0;
    this.pathNodeKey = 0;
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
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
    
    this.nameKey = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(bytesIn, nodeKey);
    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    if (hasHash) {
      this.hash = bytesIn.readLong();
      this.descendantCount = DeltaVarIntCodec.decodeSigned(bytesIn);
    }
    this.lazyFieldsParsed = true;
  }

  /**
   * Create a deep copy snapshot of this node.
   * Forces parsing of all lazy fields since snapshot must be independent.
   */
  public ObjectKeyNode toSnapshot() {
    if (page != null) {
      // Bound mode: read all fields from page
      return new ObjectKeyNode(nodeKey,
          readDeltaField(NodeFieldLayout.OBJKEY_PARENT_KEY, nodeKey),
          readDeltaField(NodeFieldLayout.OBJKEY_PATH_NODE_KEY, nodeKey),
          readSignedField(NodeFieldLayout.OBJKEY_PREV_REVISION),
          readSignedField(NodeFieldLayout.OBJKEY_LAST_MOD_REVISION),
          readDeltaField(NodeFieldLayout.OBJKEY_RIGHT_SIB_KEY, nodeKey),
          readDeltaField(NodeFieldLayout.OBJKEY_LEFT_SIB_KEY, nodeKey),
          readDeltaField(NodeFieldLayout.OBJKEY_FIRST_CHILD_KEY, nodeKey),
          readSignedField(NodeFieldLayout.OBJKEY_NAME_KEY),
          readSignedLongField(NodeFieldLayout.OBJKEY_DESCENDANT_COUNT),
          readLongField(NodeFieldLayout.OBJKEY_HASH),
          hashFunction,
          getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return new ObjectKeyNode(nodeKey, parentKey, pathNodeKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, firstChildKey, nameKey, descendantCount, hash, hashFunction,
        getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
  }

  public String toString() {
    return "ObjectKeyNode{" +
        "nodeKey=" + nodeKey +
        ", parentKey=" + parentKey +
        ", nameKey=" + nameKey +
        ", pathNodeKey=" + pathNodeKey +
        ", rightSiblingKey=" + rightSiblingKey +
        ", leftSiblingKey=" + leftSiblingKey +
        ", firstChildKey=" + firstChildKey +
        '}';
  }

  public static Funnel<ObjectKeyNode> getFunnel() {
    return (ObjectKeyNode node, PrimitiveSink into) -> {
      into.putLong(node.getParentKey())
          .putInt(node.getNameKey())
          .putLong(node.getPathNodeKey())
          .putLong(node.getRightSiblingKey())
          .putLong(node.getLeftSiblingKey())
          .putLong(node.getFirstChildKey());
    };
  }
}
