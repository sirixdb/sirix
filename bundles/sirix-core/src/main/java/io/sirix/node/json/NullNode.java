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
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.Bytes;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import io.sirix.node.immutable.json.ImmutableNullNode;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * JSON Null node.
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class NullNode implements StructNode, ImmutableJsonNode, FlyweightNode {

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

  // Hash function for computing node hashes (mutable for singleton reuse)
  private LongHashFunction hashFunction;

  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Lazy parsing state
  private Object lazySource;
  private long lazyOffset;
  private boolean lazyFieldsParsed;
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

  private static final int FIELD_COUNT = NodeFieldLayout.NULL_VALUE_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public NullNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  /**
   * Primary constructor with all primitive fields.
   */
  public NullNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long hash,
      LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.hash = hash;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.lazyFieldsParsed = true;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public NullNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long hash,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
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
    this.hash = 0;
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
    this.parentKey = readDeltaField(NodeFieldLayout.NULLVAL_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.NULLVAL_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.NULLVAL_LEFT_SIB_KEY, nk);
    this.previousRevision = readSignedField(NodeFieldLayout.NULLVAL_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.NULLVAL_LAST_MOD_REVISION);
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
   * Encode a NullNode record directly to a MemorySegment from parameter values.
   * Static -- reads nothing from any instance. Zero field intermediation.
   *
   * @param target      the target MemorySegment (reinterpreted slotted page)
   * @param offset      absolute byte offset to write at
   * @param heapOffsets pre-allocated offset array (reused, FIELD_COUNT elements)
   * @param nodeKey     the node key (delta base for structural keys)
   * @param parentKey   the parent node key
   * @param rightSibKey the right sibling key
   * @param leftSibKey  the left sibling key
   * @param prevRev     the previous revision number
   * @param lastModRev  the last modified revision number
   * @return the total number of bytes written
   */
  public static int writeNewRecord(final MemorySegment target, final long offset,
      final int[] heapOffsets, final long nodeKey,
      final long parentKey, final long rightSibKey, final long leftSibKey,
      final int prevRev, final int lastModRev) {
    long pos = offset;

    // Write nodeKind byte
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.NULL_VALUE.getId());
    pos++;

    // Reserve space for offset table
    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    // Data region start
    final long dataStart = pos;

    // Field 0: parentKey (delta-varint)
    heapOffsets[NodeFieldLayout.NULLVAL_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    // Field 1: rightSiblingKey (delta-varint)
    heapOffsets[NodeFieldLayout.NULLVAL_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSibKey, nodeKey);

    // Field 2: leftSiblingKey (delta-varint)
    heapOffsets[NodeFieldLayout.NULLVAL_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSibKey, nodeKey);

    // Field 3: previousRevision (signed varint)
    heapOffsets[NodeFieldLayout.NULLVAL_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prevRev);

    // Field 4: lastModifiedRevision (signed varint)
    heapOffsets[NodeFieldLayout.NULLVAL_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModRev);

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
        previousRevision, lastModifiedRevision);
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
    return NodeKind.NULL_VALUE;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.NULLVAL_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.NULLVAL_PARENT_KEY) & 0xFF;
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
        NodeFieldLayout.NULLVAL_PARENT_KEY, NodeFieldLayout.NULL_VALUE_FIELD_COUNT,
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
  public void setDeweyIDBytes(final byte[] bytes) {
    this.deweyIDBytes = bytes;
    this.sirixDeweyID = null;
  }

  @Override
  public void setPreviousRevision(final int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.NULLVAL_PREV_REVISION) & 0xFF;
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
        NodeFieldLayout.NULLVAL_PREV_REVISION, NodeFieldLayout.NULL_VALUE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.NULLVAL_LAST_MOD_REVISION) & 0xFF;
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
        NodeFieldLayout.NULLVAL_LAST_MOD_REVISION, NodeFieldLayout.NULL_VALUE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public long getHash() {
    if (hash != 0L) {
      return hash;
    }
    if (hashFunction != null) {
      return computeHash(Bytes.threadLocalHashBuffer());
    }
    return 0L;
  }

  @Override
  public void setHash(final long hash) {
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

    return bytes.hashDirect(hashFunction);
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.NULLVAL_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long rightSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.NULLVAL_RIGHT_SIB_KEY) & 0xFF;
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
        NodeFieldLayout.NULLVAL_RIGHT_SIB_KEY, NodeFieldLayout.NULL_VALUE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, value, nodeKey));
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.NULLVAL_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long leftSibling) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.NULLVAL_LEFT_SIB_KEY) & 0xFF;
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
        NodeFieldLayout.NULLVAL_LEFT_SIB_KEY, NodeFieldLayout.NULL_VALUE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, value, nodeKey));
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
      return readSignedField(NodeFieldLayout.NULLVAL_PREV_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.NULLVAL_LAST_MOD_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
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

  public void readFrom(final BytesIn<?> source, final long nodeKey, final byte[] deweyId,
                       final LongHashFunction hashFunction, final ResourceConfiguration config) {
    // Unbind flyweight — ensures getters use Java fields, not stale page reference
    this.page = null;
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;

    // STRUCTURAL FIELDS
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

    // Store for lazy parsing
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.lazyFieldsParsed = false;
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
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
    this.lazyFieldsParsed = true;
  }

  public NullNode toSnapshot() {
    if (page != null) {
      // Bound mode: read all fields from page
      return new NullNode(nodeKey,
          readDeltaField(NodeFieldLayout.NULLVAL_PARENT_KEY, nodeKey),
          readSignedField(NodeFieldLayout.NULLVAL_PREV_REVISION),
          readSignedField(NodeFieldLayout.NULLVAL_LAST_MOD_REVISION),
          readDeltaField(NodeFieldLayout.NULLVAL_RIGHT_SIB_KEY, nodeKey),
          readDeltaField(NodeFieldLayout.NULLVAL_LEFT_SIB_KEY, nodeKey),
          hash,
          hashFunction,
          getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return new NullNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, hash, hashFunction,
        getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null);
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
    return visitor.visit(ImmutableNullNode.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("parentKey", parentKey)
                      .add("previousRevision", previousRevision)
                      .add("lastModifiedRevision", lastModifiedRevision)
                      .add("rightSibling", rightSiblingKey)
                      .add("leftSibling", leftSiblingKey)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final NullNode other))
      return false;

    return nodeKey == other.nodeKey
        && parentKey == other.parentKey;
  }
}
