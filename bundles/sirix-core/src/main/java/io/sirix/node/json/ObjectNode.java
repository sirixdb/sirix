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
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.page.NodeFieldLayout;
import io.sirix.page.PageLayout;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * JSON Object node.
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.
 * This eliminates MemorySegment/VarHandle overhead and enables compact serialization.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class ObjectNode implements StructNode, ImmutableJsonNode, FlyweightNode {

  // Node identity (mutable for singleton reuse)
  private long nodeKey;
  
  // Mutable structural fields (updated during tree modifications)
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private long firstChildKey;
  private long lastChildKey;
  
  // Mutable revision tracking
  private int previousRevision;
  private int lastModifiedRevision;
  
  // Mutable counters
  private long childCount;
  private long descendantCount;
  
  // Mutable hash
  private long hash;
  
  // Hash function for computing node hashes (mutable for singleton reuse)
  private LongHashFunction hashFunction;
  
  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Lazy parsing state (single-stage for metadata)
  private Object lazySource;
  private long lazyOffset;
  private boolean lazyFieldsParsed;
  private boolean hasHash;
  private boolean storeChildCount;

  // ==================== FLYWEIGHT BINDING (LeanStore page-direct access) ====================

  /** Page MemorySegment when bound (null = primitive mode). */
  private MemorySegment page;

  /** Absolute byte offset of this record in the page (after HEAP_START + heapOffset). */
  private long recordBase;

  /** Absolute byte offset where the data region starts (recordBase + 1 + FIELD_COUNT). */
  private long dataRegionStart;

  /** Slot index in the page directory (for re-serialization). */
  private int slotIndex;

  private static final int FIELD_COUNT = NodeFieldLayout.OBJECT_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public ObjectNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
  }

  /**
   * Primary constructor with all primitive fields.
   * Used by deserialization (NodeKind.OBJECT.deserialize).
   */
  public ObjectNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long firstChildKey,
      long lastChildKey, long childCount, long descendantCount, long hash,
      LongHashFunction hashFunction, byte[] deweyID) {
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
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.lazyFieldsParsed = true;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   * Used by factory methods when creating new nodes.
   */
  public ObjectNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long firstChildKey,
      long lastChildKey, long childCount, long descendantCount, long hash,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
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
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.lazyFieldsParsed = true;
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
    this.parentKey = readDeltaField(NodeFieldLayout.OBJECT_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.OBJECT_LEFT_SIB_KEY, nk);
    this.firstChildKey = readDeltaField(NodeFieldLayout.OBJECT_FIRST_CHILD_KEY, nk);
    this.lastChildKey = readDeltaField(NodeFieldLayout.OBJECT_LAST_CHILD_KEY, nk);
    this.previousRevision = readSignedField(NodeFieldLayout.OBJECT_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.OBJECT_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.OBJECT_HASH);
    this.childCount = readSignedLongField(NodeFieldLayout.OBJECT_CHILD_COUNT);
    this.descendantCount = readSignedLongField(NodeFieldLayout.OBJECT_DESCENDANT_COUNT);
    this.page = null;
  }

  /** Check if this node is bound to a page MemorySegment. */
  public boolean isBound() {
    return page != null;
  }

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

  private long readSignedLongField(final int fieldIndex) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.decodeSignedLongFromSegment(page, dataRegionStart + fieldOff);
  }

  private long readLongField(final int fieldIndex) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.readLongFromSegment(page, (int) (dataRegionStart + fieldOff));
  }

  // ==================== FLYWEIGHT FIELD WRITE HELPERS ====================

  /**
   * Write a delta-encoded field in-place if width matches, otherwise re-serialize.
   */
  private void setDeltaFieldInPlace(final int fieldIndex, final long newKey) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    final long absOff = dataRegionStart + fieldOff;
    final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
    final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(newKey, nodeKey);
    if (newWidth == currentWidth) {
      DeltaVarIntCodec.writeDeltaToSegment(page, absOff, newKey, nodeKey);
    } else {
      // Width changed: unbind, set field, re-serialize
      unbind();
      // The specific field will be set by the caller after this method returns
    }
  }

  /**
   * Write a signed varint field in-place if width matches, otherwise re-serialize.
   */
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
   * Write a signed long varint field in-place if width matches, otherwise re-serialize.
   */
  private boolean setSignedLongFieldInPlace(final int fieldIndex, final long newValue) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    final long absOff = dataRegionStart + fieldOff;
    final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
    final int newWidth = DeltaVarIntCodec.computeSignedLongEncodedWidth(newValue);
    if (newWidth == currentWidth) {
      DeltaVarIntCodec.writeSignedLongToSegment(page, absOff, newValue);
      return true;
    }
    unbind();
    return false;
  }

  // ==================== SERIALIZE TO HEAP ====================

  /**
   * Serialize this node (from Java fields) into the new unified format with offset table.
   * Writes: [nodeKind:1][offsetTable:FIELD_COUNT][data fields].
   *
   * @param target the target MemorySegment
   * @param offset the absolute byte offset to write at
   * @return the total number of bytes written
   */
  public int serializeToHeap(final MemorySegment target, final long offset) {
    // Ensure all lazy fields are materialized
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }

    long pos = offset;

    // Write nodeKind byte
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.OBJECT.getId());
    pos++;

    // Reserve space for offset table (will be written after computing offsets)
    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    // Data region start
    final long dataStart = pos;
    final int[] offsets = new int[FIELD_COUNT];

    // Field 0: parentKey (delta-varint)
    offsets[NodeFieldLayout.OBJECT_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    // Field 1: rightSiblingKey (delta-varint)
    offsets[NodeFieldLayout.OBJECT_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSiblingKey, nodeKey);

    // Field 2: leftSiblingKey (delta-varint)
    offsets[NodeFieldLayout.OBJECT_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSiblingKey, nodeKey);

    // Field 3: firstChildKey (delta-varint)
    offsets[NodeFieldLayout.OBJECT_FIRST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, firstChildKey, nodeKey);

    // Field 4: lastChildKey (delta-varint)
    offsets[NodeFieldLayout.OBJECT_LAST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, lastChildKey, nodeKey);

    // Field 5: previousRevision (signed varint)
    offsets[NodeFieldLayout.OBJECT_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, previousRevision);

    // Field 6: lastModifiedRevision (signed varint)
    offsets[NodeFieldLayout.OBJECT_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModifiedRevision);

    // Field 7: hash (fixed 8 bytes)
    offsets[NodeFieldLayout.OBJECT_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    // Field 8: childCount (signed varint)
    offsets[NodeFieldLayout.OBJECT_CHILD_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, childCount);

    // Field 9: descendantCount (signed varint)
    offsets[NodeFieldLayout.OBJECT_DESCENDANT_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, descendantCount);

    // Write offset table
    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) offsets[i]);
    }

    return (int) (pos - offset);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJECT_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.OBJECT_PARENT_KEY, parentKey);
      if (page != null) return; // In-place write succeeded
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
    this.deweyIDBytes = null; // Clear cached bytes
  }

  @Override
  public void setPreviousRevision(final int revision) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.OBJECT_PREV_REVISION, revision)) return;
    }
    this.previousRevision = revision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.OBJECT_LAST_MOD_REVISION, revision)) return;
    }
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (page != null) {
      return readLongField(NodeFieldLayout.OBJECT_HASH);
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
          recordBase + 1 + NodeFieldLayout.OBJECT_HASH) & 0xFF;
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

    return hashFunction.hashBytes(bytes.toByteArray());
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long rightSibling) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY, rightSibling);
      if (page != null) return;
    }
    this.rightSiblingKey = rightSibling;
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJECT_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long leftSibling) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.OBJECT_LEFT_SIB_KEY, leftSibling);
      if (page != null) return;
    }
    this.leftSiblingKey = leftSibling;
  }

  @Override
  public long getFirstChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJECT_FIRST_CHILD_KEY, nodeKey);
    }
    return firstChildKey;
  }

  public void setFirstChildKey(final long firstChild) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.OBJECT_FIRST_CHILD_KEY, firstChild);
      if (page != null) return;
    }
    this.firstChildKey = firstChild;
  }

  @Override
  public long getLastChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJECT_LAST_CHILD_KEY, nodeKey);
    }
    return lastChildKey;
  }

  public void setLastChildKey(final long lastChild) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.OBJECT_LAST_CHILD_KEY, lastChild);
      if (page != null) return;
    }
    this.lastChildKey = lastChild;
  }

  @Override
  public long getChildCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.OBJECT_CHILD_COUNT);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return childCount;
  }

  public void setChildCount(final long childCount) {
    if (page != null) {
      if (setSignedLongFieldInPlace(NodeFieldLayout.OBJECT_CHILD_COUNT, childCount)) return;
      // Width changed — unbind already happened in the helper
    }
    this.childCount = childCount;
  }

  @Override
  public long getDescendantCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.OBJECT_DESCENDANT_COUNT);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return descendantCount;
  }

  public void setDescendantCount(final long descendantCount) {
    if (page != null) {
      if (setSignedLongFieldInPlace(NodeFieldLayout.OBJECT_DESCENDANT_COUNT, descendantCount)) return;
    }
    this.descendantCount = descendantCount;
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
      return readSignedField(NodeFieldLayout.OBJECT_PREV_REVISION);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJECT_LAST_MOD_REVISION);
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

    // STRUCTURAL FIELDS - parse immediately (NEW ORDER)
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.lastChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

    // Store for lazy parsing
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
  
  /**
   * Populate this node directly from a MemorySegment, bypassing BytesIn overhead.
   * ZERO ALLOCATION - reads directly from memory segment.
   * 
   * @param segment     the MemorySegment containing the serialized node data (after kind byte)
   * @param startOffset the byte offset within the segment to start reading
   * @param nodeKey     the node key
   * @param deweyId     the DeweyID bytes (may be null)
   * @param hashFunction the hash function
   * @param config      the resource configuration
   * @return the byte offset after reading all structural fields (for lazy field position)
   */
  public int readFromSegment(final MemorySegment segment, final int startOffset, final long nodeKey,
                              final byte[] deweyId, final LongHashFunction hashFunction,
                              final ResourceConfiguration config) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    
    int offset = startOffset;
    
    // STRUCTURAL FIELDS - read directly from segment (no BytesIn overhead)
    this.parentKey = DeltaVarIntCodec.decodeDeltaFromSegment(segment, offset, nodeKey);
    offset += DeltaVarIntCodec.deltaLength(segment, offset);
    
    this.rightSiblingKey = DeltaVarIntCodec.decodeDeltaFromSegment(segment, offset, nodeKey);
    offset += DeltaVarIntCodec.deltaLength(segment, offset);
    
    this.leftSiblingKey = DeltaVarIntCodec.decodeDeltaFromSegment(segment, offset, nodeKey);
    offset += DeltaVarIntCodec.deltaLength(segment, offset);
    
    this.firstChildKey = DeltaVarIntCodec.decodeDeltaFromSegment(segment, offset, nodeKey);
    offset += DeltaVarIntCodec.deltaLength(segment, offset);
    
    this.lastChildKey = DeltaVarIntCodec.decodeDeltaFromSegment(segment, offset, nodeKey);
    offset += DeltaVarIntCodec.deltaLength(segment, offset);
    
    // Store for lazy parsing
    this.lazySource = segment;
    this.lazyOffset = offset;
    this.lazyFieldsParsed = false;
    this.hasHash = config.hashType != HashType.NONE;
    this.storeChildCount = config.storeChildCount();
    
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.childCount = 0;
    this.hash = 0;
    this.descendantCount = 0;
    
    return offset;
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
    this.childCount = storeChildCount ? DeltaVarIntCodec.decodeSigned(bytesIn) : 0;
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
  public ObjectNode toSnapshot() {
    if (page != null) {
      // Bound mode: read all fields from page
      return new ObjectNode(nodeKey,
          readDeltaField(NodeFieldLayout.OBJECT_PARENT_KEY, nodeKey),
          readSignedField(NodeFieldLayout.OBJECT_PREV_REVISION),
          readSignedField(NodeFieldLayout.OBJECT_LAST_MOD_REVISION),
          readDeltaField(NodeFieldLayout.OBJECT_RIGHT_SIB_KEY, nodeKey),
          readDeltaField(NodeFieldLayout.OBJECT_LEFT_SIB_KEY, nodeKey),
          readDeltaField(NodeFieldLayout.OBJECT_FIRST_CHILD_KEY, nodeKey),
          readDeltaField(NodeFieldLayout.OBJECT_LAST_CHILD_KEY, nodeKey),
          readSignedLongField(NodeFieldLayout.OBJECT_CHILD_COUNT),
          readSignedLongField(NodeFieldLayout.OBJECT_DESCENDANT_COUNT),
          readLongField(NodeFieldLayout.OBJECT_HASH),
          hashFunction,
          deweyIDBytes != null ? deweyIDBytes.clone() : null);
    }
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return new ObjectNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, firstChildKey, lastChildKey, childCount,
        descendantCount, hash, hashFunction,
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
    return visitor.visit(ImmutableObjectNode.of(this));
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
                      .add("firstChild", firstChildKey)
                      .add("lastChild", lastChildKey)
                      .add("childCount", childCount)
                      .add("hash", hash)
                      .add("descendantCount", descendantCount)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ObjectNode other))
      return false;

    return nodeKey == other.nodeKey
        && parentKey == other.parentKey;
  }
}
