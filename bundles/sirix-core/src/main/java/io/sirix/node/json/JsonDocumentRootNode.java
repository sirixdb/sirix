/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.node.json;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Fixed;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Node representing the root of a JSON document. This node is guaranteed to exist in revision 0 and
 * cannot be removed.
 *
 * <p>Uses primitive fields for efficient storage following the ObjectNode pattern.
 * Document root has fixed values for nodeKey (0), parentKey (-1), and no siblings.</p>
 *
 * <p>Supports flyweight binding to a page MemorySegment for zero-copy field access.</p>
 */
public final class JsonDocumentRootNode implements StructNode, ImmutableJsonNode, FlyweightNode {

  // === STRUCTURAL FIELDS (immediate) ===

  /** The unique node key (always 0 for document root). */
  private long nodeKey;

  /** First child key. */
  private long firstChildKey;

  /** Last child key (same as first for document root). */
  private long lastChildKey;

  // === METADATA FIELDS (lazy) ===

  /** Child count. */
  private long childCount;

  /** Descendant count. */
  private long descendantCount;

  /** The hash code of the node. */
  private long hash;

  // === NON-SERIALIZED FIELDS ===

  /** Hash function for computing node hashes. */
  private LongHashFunction hashFunction;

  /** DeweyID support (always root ID for document root). */
  private SirixDeweyID sirixDeweyID;

  /** DeweyID as bytes. */
  private byte[] deweyIDBytes;

  // ==================== FLYWEIGHT BINDING (LeanStore page-direct access) ====================

  /** Page MemorySegment when bound (null = primitive mode). */
  private MemorySegment page;

  /** Absolute byte offset of this record in the page (after HEAP_START + heapOffset). */
  private long recordBase;

  /** Absolute byte offset where the data region starts (recordBase + 1 + FIELD_COUNT). */
  private long dataRegionStart;

  /** Slot index in the page directory (for re-serialization). */
  private int slotIndex;

  private static final int FIELD_COUNT = NodeFieldLayout.JSON_DOCUMENT_ROOT_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public JsonDocumentRootNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
  }

  /**
   * Primary constructor with all primitive fields.
   * Used by deserialization (NodeKind.JSON_DOCUMENT.deserialize).
   *
   * @param nodeKey the node key (always 0 for document root)
   * @param firstChildKey the first child key
   * @param lastChildKey the last child key
   * @param childCount the child count
   * @param descendantCount the descendant count
   * @param hashFunction the hash function
   */
  public JsonDocumentRootNode(long nodeKey, long firstChildKey, long lastChildKey,
      long childCount, long descendantCount, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = SirixDeweyID.newRootID().toBytes();
  }

  /**
   * Constructor with SirixDeweyID.
   *
   * @param nodeKey the node key
   * @param firstChildKey the first child key
   * @param lastChildKey the last child key
   * @param childCount the child count
   * @param descendantCount the descendant count
   * @param hashFunction the hash function
   * @param deweyID the DeweyID
   */
  public JsonDocumentRootNode(long nodeKey, long firstChildKey, long lastChildKey,
      long childCount, long descendantCount, LongHashFunction hashFunction,
      SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
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
    this.firstChildKey = readDeltaField(NodeFieldLayout.JDOCROOT_FIRST_CHILD_KEY, nk);
    this.lastChildKey = readDeltaField(NodeFieldLayout.JDOCROOT_LAST_CHILD_KEY, nk);
    this.childCount = readSignedLongField(NodeFieldLayout.JDOCROOT_CHILD_COUNT);
    this.descendantCount = readSignedLongField(NodeFieldLayout.JDOCROOT_DESCENDANT_COUNT);
    this.hash = readLongField(NodeFieldLayout.JDOCROOT_HASH);
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
   * Write a delta-encoded field in-place if width matches, otherwise unbind and re-serialize.
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
    long pos = offset;

    // Write nodeKind byte
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.JSON_DOCUMENT.getId());
    pos++;

    // Reserve space for offset table (will be written after computing offsets)
    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    // Data region start
    final long dataStart = pos;
    final int[] offsets = new int[FIELD_COUNT];

    // Field 0: firstChildKey (delta-varint from nodeKey)
    offsets[NodeFieldLayout.JDOCROOT_FIRST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, firstChildKey, nodeKey);

    // Field 1: lastChildKey (delta-varint from nodeKey)
    offsets[NodeFieldLayout.JDOCROOT_LAST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, lastChildKey, nodeKey);

    // Field 2: childCount (signed long varint)
    offsets[NodeFieldLayout.JDOCROOT_CHILD_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, childCount);

    // Field 3: descendantCount (signed long varint)
    offsets[NodeFieldLayout.JDOCROOT_DESCENDANT_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, descendantCount);

    // Field 4: previousRevision (signed varint) — always 0 for document root
    offsets[NodeFieldLayout.JDOCROOT_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, 0);

    // Field 5: lastModifiedRevision (signed varint) — always 0 for document root
    offsets[NodeFieldLayout.JDOCROOT_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, 0);

    // Field 6: hash (fixed 8 bytes)
    offsets[NodeFieldLayout.JDOCROOT_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    // Write offset table
    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) offsets[i]);
    }

    return (int) (pos - offset);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.JSON_DOCUMENT;
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
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public void setParentKey(long parentKey) {
    // Document root has no parent - ignored
  }

  @Override
  public boolean hasParent() {
    return false;
  }

  @Override
  public long getRightSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public void setRightSiblingKey(long key) {
    // Document root has no siblings - ignored
  }

  @Override
  public boolean hasRightSibling() {
    return false;
  }

  @Override
  public long getLeftSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public void setLeftSiblingKey(long key) {
    // Document root has no siblings - ignored
  }

  @Override
  public boolean hasLeftSibling() {
    return false;
  }

  @Override
  public long getFirstChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.JDOCROOT_FIRST_CHILD_KEY, nodeKey);
    }
    return firstChildKey;
  }

  @Override
  public void setFirstChildKey(long key) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.JDOCROOT_FIRST_CHILD_KEY, key);
      if (page != null) return; // In-place write succeeded
    }
    this.firstChildKey = key;
  }

  @Override
  public boolean hasFirstChild() {
    return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLastChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.JDOCROOT_LAST_CHILD_KEY, nodeKey);
    }
    return lastChildKey;
  }

  @Override
  public void setLastChildKey(long key) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.JDOCROOT_LAST_CHILD_KEY, key);
      if (page != null) return; // In-place write succeeded
    }
    this.lastChildKey = key;
  }

  @Override
  public boolean hasLastChild() {
    return getLastChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getChildCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.JDOCROOT_CHILD_COUNT);
    }
    return childCount;
  }

  public void setChildCount(long childCount) {
    if (page != null) {
      if (setSignedLongFieldInPlace(NodeFieldLayout.JDOCROOT_CHILD_COUNT, childCount)) return;
      // Width changed — unbind already happened in the helper
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
      return readSignedLongField(NodeFieldLayout.JDOCROOT_DESCENDANT_COUNT);
    }
    return descendantCount;
  }

  @Override
  public void setDescendantCount(long descendantCount) {
    if (page != null) {
      if (setSignedLongFieldInPlace(NodeFieldLayout.JDOCROOT_DESCENDANT_COUNT, descendantCount)) return;
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
  public long computeHash(BytesOut<?> bytes) {
    if (hashFunction == null) {
      return 0L;
    }

    bytes.clear();
    bytes.writeLong(nodeKey)
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

    final var buffer = ((java.nio.ByteBuffer) bytes.underlyingObject()).rewind();
    buffer.limit((int) bytes.readLimit());

    return hashFunction.hashBytes(buffer);
  }

  @Override
  public void setHash(long hash) {
    if (page != null) {
      // Hash is ALWAYS in-place (fixed 8 bytes)
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.JDOCROOT_HASH) & 0xFF;
      DeltaVarIntCodec.writeLongToSegment(page, dataRegionStart + fieldOff, hash);
      return;
    }
    this.hash = hash;
  }

  @Override
  public long getHash() {
    if (page != null) {
      return readLongField(NodeFieldLayout.JDOCROOT_HASH);
    }
    return hash;
  }

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.JDOCROOT_PREV_REVISION);
    }
    return 0; // Document root is always in revision 0
  }

  @Override
  public void setPreviousRevision(int revision) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.JDOCROOT_PREV_REVISION, revision)) return;
    }
    // Document root doesn't track previous revision in primitive mode
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.JDOCROOT_LAST_MOD_REVISION);
    }
    return 0;
  }

  @Override
  public void setLastModifiedRevision(int revision) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.JDOCROOT_LAST_MOD_REVISION, revision)) return;
    }
    // Document root doesn't track last modified revision in primitive mode
  }

  @Override
  public boolean isSameItem(@Nullable Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  @Override
  public void setTypeKey(int typeKey) {
    // Not supported for JSON nodes
  }

  @Override
  public SirixDeweyID getDeweyID() {
    if (sirixDeweyID == null && deweyIDBytes != null) {
      sirixDeweyID = new SirixDeweyID(deweyIDBytes);
    }
    if (sirixDeweyID == null) {
      sirixDeweyID = SirixDeweyID.newRootID();
    }
    return sirixDeweyID;
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    if (deweyIDBytes == null && sirixDeweyID != null) {
      deweyIDBytes = sirixDeweyID.toBytes();
    }
    if (deweyIDBytes == null) {
      deweyIDBytes = SirixDeweyID.newRootID().toBytes();
    }
    return deweyIDBytes;
  }

  @Override
  public void setDeweyID(SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = null;
  }

  /**
   * Get the hash function.
   *
   * @return the hash function
   */
  public LongHashFunction getHashFunction() {
    return hashFunction;
  }

  /**
   * Create a deep copy snapshot of this node.
   *
   * @return a new instance with copied values
   */
  public JsonDocumentRootNode toSnapshot() {
    if (page != null) {
      // Bound mode: read all fields from page
      final long nk = this.nodeKey;
      final JsonDocumentRootNode snapshot = new JsonDocumentRootNode(
          nk,
          readDeltaField(NodeFieldLayout.JDOCROOT_FIRST_CHILD_KEY, nk),
          readDeltaField(NodeFieldLayout.JDOCROOT_LAST_CHILD_KEY, nk),
          readSignedLongField(NodeFieldLayout.JDOCROOT_CHILD_COUNT),
          readSignedLongField(NodeFieldLayout.JDOCROOT_DESCENDANT_COUNT),
          hashFunction);
      snapshot.hash = readLongField(NodeFieldLayout.JDOCROOT_HASH);
      if (deweyIDBytes != null) {
        snapshot.deweyIDBytes = deweyIDBytes.clone();
      }
      if (sirixDeweyID != null) {
        snapshot.sirixDeweyID = sirixDeweyID;
      }
      return snapshot;
    }
    final JsonDocumentRootNode snapshot = new JsonDocumentRootNode(
        nodeKey, firstChildKey, lastChildKey, childCount, descendantCount, hashFunction);
    snapshot.hash = this.hash;
    if (deweyIDBytes != null) {
      snapshot.deweyIDBytes = deweyIDBytes.clone();
    }
    if (sirixDeweyID != null) {
      snapshot.sirixDeweyID = sirixDeweyID;
    }
    return snapshot;
  }

  @Override
  public VisitResult acceptVisitor(JsonNodeVisitor visitor) {
    return visitor.visit(ImmutableJsonDocumentRootNode.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey);
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj instanceof JsonDocumentRootNode other) {
      return nodeKey == other.nodeKey;
    }
    return false;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("firstChildKey", firstChildKey)
                      .add("lastChildKey", lastChildKey)
                      .add("childCount", childCount)
                      .add("descendantCount", descendantCount)
                      .add("hash", hash)
                      .toString();
  }
}
