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
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.BytesIn;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.xml.ImmutableXmlDocumentRootNode;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Fixed;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Node representing the root of an XML document. This node is guaranteed to exist in revision 0 and
 * cannot be removed.
 *
 * <p>Uses primitive fields for efficient storage following the ObjectNode pattern.
 * Document root has fixed values for nodeKey (0), parentKey (-1), and no siblings.</p>
 *
 * <p>Supports flyweight binding to a page MemorySegment for zero-copy field access.</p>
 */
public final class XmlDocumentRootNode implements StructNode, ImmutableXmlNode, FlyweightNode {

  // === STRUCTURAL FIELDS (immediate) ===

  /** The unique node key (always 0 for document root). */
  private long nodeKey;

  /** First child key. */
  private long firstChildKey;

  /** Last child key. */
  private long lastChildKey;

  // === METADATA FIELDS ===

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

  /** True if this node is a factory-managed write singleton (must not be stored in records[]). */
  private boolean writeSingleton;

  /** Owning page for resize-in-place operations. */
  private KeyValueLeafPage ownerPage;

  /** Reusable offset array for serializeToHeap (avoids allocation). */
  private final int[] heapOffsets;

  private static final int FIELD_COUNT = NodeFieldLayout.XML_DOCUMENT_ROOT_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public XmlDocumentRootNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.heapOffsets = new int[FIELD_COUNT];
  }

  /**
   * Primary constructor with all primitive fields. Used by deserialization
   * (NodeKind.XML_DOCUMENT.deserialize).
   *
   * @param nodeKey the node key (always 0 for document root)
   * @param firstChildKey the first child key
   * @param lastChildKey the last child key
   * @param childCount the child count
   * @param descendantCount the descendant count
   * @param hashFunction the hash function
   */
  public XmlDocumentRootNode(long nodeKey, long firstChildKey, long lastChildKey, long childCount, long descendantCount,
      LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = SirixDeweyID.newRootID().toBytes();
    this.heapOffsets = new int[FIELD_COUNT];
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
  public XmlDocumentRootNode(long nodeKey, long firstChildKey, long lastChildKey, long childCount, long descendantCount,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
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
  @Override
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
  @Override
  public void unbind() {
    if (page == null) {
      return;
    }
    // Materialize all fields from page to Java primitives
    final long nk = this.nodeKey;
    this.firstChildKey = readDeltaField(NodeFieldLayout.XDOCROOT_FIRST_CHILD_KEY, nk);
    this.lastChildKey = readDeltaField(NodeFieldLayout.XDOCROOT_LAST_CHILD_KEY, nk);
    this.childCount = readSignedLongField(NodeFieldLayout.XDOCROOT_CHILD_COUNT);
    this.descendantCount = readSignedLongField(NodeFieldLayout.XDOCROOT_DESCENDANT_COUNT);
    this.hash = readLongField(NodeFieldLayout.XDOCROOT_HASH);
    this.ownerPage = null;
    this.page = null;
  }

  @Override
  public void clearBinding() {
    this.page = null;
    this.ownerPage = null;
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

  @Override public boolean isWriteSingleton() { return writeSingleton; }
  @Override public void setWriteSingleton(final boolean ws) { this.writeSingleton = ws; }
  @Override public KeyValueLeafPage getOwnerPage() { return ownerPage; }
  @Override public void setOwnerPage(final KeyValueLeafPage p) { this.ownerPage = p; }

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

  // ==================== DIRECT WRITE ====================

  /**
   * Encode an XmlDocumentRootNode record directly to a MemorySegment from parameter values.
   * Static -- reads nothing from any instance. Zero field intermediation.
   *
   * @param target          the target MemorySegment (reinterpreted slotted page)
   * @param offset          absolute byte offset to write at
   * @param heapOffsets     pre-allocated offset array (reused, FIELD_COUNT elements)
   * @param nodeKey         the node key (delta base for structural keys)
   * @param firstChildKey   the first child key
   * @param lastChildKey    the last child key
   * @param childCount      the child count
   * @param descendantCount the descendant count
   * @param hash            the hash value
   * @return the total number of bytes written
   */
  public static int writeNewRecord(final MemorySegment target, final long offset,
      final int[] heapOffsets, final long nodeKey,
      final long firstChildKey, final long lastChildKey,
      final long childCount, final long descendantCount, final long hash) {
    long pos = offset;

    // Write nodeKind byte
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.XML_DOCUMENT.getId());
    pos++;

    // Reserve space for offset table
    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    // Data region start
    final long dataStart = pos;

    // Field 0: firstChildKey (delta-varint from nodeKey)
    heapOffsets[NodeFieldLayout.XDOCROOT_FIRST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, firstChildKey, nodeKey);

    // Field 1: lastChildKey (delta-varint from nodeKey)
    heapOffsets[NodeFieldLayout.XDOCROOT_LAST_CHILD_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, lastChildKey, nodeKey);

    // Field 2: childCount (signed long varint)
    heapOffsets[NodeFieldLayout.XDOCROOT_CHILD_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, childCount);

    // Field 3: descendantCount (signed long varint)
    heapOffsets[NodeFieldLayout.XDOCROOT_DESCENDANT_COUNT] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedLongToSegment(target, pos, descendantCount);

    // Field 4: previousRevision (signed varint) -- always 0 for document root
    heapOffsets[NodeFieldLayout.XDOCROOT_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, 0);

    // Field 5: lastModifiedRevision (signed varint) -- always 0 for document root
    heapOffsets[NodeFieldLayout.XDOCROOT_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, 0);

    // Field 6: hash (fixed 8 bytes)
    heapOffsets[NodeFieldLayout.XDOCROOT_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    // Write offset table
    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) heapOffsets[i]);
    }

    return (int) (pos - offset);
  }

  /**
   * Serialize this node from Java fields. Delegates to static writeNewRecord.
   */
  @Override
  public int serializeToHeap(final MemorySegment target, final long offset) {
    return writeNewRecord(target, offset, heapOffsets, nodeKey,
        firstChildKey, lastChildKey, childCount, descendantCount, hash);
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
    return NodeKind.XML_DOCUMENT;
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
      return readDeltaField(NodeFieldLayout.XDOCROOT_FIRST_CHILD_KEY, nodeKey);
    }
    return firstChildKey;
  }

  @Override
  public void setFirstChildKey(long key) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.XDOCROOT_FIRST_CHILD_KEY) & 0xFF;
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
        NodeFieldLayout.XDOCROOT_FIRST_CHILD_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, key, nodeKey));
  }

  @Override
  public boolean hasFirstChild() {
    return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLastChildKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.XDOCROOT_LAST_CHILD_KEY, nodeKey);
    }
    return lastChildKey;
  }

  @Override
  public void setLastChildKey(long key) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.XDOCROOT_LAST_CHILD_KEY) & 0xFF;
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
        NodeFieldLayout.XDOCROOT_LAST_CHILD_KEY, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, key, nodeKey));
  }

  @Override
  public boolean hasLastChild() {
    return getLastChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getChildCount() {
    if (page != null) {
      return readSignedLongField(NodeFieldLayout.XDOCROOT_CHILD_COUNT);
    }
    return childCount;
  }

  public void setChildCount(long childCount) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.XDOCROOT_CHILD_COUNT) & 0xFF;
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
        NodeFieldLayout.XDOCROOT_CHILD_COUNT, FIELD_COUNT,
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
      return readSignedLongField(NodeFieldLayout.XDOCROOT_DESCENDANT_COUNT);
    }
    return descendantCount;
  }

  @Override
  public void setDescendantCount(long descendantCount) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.XDOCROOT_DESCENDANT_COUNT) & 0xFF;
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
        NodeFieldLayout.XDOCROOT_DESCENDANT_COUNT, FIELD_COUNT,
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
          recordBase + 1 + NodeFieldLayout.XDOCROOT_HASH) & 0xFF;
      DeltaVarIntCodec.writeLongToSegment(page, dataRegionStart + fieldOff, hash);
      return;
    }
    this.hash = hash;
  }

  @Override
  public long getHash() {
    if (page != null) {
      final long h = readLongField(NodeFieldLayout.XDOCROOT_HASH);
      if (h != 0L) return h;
      if (hashFunction != null) {
        final long computed = computeHash(Bytes.threadLocalHashBuffer());
        setHash(computed);
        return computed;
      }
      return 0L;
    }
    if (hash == 0L && hashFunction != null) {
      hash = computeHash(Bytes.threadLocalHashBuffer());
    }
    return hash;
  }

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.XDOCROOT_PREV_REVISION);
    }
    return 0; // Document root is always in revision 0
  }

  @Override
  public void setPreviousRevision(int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.XDOCROOT_PREV_REVISION) & 0xFF;
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
    // Document root doesn't track previous revision in primitive mode
  }

  private void resizePreviousRevision(final int revision) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.XDOCROOT_PREV_REVISION, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.XDOCROOT_LAST_MOD_REVISION);
    }
    return 0;
  }

  @Override
  public void setLastModifiedRevision(int revision) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.XDOCROOT_LAST_MOD_REVISION) & 0xFF;
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
    // Document root doesn't track last modified revision in primitive mode
  }

  private void resizeLastModifiedRevision(final int revision) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.XDOCROOT_LAST_MOD_REVISION, FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public boolean isSameItem(@Nullable Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  @Override
  public void setTypeKey(int typeKey) {
    // Not supported for document root
  }

  @Override
  public int getTypeKey() {
    return 0;
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

  /**
   * Get the hash function.
   *
   * @return the hash function
   */
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

    final long firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    final long childCount = firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty()
        ? 0L
        : 1L;

    final long hash;
    final long descendantCount;
    if (config.hashType != HashType.NONE) {
      hash = source.readLong();
      descendantCount = DeltaVarIntCodec.decodeSignedLong(source);
    } else {
      hash = 0L;
      descendantCount = 0L;
    }

    this.nodeKey = Fixed.DOCUMENT_NODE_KEY.getStandardProperty();
    this.firstChildKey = firstChildKey;
    this.lastChildKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    this.childCount = childCount;
    this.hash = hash;
    this.descendantCount = descendantCount;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
  }

  /**
   * Create a deep copy snapshot of this node.
   *
   * @return a new instance with copied values
   */
  public XmlDocumentRootNode toSnapshot() {
    if (page != null) {
      // Bound mode: read all fields from page
      final long nk = this.nodeKey;
      final XmlDocumentRootNode snapshot = new XmlDocumentRootNode(
          nk,
          readDeltaField(NodeFieldLayout.XDOCROOT_FIRST_CHILD_KEY, nk),
          readDeltaField(NodeFieldLayout.XDOCROOT_LAST_CHILD_KEY, nk),
          readSignedLongField(NodeFieldLayout.XDOCROOT_CHILD_COUNT),
          readSignedLongField(NodeFieldLayout.XDOCROOT_DESCENDANT_COUNT),
          hashFunction);
      snapshot.hash = readLongField(NodeFieldLayout.XDOCROOT_HASH);
      if (deweyIDBytes != null) {
        snapshot.deweyIDBytes = deweyIDBytes.clone();
      }
      if (sirixDeweyID != null) {
        snapshot.sirixDeweyID = sirixDeweyID;
      }
      return snapshot;
    }
    final XmlDocumentRootNode snapshot = new XmlDocumentRootNode(
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
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableXmlDocumentRootNode.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey);
  }

  @Override
  public boolean equals(@Nullable Object obj) {
    if (obj instanceof XmlDocumentRootNode other) {
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
