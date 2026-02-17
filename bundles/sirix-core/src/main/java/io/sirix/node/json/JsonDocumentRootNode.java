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
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.json.ImmutableJsonDocumentRootNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.node.layout.NodeKindLayout;
import io.sirix.node.layout.SlotLayoutAccessors;
import io.sirix.node.layout.StructuralField;
import io.sirix.settings.Fixed;

import java.lang.foreign.MemorySegment;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Node representing the root of a JSON document. This node is guaranteed to exist in revision 0 and
 * cannot be removed.
 *
 * <p>Uses primitive fields for efficient storage following the ObjectNode pattern.
 * Document root has fixed values for nodeKey (0), parentKey (-1), and no siblings.</p>
 */
public final class JsonDocumentRootNode implements StructNode, ImmutableJsonNode {

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

  // Fixed-slot lazy support
  private Object lazySource;
  private NodeKindLayout fixedSlotLayout;
  private boolean lazyFieldsParsed = true;

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
    return firstChildKey;
  }

  @Override
  public void setFirstChildKey(long key) {
    this.firstChildKey = key;
  }

  @Override
  public boolean hasFirstChild() {
    return firstChildKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLastChildKey() {
    return lastChildKey;
  }

  @Override
  public void setLastChildKey(long key) {
    this.lastChildKey = key;
  }

  @Override
  public boolean hasLastChild() {
    return lastChildKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getChildCount() {
    if (!lazyFieldsParsed) parseLazyFields();
    return childCount;
  }

  public void setChildCount(long childCount) {
    this.childCount = childCount;
  }

  @Override
  public void incrementChildCount() {
    if (!lazyFieldsParsed) parseLazyFields();
    childCount++;
  }

  @Override
  public void decrementChildCount() {
    if (!lazyFieldsParsed) parseLazyFields();
    childCount--;
  }

  @Override
  public long getDescendantCount() {
    if (!lazyFieldsParsed) parseLazyFields();
    return descendantCount;
  }

  @Override
  public void setDescendantCount(long descendantCount) {
    this.descendantCount = descendantCount;
  }

  @Override
  public void incrementDescendantCount() {
    if (!lazyFieldsParsed) parseLazyFields();
    descendantCount++;
  }

  @Override
  public void decrementDescendantCount() {
    if (!lazyFieldsParsed) parseLazyFields();
    descendantCount--;
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

    bytes.writeLong(childCount)
         .writeLong(descendantCount)
         .writeLong(getLeftSiblingKey())
         .writeLong(getRightSiblingKey())
         .writeLong(firstChildKey);

    if (lastChildKey != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      bytes.writeLong(lastChildKey);
    }

    final var buffer = ((java.nio.ByteBuffer) bytes.underlyingObject()).rewind();
    buffer.limit((int) bytes.readLimit());

    return hashFunction.hashBytes(buffer);
  }

  @Override
  public void setHash(long hash) {
    this.hash = hash;
  }

  @Override
  public long getHash() {
    if (!lazyFieldsParsed) parseLazyFields();
    return hash;
  }

  @Override
  public int getPreviousRevisionNumber() {
    return 0; // Document root is always in revision 0
  }

  @Override
  public void setPreviousRevision(int revision) {
    // Document root doesn't track previous revision
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return 0;
  }

  @Override
  public void setLastModifiedRevision(int revision) {
    // Document root doesn't track last modified revision
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

  public void setDeweyIDBytes(final byte[] deweyIDBytes) {
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
  public void readFrom(BytesIn<?> source, long nodeKey, byte[] deweyId,
      LongHashFunction hashFunction, ResourceConfiguration config) {
    final long firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

    this.nodeKey = Fixed.DOCUMENT_NODE_KEY.getStandardProperty();
    this.firstChildKey = firstChildKey;
    this.lastChildKey = firstChildKey;
    this.childCount = firstChildKey == Fixed.NULL_NODE_KEY.getStandardProperty() ? 0L : 1L;
    this.descendantCount = DeltaVarIntCodec.decodeSignedLong(source);
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    this.hash = 0L;
  }

  public void bindFixedSlotLazy(final MemorySegment slotData, final NodeKindLayout layout) {
    this.lazySource = slotData;
    this.fixedSlotLayout = layout;
    this.lazyFieldsParsed = false;
  }

  private void parseLazyFields() {
    if (lazyFieldsParsed) {
      return;
    }

    if (fixedSlotLayout != null) {
      final MemorySegment sd = (MemorySegment) lazySource;
      final NodeKindLayout ly = fixedSlotLayout;
      this.childCount = SlotLayoutAccessors.readLongField(sd, ly, StructuralField.CHILD_COUNT);
      this.descendantCount = SlotLayoutAccessors.readLongField(sd, ly, StructuralField.DESCENDANT_COUNT);
      this.hash = SlotLayoutAccessors.readLongField(sd, ly, StructuralField.HASH);
      this.fixedSlotLayout = null;
      this.lazyFieldsParsed = true;
      return;
    }

    this.lazyFieldsParsed = true;
  }

  /**
   * Create a deep copy snapshot of this node.
   *
   * @return a new instance with copied values
   */
  public JsonDocumentRootNode toSnapshot() {
    if (!lazyFieldsParsed) parseLazyFields();
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
