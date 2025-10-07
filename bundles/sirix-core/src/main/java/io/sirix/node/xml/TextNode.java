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
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.xml.ImmutableText;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.node.interfaces.Node;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.utils.Compression;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;

/**
 * Text node implementation backed by MemorySegment.
 *
 * <p><strong>All instances are backed by MemorySegment for consistent memory layout.</strong></p>
 * <p><strong>Uses MemoryLayout and VarHandles for type-safe field access.</strong></p>
 * <p><strong>This class is not part of the public API and might change.</strong></p>
 */
public final class TextNode implements StructNode, ValueNode, ImmutableXmlNode {

  // MemorySegment layout with FIXED offsets:
  // NodeDelegate data (16 bytes):
  //   - parentKey (8 bytes)            - offset 0
  //   - previousRevision (4 bytes)     - offset 8
  //   - lastModifiedRevision (4 bytes) - offset 12
  // Sibling keys (16 bytes):
  //   - rightSiblingKey (8 bytes)      - offset 16
  //   - leftSiblingKey (8 bytes)       - offset 24

  /**
   * Core layout (always present) - 32 bytes total
   */
  public static final MemoryLayout CORE_LAYOUT = MemoryLayout.structLayout(
      // NodeDelegate fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("parentKey"),                    // offset 0
      ValueLayout.JAVA_INT.withName("previousRevision"),              // offset 8
      ValueLayout.JAVA_INT.withName("lastModifiedRevision"),          // offset 12
      // Sibling keys only (no child keys for text nodes)
      ValueLayout.JAVA_LONG_UNALIGNED.withName("rightSiblingKey"),              // offset 16
      ValueLayout.JAVA_LONG_UNALIGNED.withName("leftSiblingKey")                // offset 24
  );

  // VarHandles for type-safe field access
  private static final VarHandle PARENT_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("parentKey"));
  private static final VarHandle PREVIOUS_REVISION_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("previousRevision"));
  private static final VarHandle LAST_MODIFIED_REVISION_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("lastModifiedRevision"));
  private static final VarHandle RIGHT_SIBLING_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("rightSiblingKey"));
  private static final VarHandle LEFT_SIBLING_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("leftSiblingKey"));

  /** The underlying MemorySegment storing node data. */
  private final MemorySegment segment;

  /** The node key. */
  private final long nodeKey;

  /** The hash function for this node. */
  private final LongHashFunction hashFunction;

  /** Optional Dewey ID. */
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  /** The text value (lazily loaded). */
  private byte[] value;

  /** Whether the value is compressed. */
  private final boolean isCompressed;

  /** Computed hash (lazily computed). */
  private long hash;

  /**
   * Constructor for MemorySegment-based TextNode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param deweyID        optional DeweyID bytes
   * @param hashFunction   hash function for computing node hashes
   * @param value          the text value
   * @param isCompressed   whether the value is compressed
   */
  public TextNode(final MemorySegment segment, final long nodeKey, final byte[] deweyID,
      final LongHashFunction hashFunction, final byte[] value, final boolean isCompressed) {
    this(segment, nodeKey, deweyID != null ? new SirixDeweyID(deweyID) : null, 
         hashFunction, value, isCompressed);
    this.deweyIDBytes = deweyID;
  }

  /**
   * Constructor for MemorySegment-based TextNode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param id             optional DeweyID
   * @param hashFunction   hash function for computing node hashes
   * @param value          the text value
   * @param isCompressed   whether the value is compressed
   */
  public TextNode(final MemorySegment segment, final long nodeKey, final SirixDeweyID id,
      final LongHashFunction hashFunction, final byte[] value, final boolean isCompressed) {
    assert segment != null;
    this.segment = segment;
    this.nodeKey = nodeKey;
    this.sirixDeweyID = id;
    assert hashFunction != null;
    this.hashFunction = hashFunction;
    assert value != null;
    this.value = value;
    this.isCompressed = isCompressed;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.TEXT;
  }

  @Override
  public long computeHash(BytesOut<?> bytes) {
    bytes.clear();

    bytes.writeLong(nodeKey)
         .writeLong(getParentKey())
         .writeByte(NodeKind.TEXT.getId());

    bytes.writeLong(getLeftSiblingKey()).writeLong(getRightSiblingKey());

    bytes.write(getRawValue());

    final var buffer = ((java.nio.ByteBuffer) bytes.underlyingObject()).rewind();
    buffer.limit((int) bytes.readLimit());

    return hashFunction.hashBytes(buffer);
  }

  @Override
  public void setHash(final long hash) {
    this.hash = hash;
  }

  @Override
  public long getHash() {
    if (hash == 0L) {
      hash = computeHash(Bytes.elasticHeapByteBuffer());
    }
    return hash;
  }

  @Override
  public byte[] getRawValue() {
    if (value == null && isCompressed) {
      // Decompress if needed
      value = Compression.decompress(value);
    }
    return value;
  }

  @Override
  public void setRawValue(final byte[] value) {
    this.value = value;
    this.hash = 0L; // Invalidate hash
  }

  @Override
  public String getValue() {
    return new String(getRawValue(), Constants.DEFAULT_ENCODING);
  }

  // NodeDelegate methods
  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    return (long) PARENT_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setParentKey(final long parentKey) {
    PARENT_KEY_HANDLE.set(segment, 0L, parentKey);
  }

  @Override
  public boolean hasParent() {
    return getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public int getPreviousRevisionNumber() {
    return (int) PREVIOUS_REVISION_HANDLE.get(segment, 0L);
  }

  @Override
  public void setPreviousRevision(final int revision) {
    PREVIOUS_REVISION_HANDLE.set(segment, 0L, revision);
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return (int) LAST_MODIFIED_REVISION_HANDLE.get(segment, 0L);
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    LAST_MODIFIED_REVISION_HANDLE.set(segment, 0L, revision);
  }

  @Override
  public SirixDeweyID getDeweyID() {
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
  public void setDeweyID(final SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = null; // Clear cached bytes
  }

  @Override
  public int getTypeKey() {
    // Text nodes always have xs:untyped type
    return io.sirix.utils.NamePageHash.generateHashForString("xs:untyped");
  }

  @Override
  public void setTypeKey(final int typeKey) {
    // typeKey is not stored, so this is a no-op
  }

  @Override
  public boolean isSameItem(@Nullable Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  // StructNode methods
  @Override
  public long getRightSiblingKey() {
    return (long) RIGHT_SIBLING_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setRightSiblingKey(final long key) {
    RIGHT_SIBLING_KEY_HANDLE.set(segment, 0L, key);
  }

  @Override
  public boolean hasRightSibling() {
    return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLeftSiblingKey() {
    return (long) LEFT_SIBLING_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setLeftSiblingKey(final long key) {
    LEFT_SIBLING_KEY_HANDLE.set(segment, 0L, key);
  }

  @Override
  public boolean hasLeftSibling() {
    return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getFirstChildKey() {
    // Text nodes can't have children
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public void setFirstChildKey(final long key) {
    throw new UnsupportedOperationException("Text nodes can't have children");
  }

  @Override
  public boolean hasFirstChild() {
    return false;
  }

  @Override
  public long getLastChildKey() {
    // Text nodes can't have children
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public void setLastChildKey(final long key) {
    throw new UnsupportedOperationException("Text nodes can't have children");
  }

  @Override
  public boolean hasLastChild() {
    return false;
  }

  @Override
  public long getChildCount() {
    return 0;
  }

  @Override
  public void decrementChildCount() {
    throw new UnsupportedOperationException("Text nodes can't have children");
  }

  @Override
  public void incrementChildCount() {
    throw new UnsupportedOperationException("Text nodes can't have children");
  }

  public void setChildCount(final long childCount) {
    throw new UnsupportedOperationException("Text nodes can't have children");
  }

  @Override
  public long getDescendantCount() {
    return 0;
  }

  @Override
  public void decrementDescendantCount() {
    throw new UnsupportedOperationException("Text nodes can't have descendants");
  }

  @Override
  public void incrementDescendantCount() {
    throw new UnsupportedOperationException("Text nodes can't have descendants");
  }

  @Override
  public void setDescendantCount(final long descendantCount) {
    throw new UnsupportedOperationException("Text nodes can't have descendants");
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableText.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, getParentKey(), value);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final TextNode other) {
      return nodeKey == other.nodeKey 
          && getParentKey() == other.getParentKey() 
          && java.util.Arrays.equals(value, other.value);
    }
    return false;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("parentKey", getParentKey())
                      .add("previousRevision", getPreviousRevisionNumber())
                      .add("lastModifiedRevision", getLastModifiedRevisionNumber())
                      .add("rightSiblingKey", getRightSiblingKey())
                      .add("leftSiblingKey", getLeftSiblingKey())
                      .add("value", getValue())
                      .add("compressed", isCompressed)
                      .toString();
  }

  public boolean isCompressed() {
    return isCompressed;
  }

  public LongHashFunction getHashFunction() {
    return hashFunction;
  }
}
