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
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
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
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.interfaces.Node;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.xml.ImmutablePI;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Processing Instruction node implementation backed by MemorySegment.
 *
 * <p><strong>All instances are backed by MemorySegment for consistent memory layout.</strong></p>
 * <p><strong>Uses MemoryLayout and VarHandles for type-safe field access.</strong></p>
 * <p><strong>This class is not part of the public API and might change.</strong></p>
 */
public final class PINode implements StructNode, NameNode, ValueNode, ImmutableXmlNode {

  // MemorySegment layout with FIXED offsets (same as ElementNode):
  // NodeDelegate data (16 bytes):
  //   - parentKey (8 bytes)            - offset 0
  //   - previousRevision (4 bytes)     - offset 8
  //   - lastModifiedRevision (4 bytes) - offset 12
  // Fixed StructNode fields (32 bytes):
  //   - rightSiblingKey (8 bytes)      - offset 16
  //   - leftSiblingKey (8 bytes)       - offset 24
  //   - firstChildKey (8 bytes)        - offset 32
  //   - lastChildKey (8 bytes)         - offset 40
  // NameNode fields (20 bytes):
  //   - pathNodeKey (8 bytes)          - offset 48
  //   - prefixKey (4 bytes)            - offset 56
  //   - localNameKey (4 bytes)         - offset 60
  //   - uriKey (4 bytes)               - offset 64
  // Optional fields:
  //   - childCount (8 bytes)           - offset 68 (if storeChildCount)
  //   - hash (8 bytes)                 - offset 68/76 (if hashType != NONE)
  //   - descendantCount (8 bytes)      - after hash (if hashType != NONE)

  /**
   * Core layout (always present) - 68 bytes total
   */
  public static final MemoryLayout CORE_LAYOUT = MemoryLayout.structLayout(
      // NodeDelegate fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("parentKey"),                    // offset 0
      ValueLayout.JAVA_INT_UNALIGNED.withName("previousRevision"),              // offset 8
      ValueLayout.JAVA_INT_UNALIGNED.withName("lastModifiedRevision"),          // offset 12
      // StructNode fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("rightSiblingKey"),              // offset 16
      ValueLayout.JAVA_LONG_UNALIGNED.withName("leftSiblingKey"),               // offset 24
      ValueLayout.JAVA_LONG_UNALIGNED.withName("firstChildKey"),                // offset 32
      ValueLayout.JAVA_LONG_UNALIGNED.withName("lastChildKey"),                 // offset 40
      // NameNode fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("pathNodeKey"),                  // offset 48
      ValueLayout.JAVA_INT_UNALIGNED.withName("prefixKey"),                     // offset 56
      ValueLayout.JAVA_INT_UNALIGNED.withName("localNameKey"),                  // offset 60
      ValueLayout.JAVA_INT_UNALIGNED.withName("uriKey")                         // offset 64
  );

  /**
   * Optional child count layout (only when storeChildCount == true) - 8 bytes
   */
  public static final MemoryLayout CHILD_COUNT_LAYOUT = MemoryLayout.structLayout(
      ValueLayout.JAVA_LONG_UNALIGNED.withName("childCount")
  );

  /**
   * Optional hash layout (only when hashType != NONE) - 16 bytes
   */
  public static final MemoryLayout HASH_LAYOUT = MemoryLayout.structLayout(
      ValueLayout.JAVA_LONG_UNALIGNED.withName("hash"),                         // variable offset
      ValueLayout.JAVA_LONG_UNALIGNED.withName("descendantCount")               // after hash
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
  private static final VarHandle FIRST_CHILD_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("firstChildKey"));
  private static final VarHandle LAST_CHILD_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("lastChildKey"));
  private static final VarHandle PATH_NODE_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("pathNodeKey"));
  private static final VarHandle PREFIX_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("prefixKey"));
  private static final VarHandle LOCAL_NAME_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("localNameKey"));
  private static final VarHandle URI_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("uriKey"));

  /** The underlying MemorySegment storing node data. */
  private final MemorySegment segment;

  /** The node key. Non-final for singleton node interface compliance. */
  private long nodeKey;

  /** The hash function for this node. */
  private final LongHashFunction hashFunction;
  
  /** Resource configuration (stored for toSnapshot). */
  private final ResourceConfiguration resourceConfig;

  /** Optional Dewey ID. */
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  /** The PI content value (lazily loaded). */
  private byte[] value;

  /** Whether the value is compressed. */
  private final boolean isCompressed;

  /** Cached offsets for optional fields (computed once at construction). */
  private final long childCountOffset;
  private final long hashOffset;
  private final long descendantCountOffset;

  /**
   * Constructor for MemorySegment-based PINode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param deweyID        optional DeweyID bytes
   * @param resourceConfig the resource configuration
   * @param hashFunction   hash function for computing node hashes
   * @param value          the PI content value
   * @param isCompressed   whether the value is compressed
   */
  public PINode(final MemorySegment segment, final long nodeKey, final byte[] deweyID,
      final ResourceConfiguration resourceConfig, final LongHashFunction hashFunction,
      final byte[] value, final boolean isCompressed) {
    this(segment, nodeKey, deweyID != null ? new SirixDeweyID(deweyID) : null, 
         resourceConfig, hashFunction, value, isCompressed);
    this.deweyIDBytes = deweyID;
  }

  /**
   * Constructor for MemorySegment-based PINode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param id             optional DeweyID
   * @param resourceConfig the resource configuration
   * @param hashFunction   hash function for computing node hashes
   * @param value          the PI content value
   * @param isCompressed   whether the value is compressed
   */
  public PINode(final MemorySegment segment, final long nodeKey, final SirixDeweyID id,
      final ResourceConfiguration resourceConfig, final LongHashFunction hashFunction,
      final byte[] value, final boolean isCompressed) {
    assert segment != null;
    this.segment = segment;
    this.nodeKey = nodeKey;
    this.sirixDeweyID = id;
    assert hashFunction != null;
    this.hashFunction = hashFunction;
    assert resourceConfig != null;
    this.resourceConfig = resourceConfig;
    assert value != null;
    this.value = value;
    this.isCompressed = isCompressed;

    // Compute offsets once for maximum performance
    long currentOffset = CORE_LAYOUT.byteSize();
    
    if (resourceConfig.storeChildCount()) {
      this.childCountOffset = currentOffset;
      currentOffset += CHILD_COUNT_LAYOUT.byteSize();
    } else {
      this.childCountOffset = -1;
    }
    
    if (resourceConfig.hashType != HashType.NONE) {
      this.hashOffset = currentOffset;
      this.descendantCountOffset = currentOffset + 8;
    } else {
      this.hashOffset = -1;
      this.descendantCountOffset = -1;
    }
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.PROCESSING_INSTRUCTION;
  }

  @Override
  public long computeHash(BytesOut<?> bytes) {
    bytes.clear();

    bytes.writeLong(nodeKey)
         .writeLong(getParentKey())
         .writeByte(NodeKind.PROCESSING_INSTRUCTION.getId());

    bytes.writeLong(getChildCount())
         .writeLong(getDescendantCount())
         .writeLong(getLeftSiblingKey())
         .writeLong(getRightSiblingKey())
         .writeLong(getFirstChildKey());

    if (getLastChildKey() != Fixed.INVALID_KEY_FOR_TYPE_CHECK.getStandardProperty()) {
      bytes.writeLong(getLastChildKey());
    }

    bytes.writeLong(getPrefixKey())
         .writeLong(getLocalNameKey())
         .writeLong(getURIKey());

    bytes.writeUtf8(new String(getRawValue(), Constants.DEFAULT_ENCODING));

    final var buffer = ((java.nio.ByteBuffer) bytes.underlyingObject()).rewind();
    buffer.limit((int) bytes.readLimit());

    return hashFunction.hashBytes(buffer);
  }

  @Override
  public void setHash(final long hash) {
    if (hashOffset >= 0) {
      segment.set(ValueLayout.JAVA_LONG_UNALIGNED, hashOffset, hash);
    }
  }

  @Override
  public long getHash() {
    if (hashOffset >= 0) {
      long hash = segment.get(ValueLayout.JAVA_LONG_UNALIGNED, hashOffset);
      if (hash == 0L) {
        hash = computeHash(Bytes.elasticOffHeapByteBuffer());
        setHash(hash);
      }
      return hash;
    }
    return 0L;
  }

  @Override
  public byte[] getRawValue() {
    return value;
  }

  @Override
  public void setRawValue(final byte[] value) {
    this.value = value;
    setHash(0L); // Invalidate hash
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
    // Processing instructions always have xs:untyped type
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
    return (long) FIRST_CHILD_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setFirstChildKey(final long key) {
    FIRST_CHILD_KEY_HANDLE.set(segment, 0L, key);
  }

  @Override
  public boolean hasFirstChild() {
    return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLastChildKey() {
    return (long) LAST_CHILD_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setLastChildKey(final long key) {
    LAST_CHILD_KEY_HANDLE.set(segment, 0L, key);
  }

  @Override
  public boolean hasLastChild() {
    return getLastChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getChildCount() {
    if (childCountOffset >= 0) {
      return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, childCountOffset);
    }
    return 0;
  }

  @Override
  public void decrementChildCount() {
    if (childCountOffset >= 0) {
      long count = getChildCount();
      if (count > 0) {
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, childCountOffset, count - 1);
      }
    }
  }

  @Override
  public void incrementChildCount() {
    if (childCountOffset >= 0) {
      segment.set(ValueLayout.JAVA_LONG_UNALIGNED, childCountOffset, getChildCount() + 1);
    }
  }

  public void setChildCount(final long childCount) {
    if (childCountOffset >= 0) {
      segment.set(ValueLayout.JAVA_LONG_UNALIGNED, childCountOffset, childCount);
    }
  }

  @Override
  public long getDescendantCount() {
    if (descendantCountOffset >= 0) {
      return segment.get(ValueLayout.JAVA_LONG_UNALIGNED, descendantCountOffset);
    }
    return 0;
  }

  @Override
  public void decrementDescendantCount() {
    if (descendantCountOffset >= 0) {
      long count = getDescendantCount();
      if (count > 0) {
        segment.set(ValueLayout.JAVA_LONG_UNALIGNED, descendantCountOffset, count - 1);
      }
    }
  }

  @Override
  public void incrementDescendantCount() {
    if (descendantCountOffset >= 0) {
      segment.set(ValueLayout.JAVA_LONG_UNALIGNED, descendantCountOffset, getDescendantCount() + 1);
    }
  }

  @Override
  public void setDescendantCount(final long descendantCount) {
    if (descendantCountOffset >= 0) {
      segment.set(ValueLayout.JAVA_LONG_UNALIGNED, descendantCountOffset, descendantCount);
    }
  }

  // NameNode methods
  @Override
  public int getPrefixKey() {
    return (int) PREFIX_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    PREFIX_KEY_HANDLE.set(segment, 0L, prefixKey);
    setHash(0L); // Invalidate hash
  }

  @Override
  public int getLocalNameKey() {
    return (int) LOCAL_NAME_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    LOCAL_NAME_KEY_HANDLE.set(segment, 0L, localNameKey);
    setHash(0L); // Invalidate hash
  }

  @Override
  public int getURIKey() {
    return (int) URI_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setURIKey(final int uriKey) {
    URI_KEY_HANDLE.set(segment, 0L, uriKey);
    setHash(0L); // Invalidate hash
  }

  @Override
  public long getPathNodeKey() {
    return (long) PATH_NODE_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    PATH_NODE_KEY_HANDLE.set(segment, 0L, pathNodeKey);
    setHash(0L); // Invalidate hash
  }

  @Override
  public QNm getName() {
    return null;
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutablePI.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, getParentKey(), getPrefixKey(), getLocalNameKey(), 
                           getURIKey(), value);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final PINode other) {
      return nodeKey == other.nodeKey 
          && getParentKey() == other.getParentKey() 
          && getPrefixKey() == other.getPrefixKey()
          && getLocalNameKey() == other.getLocalNameKey()
          && getURIKey() == other.getURIKey()
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
                      .add("firstChildKey", getFirstChildKey())
                      .add("lastChildKey", getLastChildKey())
                      .add("childCount", getChildCount())
                      .add("descendantCount", getDescendantCount())
                      .add("prefixKey", getPrefixKey())
                      .add("localNameKey", getLocalNameKey())
                      .add("uriKey", getURIKey())
                      .add("pathNodeKey", getPathNodeKey())
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

  @Override
  public void setNodeKey(final long nodeKey) {
    this.nodeKey = nodeKey;
  }

  /**
   * Create a deep copy snapshot of this node.
   *
   * @return a new PINode with all values copied
   */
  public PINode toSnapshot() {
    MemorySegment newSegment = MemorySegment.ofArray(new byte[(int) segment.byteSize()]);
    MemorySegment.copy(segment, 0, newSegment, 0, segment.byteSize());
    
    return new PINode(newSegment, nodeKey, 
        deweyIDBytes != null ? deweyIDBytes.clone() : null,
        resourceConfig, hashFunction,
        value != null ? value.clone() : null,
        isCompressed);
  }
}
