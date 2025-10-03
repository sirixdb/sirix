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
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * Node representing an object key/field.
 *
 * <p><strong>All instances are backed by MemorySegment for consistent memory layout.</strong></p>
 * <p><strong>Uses MemoryLayout and VarHandles for type-safe field access.</strong></p>
 * <p><strong>This class is not part of the public API and might change.</strong></p>
 */
public final class ObjectKeyNode implements StructNode, NameNode, ImmutableJsonNode {

  // MemorySegment layout with FULLY ALIGNED fields for optimal performance:
  // NodeDelegate data (16 bytes):
  //   - parentKey (8 bytes)            - offset 0
  //   - previousRevision (4 bytes)     - offset 8
  //   - lastModifiedRevision (4 bytes) - offset 12
  // Fixed fields (36 bytes) - longs first for alignment:
  //   - pathNodeKey (8 bytes)          - offset 16 ✅ ALIGNED (16 % 8 = 0)
  //   - rightSiblingKey (8 bytes)      - offset 24 ✅ ALIGNED
  //   - leftSiblingKey (8 bytes)       - offset 32 ✅ ALIGNED
  //   - firstChildKey (8 bytes)        - offset 40 ✅ ALIGNED
  //   - nameKey (4 bytes)              - offset 48 (moved to end)
  // Optional fields (only if hashType != NONE):
  //   - hash (8 bytes)                 - offset 52 (unaligned, unavoidable)
  //   - descendantCount (8 bytes)      - offset 60

  /**
   * Core layout (always present) - 52 bytes total
   * Uses UNALIGNED layouts since the MemorySegment from deserialization 
   * may not be 8-byte aligned (depends on stream position)
   */
  public static final MemoryLayout CORE_LAYOUT = MemoryLayout.structLayout(
      // NodeDelegate fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("parentKey"),                    // offset 0
      ValueLayout.JAVA_INT.withName("previousRevision"),              // offset 8
      ValueLayout.JAVA_INT.withName("lastModifiedRevision"),          // offset 12
      // Fixed ObjectKeyNode fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("pathNodeKey"),                  // offset 16
      ValueLayout.JAVA_LONG_UNALIGNED.withName("rightSiblingKey"),              // offset 24
      ValueLayout.JAVA_LONG_UNALIGNED.withName("leftSiblingKey"),               // offset 32
      ValueLayout.JAVA_LONG_UNALIGNED.withName("firstChildKey"),                // offset 40
      ValueLayout.JAVA_INT.withName("nameKey")                        // offset 48
  );

  /**
   * Optional hash layout (only when hashType != NONE) - 16 bytes
   * Note: These must use UNALIGNED because they come after a 4-byte int at offset 48
   */
  public static final MemoryLayout HASH_LAYOUT = MemoryLayout.structLayout(
      ValueLayout.JAVA_LONG_UNALIGNED.withName("hash"),               // offset 52 (unavoidable unalignment)
      ValueLayout.JAVA_LONG_UNALIGNED.withName("descendantCount")     // offset 60
  );

  // VarHandles for type-safe field access
  private static final VarHandle PARENT_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("parentKey"));
  private static final VarHandle PREVIOUS_REVISION_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("previousRevision"));
  private static final VarHandle LAST_MODIFIED_REVISION_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("lastModifiedRevision"));
  private static final VarHandle PATH_NODE_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("pathNodeKey"));
  private static final VarHandle RIGHT_SIBLING_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("rightSiblingKey"));
  private static final VarHandle LEFT_SIBLING_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("leftSiblingKey"));
  private static final VarHandle FIRST_CHILD_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("firstChildKey"));
  private static final VarHandle NAME_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("nameKey"));
  
  // VarHandles for optional hash fields (offset by CORE_LAYOUT.byteSize())
  private static final VarHandle HASH_HANDLE = 
      HASH_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("hash"));
  private static final VarHandle DESCENDANT_COUNT_HANDLE = 
      HASH_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("descendantCount"));

  // All nodes are MemorySegment-based
  private final MemorySegment segment;
  private final long nodeKey;
  private final ResourceConfiguration resourceConfig;
  
  // DeweyID support (stored separately, not in MemorySegment)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;
  
  // Cache for name (not stored in MemorySegment, only nameKey is serialized)
  private QNm cachedName;
  
  // Cached offsets for maximum performance (computed once at construction)
  private final long hashOffset;
  private final long descendantCountOffset;

  /**
   * Constructor for MemorySegment-based ObjectKeyNode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param resourceConfig the resource configuration
   */
  public ObjectKeyNode(final MemorySegment segment, final long nodeKey, final byte[] deweyID,
      final ResourceConfiguration resourceConfig) {
    this(segment, nodeKey, deweyID != null ? new SirixDeweyID(deweyID) : null, resourceConfig);
    this.deweyIDBytes = deweyID;
  }

  public ObjectKeyNode(final MemorySegment segment, final long nodeKey, final SirixDeweyID id,
      final ResourceConfiguration resourceConfig) {
    this.segment = segment;
    this.nodeKey = nodeKey;
    this.sirixDeweyID = id;
    this.resourceConfig = resourceConfig;
    this.cachedName = null;
    
    // Compute offsets once for maximum performance
    // ObjectKeyNode does not store childCount, hash is after core layout + 4 bytes padding
    this.hashOffset = CORE_LAYOUT.byteSize() + 4; // +4 for alignment padding
    this.descendantCountOffset = CORE_LAYOUT.byteSize() + 4; // VarHandle adds offset within HASH_LAYOUT
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
    return (long) PARENT_KEY_HANDLE.get(segment, 0L);
  }
  
  public void setParentKey(final long parentKey) {
    PARENT_KEY_HANDLE.set(segment, 0L, parentKey);
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
    // Not supported for MemorySegment-backed JSON nodes
  }

  @Override
  public void setDeweyID(final SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = null; // Clear cached bytes
  }

  @Override
  public void setPreviousRevision(final int revision) {
    PREVIOUS_REVISION_HANDLE.set(segment, 0L, revision);
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    LAST_MODIFIED_REVISION_HANDLE.set(segment, 0L, revision);
  }

  @Override
  public long getHash() {
    if (resourceConfig.hashType != HashType.NONE) {
      return (long) HASH_HANDLE.get(segment, hashOffset);
    }
    return 0;
  }

  @Override
  public void setHash(final long hash) {
    if (resourceConfig.hashType != HashType.NONE) {
      HASH_HANDLE.set(segment, hashOffset, hash);
    }
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

    return getHashFunction().hashBytes(bytes.toByteArray());
  }

  public int getNameKey() {
    return (int) NAME_KEY_HANDLE.get(segment, 0L);
  }

  public void setNameKey(final int nameKey) {
    NAME_KEY_HANDLE.set(segment, 0L, nameKey);
  }

  public QNm getName() {
    return cachedName;
  }

  public void setName(final String name) {
    this.cachedName = new QNm(name);
  }

  // NameNode interface methods (required for JSON object keys)
  public int getLocalNameKey() {
    return getNameKey();
  }

  public int getPrefixKey() {
    return -1; // No prefix for JSON keys
  }

  public void setPrefixKey(final int prefixKey) {
    // Not supported for JSON nodes
  }

  public int getURIKey() {
    return -1; // No URI for JSON keys
  }

  public void setURIKey(final int uriKey) {
    // Not supported for JSON nodes
  }

  public void setLocalNameKey(final int localNameKey) {
    setNameKey(localNameKey);
  }

  public long getPathNodeKey() {
    return (long) PATH_NODE_KEY_HANDLE.get(segment, 0L);
  }

  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    PATH_NODE_KEY_HANDLE.set(segment, 0L, pathNodeKey);
  }

  @Override
  public long getRightSiblingKey() {
    return (long) RIGHT_SIBLING_KEY_HANDLE.get(segment, 0L);
  }
  
  public void setRightSiblingKey(final long rightSibling) {
    RIGHT_SIBLING_KEY_HANDLE.set(segment, 0L, rightSibling);
  }

  @Override
  public long getLeftSiblingKey() {
    return (long) LEFT_SIBLING_KEY_HANDLE.get(segment, 0L);
  }
  
  public void setLeftSiblingKey(final long leftSibling) {
    LEFT_SIBLING_KEY_HANDLE.set(segment, 0L, leftSibling);
  }

  @Override
  public long getFirstChildKey() {
    return (long) FIRST_CHILD_KEY_HANDLE.get(segment, 0L);
  }
  
  public void setFirstChildKey(final long firstChild) {
    FIRST_CHILD_KEY_HANDLE.set(segment, 0L, firstChild);
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
    return hasFirstChild() ? 1 : 0;
  }

  public void setChildCount(final long childCount) {
    // Not stored for ObjectKeyNode
  }

  @Override
  public void incrementChildCount() {
    // Not supported for ObjectKeyNode - always has 0 or 1 child
  }

  @Override
  public void decrementChildCount() {
    // Not supported for ObjectKeyNode - always has 0 or 1 child
  }

  @Override
  public long getDescendantCount() {
    if (resourceConfig.hashType != HashType.NONE) {
      return (long) DESCENDANT_COUNT_HANDLE.get(segment, descendantCountOffset);
    }
    return 0;
  }

  @Override
  public void setDescendantCount(final long descendantCount) {
    if (resourceConfig.hashType != HashType.NONE) {
      DESCENDANT_COUNT_HANDLE.set(segment, descendantCountOffset, descendantCount);
    }
  }

  @Override
  public void decrementDescendantCount() {
    if (resourceConfig.hashType != HashType.NONE) {
      setDescendantCount(getDescendantCount() - 1);
    }
  }

  @Override
  public void incrementDescendantCount() {
    if (resourceConfig.hashType != HashType.NONE) {
      setDescendantCount(getDescendantCount() + 1);
    }
  }

  @Override
  public int getPreviousRevisionNumber() {
    return (int) PREVIOUS_REVISION_HANDLE.get(segment, 0L);
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return (int) LAST_MODIFIED_REVISION_HANDLE.get(segment, 0L);
  }

  public int getTypeKey() {
    return -1; // Not used for JSON nodes
  }

  @Override
  public boolean hasFirstChild() {
    return getFirstChildKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasLastChild() {
    return hasFirstChild(); // Same as first child for ObjectKeyNode
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
  public @Nullable SirixDeweyID getDeweyID() {
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
    return resourceConfig.nodeHashFunction;
  }

  public String toString() {
    return "ObjectKeyNode{" +
        "nodeKey=" + nodeKey +
        ", parentKey=" + getParentKey() +
        ", nameKey=" + getNameKey() +
        ", pathNodeKey=" + getPathNodeKey() +
        ", rightSiblingKey=" + getRightSiblingKey() +
        ", leftSiblingKey=" + getLeftSiblingKey() +
        ", firstChildKey=" + getFirstChildKey() +
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
