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
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.json.ImmutableObjectNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;

/**
 * JSON Object node.
 *
 * <p><strong>All instances are backed by MemorySegment for consistent memory layout.</strong></p>
 * <p><strong>Uses MemoryLayout and VarHandles for type-safe field access.</strong></p>
 * 
 * @author Johannes Lichtenberger
 */
public final class ObjectNode implements StructNode, ImmutableJsonNode {

  // MemorySegment layout with FIXED offsets (hash moved to end):
  // NodeDelegate data (16 bytes):
  //   - parentKey (8 bytes)            - offset 0
  //   - previousRevision (4 bytes)     - offset 8
  //   - lastModifiedRevision (4 bytes) - offset 12
  // Fixed StructNode fields (32 bytes):
  //   - rightSiblingKey (8 bytes)      - offset 16
  //   - leftSiblingKey (8 bytes)       - offset 24
  //   - firstChildKey (8 bytes)        - offset 32
  //   - lastChildKey (8 bytes)         - offset 40
  // Optional fields:
  //   - childCount (8 bytes)           - offset 48 (if storeChildCount)
  //   - hash (8 bytes)                 - offset 48/56 (if hashType != NONE)
  //   - descendantCount (8 bytes)      - after hash (if hashType != NONE)

  /**
   * Core layout (always present) - 48 bytes total
   */
  public static final MemoryLayout CORE_LAYOUT = MemoryLayout.structLayout(
      // NodeDelegate fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("parentKey"),                    // offset 0
      ValueLayout.JAVA_INT.withName("previousRevision"),              // offset 8
      ValueLayout.JAVA_INT.withName("lastModifiedRevision"),          // offset 12
      // StructNode fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("rightSiblingKey"),              // offset 16
      ValueLayout.JAVA_LONG_UNALIGNED.withName("leftSiblingKey"),               // offset 24
      ValueLayout.JAVA_LONG_UNALIGNED.withName("firstChildKey"),                // offset 32
      ValueLayout.JAVA_LONG_UNALIGNED.withName("lastChildKey")                  // offset 40
  );

  /**
   * Optional child count layout (only when storeChildCount == true) - 8 bytes
   */
  public static final MemoryLayout CHILD_COUNT_LAYOUT = MemoryLayout.structLayout(
      ValueLayout.JAVA_LONG_UNALIGNED.withName("childCount")                    // offset 48
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
  
  // VarHandles for optional fields
  private static final VarHandle CHILD_COUNT_HANDLE = 
      CHILD_COUNT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("childCount"));
  private static final VarHandle HASH_HANDLE = 
      HASH_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("hash"));
  private static final VarHandle DESCENDANT_COUNT_HANDLE = 
      HASH_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("descendantCount"));

  // All nodes are MemorySegment-based
  private final MemorySegment segment;
  private final long nodeKey;
  private final ResourceConfiguration resourceConfig;
  
  // Cached offsets for maximum performance (computed once at construction)
  private final long childCountOffset;
  private final long hashOffset;
  private final long descendantCountOffset;
  
  // DeweyID support (stored separately, not in MemorySegment)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  /**
   * Constructor for MemorySegment-based ObjectNode (deserialization from storage)
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param deweyID        the DeweyID as byte array (can be null)
   * @param resourceConfig the resource configuration
   */
  public ObjectNode(final MemorySegment segment, final long nodeKey, final byte[] deweyID,
      final ResourceConfiguration resourceConfig) {
    this.deweyIDBytes = deweyID; // Cache the byte array to avoid re-conversion
    this.segment = segment;
    this.nodeKey = nodeKey;
    this.resourceConfig = resourceConfig;
    
    // Compute offsets once for maximum performance
    this.childCountOffset = CORE_LAYOUT.byteSize();
    
    long hashBaseOffset = CORE_LAYOUT.byteSize();
    if (resourceConfig.storeChildCount()) {
      hashBaseOffset += CHILD_COUNT_LAYOUT.byteSize();
    }
    this.hashOffset = hashBaseOffset;
    // descendantCount is accessed via HASH_LAYOUT VarHandle, which adds offset within struct
    this.descendantCountOffset = hashBaseOffset;
  }

  /**
   * Constructor for MemorySegment-based ObjectNode (creation from factory)
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param id             the SirixDeweyID (can be null)
   * @param resourceConfig the resource configuration
   */
  public ObjectNode(final MemorySegment segment, final long nodeKey, final SirixDeweyID id,
      final ResourceConfiguration resourceConfig) {
    this.segment = segment;
    this.nodeKey = nodeKey;
    this.sirixDeweyID = id;
    this.resourceConfig = resourceConfig;
    
    // Compute offsets once for maximum performance
    this.childCountOffset = CORE_LAYOUT.byteSize();
    
    long hashBaseOffset = CORE_LAYOUT.byteSize();
    if (resourceConfig.storeChildCount()) {
      hashBaseOffset += CHILD_COUNT_LAYOUT.byteSize();
    }
    this.hashOffset = hashBaseOffset;
    // descendantCount is accessed via HASH_LAYOUT VarHandle, which adds offset within struct
    this.descendantCountOffset = hashBaseOffset;
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

    return getHashFunction().hashBytes(bytes.toByteArray());
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
    return (long) LAST_CHILD_KEY_HANDLE.get(segment, 0L);
  }
  
  public void setLastChildKey(final long lastChild) {
    LAST_CHILD_KEY_HANDLE.set(segment, 0L, lastChild);
  }

  @Override
  public long getChildCount() {
    if (resourceConfig.storeChildCount()) {
      return (long) CHILD_COUNT_HANDLE.get(segment, childCountOffset);
    }
    return 0;
  }
  
  public void setChildCount(final long childCount) {
    if (resourceConfig.storeChildCount()) {
      CHILD_COUNT_HANDLE.set(segment, childCountOffset, childCount);
    }
  }

  @Override
  public long getDescendantCount() {
    if (resourceConfig.hashType != HashType.NONE) {
      return (long) DESCENDANT_COUNT_HANDLE.get(segment, descendantCountOffset);
    }
    return 0;
  }
  
  public void setDescendantCount(final long descendantCount) {
    if (resourceConfig.hashType != HashType.NONE) {
      DESCENDANT_COUNT_HANDLE.set(segment, descendantCountOffset, descendantCount);
    }
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
    if (resourceConfig.storeChildCount()) {
      setChildCount(getChildCount() + 1);
    }
  }

  @Override
  public void decrementChildCount() {
    if (resourceConfig.storeChildCount()) {
      setChildCount(getChildCount() - 1);
    }
  }

  @Override
  public void incrementDescendantCount() {
    if (resourceConfig.hashType != HashType.NONE) {
      setDescendantCount(getDescendantCount() + 1);
    }
  }

  @Override
  public void decrementDescendantCount() {
    if (resourceConfig.hashType != HashType.NONE) {
      setDescendantCount(getDescendantCount() - 1);
    }
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
    return (int) PREVIOUS_REVISION_HANDLE.get(segment, 0L);
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return (int) LAST_MODIFIED_REVISION_HANDLE.get(segment, 0L);
  }

  public LongHashFunction getHashFunction() {
    return resourceConfig.nodeHashFunction;
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
                      .add("parentKey", getParentKey())
                      .add("previousRevision", getPreviousRevisionNumber())
                      .add("lastModifiedRevision", getLastModifiedRevisionNumber())
                      .add("rightSibling", getRightSiblingKey())
                      .add("leftSibling", getLeftSiblingKey())
                      .add("firstChild", getFirstChildKey())
                      .add("lastChild", getLastChildKey())
                      .add("childCount", getChildCount())
                      .add("hash", getHash())
                      .add("descendantCount", getDescendantCount())
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, getParentKey());
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ObjectNode other))
      return false;

    return nodeKey == other.nodeKey
        && getParentKey() == other.getParentKey();
  }
}
