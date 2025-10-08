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
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.json.ImmutableBooleanNode;
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
 * JSON Boolean node.
 *
 * <p><strong>All instances are backed by MemorySegment for consistent memory layout.</strong></p>
 * <p><strong>Uses MemoryLayout and VarHandles for type-safe field access.</strong></p>
 * 
 * @author Johannes Lichtenberger
 */
public final class BooleanNode implements StructNode, ImmutableJsonNode {

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
  // Boolean value (1 byte):
  //   - boolValue (1 byte)             - offset 48
  // Optional fields:
  //   - childCount (8 bytes)           - offset 49 (if storeChildCount)
  //   - hash (8 bytes)                 - offset 49/57 (if hashType != NONE)
  //   - descendantCount (8 bytes)      - after hash (if hashType != NONE)

  /**
   * Core layout (always present) - 33 bytes total
   * Note: Value nodes are leaf nodes and cannot have children, so no firstChild/lastChild fields
   */
  public static final MemoryLayout CORE_LAYOUT = MemoryLayout.structLayout(
      // NodeDelegate fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("parentKey"),                    // offset 0
      ValueLayout.JAVA_INT.withName("previousRevision"),              // offset 8
      ValueLayout.JAVA_INT.withName("lastModifiedRevision"),          // offset 12
      // StructNode fields (siblings only, no children for value nodes)
      ValueLayout.JAVA_LONG_UNALIGNED.withName("rightSiblingKey"),              // offset 16
      ValueLayout.JAVA_LONG_UNALIGNED.withName("leftSiblingKey"),               // offset 24
      // Boolean value
      ValueLayout.JAVA_BOOLEAN.withName("boolValue")                  // offset 32
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
  private static final VarHandle BOOL_VALUE_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("boolValue"));

  // All nodes are MemorySegment-based
  private final MemorySegment segment;
  private final long nodeKey;
  private final ResourceConfiguration resourceConfig;
  
  // DeweyID support (stored separately, not in MemorySegment)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;
  
  // Cached hash value (computed on-demand, not stored in MemorySegment)
  private long cachedHash = 0;

  /**
   * Constructor for MemorySegment-based BooleanNode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param resourceConfig the resource configuration
   */
  public BooleanNode(final MemorySegment segment, final long nodeKey, final byte[] deweyID,
      final ResourceConfiguration resourceConfig) {
    this(segment, nodeKey, deweyID != null ? new SirixDeweyID(deweyID) : null, resourceConfig);
    this.deweyIDBytes = deweyID;
  }

  public BooleanNode(final MemorySegment segment, final long nodeKey, final SirixDeweyID id,
      final ResourceConfiguration resourceConfig) {
    this.segment = segment;
    this.nodeKey = nodeKey;
    this.sirixDeweyID = id;
    this.resourceConfig = resourceConfig;
    // BooleanNode is a leaf node - no children, descendants, or hash stored
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.BOOLEAN_VALUE;
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
    // Value nodes don't store hash in MemorySegment, but cache it in memory
    // If hash is 0 and hashing is enabled, compute it on-demand
    if (cachedHash == 0 && resourceConfig.hashType != HashType.NONE) {
      cachedHash = computeHash(Bytes.elasticHeapByteBuffer());
    }
    return cachedHash;
  }

  @Override
  public void setHash(final long hash) {
    // Value nodes don't store hash in MemorySegment, but cache it in memory
    this.cachedHash = hash;
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

    bytes.writeBoolean(getValue());

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
    // Value nodes are leaf nodes and cannot have children
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setFirstChildKey(final long firstChild) {
    // Value nodes are leaf nodes - this is a no-op
  }

  @Override
  public long getLastChildKey() {
    // Value nodes are leaf nodes and cannot have children
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setLastChildKey(final long lastChild) {
    // Value nodes are leaf nodes - this is a no-op
  }

  @Override
  public long getChildCount() {
    // Value nodes are leaf nodes - always 0 children
    return 0;
  }
  
  public void setChildCount(final long childCount) {
    // Value nodes are leaf nodes - no-op
  }

  @Override
  public long getDescendantCount() {
    // Value nodes are leaf nodes: return 0 (no descendants)
    // The parent's calculation logic handles this correctly
    return 0;
  }
  
  public void setDescendantCount(final long descendantCount) {
    // Value nodes are leaf nodes - no-op
  }

  public boolean getValue() {
    return (boolean) BOOL_VALUE_HANDLE.get(segment, 0L);
  }

  public void setValue(final boolean value) {
    BOOL_VALUE_HANDLE.set(segment, 0L, value);
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
    // No-op: value nodes are leaf nodes and cannot have children
  }

  @Override
  public void decrementChildCount() {
    // No-op: value nodes are leaf nodes and cannot have children
  }

  @Override
  public void incrementDescendantCount() {
    // No-op: value nodes are leaf nodes and cannot have descendants
  }

  @Override
  public void decrementDescendantCount() {
    // No-op: value nodes are leaf nodes and cannot have descendants
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
    return visitor.visit(ImmutableBooleanNode.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("boolValue", getValue())
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
    return Objects.hashCode(nodeKey, getParentKey(), getValue());
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final BooleanNode other))
      return false;

    return nodeKey == other.nodeKey
        && getParentKey() == other.getParentKey()
        && getValue() == other.getValue();
  }
}
