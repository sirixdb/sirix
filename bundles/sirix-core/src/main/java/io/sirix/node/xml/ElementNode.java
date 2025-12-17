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
import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.xml.ImmutableElement;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.settings.Fixed;
import it.unimi.dsi.fastutil.longs.LongList;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.Collections;
import java.util.List;

/**
 * <p>
 * Node representing an XML element.
 * </p>
 *
 * <p><strong>All instances are backed by MemorySegment for consistent memory layout.</strong></p>
 * <p><strong>Uses MemoryLayout and VarHandles for type-safe field access.</strong></p>
 * <p><strong>This class is not part of the public API and might change.</strong></p>
 */
public final class ElementNode implements StructNode, NameNode, ImmutableXmlNode {

  // MemorySegment layout with FIXED offsets:
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
      ValueLayout.JAVA_LONG_UNALIGNED.withName("childCount")                    // variable offset
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

  // VarHandles for optional child count field
  private static final VarHandle CHILD_COUNT_HANDLE = 
      CHILD_COUNT_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("childCount"));

  // VarHandles for optional hash fields
  private static final VarHandle HASH_HANDLE = 
      HASH_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("hash"));
  private static final VarHandle DESCENDANT_COUNT_HANDLE = 
      HASH_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("descendantCount"));

  // All nodes are MemorySegment-based
  private final MemorySegment segment;
  private final long nodeKey;
  private final ResourceConfiguration resourceConfig;

  // Keys of attributes (stored separately from MemorySegment)
  private final LongList attributeKeys;

  // Keys of namespace declarations (stored separately from MemorySegment)
  private final LongList namespaceKeys;

  // The qualified name (cached, not stored in MemorySegment, can be updated via setName)
  private QNm qNm;

  // DeweyID support (stored separately, not in MemorySegment)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Cached offsets for optional fields (computed once at construction)
  private final long childCountOffset;
  private final long hashOffset;
  private final long descendantCountOffset;

  /**
   * Constructor for MemorySegment-based ElementNode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param deweyID        optional DeweyID bytes
   * @param resourceConfig the resource configuration
   * @param attributeKeys  list of attribute keys
   * @param namespaceKeys  keys of namespaces to be set
   * @param qNm            the qualified name
   */
  public ElementNode(final MemorySegment segment, final long nodeKey, final byte[] deweyID,
      final ResourceConfiguration resourceConfig, final LongList attributeKeys, 
      final LongList namespaceKeys, final QNm qNm) {
    this(segment, nodeKey, deweyID != null ? new SirixDeweyID(deweyID) : null, 
         resourceConfig, attributeKeys, namespaceKeys, qNm);
    this.deweyIDBytes = deweyID;
  }

  /**
   * Constructor for MemorySegment-based ElementNode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param id             optional DeweyID
   * @param resourceConfig the resource configuration
   * @param attributeKeys  list of attribute keys
   * @param namespaceKeys  keys of namespaces to be set
   * @param qNm            the qualified name
   */
  public ElementNode(final MemorySegment segment, final long nodeKey, final SirixDeweyID id,
      final ResourceConfiguration resourceConfig, final LongList attributeKeys, 
      final LongList namespaceKeys, final QNm qNm) {
    assert segment != null;
    this.segment = segment;
    this.nodeKey = nodeKey;
    this.sirixDeweyID = id;
    assert resourceConfig != null;
    this.resourceConfig = resourceConfig;
    assert attributeKeys != null;
    this.attributeKeys = attributeKeys;
    assert namespaceKeys != null;
    this.namespaceKeys = namespaceKeys;
    assert qNm != null;
    this.qNm = qNm;

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
      this.descendantCountOffset = currentOffset; // VarHandle adds offset within HASH_LAYOUT
    } else {
      this.hashOffset = -1;
      this.descendantCountOffset = -1;
    }
  }

  /**
   * Getting the count of attributes.
   *
   * @return the count of attributes
   */
  public int getAttributeCount() {
    return attributeKeys.size();
  }

  /**
   * Getting the attribute key for an given index.
   *
   * @param index index of the attribute
   * @return the attribute key
   */
  public long getAttributeKey(final @NonNegative int index) {
    if (attributeKeys.size() <= index) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return attributeKeys.getLong(index);
  }

  /**
   * Inserting an attribute.
   *
   * @param attrKey the new attribute key
   */
  public void insertAttribute(final @NonNegative long attrKey) {
    attributeKeys.add(attrKey);
  }

  /**
   * Removing an attribute.
   *
   * @param attrNodeKey the key of the attribute to be removed
   */
  public void removeAttribute(final @NonNegative long attrNodeKey) {
    attributeKeys.removeIf(key -> key == attrNodeKey);
  }

  /**
   * Getting the count of namespaces.
   *
   * @return the count of namespaces
   */
  public int getNamespaceCount() {
    return namespaceKeys.size();
  }

  /**
   * Getting the namespace key for a given index.
   *
   * @param namespaceKey index of the namespace
   * @return the namespace key
   */
  public long getNamespaceKey(final @NonNegative int namespaceKey) {
    if (namespaceKeys.size() <= namespaceKey) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return namespaceKeys.getLong(namespaceKey);
  }

  /**
   * Inserting a namespace.
   *
   * @param namespaceKey new namespace key
   */
  public void insertNamespace(final long namespaceKey) {
    namespaceKeys.add(namespaceKey);
  }

  /**
   * Removing a namepsace.
   *
   * @param namespaceKey the key of the namespace to be removed
   */
  public void removeNamespace(final long namespaceKey) {
    namespaceKeys.removeIf(key -> key == namespaceKey);
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ELEMENT;
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
  public int getTypeKey() {
    // typeKey is not stored in the segment (not serialized), but ElementNode always has the "xs:untyped" type
    // Return the hash for "xs:untyped" which is the default type for untyped XML elements
    return io.sirix.utils.NamePageHash.generateHashForString("xs:untyped");
  }

  @Override
  public void setTypeKey(final int typeKey) {
    // typeKey is not stored in the segment (not serialized), so this is a no-op
    // This matches the old behavior where typeKey was in NodeDelegate but not persisted
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
  public int getPreviousRevisionNumber() {
    return (int) PREVIOUS_REVISION_HANDLE.get(segment, 0L);
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    return (int) LAST_MODIFIED_REVISION_HANDLE.get(segment, 0L);
  }

  @Override
  public int getPrefixKey() {
    return (int) PREFIX_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public int getLocalNameKey() {
    return (int) LOCAL_NAME_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public int getURIKey() {
    return (int) URI_KEY_HANDLE.get(segment, 0L);
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    PREFIX_KEY_HANDLE.set(segment, 0L, prefixKey);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    LOCAL_NAME_KEY_HANDLE.set(segment, 0L, localNameKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    URI_KEY_HANDLE.set(segment, 0L, uriKey);
  }

  @Override
  public long getPathNodeKey() {
    return (long) PATH_NODE_KEY_HANDLE.get(segment, 0L);
  }

  @Override
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
    return (long) LAST_CHILD_KEY_HANDLE.get(segment, 0L);
  }

  public void setLastChildKey(final long lastChild) {
    LAST_CHILD_KEY_HANDLE.set(segment, 0L, lastChild);
  }

  @Override
  public long getChildCount() {
    if (resourceConfig.storeChildCount() && childCountOffset != -1) {
      return (long) CHILD_COUNT_HANDLE.get(segment, childCountOffset);
    }
    return 0;
  }

  public void setChildCount(final long childCount) {
    if (resourceConfig.storeChildCount() && childCountOffset != -1) {
      CHILD_COUNT_HANDLE.set(segment, childCountOffset, childCount);
    }
  }

  @Override
  public long getDescendantCount() {
    if (resourceConfig.hashType != HashType.NONE && descendantCountOffset != -1) {
      return (long) DESCENDANT_COUNT_HANDLE.get(segment, descendantCountOffset);
    }
    return 0;
  }

  public void setDescendantCount(final long descendantCount) {
    if (resourceConfig.hashType != HashType.NONE && descendantCountOffset != -1) {
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
  public QNm getName() {
    return qNm;
  }

  public void setName(final QNm name) {
    this.qNm = name;
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

  public LongHashFunction getHashFunction() {
    return resourceConfig.nodeHashFunction;
  }

  @Override
  public long getHash() {
    if (resourceConfig.hashType != HashType.NONE && hashOffset != -1) {
      return (long) HASH_HANDLE.get(segment, hashOffset);
    }
    return 0;
  }

  @Override
  public void setHash(final long hash) {
    if (resourceConfig.hashType != HashType.NONE && hashOffset != -1) {
      HASH_HANDLE.set(segment, hashOffset, hash);
    }
  }

  @Override
  public long computeHash(BytesOut<?> bytes) {
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

    bytes.writeInt(getPrefixKey())
         .writeInt(getLocalNameKey())
         .writeInt(getURIKey());

    return getHashFunction().hashBytes(bytes.toByteArray());
  }

  /**
   * Get a {@link List} with all attribute keys.
   *
   * @return unmodifiable view of {@link List} with all attribute keys
   */
  public List<Long> getAttributeKeys() {
    return Collections.unmodifiableList(attributeKeys);
  }

  /**
   * Get a {@link List} with all namespace keys.
   *
   * @return unmodifiable view of {@link List} with all namespace keys
   */
  public List<Long> getNamespaceKeys() {
    return Collections.unmodifiableList(namespaceKeys);
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableElement.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("qName", qNm)
                      .add("parentKey", getParentKey())
                      .add("previousRevision", getPreviousRevisionNumber())
                      .add("lastModifiedRevision", getLastModifiedRevisionNumber())
                      .add("rightSibling", getRightSiblingKey())
                      .add("leftSibling", getLeftSiblingKey())
                      .add("firstChild", getFirstChildKey())
                      .add("lastChild", getLastChildKey())
                      .add("childCount", getChildCount())
                      .add("descendantCount", getDescendantCount())
                      .add("hash", getHash())
                      .add("prefixKey", getPrefixKey())
                      .add("localNameKey", getLocalNameKey())
                      .add("uriKey", getURIKey())
                      .add("pathNodeKey", getPathNodeKey())
                      .add("attributeKeys", attributeKeys)
                      .add("namespaceKeys", namespaceKeys)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, getParentKey(), getPrefixKey(), getLocalNameKey(), getURIKey());
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ElementNode other))
      return false;

    return nodeKey == other.nodeKey
        && getParentKey() == other.getParentKey()
        && getPrefixKey() == other.getPrefixKey()
        && getLocalNameKey() == other.getLocalNameKey()
        && getURIKey() == other.getURIKey();
  }
}
