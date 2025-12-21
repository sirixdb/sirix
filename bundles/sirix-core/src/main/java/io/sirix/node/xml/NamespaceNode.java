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
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.xml.ImmutableNamespace;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
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
 * <p>
 * Node representing an XML namespace.
 * </p>
 *
 * <p><strong>All instances are backed by MemorySegment for consistent memory layout.</strong></p>
 * <p><strong>Uses MemoryLayout and VarHandles for type-safe field access.</strong></p>
 * <p><strong>This class is not part of the public API and might change.</strong></p>
 */
public final class NamespaceNode implements NameNode, ImmutableXmlNode, Node {

  // MemorySegment layout with FIXED offsets:
  // NodeDelegate data (16 bytes):
  //   - parentKey (8 bytes)            - offset 0
  //   - previousRevision (4 bytes)     - offset 8
  //   - lastModifiedRevision (4 bytes) - offset 12
  // NameNode fields (20 bytes):
  //   - pathNodeKey (8 bytes)          - offset 16
  //   - prefixKey (4 bytes)            - offset 24
  //   - localNameKey (4 bytes)         - offset 28
  //   - uriKey (4 bytes)               - offset 32
  // Total: 36 bytes

  /**
   * Core layout (always present) - 36 bytes total
   */
  public static final MemoryLayout CORE_LAYOUT = MemoryLayout.structLayout(
      // NodeDelegate fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("parentKey"),                    // offset 0
      ValueLayout.JAVA_INT_UNALIGNED.withName("previousRevision"),              // offset 8
      ValueLayout.JAVA_INT_UNALIGNED.withName("lastModifiedRevision"),          // offset 12
      // NameNode fields
      ValueLayout.JAVA_LONG_UNALIGNED.withName("pathNodeKey"),                  // offset 16
      ValueLayout.JAVA_INT_UNALIGNED.withName("prefixKey"),                     // offset 24
      ValueLayout.JAVA_INT_UNALIGNED.withName("localNameKey"),                  // offset 28
      ValueLayout.JAVA_INT_UNALIGNED.withName("uriKey")                         // offset 32
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
  private static final VarHandle PREFIX_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("prefixKey"));
  private static final VarHandle LOCAL_NAME_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("localNameKey"));
  private static final VarHandle URI_KEY_HANDLE = 
      CORE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("uriKey"));

  /** MemorySegment containing all node data */
  private final MemorySegment segment;

  /** Unique node key. Non-final for singleton node interface compliance. */
  private long nodeKey;

  /** Optional Dewey ID */
  private SirixDeweyID sirixDeweyID;
  
  /** Cached Dewey ID bytes */
  private byte[] deweyIDBytes;

  /** The qualified name */
  private QNm qNm;

  /** Hash value */
  private long hash;

  /** Hash function for this node */
  private final LongHashFunction hashFunction;

  /**
   * Constructor for MemorySegment-based NamespaceNode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param deweyID        optional DeweyID bytes
   * @param hashFunction   hash function for computing node hash
   * @param qNm            the qualified name
   */
  public NamespaceNode(final MemorySegment segment, final long nodeKey, final byte[] deweyID,
      final LongHashFunction hashFunction, final QNm qNm) {
    this(segment, nodeKey, deweyID != null ? new SirixDeweyID(deweyID) : null, hashFunction, qNm);
    this.deweyIDBytes = deweyID;
  }

  /**
   * Constructor for MemorySegment-based NamespaceNode
   *
   * @param segment        the MemorySegment containing all node data
   * @param nodeKey        the node key (record ID)
   * @param id             optional DeweyID
   * @param hashFunction   hash function for computing node hash
   * @param qNm            the qualified name
   */
  public NamespaceNode(final MemorySegment segment, final long nodeKey, final SirixDeweyID id,
      final LongHashFunction hashFunction, final QNm qNm) {
    assert segment != null;
    this.segment = segment;
    this.nodeKey = nodeKey;
    this.sirixDeweyID = id;
    assert hashFunction != null;
    this.hashFunction = hashFunction;
    this.qNm = qNm;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.NAMESPACE;
  }

  @Override
  public long computeHash(BytesOut<?> bytes) {
    bytes.clear();
    bytes.writeLong(nodeKey)
         .writeLong(getParentKey())
         .writeByte(getKind().getId());

    bytes.writeLong(getPrefixKey())
         .writeLong(getLocalNameKey())
         .writeLong(getURIKey());

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
    return hash;
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
    hash = 0L;
    PREFIX_KEY_HANDLE.set(segment, 0L, prefixKey);
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    hash = 0L;
    LOCAL_NAME_KEY_HANDLE.set(segment, 0L, localNameKey);
  }

  @Override
  public void setURIKey(final int uriKey) {
    hash = 0L;
    URI_KEY_HANDLE.set(segment, 0L, uriKey);
  }

  @Override
  public VisitResult acceptVisitor(final XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableNamespace.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, getParentKey(), getPreviousRevisionNumber(), 
        getLastModifiedRevisionNumber(), getPathNodeKey(), getPrefixKey(), 
        getLocalNameKey(), getURIKey());
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof final NamespaceNode other) {
      return nodeKey == other.nodeKey
          && getParentKey() == other.getParentKey()
          && getPreviousRevisionNumber() == other.getPreviousRevisionNumber()
          && getLastModifiedRevisionNumber() == other.getLastModifiedRevisionNumber()
          && getPathNodeKey() == other.getPathNodeKey()
          && getPrefixKey() == other.getPrefixKey()
          && getLocalNameKey() == other.getLocalNameKey()
          && getURIKey() == other.getURIKey();
    }
    return false;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
        .add("nodeKey", nodeKey)
        .add("parentKey", getParentKey())
        .add("prevRevision", getPreviousRevisionNumber())
        .add("lastModRevision", getLastModifiedRevisionNumber())
        .add("pathNodeKey", getPathNodeKey())
        .add("prefixKey", getPrefixKey())
        .add("localNameKey", getLocalNameKey())
        .add("uriKey", getURIKey())
        .toString();
  }

  @Override
  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    PATH_NODE_KEY_HANDLE.set(segment, 0L, pathNodeKey);
  }

  @Override
  public long getPathNodeKey() {
    return (long) PATH_NODE_KEY_HANDLE.get(segment, 0L);
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
  public void setDeweyID(final SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = null; // Clear cached bytes
  }

  @Override
  public byte[] getDeweyIDAsBytes() {
    if (deweyIDBytes == null && sirixDeweyID != null) {
      deweyIDBytes = sirixDeweyID.toBytes();
    }
    return deweyIDBytes;
  }

  @Override
  public int getTypeKey() {
    // Namespace nodes don't have type information
    return -1;
  }

  @Override
  public void setTypeKey(final int typeKey) {
    // Namespace nodes don't have type information, so this is a no-op
  }

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

  public LongHashFunction getHashFunction() {
    return hashFunction;
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
  public void setLastModifiedRevision(final int revision) {
    LAST_MODIFIED_REVISION_HANDLE.set(segment, 0L, revision);
  }

  @Override
  public void setPreviousRevision(final int revision) {
    PREVIOUS_REVISION_HANDLE.set(segment, 0L, revision);
  }

  @Override
  public boolean isSameItem(@Nullable Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  @Override
  public void setNodeKey(final long nodeKey) {
    this.nodeKey = nodeKey;
  }

  /**
   * Create a deep copy snapshot of this node.
   *
   * @return a new NamespaceNode with all values copied
   */
  public NamespaceNode toSnapshot() {
    MemorySegment newSegment = MemorySegment.ofArray(new byte[(int) segment.byteSize()]);
    MemorySegment.copy(segment, 0, newSegment, 0, segment.byteSize());
    
    return new NamespaceNode(newSegment, nodeKey, 
        deweyIDBytes != null ? deweyIDBytes.clone() : null,
        hashFunction, qNm);
  }
}
