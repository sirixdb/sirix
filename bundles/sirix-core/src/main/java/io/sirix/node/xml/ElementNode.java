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
import io.sirix.node.Bytes;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.xml.ImmutableElement;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.settings.Fixed;
import io.sirix.utils.NamePageHash;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.lang.foreign.MemorySegment;
import java.util.Collections;
import java.util.List;

/**
 * Node representing an XML element.
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.
 * Structural fields are parsed immediately for tree navigation; other fields
 * are parsed lazily on demand.</p>
 *
 * @author Johannes Lichtenberger
 */
public final class ElementNode implements StructNode, NameNode, ImmutableXmlNode {

  // === IMMEDIATE STRUCTURAL FIELDS ===
  private long nodeKey;
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private long firstChildKey;
  private long lastChildKey;

  // === LAZY FIELDS (NameNode) ===
  private long pathNodeKey;
  private int prefixKey;
  private int localNameKey;
  private int uriKey;

  // === LAZY FIELDS (Metadata) ===
  private int previousRevision;
  private int lastModifiedRevision;
  private long childCount;
  private long descendantCount;
  private long hash;

  // === NON-SERIALIZED FIELDS ===
  private LongHashFunction hashFunction;
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;
  private LongList attributeKeys;
  private LongList namespaceKeys;
  private QNm qNm;

  // === LAZY PARSING STATE ===
  private Object lazySource;
  private long lazyOffset;
  private boolean lazyFieldsParsed;
  private boolean hasHash;
  private boolean storeChildCount;

  /**
   * Primary constructor with all primitive fields.
   * Used by deserialization (NodeKind.ELEMENT.deserialize).
   */
  public ElementNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey,
      long firstChildKey, long lastChildKey, long childCount, long descendantCount,
      long hash, long pathNodeKey, int prefixKey, int localNameKey, int uriKey,
      LongHashFunction hashFunction, byte[] deweyID,
      LongList attributeKeys, LongList namespaceKeys, QNm qNm) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.pathNodeKey = pathNodeKey;
    this.prefixKey = prefixKey;
    this.localNameKey = localNameKey;
    this.uriKey = uriKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.attributeKeys = attributeKeys != null ? attributeKeys : new LongArrayList();
    this.namespaceKeys = namespaceKeys != null ? namespaceKeys : new LongArrayList();
    this.qNm = qNm;
    this.lazyFieldsParsed = true;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public ElementNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey,
      long firstChildKey, long lastChildKey, long childCount, long descendantCount,
      long hash, long pathNodeKey, int prefixKey, int localNameKey, int uriKey,
      LongHashFunction hashFunction, SirixDeweyID deweyID,
      LongList attributeKeys, LongList namespaceKeys, QNm qNm) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.lastChildKey = lastChildKey;
    this.childCount = childCount;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.pathNodeKey = pathNodeKey;
    this.prefixKey = prefixKey;
    this.localNameKey = localNameKey;
    this.uriKey = uriKey;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.attributeKeys = attributeKeys != null ? attributeKeys : new LongArrayList();
    this.namespaceKeys = namespaceKeys != null ? namespaceKeys : new LongArrayList();
    this.qNm = qNm;
    this.lazyFieldsParsed = true;
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
  public void setNodeKey(long nodeKey) {
    this.nodeKey = nodeKey;
  }

  // === IMMEDIATE STRUCTURAL GETTERS (no lazy parsing) ===

  @Override
  public long getParentKey() {
    return parentKey;
  }

  public void setParentKey(long parentKey) {
    this.parentKey = parentKey;
  }

  @Override
  public boolean hasParent() {
    return parentKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getRightSiblingKey() {
    return rightSiblingKey;
  }

  public void setRightSiblingKey(long rightSibling) {
    this.rightSiblingKey = rightSibling;
  }

  @Override
  public boolean hasRightSibling() {
    return rightSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLeftSiblingKey() {
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(long leftSibling) {
    this.leftSiblingKey = leftSibling;
  }

  @Override
  public boolean hasLeftSibling() {
    return leftSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getFirstChildKey() {
    return firstChildKey;
  }

  public void setFirstChildKey(long firstChild) {
    this.firstChildKey = firstChild;
  }

  @Override
  public boolean hasFirstChild() {
    return firstChildKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getLastChildKey() {
    return lastChildKey;
  }

  public void setLastChildKey(long lastChild) {
    this.lastChildKey = lastChild;
  }

  @Override
  public boolean hasLastChild() {
    return lastChildKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  // === LAZY GETTERS (trigger parsing) ===

  @Override
  public long getPathNodeKey() {
    if (!lazyFieldsParsed) parseLazyFields();
    return pathNodeKey;
  }

  @Override
  public void setPathNodeKey(@NonNegative long pathNodeKey) {
    this.pathNodeKey = pathNodeKey;
  }

  @Override
  public int getPrefixKey() {
    if (!lazyFieldsParsed) parseLazyFields();
    return prefixKey;
  }

  @Override
  public void setPrefixKey(int prefixKey) {
    this.prefixKey = prefixKey;
  }

  @Override
  public int getLocalNameKey() {
    if (!lazyFieldsParsed) parseLazyFields();
    return localNameKey;
  }

  @Override
  public void setLocalNameKey(int localNameKey) {
    this.localNameKey = localNameKey;
  }

  @Override
  public int getURIKey() {
    if (!lazyFieldsParsed) parseLazyFields();
    return uriKey;
  }

  @Override
  public void setURIKey(int uriKey) {
    this.uriKey = uriKey;
  }

  @Override
  public int getPreviousRevisionNumber() {
    if (!lazyFieldsParsed) parseLazyFields();
    return previousRevision;
  }

  @Override
  public void setPreviousRevision(int revision) {
    this.previousRevision = revision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (!lazyFieldsParsed) parseLazyFields();
    return lastModifiedRevision;
  }

  @Override
  public void setLastModifiedRevision(int revision) {
    this.lastModifiedRevision = revision;
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
  public long getDescendantCount() {
    if (!lazyFieldsParsed) parseLazyFields();
    return descendantCount;
  }

  @Override
  public void setDescendantCount(long descendantCount) {
    this.descendantCount = descendantCount;
  }

  @Override
  public long getHash() {
    if (!lazyFieldsParsed) parseLazyFields();
    if (hash == 0L && hashFunction != null) {
      hash = computeHash(Bytes.elasticOffHeapByteBuffer());
    }
    return hash;
  }

  @Override
  public void setHash(long hash) {
    this.hash = hash;
  }

  @Override
  public void incrementChildCount() {
    childCount++;
  }

  @Override
  public void decrementChildCount() {
    childCount--;
  }

  @Override
  public void incrementDescendantCount() {
    descendantCount++;
  }

  @Override
  public void decrementDescendantCount() {
    descendantCount--;
  }

  // === NON-SERIALIZED FIELD ACCESSORS ===

  @Override
  public QNm getName() {
    return qNm;
  }

  public void setName(QNm name) {
    this.qNm = name;
  }

  @Override
  public boolean isSameItem(@Nullable Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  @Override
  public int getTypeKey() {
    return NamePageHash.generateHashForString("xs:untyped");
  }

  @Override
  public void setTypeKey(int typeKey) {
    // Not stored for elements
  }

  @Override
  public void setDeweyID(SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = null;
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

  public LongHashFunction getHashFunction() {
    return hashFunction;
  }

  // === ATTRIBUTE METHODS ===

  public int getAttributeCount() {
    return attributeKeys.size();
  }

  public long getAttributeKey(@NonNegative int index) {
    if (attributeKeys.size() <= index) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return attributeKeys.getLong(index);
  }

  public void insertAttribute(@NonNegative long attrKey) {
    attributeKeys.add(attrKey);
  }

  public void removeAttribute(@NonNegative long attrNodeKey) {
    attributeKeys.removeIf(key -> key == attrNodeKey);
  }

  public List<Long> getAttributeKeys() {
    return Collections.unmodifiableList(attributeKeys);
  }

  // === NAMESPACE METHODS ===

  public int getNamespaceCount() {
    return namespaceKeys.size();
  }

  public long getNamespaceKey(@NonNegative int namespaceKey) {
    if (namespaceKeys.size() <= namespaceKey) {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
    return namespaceKeys.getLong(namespaceKey);
  }

  public void insertNamespace(long namespaceKey) {
    namespaceKeys.add(namespaceKey);
  }

  public void removeNamespace(long namespaceKey) {
    namespaceKeys.removeIf(key -> key == namespaceKey);
  }

  public List<Long> getNamespaceKeys() {
    return Collections.unmodifiableList(namespaceKeys);
  }

  // === HASH COMPUTATION ===

  @Override
  public long computeHash(BytesOut<?> bytes) {
    if (hashFunction == null) {
      return 0L;
    }

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

    return hashFunction.hashBytes(bytes.toByteArray());
  }

  // === LAZY PARSING ===

  /**
   * Populate this node from a BytesIn source for singleton reuse.
   * LAZY OPTIMIZATION: Only parses structural fields immediately.
   */
  public void readFrom(BytesIn<?> source, long nodeKey, byte[] deweyId,
      LongHashFunction hashFunction, ResourceConfiguration config,
      LongList attributeKeys, LongList namespaceKeys, QNm qNm) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    this.attributeKeys = attributeKeys != null ? attributeKeys : new LongArrayList();
    this.namespaceKeys = namespaceKeys != null ? namespaceKeys : new LongArrayList();
    this.qNm = qNm;

    // IMMEDIATE: Only structural relationships
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.lastChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

    // LAZY: Everything else
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.lazyFieldsParsed = false;
    this.hasHash = config.hashType != HashType.NONE;
    this.storeChildCount = config.storeChildCount();

    // Initialize lazy fields to defaults
    this.pathNodeKey = 0;
    this.prefixKey = 0;
    this.localNameKey = 0;
    this.uriKey = 0;
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.childCount = 0;
    this.descendantCount = 0;
    this.hash = 0;
  }

  private void parseLazyFields() {
    if (lazyFieldsParsed) {
      return;
    }

    if (lazySource == null) {
      lazyFieldsParsed = true;
      return;
    }

    BytesIn<?> bytesIn;
    if (lazySource instanceof MemorySegment segment) {
      bytesIn = new MemorySegmentBytesIn(segment);
      bytesIn.position(lazyOffset);
    } else if (lazySource instanceof byte[] bytes) {
      bytesIn = new ByteArrayBytesIn(bytes);
      bytesIn.position(lazyOffset);
    } else {
      throw new IllegalStateException("Unknown lazy source type: " + lazySource.getClass());
    }

    // NameNode fields
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(bytesIn, nodeKey);
    this.prefixKey = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.localNameKey = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.uriKey = DeltaVarIntCodec.decodeSigned(bytesIn);

    // Metadata fields
    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.childCount = storeChildCount ? DeltaVarIntCodec.decodeSigned(bytesIn) : 0;
    if (hasHash) {
      this.hash = bytesIn.readLong();
      this.descendantCount = DeltaVarIntCodec.decodeSigned(bytesIn);
    }

    this.lazyFieldsParsed = true;
  }

  /**
   * Create a deep copy snapshot of this node.
   * Forces parsing of all lazy fields since snapshot must be independent.
   */
  public ElementNode toSnapshot() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return new ElementNode(
        nodeKey, parentKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, firstChildKey, lastChildKey,
        childCount, descendantCount, hash, pathNodeKey, prefixKey,
        localNameKey, uriKey, hashFunction,
        deweyIDBytes != null ? deweyIDBytes.clone() : null,
        new LongArrayList(attributeKeys),
        new LongArrayList(namespaceKeys),
        qNm);
  }

  @Override
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableElement.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("qName", qNm)
                      .add("parentKey", parentKey)
                      .add("rightSibling", rightSiblingKey)
                      .add("leftSibling", leftSiblingKey)
                      .add("firstChild", firstChildKey)
                      .add("lastChild", lastChildKey)
                      .add("attributeKeys", attributeKeys)
                      .add("namespaceKeys", namespaceKeys)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey, getPrefixKey(), getLocalNameKey(), getURIKey());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ElementNode other))
      return false;

    return nodeKey == other.nodeKey
        && parentKey == other.parentKey
        && getPrefixKey() == other.getPrefixKey()
        && getLocalNameKey() == other.getLocalNameKey()
        && getURIKey() == other.getURIKey();
  }
}
