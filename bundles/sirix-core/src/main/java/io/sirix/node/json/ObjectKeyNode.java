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
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;

import java.lang.foreign.MemorySegment;
import io.sirix.node.immutable.json.ImmutableObjectKeyNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Node representing an object key/field.
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.</p>
 */
public final class ObjectKeyNode implements StructNode, NameNode, ImmutableJsonNode {

  // Node identity (mutable for singleton reuse)
  private long nodeKey;
  
  // Mutable structural fields
  private long parentKey;
  private long pathNodeKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private long firstChildKey;
  
  // Name key (hash of the name string)
  private int nameKey;
  
  // Mutable revision tracking
  private int previousRevision;
  private int lastModifiedRevision;
  
  // Mutable hash and descendant count
  private long hash;
  private long descendantCount;
  
  // Hash function for computing node hashes (mutable for singleton reuse)
  private LongHashFunction hashFunction;
  
  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;
  
  // Cache for name (not serialized, only nameKey is)
  private QNm cachedName;

  // Lazy parsing state
  private Object lazySource;
  private long lazyOffset;
  private boolean lazyFieldsParsed;
  private boolean hasHash;

  /**
   * Primary constructor with all primitive fields.
   */
  public ObjectKeyNode(long nodeKey, long parentKey, long pathNodeKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long firstChildKey,
      int nameKey, long descendantCount, long hash,
      LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.nameKey = nameKey;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.lazyFieldsParsed = true;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public ObjectKeyNode(long nodeKey, long parentKey, long pathNodeKey, int previousRevision,
      int lastModifiedRevision, long rightSiblingKey, long leftSiblingKey, long firstChildKey,
      int nameKey, long descendantCount, long hash,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.firstChildKey = firstChildKey;
    this.nameKey = nameKey;
    this.descendantCount = descendantCount;
    this.hash = hash;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.lazyFieldsParsed = true;
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
    return parentKey;
  }
  
  public void setParentKey(final long parentKey) {
    this.parentKey = parentKey;
  }

  @Override
  public boolean hasParent() {
    return parentKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean isSameItem(@Nullable Node other) {
    return other != null && other.getNodeKey() == nodeKey;
  }

  @Override
  public void setTypeKey(final int typeKey) {
    // Not supported for JSON nodes
  }

  @Override
  public void setDeweyID(final SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = null;
  }

  @Override
  public void setPreviousRevision(final int revision) {
    this.previousRevision = revision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return hash;
  }

  @Override
  public void setHash(final long hash) {
    this.hash = hash;
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

    return hashFunction.hashBytes(bytes.toByteArray());
  }

  public int getNameKey() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return nameKey;
  }

  public void setNameKey(final int nameKey) {
    this.nameKey = nameKey;
  }

  public QNm getName() {
    return cachedName;
  }

  public void setName(final String name) {
    this.cachedName = new QNm(name);
  }

  // NameNode interface methods
  public int getLocalNameKey() {
    return nameKey;
  }

  public int getPrefixKey() {
    return -1;
  }

  public void setPrefixKey(final int prefixKey) {
    // Not supported for JSON nodes
  }

  public int getURIKey() {
    return -1;
  }

  public void setURIKey(final int uriKey) {
    // Not supported for JSON nodes
  }

  public void setLocalNameKey(final int localNameKey) {
    this.nameKey = localNameKey;
  }

  public long getPathNodeKey() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return pathNodeKey;
  }

  public void setPathNodeKey(final @NonNegative long pathNodeKey) {
    this.pathNodeKey = pathNodeKey;
  }

  @Override
  public long getRightSiblingKey() {
    return rightSiblingKey;
  }
  
  public void setRightSiblingKey(final long rightSibling) {
    this.rightSiblingKey = rightSibling;
  }

  @Override
  public long getLeftSiblingKey() {
    return leftSiblingKey;
  }
  
  public void setLeftSiblingKey(final long leftSibling) {
    this.leftSiblingKey = leftSibling;
  }

  @Override
  public long getFirstChildKey() {
    return firstChildKey;
  }
  
  public void setFirstChildKey(final long firstChild) {
    this.firstChildKey = firstChild;
  }

  @Override
  public long getLastChildKey() {
    // ObjectKeyNode only has one child (the value), so first == last
    return firstChildKey;
  }

  public void setLastChildKey(final long lastChild) {
    // Not used for ObjectKeyNode
  }

  @Override
  public long getChildCount() {
    // ObjectKeyNode always has exactly 1 child (the value)
    return 1;
  }

  public void setChildCount(final long childCount) {
    // Not stored for ObjectKeyNode
  }

  @Override
  public void incrementChildCount() {
    // Not supported for ObjectKeyNode - always has 1 child
  }

  @Override
  public void decrementChildCount() {
    // Not supported for ObjectKeyNode - always has 1 child
  }

  @Override
  public long getDescendantCount() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return descendantCount;
  }

  @Override
  public void setDescendantCount(final long descendantCount) {
    this.descendantCount = descendantCount;
  }

  @Override
  public void decrementDescendantCount() {
    descendantCount--;
  }

  @Override
  public void incrementDescendantCount() {
    descendantCount++;
  }

  @Override
  public int getPreviousRevisionNumber() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return lastModifiedRevision;
  }

  public int getTypeKey() {
    return -1;
  }

  @Override
  public boolean hasFirstChild() {
    return firstChildKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasLastChild() {
    return firstChildKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasLeftSibling() {
    return leftSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasRightSibling() {
    return rightSiblingKey != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public @Nullable SirixDeweyID getDeweyID() {
    if (sirixDeweyID == null && deweyIDBytes != null) {
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
    return visitor.visit(ImmutableObjectKeyNode.of(this));
  }

  public LongHashFunction getHashFunction() {
    return hashFunction;
  }

  @Override
  public void setNodeKey(final long nodeKey) {
    this.nodeKey = nodeKey;
  }

  /**
   * Populate this node from a BytesIn source for singleton reuse.
   * LAZY OPTIMIZATION: Only parses structural fields immediately (NEW ORDER).
   */
  public void readFrom(final BytesIn<?> source, final long nodeKey, final byte[] deweyId,
                       final LongHashFunction hashFunction, final ResourceConfiguration config) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    this.cachedName = null;
    
    // STRUCTURAL FIELDS - parse immediately (NEW ORDER)
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.firstChildKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    
    // Store for lazy parsing
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.lazyFieldsParsed = false;
    this.hasHash = config.hashType != HashType.NONE;
    
    this.nameKey = 0;
    this.pathNodeKey = 0;
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
    this.descendantCount = 0;
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
    
    this.nameKey = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(bytesIn, nodeKey);
    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
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
  public ObjectKeyNode toSnapshot() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return new ObjectKeyNode(nodeKey, parentKey, pathNodeKey, previousRevision, lastModifiedRevision,
        rightSiblingKey, leftSiblingKey, firstChildKey, nameKey, descendantCount, hash, hashFunction,
        deweyIDBytes != null ? deweyIDBytes.clone() : null);
  }

  public String toString() {
    return "ObjectKeyNode{" +
        "nodeKey=" + nodeKey +
        ", parentKey=" + parentKey +
        ", nameKey=" + nameKey +
        ", pathNodeKey=" + pathNodeKey +
        ", rightSiblingKey=" + rightSiblingKey +
        ", leftSiblingKey=" + leftSiblingKey +
        ", firstChildKey=" + firstChildKey +
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
