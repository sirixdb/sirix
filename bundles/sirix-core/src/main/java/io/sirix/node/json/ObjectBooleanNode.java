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
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;

import java.lang.foreign.MemorySegment;
import io.sirix.node.immutable.json.ImmutableObjectBooleanNode;
import io.sirix.node.interfaces.BooleanValueNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.ReusableNodeProxy;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * JSON Object Boolean node (direct child of ObjectKeyNode, no siblings).
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class ObjectBooleanNode implements StructNode, ImmutableJsonNode, BooleanValueNode, ReusableNodeProxy {

  // Node identity (mutable for singleton reuse)
  private long nodeKey;
  
  // Mutable structural fields (only parent, no siblings for object values)
  private long parentKey;
  
  // Mutable revision tracking
  private int previousRevision;
  private int lastModifiedRevision;
  
  // Mutable hash
  private long hash;
  
  // Boolean value
  private boolean value;
  
  // Hash function for computing node hashes (mutable for singleton reuse)
  private LongHashFunction hashFunction;
  
  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Lazy parsing state (single-stage since boolean value is cheap)
  private Object lazySource;
  private long lazyOffset;
  private boolean lazyFieldsParsed;
  private boolean hasHash;

  /**
   * Primary constructor with all primitive fields.
   */
  public ObjectBooleanNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long hash, boolean value,
      LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.lazyFieldsParsed = true;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public ObjectBooleanNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long hash, boolean value,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.lazyFieldsParsed = true;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_BOOLEAN_VALUE;
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

    return bytes.hashDirect(hashFunction);
  }

  @Override
  public long getRightSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setRightSiblingKey(final long rightSibling) {
    // Object value nodes don't have siblings
  }

  @Override
  public long getLeftSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setLeftSiblingKey(final long leftSibling) {
    // Object value nodes don't have siblings
  }

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setFirstChildKey(final long firstChild) {
    // Value nodes are leaf nodes - no-op
  }

  @Override
  public long getLastChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setLastChildKey(final long lastChild) {
    // Value nodes are leaf nodes - no-op
  }

  @Override
  public long getChildCount() {
    return 0;
  }
  
  public void setChildCount(final long childCount) {
    // Value nodes are leaf nodes - no-op
  }

  @Override
  public long getDescendantCount() {
    return 0;
  }
  
  public void setDescendantCount(final long descendantCount) {
    // Value nodes are leaf nodes - no-op
  }

  public boolean getValue() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return value;
  }

  public void setValue(final boolean value) {
    this.value = value;
  }

  @Override
  public boolean hasFirstChild() {
    return false;
  }

  @Override
  public boolean hasLastChild() {
    return false;
  }

  @Override
  public void incrementChildCount() {}

  @Override
  public void decrementChildCount() {}

  @Override
  public void incrementDescendantCount() {}

  @Override
  public void decrementDescendantCount() {}

  @Override
  public boolean hasLeftSibling() {
    return false;
  }

  @Override
  public boolean hasRightSibling() {
    return false;
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

  public LongHashFunction getHashFunction() {
    return hashFunction;
  }

  @Override
  public void setNodeKey(final long nodeKey) {
    this.nodeKey = nodeKey;
  }

  public void readFrom(final BytesIn<?> source, final long nodeKey, final byte[] deweyId,
                       final LongHashFunction hashFunction, final ResourceConfiguration config) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    
    // STRUCTURAL FIELD
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    
    // Store for lazy parsing
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.lazyFieldsParsed = false;
    this.hasHash = config.hashType != HashType.NONE;
    
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.value = false;
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
    
    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.value = bytesIn.readBoolean();
    if (hasHash) {
      this.hash = bytesIn.readLong();
    }
    this.lazyFieldsParsed = true;
  }

  public ObjectBooleanNode toSnapshot() {
    if (!lazyFieldsParsed) {
      parseLazyFields();
    }
    return new ObjectBooleanNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        hash, value, hashFunction,
        deweyIDBytes != null ? deweyIDBytes.clone() : null);
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
    return visitor.visit(ImmutableObjectBooleanNode.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("boolValue", value)
                      .add("parentKey", parentKey)
                      .add("previousRevision", previousRevision)
                      .add("lastModifiedRevision", lastModifiedRevision)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey, value);
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ObjectBooleanNode other))
      return false;

    return nodeKey == other.nodeKey
        && parentKey == other.parentKey
        && value == other.value;
  }
}
