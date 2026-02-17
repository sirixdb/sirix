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
import io.sirix.node.immutable.json.ImmutableObjectNumberNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.NumericValueNode;
import io.sirix.node.interfaces.ReusableNodeProxy;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Fixed;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * JSON Object Number node (direct child of ObjectKeyNode, no siblings).
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class ObjectNumberNode implements StructNode, ImmutableJsonNode, NumericValueNode, ReusableNodeProxy {

  // Node identity (mutable for singleton reuse)
  private long nodeKey;
  
  // Mutable structural fields (only parent, no siblings for object values)
  private long parentKey;
  
  // Mutable revision tracking
  private int previousRevision;
  private int lastModifiedRevision;
  
  // Mutable hash
  private long hash;
  
  // Number value
  private Number value;
  
  // Hash function for computing node hashes (mutable for singleton reuse)
  private LongHashFunction hashFunction;
  
  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Lazy parsing state (for singleton reuse optimization)
  private Object lazySource;
  private long lazyOffset;
  private boolean metadataParsed;
  private boolean valueParsed;
  private boolean hasHash;
  private long valueOffset;

  // Fixed-slot value encoding state (for read path via populateSingletonFromFixedSlot)
  private boolean fixedValueEncoding;   // Whether value comes from fixed-slot inline payload
  private int fixedValueLength;         // Length of inline payload bytes

  /**
   * Primary constructor with all primitive fields.
   */
  public ObjectNumberNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long hash, Number value,
      LongHashFunction hashFunction, byte[] deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   */
  public ObjectNumberNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long hash, Number value,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_NUMBER_VALUE;
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
  public void setTypeKey(final int typeKey) {}

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
    if (!metadataParsed) {
      parseMetadataFields();
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

    final Number number = getValue();
    switch (number) {
      case Float floatVal -> bytes.writeFloat(floatVal);
      case Double doubleVal -> bytes.writeDouble(doubleVal);
      case BigDecimal bigDecimalVal -> bytes.writeBigDecimal(bigDecimalVal);
      case Integer intVal -> bytes.writeInt(intVal);
      case Long longVal -> bytes.writeLong(longVal);
      case BigInteger bigIntegerVal -> bytes.writeBigInteger(bigIntegerVal);
      default -> throw new IllegalStateException("Unexpected value: " + number);
    }

    return bytes.hashDirect(hashFunction);
  }

  @Override
  public long getRightSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setRightSiblingKey(final long rightSibling) {}

  @Override
  public long getLeftSiblingKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setLeftSiblingKey(final long leftSibling) {}

  @Override
  public long getFirstChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setFirstChildKey(final long firstChild) {}

  @Override
  public long getLastChildKey() {
    return Fixed.NULL_NODE_KEY.getStandardProperty();
  }
  
  public void setLastChildKey(final long lastChild) {}

  @Override
  public long getChildCount() {
    return 0;
  }
  
  public void setChildCount(final long childCount) {}

  @Override
  public long getDescendantCount() {
    return 0;
  }
  
  public void setDescendantCount(final long descendantCount) {}

  public Number getValue() {
    if (!valueParsed) {
      parseValueField();
    }
    return value;
  }

  public void setValue(final Number value) {
    this.value = value;
    this.fixedValueEncoding = false;
    this.valueParsed = true;
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
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (!metadataParsed) {
      parseMetadataFields();
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

  public void setDeweyIDBytes(final byte[] deweyIDBytes) {
    this.deweyIDBytes = deweyIDBytes;
    this.sirixDeweyID = null;
  }

  public void readFrom(final BytesIn<?> source, final long nodeKey, final byte[] deweyId,
                       final LongHashFunction hashFunction, final ResourceConfiguration config) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    
    // STRUCTURAL FIELD - parse immediately
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    
    // Store state for lazy parsing
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.metadataParsed = false;
    this.valueParsed = false;
    this.hasHash = config.hashType != HashType.NONE;
    this.valueOffset = 0;
    
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
    this.value = null;
  }
  
  /**
   * Populate this singleton from fixed-slot inline payload (zero allocation).
   * Sets up lazy value parsing from the fixed-slot MemorySegment.
   * CRITICAL: Resets hash to 0 — caller MUST call setHash() AFTER this method.
   *
   * @param source the slot data (MemorySegment) containing inline payload
   * @param valueOffset byte offset within source where payload bytes start
   * @param valueLength length of payload bytes
   */
  public void setLazyNumberValue(final Object source, final long valueOffset, final int valueLength) {
    this.lazySource = source;
    this.valueOffset = valueOffset;
    this.metadataParsed = true;
    this.valueParsed = false;
    this.fixedValueEncoding = true;
    this.fixedValueLength = valueLength;
    this.value = null;
    this.hash = 0L;
  }

  private void parseMetadataFields() {
    if (metadataParsed) {
      return;
    }

    if (lazySource == null) {
      metadataParsed = true;
      return;
    }

    final BytesIn<?> bytesIn = createBytesIn(lazyOffset);
    
    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    if (hasHash) {
      this.hash = bytesIn.readLong();
    }
    this.valueOffset = bytesIn.position();
    this.metadataParsed = true;
  }
  
  private void parseValueField() {
    if (valueParsed) {
      return;
    }

    // Fixed-slot inline payload path (from setLazyNumberValue)
    if (fixedValueEncoding) {
      if (fixedValueLength > 0) {
        final BytesIn<?> bytesIn = createBytesIn(valueOffset);
        this.value = NodeKind.deserializeNumber(bytesIn);
      } else {
        this.value = 0;
      }
      this.valueParsed = true;
      return;
    }

    if (!metadataParsed) {
      parseMetadataFields();
    }

    if (lazySource == null) {
      valueParsed = true;
      return;
    }

    final BytesIn<?> bytesIn = createBytesIn(valueOffset);
    this.value = NodeKind.deserializeNumber(bytesIn);
    this.valueParsed = true;
  }
  
  private BytesIn<?> createBytesIn(long offset) {
    if (lazySource instanceof MemorySegment segment) {
      var bytesIn = new MemorySegmentBytesIn(segment);
      bytesIn.position(offset);
      return bytesIn;
    } else if (lazySource instanceof byte[] bytes) {
      var bytesIn = new ByteArrayBytesIn(bytes);
      bytesIn.position(offset);
      return bytesIn;
    } else {
      throw new IllegalStateException("Unknown lazy source type: " + lazySource.getClass());
    }
  }

  public ObjectNumberNode toSnapshot() {
    if (!metadataParsed) {
      parseMetadataFields();
    }
    if (!valueParsed) {
      parseValueField();
    }
    return new ObjectNumberNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
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
    return visitor.visit(ImmutableObjectNumberNode.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("number", value)
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
    if (!(obj instanceof final ObjectNumberNode other))
      return false;

    return nodeKey == other.nodeKey
        && parentKey == other.parentKey
        && Objects.equal(value, other.value);
  }
}
