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
import io.sirix.node.immutable.json.ImmutableObjectStringNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.settings.StringCompressionType;
import io.sirix.utils.FSSTCompressor;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * JSON Object String node (direct child of ObjectKeyNode, no siblings).
 *
 * <p>Uses primitive fields for efficient storage with delta+varint encoding.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class ObjectStringNode implements StructNode, ValueNode, ImmutableJsonNode {

  // Node identity (mutable for singleton reuse)
  private long nodeKey;
  
  // Mutable structural fields (only parent, no siblings for object values)
  private long parentKey;
  
  // Mutable revision tracking
  private int previousRevision;
  private int lastModifiedRevision;
  
  // Mutable hash
  private long hash;
  
  // String value (stored as bytes)
  private byte[] value;
  
  // Hash function for computing node hashes (mutable for singleton reuse)
  private LongHashFunction hashFunction;
  
  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // FSST Compression support (for columnar string storage)
  /** Whether the stored value is FSST compressed */
  private boolean isCompressed;
  /** FSST symbol table for decompression (shared from KeyValueLeafPage) */
  private byte[] fsstSymbolTable;
  /** Decompressed value (lazy allocated on first access if compressed) */
  private byte[] decodedValue;

  // Lazy parsing state (for singleton reuse optimization)
  // Two-stage lazy parsing: metadata (cheap) vs value (expensive byte[] allocation)
  private Object lazySource;            // Source for lazy parsing (MemorySegment or byte[])
  private long lazyOffset;              // Offset where lazy metadata fields start
  private boolean metadataParsed;       // Whether prevRev, lastModRev, hash are parsed
  private boolean valueParsed;          // Whether value byte[] is parsed
  private boolean hasHash;              // Whether hash is stored (from config)
  private long valueOffset;             // Offset where value starts (after metadata)

  /**
   * Primary constructor with all primitive fields.
   * All fields are already parsed - no lazy loading needed.
   */
  public ObjectStringNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long hash, byte[] value,
      LongHashFunction hashFunction, byte[] deweyID) {
    this(nodeKey, parentKey, previousRevision, lastModifiedRevision, hash, value,
        hashFunction, deweyID, false, null);
  }

  /**
   * Primary constructor with all primitive fields and compression support.
   * All fields are already parsed - no lazy loading needed.
   */
  public ObjectStringNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long hash, byte[] value,
      LongHashFunction hashFunction, byte[] deweyID,
      boolean isCompressed, byte[] fsstSymbolTable) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.isCompressed = isCompressed;
    this.fsstSymbolTable = fsstSymbolTable;
    // Constructed with all values - mark as fully parsed
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  /**
   * Constructor with SirixDeweyID instead of byte array.
   * All fields are already parsed - no lazy loading needed.
   */
  public ObjectStringNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long hash, byte[] value,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this(nodeKey, parentKey, previousRevision, lastModifiedRevision, hash, value,
        hashFunction, deweyID, false, null);
  }

  /**
   * Constructor with SirixDeweyID and compression support.
   * All fields are already parsed - no lazy loading needed.
   */
  public ObjectStringNode(long nodeKey, long parentKey, int previousRevision,
      int lastModifiedRevision, long hash, byte[] value,
      LongHashFunction hashFunction, SirixDeweyID deweyID,
      boolean isCompressed, byte[] fsstSymbolTable) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.isCompressed = isCompressed;
    this.fsstSymbolTable = fsstSymbolTable;
    // Constructed with all values - mark as fully parsed
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_STRING_VALUE;
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

    bytes.writeUtf8(getValue());

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

  @Override
  public byte[] getRawValue() {
    if (!valueParsed) {
      parseValueField();
    }
    // If compressed, decode on first access
    if (isCompressed && decodedValue == null && value != null) {
      decodedValue = FSSTCompressor.decode(value, fsstSymbolTable);
    }
    return isCompressed ? decodedValue : value;
  }

  /**
   * Get the raw (possibly compressed) value bytes without FSST decoding.
   * Use this for serialization to preserve compression.
   * 
   * @return the raw bytes as stored, possibly FSST compressed
   */
  public byte[] getRawValueWithoutDecompression() {
    if (!valueParsed) {
      parseValueField();
    }
    return value;
  }

  @Override
  public void setRawValue(final byte[] value) {
    this.value = value;
    this.decodedValue = null;
    this.valueParsed = true;
  }

  /**
   * Set the raw value with compression information.
   * 
   * @param value the value bytes (possibly compressed)
   * @param isCompressed true if value is FSST compressed
   * @param fsstSymbolTable the symbol table for decompression (or null if not compressed)
   */
  public void setRawValue(final byte[] value, boolean isCompressed, byte[] fsstSymbolTable) {
    this.value = value;
    this.isCompressed = isCompressed;
    this.fsstSymbolTable = fsstSymbolTable;
    this.decodedValue = null;
    this.valueParsed = true;
  }

  /**
   * Check if the stored value is FSST compressed.
   * 
   * @return true if compressed
   */
  public boolean isCompressed() {
    return isCompressed;
  }

  /**
   * Set compression state.
   * 
   * @param isCompressed true if value is compressed
   */
  public void setCompressed(boolean isCompressed) {
    this.isCompressed = isCompressed;
  }

  /**
   * Get the FSST symbol table.
   * 
   * @return the symbol table, or null if not using FSST
   */
  public byte[] getFsstSymbolTable() {
    return fsstSymbolTable;
  }

  /**
   * Set the FSST symbol table.
   * 
   * @param fsstSymbolTable the symbol table
   */
  public void setFsstSymbolTable(byte[] fsstSymbolTable) {
    this.fsstSymbolTable = fsstSymbolTable;
    this.decodedValue = null;
  }

  @Override
  public String getValue() {
    // Use getRawValue() which handles FSST decompression
    final byte[] rawValue = getRawValue();
    return new String(rawValue, Constants.DEFAULT_ENCODING);
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

  /**
   * Populate this node from a BytesIn source for singleton reuse.
   * LAZY OPTIMIZATION: Only parses structural field (parentKey) immediately.
   * Two-stage lazy parsing: metadata (cheap) vs value (expensive byte[] allocation).
   */
  public void readFrom(final BytesIn<?> source, final long nodeKey, final byte[] deweyId,
                       final LongHashFunction hashFunction, final ResourceConfiguration config) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    
    // STRUCTURAL FIELD - parse immediately (parentKey is the only one for leaf nodes)
    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    
    // Store state for lazy parsing - DON'T parse remaining fields yet
    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.metadataParsed = false;
    this.valueParsed = false;
    this.hasHash = config.hashType != HashType.NONE;
    this.valueOffset = 0;
    
    // Initialize lazy fields to defaults (will be populated on demand)
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
    this.value = null;
  }
  
  /**
   * Parse metadata fields on demand (cheap - just varints and optionally a long).
   */
  private void parseMetadataFields() {
    if (metadataParsed) {
      return;
    }
    
    if (lazySource == null) {
      metadataParsed = true;
      return;
    }
    
    BytesIn<?> bytesIn = createBytesIn(lazyOffset);
    
    this.previousRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(bytesIn);
    if (hasHash) {
      this.hash = bytesIn.readLong();
    }
    this.valueOffset = bytesIn.position();
    this.metadataParsed = true;
  }
  
  /**
   * Parse value field on demand (expensive - allocates byte[]).
   */
  private void parseValueField() {
    if (valueParsed) {
      return;
    }
    
    if (!metadataParsed) {
      parseMetadataFields();
    }
    
    if (lazySource == null) {
      valueParsed = true;
      return;
    }
    
    BytesIn<?> bytesIn = createBytesIn(valueOffset);
    
    // Read compression flag (1 byte: 0 = none, 1 = FSST)
    byte compressionByte = bytesIn.readByte();
    this.isCompressed = compressionByte == 1;
    
    int length = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.value = new byte[length];
    bytesIn.read(this.value);
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

  /**
   * Create a deep copy snapshot of this node.
   * Forces parsing of all lazy fields since snapshot must be independent.
   */
  public ObjectStringNode toSnapshot() {
    if (!metadataParsed) {
      parseMetadataFields();
    }
    if (!valueParsed) {
      parseValueField();
    }
    return new ObjectStringNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        hash, value != null ? value.clone() : null, hashFunction,
        deweyIDBytes != null ? deweyIDBytes.clone() : null,
        isCompressed, fsstSymbolTable != null ? fsstSymbolTable.clone() : null);
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
    return visitor.visit(ImmutableObjectStringNode.of(this));
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("value", getValue())
                      .add("parentKey", parentKey)
                      .add("previousRevision", previousRevision)
                      .add("lastModifiedRevision", lastModifiedRevision)
                      .toString();
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, parentKey, getValue());
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ObjectStringNode other))
      return false;

    return nodeKey == other.nodeKey
        && parentKey == other.parentKey
        && Objects.equal(getValue(), other.getValue());
  }
}
