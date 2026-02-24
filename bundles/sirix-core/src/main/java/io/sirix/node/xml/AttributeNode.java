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
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.XmlNodeVisitor;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.immutable.xml.ImmutableAttributeNode;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableXmlNode;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.NamePageHash;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import net.openhft.hashing.LongHashFunction;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Node representing an attribute, using primitive fields.
 *
 * @author Johannes Lichtenberger
 */
public final class AttributeNode implements ValueNode, NameNode, ImmutableXmlNode, FlyweightNode {

  // === PRIMITIVE FIELDS ===
  private long nodeKey;
  private long parentKey;
  private long pathNodeKey;
  private int prefixKey;
  private int localNameKey;
  private int uriKey;
  private int previousRevision;
  private int lastModifiedRevision;
  private long hash;

  // === VALUE ===
  private byte[] value;
  private Object lazyValueSource;
  private long lazyValueOffset;
  private int lazyValueLength;
  private boolean valueParsed = true;

  // === NON-SERIALIZED FIELDS ===
  private LongHashFunction hashFunction;
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;
  private QNm qNm;

  // ==================== FLYWEIGHT BINDING (LeanStore page-direct access) ====================
  private MemorySegment page;
  private long recordBase;
  private long dataRegionStart;
  private int slotIndex;
  private static final int FIELD_COUNT = NodeFieldLayout.ATTRIBUTE_FIELD_COUNT;

  /**
   * Constructor for flyweight binding.
   * All fields except nodeKey and hashFunction will be read from page memory after bind().
   *
   * @param nodeKey the node key
   * @param hashFunction the hash function from resource config
   */
  public AttributeNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
  }

  /**
   * Primary constructor with all primitive fields.
   */
  public AttributeNode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision, long pathNodeKey,
      int prefixKey, int localNameKey, int uriKey, long hash, byte[] value, LongHashFunction hashFunction,
      byte[] deweyID, QNm qNm) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.pathNodeKey = pathNodeKey;
    this.prefixKey = prefixKey;
    this.localNameKey = localNameKey;
    this.uriKey = uriKey;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.qNm = qNm;
  }

  /**
   * Constructor with SirixDeweyID.
   */
  public AttributeNode(long nodeKey, long parentKey, int previousRevision, int lastModifiedRevision, long pathNodeKey,
      int prefixKey, int localNameKey, int uriKey, long hash, byte[] value, LongHashFunction hashFunction,
      SirixDeweyID deweyID, QNm qNm) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.pathNodeKey = pathNodeKey;
    this.prefixKey = prefixKey;
    this.localNameKey = localNameKey;
    this.uriKey = uriKey;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.qNm = qNm;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.ATTRIBUTE;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public void setNodeKey(long nodeKey) {
    this.nodeKey = nodeKey;
  }

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.ATTR_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.ATTR_PARENT_KEY, parentKey);
      if (page != null) return;
    }
    this.parentKey = parentKey;
  }

  @Override
  public boolean hasParent() {
    return getParentKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public long getPathNodeKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.ATTR_PATH_NODE_KEY, nodeKey);
    }
    return pathNodeKey;
  }

  @Override
  public void setPathNodeKey(@NonNegative final long pathNodeKey) {
    if (page != null) {
      setDeltaFieldInPlace(NodeFieldLayout.ATTR_PATH_NODE_KEY, pathNodeKey);
      if (page != null) return;
    }
    this.pathNodeKey = pathNodeKey;
  }

  @Override
  public int getPrefixKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ATTR_PREFIX_KEY);
    }
    return prefixKey;
  }

  @Override
  public void setPrefixKey(final int prefixKey) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.ATTR_PREFIX_KEY, prefixKey)) return;
    }
    this.prefixKey = prefixKey;
  }

  @Override
  public int getLocalNameKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ATTR_LOCAL_NAME_KEY);
    }
    return localNameKey;
  }

  @Override
  public void setLocalNameKey(final int localNameKey) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.ATTR_LOCAL_NAME_KEY, localNameKey)) return;
    }
    this.localNameKey = localNameKey;
  }

  @Override
  public int getURIKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ATTR_URI_KEY);
    }
    return uriKey;
  }

  @Override
  public void setURIKey(final int uriKey) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.ATTR_URI_KEY, uriKey)) return;
    }
    this.uriKey = uriKey;
  }

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ATTR_PREV_REVISION);
    }
    return previousRevision;
  }

  @Override
  public void setPreviousRevision(final int revision) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.ATTR_PREV_REVISION, revision)) return;
    }
    this.previousRevision = revision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.ATTR_LAST_MOD_REVISION);
    }
    return lastModifiedRevision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      if (setSignedFieldInPlace(NodeFieldLayout.ATTR_LAST_MOD_REVISION, revision)) return;
    }
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (page != null) {
      // Bound: read hash from MemorySegment. If non-zero (set by rollingAdd/rollingUpdate),
      // return it as-is to preserve rolling hash arithmetic. If zero (never set), compute.
      final long storedHash = readLongField(NodeFieldLayout.ATTR_HASH);
      if (storedHash != 0L) {
        return storedHash;
      }
      if (hashFunction != null) {
        return computeHash(Bytes.threadLocalHashBuffer());
      }
      return 0L;
    }
    // Unbound (in-memory): return stored hash if set by rollingAdd, else compute
    if (hash == 0L && hashFunction != null) {
      hash = computeHash(Bytes.threadLocalHashBuffer());
    }
    return hash;
  }

  @Override
  public void setHash(final long hash) {
    if (page != null) {
      // Hash is ALWAYS in-place (fixed 8 bytes)
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE,
          recordBase + 1 + NodeFieldLayout.ATTR_HASH) & 0xFF;
      DeltaVarIntCodec.writeLongToSegment(page, dataRegionStart + fieldOff, hash);
      return;
    }
    this.hash = hash;
  }

  @Override
  public byte[] getRawValue() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    } else if (!valueParsed) {
      parseLazyValue();
    }
    return value;
  }

  @Override
  public void setRawValue(final byte[] value) {
    if (page != null) unbind();
    this.value = value;
    this.valueParsed = true;
    this.lazyValueSource = null;
    this.lazyValueOffset = 0L;
    this.lazyValueLength = 0;
    this.hash = 0L;
  }

  public void setLazyRawValue(Object source, long valueOffset, int valueLength) {
    this.lazyValueSource = source;
    this.lazyValueOffset = valueOffset;
    this.lazyValueLength = valueLength;
    this.value = null;
    this.valueParsed = false;
    this.hash = 0L;
  }

  @Override
  public String getValue() {
    final byte[] rawValue = getRawValue();
    return rawValue != null
        ? new String(rawValue, Constants.DEFAULT_ENCODING)
        : "";
  }

  @Override
  public QNm getName() {
    return qNm;
  }

  public void setName(QNm name) {
    this.qNm = name;
  }

  /**
   * Returns the raw value bytes without triggering decompression. Attributes are never compressed, so
   * this is identical to getRawValue(). Provided for consistency with the fixed-slot projector
   * interface.
   */
  public byte[] getRawValueWithoutDecompression() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    } else if (!valueParsed) {
      parseLazyValue();
    }
    return value;
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
  public void setTypeKey(int typeKey) {}

  @Override
  public void setDeweyID(SirixDeweyID id) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = null;
  }

  public void setDeweyIDBytes(byte[] deweyIDBytes) {
    this.deweyIDBytes = deweyIDBytes;
    this.sirixDeweyID = null;
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

  // ==================== FLYWEIGHT BIND/UNBIND ====================

  @Override
  public void bind(final MemorySegment page, final long recordBase, final long nodeKey,
      final int slotIndex) {
    this.page = page;
    this.recordBase = recordBase;
    this.nodeKey = nodeKey;
    this.slotIndex = slotIndex;
    this.dataRegionStart = recordBase + 1 + FIELD_COUNT;
    this.valueParsed = false;
    this.lazyValueSource = null;
  }

  @Override
  public void unbind() {
    if (page == null) return;
    final long nk = this.nodeKey;
    this.parentKey = readDeltaField(NodeFieldLayout.ATTR_PARENT_KEY, nk);
    this.pathNodeKey = readDeltaField(NodeFieldLayout.ATTR_PATH_NODE_KEY, nk);
    this.prefixKey = readSignedField(NodeFieldLayout.ATTR_PREFIX_KEY);
    this.localNameKey = readSignedField(NodeFieldLayout.ATTR_LOCAL_NAME_KEY);
    this.uriKey = readSignedField(NodeFieldLayout.ATTR_URI_KEY);
    this.previousRevision = readSignedField(NodeFieldLayout.ATTR_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.ATTR_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.ATTR_HASH);
    if (!valueParsed) {
      readPayloadFromPage();
    }
    this.page = null;
  }

  @Override
  public void clearBinding() {
    this.page = null;
  }

  @Override
  public boolean isBound() { return page != null; }

  @Override
  public boolean isBoundTo(final MemorySegment page) {
    return this.page == page;
  }

  @Override
  public int getSlotIndex() {
    return slotIndex;
  }

  @Override
  public int estimateSerializedSize() {
    final int payloadLen = value != null ? value.length : 0;
    return 64 + payloadLen;
  }

  // ==================== FLYWEIGHT FIELD READ HELPERS ====================

  private long readDeltaField(final int fieldIndex, final long baseKey) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.decodeDeltaFromSegment(page, dataRegionStart + fieldOff, baseKey);
  }

  private int readSignedField(final int fieldIndex) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.decodeSignedFromSegment(page, dataRegionStart + fieldOff);
  }

  private long readLongField(final int fieldIndex) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    return DeltaVarIntCodec.readLongFromSegment(page, (int) (dataRegionStart + fieldOff));
  }

  // ==================== FLYWEIGHT FIELD WRITE HELPERS ====================

  private void setDeltaFieldInPlace(final int fieldIndex, final long newKey) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    final long absOff = dataRegionStart + fieldOff;
    final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
    final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(newKey, nodeKey);
    if (newWidth == currentWidth) {
      DeltaVarIntCodec.writeDeltaToSegment(page, absOff, newKey, nodeKey);
    } else {
      unbind();
    }
  }

  private boolean setSignedFieldInPlace(final int fieldIndex, final int newValue) {
    final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + fieldIndex) & 0xFF;
    final long absOff = dataRegionStart + fieldOff;
    final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
    final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(newValue);
    if (newWidth == currentWidth) {
      DeltaVarIntCodec.writeSignedToSegment(page, absOff, newValue);
      return true;
    }
    unbind();
    return false;
  }

  /**
   * Read the payload (value bytes) directly from page memory when bound.
   */
  private void readPayloadFromPage() {
    final int payloadFieldOff = page.get(ValueLayout.JAVA_BYTE,
        recordBase + 1 + NodeFieldLayout.ATTR_PAYLOAD) & 0xFF;
    final long payloadStart = dataRegionStart + payloadFieldOff;

    // Read isCompressed flag (1 byte) - always 0 for attributes, but read for format consistency
    // skip: payloadStart + 0
    final long lenOffset = payloadStart + 1;

    // Read value length (varint)
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(page, lenOffset);
    final int lenBytes = DeltaVarIntCodec.readSignedVarintWidth(page, lenOffset);

    // Read value bytes
    final long dataOffset = lenOffset + lenBytes;
    this.value = new byte[length];
    if (length > 0) {
      MemorySegment.copy(page, ValueLayout.JAVA_BYTE, dataOffset, this.value, 0, length);
    }
    this.valueParsed = true;
  }

  // ==================== SERIALIZE TO HEAP ====================

  @Override
  public int serializeToHeap(final MemorySegment target, final long offset) {
    // Ensure all fields are materialized before serialization
    if (!valueParsed) {
      if (page != null) {
        readPayloadFromPage();
      } else {
        parseLazyValue();
      }
    }

    long pos = offset;
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.ATTRIBUTE.getId());
    pos++;

    final long offsetTableStart = pos;
    pos += FIELD_COUNT;
    final long dataStart = pos;
    final int[] offsets = new int[FIELD_COUNT];

    // Field 0: parentKey (delta-varint)
    offsets[NodeFieldLayout.ATTR_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    // Field 1: pathNodeKey (delta-varint)
    offsets[NodeFieldLayout.ATTR_PATH_NODE_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, pathNodeKey, nodeKey);

    // Field 2: prefixKey (signed varint)
    offsets[NodeFieldLayout.ATTR_PREFIX_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prefixKey);

    // Field 3: localNameKey (signed varint)
    offsets[NodeFieldLayout.ATTR_LOCAL_NAME_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, localNameKey);

    // Field 4: uriKey (signed varint)
    offsets[NodeFieldLayout.ATTR_URI_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, uriKey);

    // Field 5: prevRevision (signed varint)
    offsets[NodeFieldLayout.ATTR_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, previousRevision);

    // Field 6: lastModRevision (signed varint)
    offsets[NodeFieldLayout.ATTR_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModifiedRevision);

    // Field 7: hash (fixed 8 bytes)
    offsets[NodeFieldLayout.ATTR_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    // Field 8: payload [isCompressed:1][valueLength:varint][value:bytes]
    offsets[NodeFieldLayout.ATTR_PAYLOAD] = (int) (pos - dataStart);
    target.set(ValueLayout.JAVA_BYTE, pos, (byte) 0); // attributes are never compressed
    pos++;
    final byte[] rawValue = value != null ? value : new byte[0];
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, rawValue.length);
    if (rawValue.length > 0) {
      MemorySegment.copy(rawValue, 0, target, ValueLayout.JAVA_BYTE, pos, rawValue.length);
      pos += rawValue.length;
    }

    // Write offset table
    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) offsets[i]);
    }

    return (int) (pos - offset);
  }

  // ==================== SNAPSHOT ====================

  /**
   * Create a deep copy snapshot of this node.
   * Forces parsing of all lazy fields since snapshot must be independent.
   */
  public AttributeNode toSnapshot() {
    if (page != null) {
      // Bound mode: read all fields from page
      if (!valueParsed) {
        readPayloadFromPage();
      }
      return new AttributeNode(nodeKey,
          readDeltaField(NodeFieldLayout.ATTR_PARENT_KEY, nodeKey),
          readSignedField(NodeFieldLayout.ATTR_PREV_REVISION),
          readSignedField(NodeFieldLayout.ATTR_LAST_MOD_REVISION),
          readDeltaField(NodeFieldLayout.ATTR_PATH_NODE_KEY, nodeKey),
          readSignedField(NodeFieldLayout.ATTR_PREFIX_KEY),
          readSignedField(NodeFieldLayout.ATTR_LOCAL_NAME_KEY),
          readSignedField(NodeFieldLayout.ATTR_URI_KEY),
          readLongField(NodeFieldLayout.ATTR_HASH),
          value != null ? value.clone() : null,
          hashFunction,
          getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null,
          qNm);
    }
    // Unbound mode: force parse all lazy fields for snapshot (must be complete and independent)
    if (!valueParsed) {
      parseLazyValue();
    }
    return new AttributeNode(nodeKey, parentKey, previousRevision, lastModifiedRevision,
        pathNodeKey, prefixKey, localNameKey, uriKey, hash,
        value != null ? value.clone() : null,
        hashFunction,
        getDeweyIDAsBytes() != null ? getDeweyIDAsBytes().clone() : null,
        qNm);
  }

  // ==================== DESERIALIZATION ====================

  /**
   * Populate this node from a BytesIn source for singleton reuse.
   */
  public void readFrom(BytesIn<?> source, long nodeKey, byte[] deweyId, LongHashFunction hashFunction,
      ResourceConfiguration config) {
    // Unbind flyweight - ensures getters use Java fields, not stale page reference
    this.page = null;
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;

    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.prefixKey = DeltaVarIntCodec.decodeSigned(source);
    this.localNameKey = DeltaVarIntCodec.decodeSigned(source);
    this.uriKey = DeltaVarIntCodec.decodeSigned(source);
    this.previousRevision = DeltaVarIntCodec.decodeSigned(source);
    this.lastModifiedRevision = DeltaVarIntCodec.decodeSigned(source);

    source.readByte(); // reserved compression flag
    final int valueLength = DeltaVarIntCodec.decodeSigned(source);
    final long valueOffset = source.position();
    setLazyRawValue(source.getSource(), valueOffset, valueLength);
    source.position(valueOffset + valueLength);

    // Attribute hash is not serialized; keep it invalidated for lazy recompute.
    this.hash = 0L;
  }

  private void parseLazyValue() {
    if (valueParsed) {
      return;
    }

    if (lazyValueSource == null) {
      valueParsed = true;
      return;
    }

    final BytesIn<?> bytesIn = createBytesIn(lazyValueSource, lazyValueOffset);
    final byte[] parsedValue = new byte[lazyValueLength];
    if (lazyValueLength > 0) {
      bytesIn.read(parsedValue, 0, lazyValueLength);
    }
    value = parsedValue;
    valueParsed = true;
    lazyValueSource = null;
  }

  private static BytesIn<?> createBytesIn(Object source, long offset) {
    if (source instanceof MemorySegment segment) {
      final var bytesIn = new MemorySegmentBytesIn(segment);
      bytesIn.position(offset);
      return bytesIn;
    }
    if (source instanceof byte[] bytes) {
      final var bytesIn = new ByteArrayBytesIn(bytes);
      bytesIn.position(offset);
      return bytesIn;
    }
    throw new IllegalStateException("Unknown lazy source type: " + source.getClass());
  }

  @Override
  public long computeHash(final BytesOut<?> bytes) {
    if (hashFunction == null)
      return 0L;
    bytes.clear();
    bytes.writeLong(getNodeKey()).writeLong(getParentKey()).writeByte(getKind().getId());
    bytes.writeInt(getPrefixKey()).writeInt(getLocalNameKey()).writeInt(getURIKey());
    final byte[] rawValue = getRawValue();
    if (rawValue != null) {
      bytes.write(rawValue);
    }
    return bytes.hashDirect(hashFunction);
  }

  @Override
  public VisitResult acceptVisitor(XmlNodeVisitor visitor) {
    return visitor.visit(ImmutableAttributeNode.of(this));
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeKey, getParentKey(), getPrefixKey(), getLocalNameKey(), getURIKey());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof AttributeNode other))
      return false;
    return nodeKey == other.nodeKey && getParentKey() == other.getParentKey()
        && getPrefixKey() == other.getPrefixKey()
        && getLocalNameKey() == other.getLocalNameKey() && getURIKey() == other.getURIKey();
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("nodeKey", nodeKey)
                      .add("parentKey", getParentKey())
                      .add("qNm", qNm)
                      .add("value", getValue())
                      .toString();
  }
}
