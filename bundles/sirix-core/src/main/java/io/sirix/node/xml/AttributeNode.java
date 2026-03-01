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
import io.sirix.page.KeyValueLeafPage;
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
  private boolean writeSingleton;
  private KeyValueLeafPage ownerPage;
  private final int[] heapOffsets;
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
    this.heapOffsets = new int[FIELD_COUNT];
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
    this.heapOffsets = new int[FIELD_COUNT];
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
    this.heapOffsets = new int[FIELD_COUNT];
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
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ATTR_PARENT_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(parentKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, parentKey, nodeKey);
        return;
      }
      resizeParentKey(parentKey);
      return;
    }
    this.parentKey = parentKey;
  }

  private void resizeParentKey(final long parentKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ATTR_PARENT_KEY, NodeFieldLayout.ATTRIBUTE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, parentKey, nodeKey));
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
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ATTR_PATH_NODE_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(pathNodeKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, pathNodeKey, nodeKey);
        return;
      }
      resizePathNodeKey(pathNodeKey);
      return;
    }
    this.pathNodeKey = pathNodeKey;
  }

  private void resizePathNodeKey(final long pathNodeKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ATTR_PATH_NODE_KEY, NodeFieldLayout.ATTRIBUTE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, pathNodeKey, nodeKey));
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
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ATTR_PREFIX_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(prefixKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, prefixKey);
        return;
      }
      resizePrefixKey(prefixKey);
      return;
    }
    this.prefixKey = prefixKey;
  }

  private void resizePrefixKey(final int prefixKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ATTR_PREFIX_KEY, NodeFieldLayout.ATTRIBUTE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, prefixKey));
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
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ATTR_LOCAL_NAME_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(localNameKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, localNameKey);
        return;
      }
      resizeLocalNameKey(localNameKey);
      return;
    }
    this.localNameKey = localNameKey;
  }

  private void resizeLocalNameKey(final int localNameKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ATTR_LOCAL_NAME_KEY, NodeFieldLayout.ATTRIBUTE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, localNameKey));
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
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ATTR_URI_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(uriKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, uriKey);
        return;
      }
      resizeURIKey(uriKey);
      return;
    }
    this.uriKey = uriKey;
  }

  private void resizeURIKey(final int uriKey) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ATTR_URI_KEY, NodeFieldLayout.ATTRIBUTE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, uriKey));
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
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ATTR_PREV_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      resizePreviousRevision(revision);
      return;
    }
    this.previousRevision = revision;
  }

  private void resizePreviousRevision(final int revision) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ATTR_PREV_REVISION, NodeFieldLayout.ATTRIBUTE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
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
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.ATTR_LAST_MOD_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      resizeLastModifiedRevision(revision);
      return;
    }
    this.lastModifiedRevision = revision;
  }

  private void resizeLastModifiedRevision(final int revision) {
    ownerPage.resizeRecordField(this, nodeKey, slotIndex,
        NodeFieldLayout.ATTR_LAST_MOD_REVISION, NodeFieldLayout.ATTRIBUTE_FIELD_COUNT,
        (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
  }

  @Override
  public long getHash() {
    // Return stored hash if set by rollingAdd/rollingUpdate, else compute on demand
    if (hash != 0L) {
      return hash;
    }
    if (hashFunction != null) {
      hash = computeHash(Bytes.threadLocalHashBuffer());
    }
    return hash;
  }

  @Override
  public void setHash(final long hash) {
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
    final var owner = this.ownerPage;
    if (owner != null) {
      final long nk = this.nodeKey;
      final int slot = this.slotIndex;
      unbind();
      this.value = value;
      this.valueParsed = true;
      this.lazyValueSource = null;
      this.lazyValueOffset = 0L;
      this.lazyValueLength = 0;
      this.hash = 0L;
      owner.resizeRecord(this, nk, slot);
      return;
    }
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
    final var owner = this.ownerPage;
    if (owner != null) {
      final long nk = this.nodeKey;
      final int slot = this.slotIndex;
      unbind();
      this.sirixDeweyID = id;
      this.deweyIDBytes = null;
      owner.resizeRecord(this, nk, slot);
      return;
    }
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
    this.hash = 0;
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
    if (!valueParsed) {
      readPayloadFromPage();
    }
    this.ownerPage = null;
    this.page = null;
  }

  @Override
  public void clearBinding() {
    this.page = null;
    this.ownerPage = null;
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

  @Override public boolean isWriteSingleton() { return writeSingleton; }
  @Override public void setWriteSingleton(final boolean ws) { this.writeSingleton = ws; }
  @Override public KeyValueLeafPage getOwnerPage() { return ownerPage; }
  @Override public void setOwnerPage(final KeyValueLeafPage p) { this.ownerPage = p; }

  @Override
  public int estimateSerializedSize() {
    final int payloadLen = value != null ? value.length : 0;
    return 55 + payloadLen;
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

  // ==================== DIRECT WRITE ====================

  /**
   * Encode an AttributeNode record directly to a MemorySegment from parameter values.
   * Static -- reads nothing from any instance. Zero field intermediation.
   *
   * @param target       the target MemorySegment (reinterpreted slotted page)
   * @param offset       absolute byte offset to write at
   * @param heapOffsets  pre-allocated offset array (reused, FIELD_COUNT elements)
   * @param nodeKey      the node key (delta base for structural keys)
   * @param parentKey    the parent node key
   * @param pathNodeKey  the path node key
   * @param prefixKey    the prefix key
   * @param localNameKey the local name key
   * @param uriKey       the URI key
   * @param prevRev      the previous revision number
   * @param lastModRev   the last modified revision number
   * @param rawValue     the raw value bytes
   * @return the total number of bytes written
   */
  public static int writeNewRecord(final MemorySegment target, final long offset,
      final int[] heapOffsets, final long nodeKey,
      final long parentKey, final long pathNodeKey,
      final int prefixKey, final int localNameKey, final int uriKey,
      final int prevRev, final int lastModRev,
      final byte[] rawValue) {
    long pos = offset;

    // Write nodeKind byte
    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.ATTRIBUTE.getId());
    pos++;

    // Reserve space for offset table
    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    // Data region start
    final long dataStart = pos;

    // Field 0: parentKey (delta-varint)
    heapOffsets[NodeFieldLayout.ATTR_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    // Field 1: pathNodeKey (delta-varint)
    heapOffsets[NodeFieldLayout.ATTR_PATH_NODE_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, pathNodeKey, nodeKey);

    // Field 2: prefixKey (signed varint)
    heapOffsets[NodeFieldLayout.ATTR_PREFIX_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prefixKey);

    // Field 3: localNameKey (signed varint)
    heapOffsets[NodeFieldLayout.ATTR_LOCAL_NAME_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, localNameKey);

    // Field 4: uriKey (signed varint)
    heapOffsets[NodeFieldLayout.ATTR_URI_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, uriKey);

    // Field 5: prevRevision (signed varint)
    heapOffsets[NodeFieldLayout.ATTR_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prevRev);

    // Field 6: lastModRevision (signed varint)
    heapOffsets[NodeFieldLayout.ATTR_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModRev);

    // Field 7: payload [isCompressed=0:1][valueLength:varint][value:bytes]
    heapOffsets[NodeFieldLayout.ATTR_PAYLOAD] = (int) (pos - dataStart);
    target.set(ValueLayout.JAVA_BYTE, pos, (byte) 0); // attributes are never compressed
    pos++;
    final byte[] val = rawValue != null ? rawValue : new byte[0];
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, val.length);
    if (val.length > 0) {
      MemorySegment.copy(val, 0, target, ValueLayout.JAVA_BYTE, pos, val.length);
      pos += val.length;
    }

    // Write offset table
    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) heapOffsets[i]);
    }

    return (int) (pos - offset);
  }

  /**
   * Serialize this node from Java fields. Delegates to static writeNewRecord.
   */
  @Override
  public int serializeToHeap(final MemorySegment target, final long offset) {
    if (!valueParsed) {
      if (page != null) {
        readPayloadFromPage();
      } else {
        parseLazyValue();
      }
    }
    return writeNewRecord(target, offset, heapOffsets, nodeKey,
        parentKey, pathNodeKey, prefixKey, localNameKey, uriKey,
        previousRevision, lastModifiedRevision, value);
  }

  /**
   * Get the pre-allocated heap offsets array for use with static writeNewRecord.
   */
  public int[] getHeapOffsets() {
    return heapOffsets;
  }

  /**
   * Set DeweyID fields directly after creation, bypassing write-through.
   * The DeweyID is already in the page trailer -- this just sets the Java cache fields.
   */
  public void setDeweyIDAfterCreation(final SirixDeweyID id, final byte[] bytes) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = bytes;
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
          hash,
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
