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

import io.sirix.node.AbstractFlyweightNode;
import io.brackit.query.atomic.QNm;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;
import io.sirix.access.trx.node.json.FusedStringCursor;
import io.sirix.api.visitor.JsonNodeVisitor;
import io.sirix.api.visitor.VisitResult;
import io.sirix.api.visitor.VisitResultType;
import io.sirix.node.ByteArrayBytesIn;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.node.MemorySegmentBytesIn;
import io.sirix.node.NodeKind;
import io.sirix.node.SirixDeweyID;
import io.sirix.node.interfaces.FlyweightNode;
import io.sirix.node.interfaces.NameNode;
import io.sirix.node.interfaces.Node;
import io.sirix.node.interfaces.StructNode;
import io.sirix.node.interfaces.ValueNode;
import io.sirix.node.interfaces.immutable.ImmutableJsonNode;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.NodeFieldLayout;
import io.sirix.settings.Constants;
import io.sirix.settings.Fixed;
import io.sirix.utils.FSSTCompressor;
import net.openhft.hashing.LongHashFunction;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Objects;

/**
 * Fused JSON node representing an object key bound to a STRING value in a single slot.
 *
 * <p>
 * Replaces the legacy pair {@code OBJECT_KEY + OBJECT_STRING_VALUE} for the common
 * {@code {"fieldname": "value"}} pattern, eliminating one record per such field.
 *
 * <h2>Wire layout</h2>
 * 
 * <pre>
 * [kindByte=46][offsetTable: FIELD_COUNT × 1 byte][data region]
 *
 * Field order (offset table indices):
 *   0 parentKey (delta-varint, base=nodeKey)
 *   1 rightSiblingKey (delta-varint)
 *   2 leftSiblingKey (delta-varint)
 *   3 nameKey (signed varint)
 *   4 pathNodeKey (delta-varint)
 *   5 previousRevision (signed varint)
 *   6 lastModifiedRevision (signed varint)
 *   7 hash (fixed 8 bytes)
 *   8 payload [isCompressed:1][valueLength:varint][value:bytes]
 * </pre>
 *
 * <p>
 * HFT contract: primitive fields only, {@code final} where possible, zero-alloc bind/unbind,
 * offset-table lookups in O(1).
 */
public final class ObjectNamedStringNode extends AbstractFlyweightNode
    implements StructNode, NameNode, ValueNode, ImmutableJsonNode, FlyweightNode {

  private long nodeKey;
  private long parentKey;
  private long rightSiblingKey;
  private long leftSiblingKey;
  private int nameKey;
  private long pathNodeKey;
  private int previousRevision;
  private int lastModifiedRevision;
  private long hash;
  private byte[] value;
  private LongHashFunction hashFunction;

  // DeweyID support (lazily parsed)
  private SirixDeweyID sirixDeweyID;
  private byte[] deweyIDBytes;

  // Cache for name (not serialized)
  private QNm cachedName;

  // FSST compression state
  private boolean isCompressed;
  private byte[] fsstSymbolTable;
  private byte[] decodedValue;

  // Lazy parsing state
  private Object lazySource;
  private long lazyOffset;
  private boolean metadataParsed;
  private boolean valueParsed;
  private long valueOffset;
  private boolean hasHash;

  // ==================== FLYWEIGHT BINDING ====================

  private MemorySegment page;
  private long recordBase;
  private long dataRegionStart;
  private int slotIndex;
  private boolean writeSingleton;
  private KeyValueLeafPage ownerPage;
  private static final int FIELD_COUNT = NodeFieldLayout.OBJECT_NAMED_STRING_FIELD_COUNT;

  /** Raw UTF-8 payload stored in the inline record. */
  public static final byte PAYLOAD_FLAG_RAW = 0;

  /** FSST-encoded payload stored in the inline record. */
  public static final byte PAYLOAD_FLAG_FSST = 1;

  /**
   * The inline record carries only the fused field metadata; the authoritative record, including its
   * value, lives in the same-key {@code OverflowPage} reference.
   *
   * <p>
   * This third state is deliberately explicit. Treating an overflow descriptor as an empty raw string
   * would let column sketches and direct-slot readers manufacture false negatives.
   * </p>
   */
  public static final byte PAYLOAD_FLAG_OVERFLOW = 2;

  /**
   * Upper bound on the serialized size of everything except the string payload: kind byte +
   * {@link #FIELD_COUNT}-byte offset table + seven delta varints (≤ 9 bytes each) + 8-byte hash +
   * compressed flag + payload-length varint. Used by {@link #estimateSerializedSize()}.
   */
  private static final int SERIALIZED_METADATA_UPPER_BOUND = 80;

  private static final byte[] EMPTY_PAYLOAD = new byte[0];

  public ObjectNamedStringNode(long nodeKey, LongHashFunction hashFunction) {
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
  }

  public ObjectNamedStringNode(long nodeKey, long parentKey, long rightSiblingKey, long leftSiblingKey, int nameKey,
      long pathNodeKey, int previousRevision, int lastModifiedRevision, long hash, byte[] value,
      LongHashFunction hashFunction, byte[] deweyID) {
    this(nodeKey, parentKey, rightSiblingKey, leftSiblingKey, nameKey, pathNodeKey, previousRevision,
        lastModifiedRevision, hash, value, hashFunction, deweyID, false, null);
  }

  public ObjectNamedStringNode(long nodeKey, long parentKey, long rightSiblingKey, long leftSiblingKey, int nameKey,
      long pathNodeKey, int previousRevision, int lastModifiedRevision, long hash, byte[] value,
      LongHashFunction hashFunction, byte[] deweyID, boolean isCompressed, byte[] fsstSymbolTable) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.nameKey = nameKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyID;
    this.isCompressed = isCompressed;
    this.fsstSymbolTable = fsstSymbolTable;
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  public ObjectNamedStringNode(long nodeKey, long parentKey, long rightSiblingKey, long leftSiblingKey, int nameKey,
      long pathNodeKey, int previousRevision, int lastModifiedRevision, long hash, byte[] value,
      LongHashFunction hashFunction, SirixDeweyID deweyID) {
    this(nodeKey, parentKey, rightSiblingKey, leftSiblingKey, nameKey, pathNodeKey, previousRevision,
        lastModifiedRevision, hash, value, hashFunction, deweyID, false, null);
  }

  public ObjectNamedStringNode(long nodeKey, long parentKey, long rightSiblingKey, long leftSiblingKey, int nameKey,
      long pathNodeKey, int previousRevision, int lastModifiedRevision, long hash, byte[] value,
      LongHashFunction hashFunction, SirixDeweyID deweyID, boolean isCompressed, byte[] fsstSymbolTable) {
    this.nodeKey = nodeKey;
    this.parentKey = parentKey;
    this.rightSiblingKey = rightSiblingKey;
    this.leftSiblingKey = leftSiblingKey;
    this.nameKey = nameKey;
    this.pathNodeKey = pathNodeKey;
    this.previousRevision = previousRevision;
    this.lastModifiedRevision = lastModifiedRevision;
    this.hash = hash;
    this.value = value;
    this.hashFunction = hashFunction;
    this.sirixDeweyID = deweyID;
    this.isCompressed = isCompressed;
    this.fsstSymbolTable = fsstSymbolTable;
    this.metadataParsed = true;
    this.valueParsed = true;
  }

  // ==================== FLYWEIGHT BIND/UNBIND ====================

  public void bind(final MemorySegment page, final long recordBase, final long nodeKey, final int slotIndex) {
    this.page = page;
    this.recordBase = recordBase;
    this.nodeKey = nodeKey;
    this.slotIndex = slotIndex;
    this.dataRegionStart = recordBase + 1 + FIELD_COUNT;
    this.metadataParsed = true;
    this.valueParsed = false;
    this.lazySource = null;
  }

  public void unbind() {
    if (page == null) {
      return;
    }
    final long nk = this.nodeKey;
    this.parentKey = readDeltaField(NodeFieldLayout.OBJNAMEDSTR_PARENT_KEY, nk);
    this.rightSiblingKey = readDeltaField(NodeFieldLayout.OBJNAMEDSTR_RIGHT_SIB_KEY, nk);
    this.leftSiblingKey = readDeltaField(NodeFieldLayout.OBJNAMEDSTR_LEFT_SIB_KEY, nk);
    this.nameKey = readSignedField(NodeFieldLayout.OBJNAMEDSTR_NAME_KEY);
    this.pathNodeKey = readDeltaField(NodeFieldLayout.OBJNAMEDSTR_PATH_NODE_KEY, nk);
    this.previousRevision = readSignedField(NodeFieldLayout.OBJNAMEDSTR_PREV_REVISION);
    this.lastModifiedRevision = readSignedField(NodeFieldLayout.OBJNAMEDSTR_LAST_MOD_REVISION);
    this.hash = readLongField(NodeFieldLayout.OBJNAMEDSTR_HASH);
    if (!valueParsed) {
      readPayloadFromPage();
    }
    this.page = null;
    this.ownerPage = null;
  }

  @Override
  public void clearBinding() {
    this.page = null;
    this.ownerPage = null;
  }

  public boolean isBound() {
    return page != null;
  }

  @Override
  public boolean isBoundTo(final MemorySegment page) {
    return this.page == page;
  }

  @Override
  public int getSlotIndex() {
    return slotIndex;
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

  private void readPayloadFromPage() {
    final int payloadFieldOff =
        page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long payloadStart = dataRegionStart + payloadFieldOff;
    final MemorySegmentBytesIn bytesIn = new MemorySegmentBytesIn(page);
    bytesIn.position(payloadStart);
    final byte flag = bytesIn.readByte();
    if (flag == PAYLOAD_FLAG_OVERFLOW) {
      throw new IllegalStateException("Overflow descriptor for node " + nodeKey
          + " was bound as an inline value instead of resolving its OverflowPage");
    }
    if (flag != PAYLOAD_FLAG_RAW && flag != PAYLOAD_FLAG_FSST) {
      throw new IllegalStateException("Corrupted fused string payload flag " + flag + " for node " + nodeKey);
    }
    this.isCompressed = flag == PAYLOAD_FLAG_FSST;
    final int length = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.value = new byte[length];
    bytesIn.read(this.value);
    this.valueParsed = true;
  }

  /**
   * Copy this fused value's decoded semantic UTF-8 bytes into caller-owned storage.
   *
   * <p>
   * The bound-page branch deliberately parses and decodes in place: neither a
   * {@link MemorySegmentBytesIn} wrapper nor an encoded/decoded value array is created, and no page
   * range escapes the call. The ordinary public value methods retain their existing materializing and
   * caching behavior.
   * </p>
   *
   * @see FusedStringCursor#readFusedStringUtf8(byte[])
   */
  public int readFusedStringUtf8(final byte[] valueOut) {
    Objects.requireNonNull(valueOut, "valueOut must not be null");
    if (page != null) {
      return readBoundSemanticUtf8(valueOut);
    }
    if (!valueParsed || value == null) {
      return FusedStringCursor.UNAVAILABLE;
    }
    return copySemanticUtf8(value, 0, value.length, isCompressed, valueOut);
  }

  private int readBoundSemanticUtf8(final byte[] valueOut) {
    final int payloadFieldOff =
        page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PAYLOAD) & 0xFF;
    final long payloadStart = dataRegionStart + payloadFieldOff;
    final byte flag = page.get(ValueLayout.JAVA_BYTE, payloadStart);
    if (flag == PAYLOAD_FLAG_OVERFLOW) {
      return FusedStringCursor.UNAVAILABLE;
    }
    if (flag != PAYLOAD_FLAG_RAW && flag != PAYLOAD_FLAG_FSST) {
      throw new IllegalStateException("Corrupted fused string payload flag " + flag + " for node " + nodeKey);
    }
    final boolean compressed = flag == PAYLOAD_FLAG_FSST;
    final long lengthOffset = payloadStart + 1;
    final int length = DeltaVarIntCodec.decodeSignedFromSegment(page, lengthOffset);
    if (length < 0) {
      throw new IllegalStateException("Corrupted fused string payload: negative byte length " + length);
    }
    final int lengthWidth = DeltaVarIntCodec.readSignedVarintWidth(page, lengthOffset);
    final long valueStart = lengthOffset + lengthWidth;
    if (valueStart < 0 || valueStart > page.byteSize() || length > page.byteSize() - valueStart) {
      throw new IllegalStateException("Corrupted fused string payload: " + length + " bytes at offset " + valueStart
          + " exceed page size " + page.byteSize());
    }
    if (!compressed) {
      if (valueOut.length < length) {
        return FusedStringCursor.insufficientCapacity(length);
      }
      if (length > 0) {
        MemorySegment.copy(page, ValueLayout.JAVA_BYTE, valueStart, valueOut, 0, length);
      }
      return length;
    }
    return decodeBoundFsst(valueStart, length, valueOut);
  }

  private int decodeBoundFsst(final long valueStart, final int encodedLength, final byte[] valueOut) {
    if (encodedLength == 0) {
      return 0;
    }
    final byte[][] symbols = FSSTCompressor.parsedFor(fsstSymbolTable);
    if (symbols.length == 0) {
      if (valueOut.length < encodedLength) {
        return FusedStringCursor.insufficientCapacity(encodedLength);
      }
      MemorySegment.copy(page, ValueLayout.JAVA_BYTE, valueStart, valueOut, 0, encodedLength);
      return encodedLength;
    }

    final int required = maximumDecodedCapacity(encodedLength);
    if (valueOut.length < required) {
      return FusedStringCursor.insufficientCapacity(required);
    }
    final byte header = page.get(ValueLayout.JAVA_BYTE, valueStart);
    if (header != FSSTCompressor.HEADER_COMPRESSED) {
      final int skip = header == FSSTCompressor.HEADER_RAW
          ? 1
          : 0;
      final int decodedLength = encodedLength - skip;
      if (decodedLength > 0) {
        MemorySegment.copy(page, ValueLayout.JAVA_BYTE, valueStart + skip, valueOut, 0, decodedLength);
      }
      return decodedLength;
    }

    final long end = valueStart + encodedLength;
    long pos = valueStart + 1;
    int outPos = 0;
    while (pos < end) {
      final int code = page.get(ValueLayout.JAVA_BYTE, pos++) & 0xFF;
      if (code == (FSSTCompressor.ESCAPE_BYTE & 0xFF)) {
        if (pos >= end) {
          throw new IllegalStateException("Corrupted FSST data: escape at end");
        }
        valueOut[outPos++] = page.get(ValueLayout.JAVA_BYTE, pos++);
      } else if (code < symbols.length) {
        final byte[] symbol = symbols[code];
        if (symbol.length > FSSTCompressor.MAX_SYMBOL_LENGTH) {
          throw new IllegalStateException("Corrupted FSST symbol table: symbol " + code + " is " + symbol.length
              + " bytes, over the " + FSSTCompressor.MAX_SYMBOL_LENGTH + "-byte maximum");
        }
        if (symbol.length == 1) {
          valueOut[outPos] = symbol[0];
        } else {
          System.arraycopy(symbol, 0, valueOut, outPos, symbol.length);
        }
        outPos += symbol.length;
      } else {
        throw new IllegalStateException("Corrupted FSST data: unexpected byte code " + code);
      }
    }
    return outPos;
  }

  private int copySemanticUtf8(final byte[] source, final int offset, final int length, final boolean compressed,
      final byte[] valueOut) {
    if (!compressed) {
      if (valueOut.length < length) {
        return FusedStringCursor.insufficientCapacity(length);
      }
      System.arraycopy(source, offset, valueOut, 0, length);
      return length;
    }
    if (length == 0) {
      return 0;
    }
    final byte[][] symbols = FSSTCompressor.parsedFor(fsstSymbolTable);
    if (symbols.length == 0) {
      if (valueOut.length < length) {
        return FusedStringCursor.insufficientCapacity(length);
      }
      System.arraycopy(source, offset, valueOut, 0, length);
      return length;
    }
    final int required = maximumDecodedCapacity(length);
    if (valueOut.length < required) {
      return FusedStringCursor.insufficientCapacity(required);
    }
    return FSSTCompressor.decodeInto(source, offset, length, symbols, valueOut, 0);
  }

  private static int maximumDecodedCapacity(final int encodedLength) {
    if (encodedLength > Integer.MAX_VALUE / FSSTCompressor.MAX_SYMBOL_LENGTH) {
      throw new IllegalStateException("Corrupted fused string payload: encoded length " + encodedLength
          + " cannot have an int-indexed decoded representation");
    }
    return FSSTCompressor.maxDecodedLength(encodedLength);
  }

  // ==================== OWNER PAGE ====================

  @Override
  public KeyValueLeafPage getOwnerPage() {
    return ownerPage;
  }

  @Override
  public void setOwnerPage(final KeyValueLeafPage ownerPage) {
    this.ownerPage = ownerPage;
  }

  // ==================== SERIALIZE TO HEAP ====================

  public static int writeNewRecord(final MemorySegment target, final long offset, final int[] heapOffsets,
      final long nodeKey, final long parentKey, final long rightSibKey, final long leftSibKey, final int nameKey,
      final long pathNodeKey, final int prevRev, final int lastModRev, final long hash, final byte[] rawValue,
      final boolean isCompressed) {
    final byte[] val = rawValue != null
        ? rawValue
        : new byte[0];
    return writeNewRecord(target, offset, heapOffsets, nodeKey, parentKey, rightSibKey, leftSibKey, nameKey,
        pathNodeKey, prevRev, lastModRev, hash, val, 0, val.length, isCompressed
            ? PAYLOAD_FLAG_FSST
            : PAYLOAD_FLAG_RAW);
  }

  public static int writeNewRecord(final MemorySegment target, final long offset, final int[] heapOffsets,
      final long nodeKey, final long parentKey, final long rightSibKey, final long leftSibKey, final int nameKey,
      final long pathNodeKey, final int prevRev, final int lastModRev, final long hash, final byte[] rawValue,
      final int rawOff, final int rawLen, final boolean isCompressed) {
    return writeNewRecord(target, offset, heapOffsets, nodeKey, parentKey, rightSibKey, leftSibKey, nameKey,
        pathNodeKey, prevRev, lastModRev, hash, rawValue, rawOff, rawLen, isCompressed
            ? PAYLOAD_FLAG_FSST
            : PAYLOAD_FLAG_RAW);
  }

  private static int writeNewRecord(final MemorySegment target, final long offset, final int[] heapOffsets,
      final long nodeKey, final long parentKey, final long rightSibKey, final long leftSibKey, final int nameKey,
      final long pathNodeKey, final int prevRev, final int lastModRev, final long hash, final byte[] rawValue,
      final int rawOff, final int rawLen, final byte payloadFlag) {
    long pos = offset;

    target.set(ValueLayout.JAVA_BYTE, pos, NodeKind.OBJECT_NAMED_STRING.getId());
    pos++;

    final long offsetTableStart = pos;
    pos += FIELD_COUNT;

    final long dataStart = pos;

    heapOffsets[NodeFieldLayout.OBJNAMEDSTR_PARENT_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, parentKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDSTR_RIGHT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, rightSibKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDSTR_LEFT_SIB_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, leftSibKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDSTR_NAME_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, nameKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDSTR_PATH_NODE_KEY] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeDeltaToSegment(target, pos, pathNodeKey, nodeKey);

    heapOffsets[NodeFieldLayout.OBJNAMEDSTR_PREV_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, prevRev);

    heapOffsets[NodeFieldLayout.OBJNAMEDSTR_LAST_MOD_REVISION] = (int) (pos - dataStart);
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, lastModRev);

    heapOffsets[NodeFieldLayout.OBJNAMEDSTR_HASH] = (int) (pos - dataStart);
    DeltaVarIntCodec.writeLongToSegment(target, pos, hash);
    pos += Long.BYTES;

    heapOffsets[NodeFieldLayout.OBJNAMEDSTR_PAYLOAD] = (int) (pos - dataStart);
    target.set(ValueLayout.JAVA_BYTE, pos, payloadFlag);
    pos++;
    pos += DeltaVarIntCodec.writeSignedToSegment(target, pos, rawLen);
    if (rawLen > 0) {
      MemorySegment.copy(rawValue, rawOff, target, ValueLayout.JAVA_BYTE, pos, rawLen);
      pos += rawLen;
    }

    for (int i = 0; i < FIELD_COUNT; i++) {
      target.set(ValueLayout.JAVA_BYTE, offsetTableStart + i, (byte) heapOffsets[i]);
    }

    return (int) (pos - offset);
  }

  public int serializeToHeap(final MemorySegment target, final long offset) {
    if (!metadataParsed) {
      parseMetadataFields();
    }
    if (!valueParsed) {
      parseValueField();
    }
    return writeNewRecord(target, offset, getHeapOffsets(), nodeKey, parentKey, rightSiblingKey, leftSiblingKey,
        nameKey, pathNodeKey, previousRevision, lastModifiedRevision, hash, value, isCompressed);
  }

  /**
   * Serialize only the fixed-size, scan-visible metadata for an out-of-line fused string record. The
   * returned bytes are a normal flyweight slot whose payload flag requires readers to resolve the
   * same-key overflow reference; they are never a user-visible empty string.
   */
  public int serializeOverflowDescriptorToHeap(final MemorySegment target, final long offset) {
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return writeNewRecord(target, offset, getHeapOffsets(), nodeKey, parentKey, rightSiblingKey, leftSiblingKey,
        nameKey, pathNodeKey, previousRevision, lastModifiedRevision, hash, EMPTY_PAYLOAD, 0, 0, PAYLOAD_FLAG_OVERFLOW);
  }

  /** Conservative upper bound for {@link #serializeOverflowDescriptorToHeap}. */
  public int estimateOverflowDescriptorSize() {
    return SERIALIZED_METADATA_UPPER_BOUND;
  }

  @Override
  protected int heapOffsetFieldCount() {
    return FIELD_COUNT;
  }

  @Override
  public int estimateSerializedSize() {
    // Without this override the FlyweightNode default of 256 bytes let a large string value
    // sail past KeyValueLeafPage#serializeToHeap's capacity check and blow the slotted-page
    // segment mid-write (issue #1076).
    if (!valueParsed) {
      parseValueField();
    }
    final int payloadLen = value != null
        ? value.length
        : 0;
    return estimateSerializedSize(payloadLen);
  }

  static int estimateSerializedSize(final int payloadLength) {
    return FlyweightNode.saturatingSerializedSize((long) SERIALIZED_METADATA_UPPER_BOUND + payloadLength);
  }

  /**
   * Everything except the payload, at its MINIMUM: kind byte + {@link #FIELD_COUNT}-byte offset table
   * + seven varints of one byte each + 8-byte hash + payload flag + one-byte payload-length varint.
   * The floor of the wire {@code writeNewRecord} emits, as the upper bound above is its ceiling — a
   * record can serialize below the ceiling but never below this.
   */
  private static final int SERIALIZED_METADATA_LOWER_BOUND = 1 + FIELD_COUNT + 7 + Long.BYTES + 1 + 1;

  @Override
  public int estimateSerializedSizeLowerBound() {
    if (!valueParsed) {
      parseValueField();
    }
    final int payloadLen = value != null
        ? value.length
        : 0;
    return FlyweightNode.saturatingSerializedSize((long) SERIALIZED_METADATA_LOWER_BOUND + payloadLen);
  }

  public void setDeweyIDAfterCreation(final SirixDeweyID id, final byte[] bytes) {
    this.sirixDeweyID = id;
    this.deweyIDBytes = bytes;
  }

  @Override
  public NodeKind getKind() {
    return NodeKind.OBJECT_NAMED_STRING;
  }

  @Override
  public long getNodeKey() {
    return nodeKey;
  }

  @Override
  public long getParentKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDSTR_PARENT_KEY, nodeKey);
    }
    return parentKey;
  }

  public void setParentKey(final long parentKey) {
    if (page != null) {
      final int fieldOff =
          page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PARENT_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(parentKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, parentKey, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex, NodeFieldLayout.OBJNAMEDSTR_PARENT_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, parentKey, nodeKey));
      return;
    }
    this.parentKey = parentKey;
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
  public void setTypeKey(final int typeKey) {}

  @Override
  public void setDeweyID(final SirixDeweyID id) {
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

  @Override
  public void setDeweyIDBytes(final byte[] bytes) {
    this.deweyIDBytes = bytes;
    this.sirixDeweyID = null;
  }

  @Override
  public void setPreviousRevision(final int revision) {
    if (page != null) {
      final int fieldOff =
          page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PREV_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex, NodeFieldLayout.OBJNAMEDSTR_PREV_REVISION, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
      return;
    }
    this.previousRevision = revision;
  }

  @Override
  public void setLastModifiedRevision(final int revision) {
    if (page != null) {
      final int fieldOff =
          page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_LAST_MOD_REVISION) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(revision);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, revision);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex, NodeFieldLayout.OBJNAMEDSTR_LAST_MOD_REVISION, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, revision));
      return;
    }
    this.lastModifiedRevision = revision;
  }

  @Override
  public long getHash() {
    if (page != null) {
      return readLongField(NodeFieldLayout.OBJNAMEDSTR_HASH);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return hash;
  }

  @Override
  public void setHash(final long hash) {
    if (page != null) {
      final int fieldOff = page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_HASH) & 0xFF;
      DeltaVarIntCodec.writeLongToSegment(page, dataRegionStart + fieldOff, hash);
      return;
    }
    this.hash = hash;
  }

  @Override
  public long computeHash(BytesOut<?> bytes) {
    bytes.clear();
    bytes.writeLong(getNodeKey())
         .writeLong(getParentKey())
         .writeByte(getKind().getId())
         .writeLong(getLeftSiblingKey())
         .writeLong(getRightSiblingKey())
         .writeInt(getNameKey());
    final byte[] rawValue = getRawValue();
    if (rawValue != null) {
      bytes.write(rawValue);
    }
    return bytes.hashDirect(hashFunction);
  }

  public int getNameKey() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDSTR_NAME_KEY);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return nameKey;
  }

  public void setNameKey(final int nameKey) {
    if (page != null) {
      final int fieldOff =
          page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_NAME_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readSignedVarintWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeSignedEncodedWidth(nameKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeSignedToSegment(page, absOff, nameKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex, NodeFieldLayout.OBJNAMEDSTR_NAME_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeSignedToSegment(target, off, nameKey));
      return;
    }
    this.nameKey = nameKey;
  }

  public QNm getName() {
    return cachedName;
  }

  public void setName(final QNm name) {
    this.cachedName = name;
  }

  public int getLocalNameKey() {
    return getNameKey();
  }

  public int getPrefixKey() {
    return -1;
  }

  public void setPrefixKey(final int prefixKey) {}

  public int getURIKey() {
    return -1;
  }

  public void setURIKey(final int uriKey) {}

  public void setLocalNameKey(final int localNameKey) {
    setNameKey(localNameKey);
  }

  public long getPathNodeKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDSTR_PATH_NODE_KEY, nodeKey);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return pathNodeKey;
  }

  public void setPathNodeKey(final long pathNodeKey) {
    if (page != null) {
      final int fieldOff =
          page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_PATH_NODE_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(pathNodeKey, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, pathNodeKey, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex, NodeFieldLayout.OBJNAMEDSTR_PATH_NODE_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, pathNodeKey, nodeKey));
      return;
    }
    this.pathNodeKey = pathNodeKey;
  }

  @Override
  public long getRightSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDSTR_RIGHT_SIB_KEY, nodeKey);
    }
    return rightSiblingKey;
  }

  public void setRightSiblingKey(final long rightSibling) {
    if (page != null) {
      final int fieldOff =
          page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_RIGHT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(rightSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, rightSibling, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex, NodeFieldLayout.OBJNAMEDSTR_RIGHT_SIB_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, rightSibling, nodeKey));
      return;
    }
    this.rightSiblingKey = rightSibling;
  }

  @Override
  public long getLeftSiblingKey() {
    if (page != null) {
      return readDeltaField(NodeFieldLayout.OBJNAMEDSTR_LEFT_SIB_KEY, nodeKey);
    }
    return leftSiblingKey;
  }

  public void setLeftSiblingKey(final long leftSibling) {
    if (page != null) {
      final int fieldOff =
          page.get(ValueLayout.JAVA_BYTE, recordBase + 1 + NodeFieldLayout.OBJNAMEDSTR_LEFT_SIB_KEY) & 0xFF;
      final long absOff = dataRegionStart + fieldOff;
      final int currentWidth = DeltaVarIntCodec.readDeltaEncodedWidth(page, absOff);
      final int newWidth = DeltaVarIntCodec.computeDeltaEncodedWidth(leftSibling, nodeKey);
      if (newWidth == currentWidth) {
        DeltaVarIntCodec.writeDeltaToSegment(page, absOff, leftSibling, nodeKey);
        return;
      }
      ownerPage.resizeRecordField(this, nodeKey, slotIndex, NodeFieldLayout.OBJNAMEDSTR_LEFT_SIB_KEY, FIELD_COUNT,
          (target, off) -> DeltaVarIntCodec.writeDeltaToSegment(target, off, leftSibling, nodeKey));
      return;
    }
    this.leftSiblingKey = leftSibling;
  }

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
  public void incrementChildCount() {}

  @Override
  public void decrementChildCount() {}

  @Override
  public long getDescendantCount() {
    return 0;
  }

  @Override
  public void setDescendantCount(final long descendantCount) {}

  @Override
  public void decrementDescendantCount() {}

  @Override
  public void incrementDescendantCount() {}

  @Override
  public int getPreviousRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDSTR_PREV_REVISION);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return previousRevision;
  }

  @Override
  public int getLastModifiedRevisionNumber() {
    if (page != null) {
      return readSignedField(NodeFieldLayout.OBJNAMEDSTR_LAST_MOD_REVISION);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    return lastModifiedRevision;
  }

  /**
   * Return the decoded value (FSST-decompressed if necessary).
   */
  @Override
  public byte[] getRawValue() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    } else if (!valueParsed) {
      parseValueField();
    }
    if (isCompressed && decodedValue == null && value != null) {
      decodedValue = FSSTCompressor.decode(value, fsstSymbolTable);
    }
    return isCompressed
        ? decodedValue
        : value;
  }

  /**
   * Get the raw (possibly compressed) value bytes without FSST decoding.
   */
  public byte[] getRawValueWithoutDecompression() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    } else if (!valueParsed) {
      parseValueField();
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
      this.isCompressed = false;
      this.fsstSymbolTable = null;
      this.decodedValue = null;
      this.valueParsed = true;
      owner.resizeRecord(this, nk, slot);
      return;
    }
    if (page != null)
      unbind();
    this.value = value;
    this.isCompressed = false;
    this.fsstSymbolTable = null;
    this.decodedValue = null;
    this.valueParsed = true;
  }

  public void setRawValue(final byte[] value, final boolean isCompressed, final byte[] fsstSymbolTable) {
    final var owner = this.ownerPage;
    if (owner != null) {
      final long nk = this.nodeKey;
      final int slot = this.slotIndex;
      unbind();
      this.value = value;
      this.isCompressed = isCompressed;
      this.fsstSymbolTable = fsstSymbolTable;
      this.decodedValue = null;
      this.valueParsed = true;
      owner.resizeRecord(this, nk, slot);
      return;
    }
    if (page != null)
      unbind();
    this.value = value;
    this.isCompressed = isCompressed;
    this.fsstSymbolTable = fsstSymbolTable;
    this.decodedValue = null;
    this.valueParsed = true;
  }

  public boolean isCompressed() {
    if (page != null && !valueParsed) {
      readPayloadFromPage();
    }
    return isCompressed;
  }

  public void setCompressed(final boolean isCompressed) {
    this.isCompressed = isCompressed;
  }

  public byte[] getFsstSymbolTable() {
    return fsstSymbolTable;
  }

  public void setFsstSymbolTable(final byte[] fsstSymbolTable) {
    this.fsstSymbolTable = fsstSymbolTable;
    this.decodedValue = null;
  }

  @Override
  public String getValue() {
    final byte[] rawValue = getRawValue();
    return new String(rawValue, Constants.DEFAULT_ENCODING);
  }

  public int getTypeKey() {
    return -1;
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
  public boolean hasLeftSibling() {
    return getLeftSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean hasRightSibling() {
    return getRightSiblingKey() != Fixed.NULL_NODE_KEY.getStandardProperty();
  }

  @Override
  public boolean isWriteSingleton() {
    return writeSingleton;
  }

  @Override
  public void setWriteSingleton(final boolean writeSingleton) {
    this.writeSingleton = writeSingleton;
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
    return visitor.visit(this);
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
    this.page = null;
    this.nodeKey = nodeKey;
    this.hashFunction = hashFunction;
    this.deweyIDBytes = deweyId;
    this.sirixDeweyID = null;
    this.cachedName = null;
    this.decodedValue = null;

    this.parentKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.rightSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);
    this.leftSiblingKey = DeltaVarIntCodec.decodeDelta(source, nodeKey);

    this.lazySource = source.getSource();
    this.lazyOffset = source.position();
    this.metadataParsed = false;
    this.valueParsed = false;
    this.valueOffset = 0;
    this.hasHash = config.hashType != HashType.NONE;

    this.nameKey = 0;
    this.pathNodeKey = 0;
    this.previousRevision = 0;
    this.lastModifiedRevision = 0;
    this.hash = 0;
    this.value = null;
    this.isCompressed = false;
    this.fsstSymbolTable = null;
  }

  private void parseMetadataFields() {
    if (metadataParsed) {
      return;
    }
    if (lazySource == null) {
      metadataParsed = true;
      return;
    }

    BytesIn<?> bytesIn = createBytesIn(lazyOffset);
    this.nameKey = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.pathNodeKey = DeltaVarIntCodec.decodeDelta(bytesIn, nodeKey);
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
    if (!metadataParsed) {
      parseMetadataFields();
    }
    if (lazySource == null) {
      valueParsed = true;
      return;
    }
    BytesIn<?> bytesIn = createBytesIn(valueOffset);
    final byte flag = bytesIn.readByte();
    if (flag == PAYLOAD_FLAG_OVERFLOW) {
      throw new IllegalStateException(
          "Overflow descriptor for node " + nodeKey + " reached the generic value parser without its OverflowPage");
    }
    if (flag != PAYLOAD_FLAG_RAW && flag != PAYLOAD_FLAG_FSST) {
      throw new IllegalStateException("Corrupted fused string payload flag " + flag + " for node " + nodeKey);
    }
    this.isCompressed = flag == PAYLOAD_FLAG_FSST;
    final int length = DeltaVarIntCodec.decodeSigned(bytesIn);
    this.value = new byte[length];
    bytesIn.read(this.value);
    this.valueParsed = true;
  }

  private BytesIn<?> createBytesIn(final long offset) {
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

  public ObjectNamedStringNode toSnapshot() {
    if (page != null) {
      if (!valueParsed) {
        readPayloadFromPage();
      }
      return new ObjectNamedStringNode(nodeKey, getParentKey(), getRightSiblingKey(), getLeftSiblingKey(), getNameKey(),
          getPathNodeKey(), getPreviousRevisionNumber(), getLastModifiedRevisionNumber(), getHash(), value != null
              ? value.clone()
              : null,
          hashFunction, getDeweyIDAsBytes() != null
              ? getDeweyIDAsBytes().clone()
              : null,
          isCompressed, fsstSymbolTable != null
              ? fsstSymbolTable.clone()
              : null);
    }
    if (!metadataParsed) {
      parseMetadataFields();
    }
    if (!valueParsed) {
      parseValueField();
    }
    return new ObjectNamedStringNode(nodeKey, parentKey, rightSiblingKey, leftSiblingKey, nameKey, pathNodeKey,
        previousRevision, lastModifiedRevision, hash, value != null
            ? value.clone()
            : null,
        hashFunction, getDeweyIDAsBytes() != null
            ? getDeweyIDAsBytes().clone()
            : null,
        isCompressed, fsstSymbolTable != null
            ? fsstSymbolTable.clone()
            : null);
  }

  @Override
  public String toString() {
    return "ObjectNamedStringNode{" + "nodeKey=" + nodeKey + ", parentKey=" + parentKey + ", nameKey=" + nameKey
        + ", value=" + (value != null
            ? Arrays.toString(value)
            : "null")
        + '}';
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeKey, parentKey, nameKey, Arrays.hashCode(value));
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof final ObjectNamedStringNode other)) {
      return false;
    }
    return nodeKey == other.nodeKey && parentKey == other.parentKey && nameKey == other.nameKey
        && Arrays.equals(value, other.value);
  }
}
