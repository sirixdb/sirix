package io.sirix.node.layout;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;

import java.util.Objects;

/**
 * Lightweight V1 page-level compact encoding helpers.
 *
 * <p>The methods here are intentionally allocation-minimal and operate directly on primitive vectors.
 */
public final class CompactPageEncoder {
  private static final int MAX_RELATIONSHIP_VECTOR_LENGTH = 1 << 20;

  private CompactPageEncoder() {
  }

  public static void encodeSlotHeader(final BytesOut<?> sink, final SlotHeader slotHeader) {
    Objects.requireNonNull(slotHeader, "slotHeader must not be null");
    CompactFieldCodec.encodeSignedLong(sink, slotHeader.nodeKey());
    sink.writeByte(slotHeader.nodeKind().getId());
    CompactFieldCodec.encodeNonNegativeInt(sink, slotHeader.fixedSlotSizeInBytes());
    CompactFieldCodec.encodeNonNegativeInt(sink, slotHeader.payloadSizeInBytes());
  }

  public static SlotHeader decodeSlotHeader(final BytesIn<?> source) {
    final MutableSlotHeader header = new MutableSlotHeader();
    decodeSlotHeader(source, header);
    return new SlotHeader(header.nodeKey(), header.nodeKind(), header.fixedSlotSizeInBytes(), header.payloadSizeInBytes());
  }

  /**
   * Decode a slot header into a caller-owned reusable object to avoid allocations.
   */
  public static void decodeSlotHeader(final BytesIn<?> source, final MutableSlotHeader target) {
    Objects.requireNonNull(source, "source must not be null");
    Objects.requireNonNull(target, "target must not be null");

    final long nodeKey = CompactFieldCodec.decodeSignedLong(source);

    final byte kindId = source.readByte();
    if (kindId < 0) {
      throw new IllegalStateException("Invalid negative node kind id: " + kindId);
    }

    final NodeKind nodeKind = NodeKind.getKind(kindId);
    if (nodeKind == null) {
      throw new IllegalStateException("Unknown node kind id: " + kindId);
    }

    final int fixedSlotSizeInBytes = CompactFieldCodec.decodeNonNegativeInt(source);
    final int payloadSizeInBytes = CompactFieldCodec.decodeNonNegativeInt(source);
    target.set(nodeKey, nodeKind, fixedSlotSizeInBytes, payloadSizeInBytes);
  }

  public static void encodeRelationshipVector(final BytesOut<?> sink, final long baseNodeKey,
      final long[] relationshipNodeKeys) {
    Objects.requireNonNull(relationshipNodeKeys, "relationshipNodeKeys must not be null");
    CompactFieldCodec.encodeNonNegativeInt(sink, relationshipNodeKeys.length);
    for (final long relationshipNodeKey : relationshipNodeKeys) {
      CompactFieldCodec.encodeNodeKeyDelta(sink, baseNodeKey, relationshipNodeKey);
    }
  }

  public static long[] decodeRelationshipVector(final BytesIn<?> source, final long baseNodeKey) {
    final int length = CompactFieldCodec.decodeNonNegativeInt(source);
    if (length > MAX_RELATIONSHIP_VECTOR_LENGTH) {
      throw new IllegalStateException("Relationship vector length exceeds limit: " + length);
    }

    final long[] relationshipNodeKeys = new long[length];
    for (int i = 0; i < length; i++) {
      relationshipNodeKeys[i] = CompactFieldCodec.decodeNodeKeyDelta(source, baseNodeKey);
    }
    return relationshipNodeKeys;
  }

  /**
   * Decode a relationship vector into a caller-owned reusable buffer.
   *
   * @return number of decoded entries written into {@code targetBuffer}
   */
  public static int decodeRelationshipVector(final BytesIn<?> source, final long baseNodeKey, final long[] targetBuffer) {
    Objects.requireNonNull(targetBuffer, "targetBuffer must not be null");
    final int length = CompactFieldCodec.decodeNonNegativeInt(source);
    if (length > MAX_RELATIONSHIP_VECTOR_LENGTH) {
      throw new IllegalStateException("Relationship vector length exceeds limit: " + length);
    }
    if (length > targetBuffer.length) {
      throw new IllegalArgumentException(
          "Target buffer too small. length=" + length + ", capacity=" + targetBuffer.length);
    }

    for (int i = 0; i < length; i++) {
      targetBuffer[i] = CompactFieldCodec.decodeNodeKeyDelta(source, baseNodeKey);
    }
    return length;
  }

  public static void encodePayloadBlockHeader(final BytesOut<?> sink, final PayloadBlockHeader payloadBlockHeader) {
    Objects.requireNonNull(payloadBlockHeader, "payloadBlockHeader must not be null");
    CompactFieldCodec.encodeSignedLong(sink, payloadBlockHeader.payloadPointer());
    CompactFieldCodec.encodeNonNegativeInt(sink, payloadBlockHeader.payloadLengthInBytes());
    CompactFieldCodec.encodeNonNegativeInt(sink, payloadBlockHeader.payloadFlags());
  }

  public static PayloadBlockHeader decodePayloadBlockHeader(final BytesIn<?> source) {
    final MutablePayloadBlockHeader payloadBlockHeader = new MutablePayloadBlockHeader();
    decodePayloadBlockHeader(source, payloadBlockHeader);
    return new PayloadBlockHeader(payloadBlockHeader.payloadPointer(), payloadBlockHeader.payloadLengthInBytes(),
        payloadBlockHeader.payloadFlags());
  }

  /**
   * Decode a payload block header into a caller-owned reusable object to avoid allocations.
   */
  public static void decodePayloadBlockHeader(final BytesIn<?> source, final MutablePayloadBlockHeader target) {
    Objects.requireNonNull(source, "source must not be null");
    Objects.requireNonNull(target, "target must not be null");

    final long payloadPointer = CompactFieldCodec.decodeSignedLong(source);
    final int payloadLengthInBytes = CompactFieldCodec.decodeNonNegativeInt(source);
    final int payloadFlags = CompactFieldCodec.decodeNonNegativeInt(source);
    target.set(payloadPointer, payloadLengthInBytes, payloadFlags);
  }

  public record SlotHeader(long nodeKey, NodeKind nodeKind, int fixedSlotSizeInBytes, int payloadSizeInBytes) {
    public SlotHeader {
      Objects.requireNonNull(nodeKind, "nodeKind must not be null");
      if (fixedSlotSizeInBytes < 0) {
        throw new IllegalArgumentException("fixedSlotSizeInBytes must be >= 0");
      }
      if (payloadSizeInBytes < 0) {
        throw new IllegalArgumentException("payloadSizeInBytes must be >= 0");
      }
    }
  }

  public record PayloadBlockHeader(long payloadPointer, int payloadLengthInBytes, int payloadFlags) {
    public PayloadBlockHeader {
      if (payloadLengthInBytes < 0) {
        throw new IllegalArgumentException("payloadLengthInBytes must be >= 0");
      }
      if (payloadFlags < 0) {
        throw new IllegalArgumentException("payloadFlags must be >= 0");
      }
    }
  }

  /**
   * Reusable mutable slot header container for allocation-free decode loops.
   */
  public static final class MutableSlotHeader {
    private long nodeKey;
    private NodeKind nodeKind;
    private int fixedSlotSizeInBytes;
    private int payloadSizeInBytes;

    public long nodeKey() {
      return nodeKey;
    }

    public NodeKind nodeKind() {
      return nodeKind;
    }

    public int fixedSlotSizeInBytes() {
      return fixedSlotSizeInBytes;
    }

    public int payloadSizeInBytes() {
      return payloadSizeInBytes;
    }

    private void set(final long nodeKey, final NodeKind nodeKind, final int fixedSlotSizeInBytes,
        final int payloadSizeInBytes) {
      this.nodeKey = nodeKey;
      this.nodeKind = nodeKind;
      this.fixedSlotSizeInBytes = fixedSlotSizeInBytes;
      this.payloadSizeInBytes = payloadSizeInBytes;
    }
  }

  /**
   * Reusable mutable payload block header for allocation-free decode loops.
   */
  public static final class MutablePayloadBlockHeader {
    private long payloadPointer;
    private int payloadLengthInBytes;
    private int payloadFlags;

    public long payloadPointer() {
      return payloadPointer;
    }

    public int payloadLengthInBytes() {
      return payloadLengthInBytes;
    }

    public int payloadFlags() {
      return payloadFlags;
    }

    private void set(final long payloadPointer, final int payloadLengthInBytes, final int payloadFlags) {
      this.payloadPointer = payloadPointer;
      this.payloadLengthInBytes = payloadLengthInBytes;
      this.payloadFlags = payloadFlags;
    }
  }
}
