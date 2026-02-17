package io.sirix.node.layout;

import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.NodeKind;
import io.sirix.settings.Fixed;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CompactPageEncoderTest {
  private static final int LENGTH_LIMIT_PLUS_ONE = (1 << 20) + 1;

  @Test
  void slotHeaderRoundTrip() {
    final CompactPageEncoder.SlotHeader header = new CompactPageEncoder.SlotHeader(42L, NodeKind.OBJECT, 96, 128);

    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactPageEncoder.encodeSlotHeader(sink, header);

    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    final CompactPageEncoder.SlotHeader decoded = CompactPageEncoder.decodeSlotHeader(source);

    assertEquals(header.nodeKey(), decoded.nodeKey());
    assertEquals(header.nodeKind(), decoded.nodeKind());
    assertEquals(header.fixedSlotSizeInBytes(), decoded.fixedSlotSizeInBytes());
    assertEquals(header.payloadSizeInBytes(), decoded.payloadSizeInBytes());
  }

  @Test
  void slotHeaderRoundTripWithReusableHeader() {
    final CompactPageEncoder.SlotHeader header = new CompactPageEncoder.SlotHeader(73L, NodeKind.ELEMENT, 160, 256);

    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactPageEncoder.encodeSlotHeader(sink, header);

    final CompactPageEncoder.MutableSlotHeader mutableHeader = new CompactPageEncoder.MutableSlotHeader();
    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    CompactPageEncoder.decodeSlotHeader(source, mutableHeader);

    assertEquals(header.nodeKey(), mutableHeader.nodeKey());
    assertEquals(header.nodeKind(), mutableHeader.nodeKind());
    assertEquals(header.fixedSlotSizeInBytes(), mutableHeader.fixedSlotSizeInBytes());
    assertEquals(header.payloadSizeInBytes(), mutableHeader.payloadSizeInBytes());
  }

  @Test
  void relationshipVectorRoundTripWithNullSentinel() {
    final long baseNodeKey = 512L;
    final long[] relationships = {513L, 511L, 512L, Fixed.NULL_NODE_KEY.getStandardProperty(), 600L};

    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactPageEncoder.encodeRelationshipVector(sink, baseNodeKey, relationships);

    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    final long[] decoded = CompactPageEncoder.decodeRelationshipVector(source, baseNodeKey);
    assertArrayEquals(relationships, decoded);
  }

  @Test
  void relationshipVectorDecodesIntoReusableBuffer() {
    final long baseNodeKey = 2048L;
    final long[] relationships = {2049L, 2050L, 2047L};
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactPageEncoder.encodeRelationshipVector(sink, baseNodeKey, relationships);

    final long[] reusableBuffer = new long[8];
    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    final int decodedLength = CompactPageEncoder.decodeRelationshipVector(source, baseNodeKey, reusableBuffer);

    assertEquals(relationships.length, decodedLength);
    for (int i = 0; i < decodedLength; i++) {
      assertEquals(relationships[i], reusableBuffer[i]);
    }
  }

  @Test
  void decodeSlotHeaderRejectsUnknownNodeKind() {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactFieldCodec.encodeSignedLong(sink, 99L);
    sink.writeByte((byte) 120);
    CompactFieldCodec.encodeNonNegativeInt(sink, 16);
    CompactFieldCodec.encodeNonNegativeInt(sink, 0);

    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    assertThrows(IllegalStateException.class, () -> CompactPageEncoder.decodeSlotHeader(source));
  }

  @Test
  void payloadBlockHeaderRoundTrip() {
    final CompactPageEncoder.PayloadBlockHeader header = new CompactPageEncoder.PayloadBlockHeader(8192L, 1024, 5);

    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactPageEncoder.encodePayloadBlockHeader(sink, header);

    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    final CompactPageEncoder.PayloadBlockHeader decoded = CompactPageEncoder.decodePayloadBlockHeader(source);

    assertEquals(header.payloadPointer(), decoded.payloadPointer());
    assertEquals(header.payloadLengthInBytes(), decoded.payloadLengthInBytes());
    assertEquals(header.payloadFlags(), decoded.payloadFlags());
  }

  @Test
  void payloadBlockHeaderRoundTripWithReusableHeader() {
    final CompactPageEncoder.PayloadBlockHeader header = new CompactPageEncoder.PayloadBlockHeader(16384L, 2048, 9);

    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactPageEncoder.encodePayloadBlockHeader(sink, header);

    final CompactPageEncoder.MutablePayloadBlockHeader mutableHeader =
        new CompactPageEncoder.MutablePayloadBlockHeader();
    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    CompactPageEncoder.decodePayloadBlockHeader(source, mutableHeader);

    assertEquals(header.payloadPointer(), mutableHeader.payloadPointer());
    assertEquals(header.payloadLengthInBytes(), mutableHeader.payloadLengthInBytes());
    assertEquals(header.payloadFlags(), mutableHeader.payloadFlags());
  }

  @Test
  void decodeRelationshipVectorRejectsInvalidLength() {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactFieldCodec.encodeSignedInt(sink, -1);
    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    assertThrows(IllegalStateException.class, () -> CompactPageEncoder.decodeRelationshipVector(source, 10L));
  }

  @Test
  void decodePayloadBlockHeaderRejectsNegativeLength() {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactFieldCodec.encodeSignedLong(sink, 123L);
    CompactFieldCodec.encodeSignedInt(sink, -1);
    CompactFieldCodec.encodeSignedInt(sink, 0);

    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    assertThrows(IllegalStateException.class, () -> CompactPageEncoder.decodePayloadBlockHeader(source));
  }

  @Test
  void decodeRelationshipVectorRejectsLengthOverLimit() {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactFieldCodec.encodeNonNegativeInt(sink, LENGTH_LIMIT_PLUS_ONE);
    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    assertThrows(IllegalStateException.class, () -> CompactPageEncoder.decodeRelationshipVector(source, 10L));
  }

  @Test
  void decodeRelationshipVectorIntoBufferRejectsInsufficientCapacity() {
    final long baseNodeKey = 7L;
    final long[] relationships = {8L, 9L, 10L};
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    CompactPageEncoder.encodeRelationshipVector(sink, baseNodeKey, relationships);

    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    final long[] tooSmallBuffer = new long[2];
    assertThrows(IllegalArgumentException.class,
        () -> CompactPageEncoder.decodeRelationshipVector(source, baseNodeKey, tooSmallBuffer));
  }
}
