package io.sirix.node.layout;

import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;
import io.sirix.settings.Fixed;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class CompactFieldCodecTest {

  @Test
  void signedIntegerRoundTrip() {
    final int[] values = {Integer.MIN_VALUE, -1000, -1, 0, 1, 1000, Integer.MAX_VALUE};
    for (final int value : values) {
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      CompactFieldCodec.encodeSignedInt(sink, value);

      final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
      assertEquals(value, CompactFieldCodec.decodeSignedInt(source), "Roundtrip mismatch for " + value);
    }
  }

  @Test
  void signedLongRoundTrip() {
    final long[] values = {Long.MIN_VALUE, -1000L, -1L, 0L, 1L, 1000L, Long.MAX_VALUE};
    for (final long value : values) {
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      CompactFieldCodec.encodeSignedLong(sink, value);

      final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
      assertEquals(value, CompactFieldCodec.decodeSignedLong(source), "Roundtrip mismatch for " + value);
    }
  }

  @Test
  void deltaNodeKeyRoundTripWithCornerCases() {
    final long nullNodeKey = Fixed.NULL_NODE_KEY.getStandardProperty();
    final long[][] cases = {{100L, 101L}, {100L, 99L}, {100L, 100L}, {100L, nullNodeKey},
        {Long.MAX_VALUE - 2, Long.MAX_VALUE - 1}, {Long.MIN_VALUE + 2, Long.MIN_VALUE + 1}};

    for (final long[] sample : cases) {
      final long baseNodeKey = sample[0];
      final long targetNodeKey = sample[1];

      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      CompactFieldCodec.encodeNodeKeyDelta(sink, baseNodeKey, targetNodeKey);

      final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
      assertEquals(targetNodeKey, CompactFieldCodec.decodeNodeKeyDelta(source, baseNodeKey),
          "Delta roundtrip mismatch for base=" + baseNodeKey + ", target=" + targetNodeKey);
    }
  }

  @Test
  void decodeNonNegativeRejectsNegativeNumbers() {
    final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
    DeltaVarIntCodec.encodeSigned(sink, -1);
    final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
    assertThrows(IllegalStateException.class, () -> CompactFieldCodec.decodeNonNegativeInt(source));
  }
}
