package io.sirix.node.layout;

import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.DeltaVarIntCodec;

import java.util.Objects;

/**
 * Shared codec primitives used by compact commit encoders.
 */
public final class CompactFieldCodec {
  private CompactFieldCodec() {
  }

  public static void encodeNodeKeyDelta(final BytesOut<?> sink, final long baseNodeKey, final long targetNodeKey) {
    DeltaVarIntCodec.encodeDelta(Objects.requireNonNull(sink), targetNodeKey, baseNodeKey);
  }

  public static long decodeNodeKeyDelta(final BytesIn<?> source, final long baseNodeKey) {
    return DeltaVarIntCodec.decodeDelta(Objects.requireNonNull(source), baseNodeKey);
  }

  public static void encodeSignedInt(final BytesOut<?> sink, final int value) {
    DeltaVarIntCodec.encodeSigned(Objects.requireNonNull(sink), value);
  }

  public static int decodeSignedInt(final BytesIn<?> source) {
    return DeltaVarIntCodec.decodeSigned(Objects.requireNonNull(source));
  }

  public static void encodeSignedLong(final BytesOut<?> sink, final long value) {
    DeltaVarIntCodec.encodeSignedLong(Objects.requireNonNull(sink), value);
  }

  public static long decodeSignedLong(final BytesIn<?> source) {
    return DeltaVarIntCodec.decodeSignedLong(Objects.requireNonNull(source));
  }

  public static void encodeNonNegativeInt(final BytesOut<?> sink, final int value) {
    if (value < 0) {
      throw new IllegalArgumentException("value must be >= 0");
    }
    encodeSignedInt(sink, value);
  }

  public static int decodeNonNegativeInt(final BytesIn<?> source, final String fieldName) {
    final int value = decodeSignedInt(source);
    if (value < 0) {
      throw new IllegalStateException(
          "Decoded negative value for " + Objects.requireNonNull(fieldName) + ": " + value);
    }
    return value;
  }

  public static int decodeNonNegativeInt(final BytesIn<?> source) {
    final int value = decodeSignedInt(source);
    if (value < 0) {
      throw new IllegalStateException("Decoded negative value: " + value);
    }
    return value;
  }
}
