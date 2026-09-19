/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.access.ResourceConfiguration;
import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Small compressed overflow frames must retain exact bytes and independent heap ownership. */
final class SmallOverflowLz77Test {

  private static final ResourceConfiguration CONFIG = ResourceConfiguration.newBuilder("small-overflow-lz77").build();
  private static final int[] LENGTHS = {0, 1, 15, 16, 17, 63, 64, 65, 255, 511, 1023, 1024, 1025};

  @Test
  void decodesLiteralAndMatchFramesAcrossSmallBufferBoundaries() throws IOException {
    final Random random = new Random(0x51A11L);
    for (final int length : LENGTHS) {
      final byte[] literal = new byte[length];
      random.nextBytes(literal);
      assertArrayEquals(literal, read(frame(literal, length)).getDataBytes(), "literal length " + length);

      final byte[] matches = new byte[length];
      for (int i = 0; i < length; i++) {
        matches[i] = (byte) (i % 7);
      }
      assertArrayEquals(matches, read(frame(matches, length)).getDataBytes(), "match length " + length);
    }
  }

  @Test
  void returnedPageDoesNotRetainReusableDecodeStorage() throws IOException {
    final byte[] original = new byte[511];
    Arrays.fill(original, (byte) 7);
    final OverflowPage first = read(frame(original, original.length));
    final byte[] replacement = new byte[128 * 1024 + 17];
    Arrays.fill(replacement, (byte) 19);
    final OverflowPage second = read(frame(replacement, replacement.length));
    assertArrayEquals(original, first.getDataBytes());
    assertArrayEquals(replacement, second.getDataBytes());
    assertEquals(original.length, first.dataLength());
    second.getDataBytes()[0] = 41;
    assertArrayEquals(original, first.getDataBytes());
  }

  @Test
  void rejectsAnEnvelopeWhoseDecodedLengthDisagreesWithTheFrame() throws IOException {
    final byte[] payload = new byte[127];
    Arrays.fill(payload, (byte) 3);
    for (final int declaredLength : new int[] {126, 128}) {
      final byte[] wire = frame(payload, declaredLength);
      assertThrows(IllegalStateException.class, () -> read(wire));
    }
  }

  @Test
  void rejectsTruncatedStoredPayloadBeforeDecoding() throws IOException {
    final byte[] payload = new byte[255];
    new Random(255).nextBytes(payload);
    final byte[] wire = frame(payload, payload.length);
    assertThrows(IllegalStateException.class, () -> read(Arrays.copyOf(wire, wire.length - 1)));
  }

  @Test
  void smallFrameUsesNativeDecoderWhenDiagnosticCountersAreEnabled() throws IOException {
    final byte[] payload = new byte[511];
    new Random(511).nextBytes(payload);
    final byte[] wire = frame(payload, payload.length);
    final long before = SirixLZ77Codec.getNativeCallCount();
    assertArrayEquals(payload, read(wire).getDataBytes());
    if (Boolean.getBoolean("sirix.lz77Codec.diag.counters") && SirixLZ77NativeDecoder.isAvailable()) {
      assertTrue(SirixLZ77Codec.getNativeCallCount() > before, "small overflow frame must reach the native decoder");
    }
  }

  private static OverflowPage read(final byte[] wire) throws IOException {
    return (OverflowPage) new PagePersister().deserializePage(CONFIG, Bytes.wrapForRead(wire), SerializationType.DATA);
  }

  private static byte[] frame(final byte[] payload, final int decodedLength) throws IOException {
    final byte[] encoded = new byte[SirixLZ77Codec.maxEncodedSize(payload.length)];
    final int length = SirixLZ77Codec.encode(MemorySegment.ofArray(payload), 0L, payload.length, encoded, 0);
    try (BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer()) {
      sink.writeByte((byte) 9); // OverflowPage page-kind id.
      sink.writeByte((byte) 0); // BinaryEncodingVersion.V0.
      sink.writeByte((byte) 1); // Compressed overflow payload flag.
      sink.writeInt(decodedLength);
      sink.writeInt(length);
      sink.writeByte((byte) 3); // LZ77 payload codec; do not depend on writer codec election.
      sink.write(encoded, 0, length);
      return sink.toByteArray();
    }
  }
}
