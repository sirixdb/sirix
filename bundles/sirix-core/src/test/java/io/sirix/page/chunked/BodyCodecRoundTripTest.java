/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.chunked;

import io.sirix.page.ByteRunCodec;
import io.sirix.page.SirixLZ77Codec;
import io.sirix.page.ZeroRunByteCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The codec-independence lemma, proven once instead of swept.
 *
 * <p>
 * The conformance sweep does not enumerate a codec axis, and this test is the reason it does not
 * have to. A frame's codec sees an opaque range of bytes and is asked for the same bytes back; it
 * knows nothing of chunk boundaries, entry ranks or META sections. So if every codec round-trips
 * every byte shape a body can contain, and the framing is correct for one codec, it is correct for
 * all of them — which turns a multiplicative axis into an additive one.
 *
 * <p>
 * The shapes below are the ones a record body actually produces: runs of zeros where hashes were
 * elided, constant runs from padding, verbatim repetition across records of the same shape,
 * incompressible payloads, and the boundary lengths.
 */
@DisplayName("Body codecs round-trip every byte shape a body contains")
final class BodyCodecRoundTripTest {

  @Test
  @DisplayName("every codec returns exactly the bytes it was given")
  void everyCodecRoundTrips() {
    for (final Sample sample : samples()) {
      for (final Codec codec : Codec.values()) {
        roundTrip(codec, sample);
      }
    }
  }

  /**
   * The bound each codec promises is what the writer sizes its output buffer from; an encode that
   * exceeded it would corrupt whatever the frame buffer holds next.
   */
  @Test
  @DisplayName("no codec exceeds the size bound the writer trusts")
  void encodedOutputStaysWithinTheBound() {
    for (final Sample sample : samples()) {
      for (final Codec codec : Codec.values()) {
        final byte[] out = new byte[codec.maxEncodedSize(sample.data().length) + 64];
        final int encoded = codec.encode(sample.segment(), sample.data().length, out);
        assertTrue(encoded <= codec.maxEncodedSize(sample.data().length), codec + " on " + sample.name() + ": encoded "
            + encoded + " bytes, bound is " + codec.maxEncodedSize(sample.data().length));
      }
    }
  }

  private static void roundTrip(final Codec codec, final Sample sample) {
    final int rawLength = sample.data().length;
    final byte[] encoded = new byte[codec.maxEncodedSize(rawLength) + 64];
    final int encodedLength = codec.encode(sample.segment(), rawLength, encoded);
    assertTrue(encodedLength >= 0, codec + " on " + sample.name() + ": encode failed");

    final MemorySegment decoded = Arena.ofAuto().allocate(Math.max(1, rawLength));
    final int decodedLength = codec.decode(encoded, encodedLength, decoded);

    assertEquals(rawLength, decodedLength, codec + " on " + sample.name() + ": decoded length");
    assertArrayEquals(sample.data(), decoded.asSlice(0, rawLength).toArray(ValueLayout.JAVA_BYTE),
        codec + " on " + sample.name() + ": decoded bytes");
  }

  private enum Codec {
    ZERO_RUN {
      @Override
      int maxEncodedSize(final int rawLength) {
        return ZeroRunByteCodec.maxEncodedSize(rawLength);
      }

      @Override
      int encode(final MemorySegment input, final int rawLength, final byte[] output) {
        return ZeroRunByteCodec.encode(input, 0L, rawLength, output, 0);
      }

      @Override
      int decode(final byte[] input, final int encodedLength, final MemorySegment output) {
        return ZeroRunByteCodec.decode(input, 0, encodedLength, output, 0L);
      }
    },
    BYTE_RUN {
      @Override
      int maxEncodedSize(final int rawLength) {
        return ByteRunCodec.maxEncodedSize(rawLength);
      }

      @Override
      int encode(final MemorySegment input, final int rawLength, final byte[] output) {
        return ByteRunCodec.encode(input, 0L, rawLength, output, 0);
      }

      @Override
      int decode(final byte[] input, final int encodedLength, final MemorySegment output) {
        return ByteRunCodec.decode(input, 0, encodedLength, output, 0L);
      }
    },
    LZ77 {
      @Override
      int maxEncodedSize(final int rawLength) {
        return SirixLZ77Codec.maxEncodedSize(rawLength);
      }

      @Override
      int encode(final MemorySegment input, final int rawLength, final byte[] output) {
        return SirixLZ77Codec.encode(input, 0L, rawLength, output, 0);
      }

      @Override
      int decode(final byte[] input, final int encodedLength, final MemorySegment output) {
        return SirixLZ77Codec.decode(input, 0, encodedLength, output, 0L);
      }
    };

    abstract int maxEncodedSize(int rawLength);

    abstract int encode(MemorySegment input, int rawLength, byte[] output);

    abstract int decode(byte[] input, int encodedLength, MemorySegment output);
  }

  private record Sample(String name, byte[] data, MemorySegment segment) {
  }

  private static List<Sample> samples() {
    final List<Sample> samples = new ArrayList<>();
    add(samples, "one byte", () -> new byte[] {0x2A});
    add(samples, "all zeros 4 KiB", () -> new byte[4096]);
    add(samples, "constant run 4 KiB", () -> filled(4096, (byte) 0x7F));
    add(samples, "repeated 40-byte record ×256", () -> {
      final byte[] data = new byte[40 * 256];
      for (int i = 0; i < data.length; i++) {
        data[i] = (byte) (i % 40);
      }
      return data;
    });
    add(samples, "zero runs between records", () -> {
      final byte[] data = new byte[3000];
      for (int i = 0; i < data.length; i += 30) {
        data[i] = (byte) 0x31;
        data[i + 1] = (byte) 0x07;
      }
      return data;
    });
    add(samples, "incompressible 4 KiB", () -> {
      final byte[] data = new byte[4096];
      new Random(20260817L).nextBytes(data);
      return data;
    });
    add(samples, "incompressible 64 KiB", () -> {
      final byte[] data = new byte[64 * 1024];
      new Random(4242L).nextBytes(data);
      return data;
    });
    // A chunk is filled to a target and then closed, so lengths land on and around it.
    add(samples, "exactly the chunk target", () -> filled(4096, (byte) 0x11));
    add(samples, "one byte under the target", () -> filled(4095, (byte) 0x11));
    add(samples, "one byte over the target", () -> filled(4097, (byte) 0x11));
    return samples;
  }

  private static void add(final List<Sample> samples, final String name, final Supplier<byte[]> content) {
    final byte[] data = content.get();
    final MemorySegment segment = Arena.ofAuto().allocate(data.length);
    MemorySegment.copy(data, 0, segment, ValueLayout.JAVA_BYTE, 0L, data.length);
    samples.add(new Sample(name, data, segment));
  }

  private static byte[] filled(final int length, final byte value) {
    final byte[] data = new byte[length];
    java.util.Arrays.fill(data, value);
    return data;
  }
}
