/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page;

import io.sirix.index.IndexType;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

final class PartitionedBodyCodecElectionTest {
  private boolean partitionedBefore;
  private boolean stickyOnlyBefore;

  @BeforeEach
  void enablePartitionedElection() {
    partitionedBefore = PartitionedBodyCodecElection.enabled;
    stickyOnlyBefore = PageKind.CODEC_BAKEOFF_STICKY_ONLY;
    PartitionedBodyCodecElection.enabled = true;
    PageKind.CODEC_BAKEOFF_STICKY_ONLY = false;
    PageKind.resetStickyCodecElectionForCurrentThread();
  }

  @AfterEach
  void restoreElection() {
    PageKind.resetStickyCodecElectionForCurrentThread();
    PartitionedBodyCodecElection.enabled = partitionedBefore;
    PageKind.CODEC_BAKEOFF_STICKY_ONLY = stickyOnlyBefore;
  }

  @Test
  void indexKindsDoNotPoisonEachOthersElection() {
    final byte[] zeros = zeroRuns(256);
    final byte[] repeated = repeatedBytes(256);
    for (int i = 0; i < 40; i++) {
      assertEquals(0, encodeAndRoundTrip(zeros, IndexType.NAME));
      assertEquals(3, encodeAndRoundTrip(repeated, IndexType.DOCUMENT));
    }
  }

  @Test
  void bodySizeClassesDoNotPoisonEachOthersElection() {
    final byte[] zeros = zeroRuns(128);
    final byte[] repeated = repeatedBytes(256);
    for (int i = 0; i < 40; i++) {
      assertEquals(0, encodeAndRoundTrip(zeros, IndexType.DOCUMENT));
      assertEquals(3, encodeAndRoundTrip(repeated, IndexType.DOCUMENT));
    }
  }

  @Test
  void aChangedDistributionIsReelectedOnTheNextProbe() {
    final byte[] zeros = zeroRuns(256);
    final byte[] repeated = repeatedBytes(256);
    for (int i = 0; i < 8; i++) {
      assertEquals(0, encodeAndRoundTrip(zeros, IndexType.DOCUMENT));
    }
    // These pages prove the fast path really uses one codec, even when a competitor is smaller.
    for (int i = 0; i < 15; i++) {
      assertEquals(0, encodeAndRoundTrip(repeated, IndexType.DOCUMENT));
    }
    assertEquals(3, encodeAndRoundTrip(repeated, IndexType.DOCUMENT));
    assertEquals(3, encodeAndRoundTrip(repeated, IndexType.DOCUMENT));
  }

  @Test
  void resetDiscardsEveryPartition() {
    for (int i = 0; i < 8; i++) {
      encodeAndRoundTrip(zeroRuns(256), IndexType.DOCUMENT);
      encodeAndRoundTrip(zeroRuns(1024), IndexType.NAME);
    }
    PageKind.resetStickyCodecElectionForCurrentThread();
    assertEquals(3, encodeAndRoundTrip(repeatedBytes(256), IndexType.DOCUMENT));
    assertEquals(3, encodeAndRoundTrip(repeatedBytes(1024), IndexType.NAME));
  }

  @Test
  void byteRunCanWinAndStayElected() {
    final byte[] constant = new byte[256];
    Arrays.fill(constant, (byte) 42);
    for (int i = 0; i < 40; i++) {
      assertEquals(2, encodeAndRoundTrip(constant, IndexType.DOCUMENT));
    }
  }

  @Test
  void everyIndexTypeWarmsUpAndRetainsItsElectionOnAFreshThread() throws ExecutionException, InterruptedException {
    final byte[] zeros = zeroRuns(256);
    final byte[] repeated = repeatedBytes(256);
    try (final ExecutorService executor = Executors.newSingleThreadExecutor()) {
      executor.submit(() -> {
        for (final IndexType type : IndexType.values()) {
          for (int i = 0; i < 8; i++) {
            assertEquals(0, encodeAndRoundTrip(zeros, type));
          }
          for (int i = 0; i < 15; i++) {
            assertEquals(0, encodeAndRoundTrip(repeated, type));
          }
          assertEquals(3, encodeAndRoundTrip(repeated, type));
          assertEquals(3, encodeAndRoundTrip(repeated, type));
        }
      }).get();
    }
  }

  @Test
  void boundarySizesRemainSelfDescribingThroughWarmupAndProbes() {
    final int[] sizes = {0, 1, 63, 64, 127, 128, 255, 256, 65535, 65536, 262144};
    final Random random = new Random(71);
    for (final int size : sizes) {
      final byte[] raw = new byte[size];
      random.nextBytes(raw);
      for (int i = 0; i < 25; i++) {
        encodeAndRoundTrip(raw, IndexType.DOCUMENT);
      }
    }
  }

  private static byte[] zeroRuns(final int length) {
    final byte[] result = new byte[length];
    new Random(29).nextBytes(result);
    for (int i = 0; i < length; i += 128) {
      Arrays.fill(result, i, Math.min(i + 100, length), (byte) 0);
    }
    return result;
  }

  private static byte[] repeatedBytes(final int length) {
    final byte[] pattern = new byte[16];
    new Random(17).nextBytes(pattern);
    final byte[] result = new byte[length];
    for (int i = 0; i < length; i++) {
      result[i] = pattern[i % pattern.length];
    }
    return result;
  }

  private static int encodeAndRoundTrip(final byte[] raw, final IndexType kind) {
    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment input = arena.allocate(Math.max(1, raw.length));
      MemorySegment.copy(raw, 0, input, ValueLayout.JAVA_BYTE, 0L, raw.length);
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      try {
        PageKind.emitSmallestBody(sink, input, raw.length, kind.getID());
        final BytesIn<?> source = Bytes.wrapForRead(sink.toByteArray());
        final int encodedLength = source.readInt();
        final int codec = source.readByte();
        assertEquals(encodedLength, source.remaining());
        final byte[] encoded = new byte[encodedLength + SirixLZ77Codec.NATIVE_INPUT_TAIL_SLACK];
        source.read(encoded, 0, encodedLength);
        final MemorySegment decoded = arena.allocate(raw.length + SirixLZ77Codec.NATIVE_OUTPUT_TAIL_SLACK);
        final int decodedLength = switch (codec) {
          case 0 -> ZeroRunByteCodec.decode(encoded, 0, encodedLength, decoded, 0L);
          case 2 -> ByteRunCodec.decode(encoded, 0, encodedLength, decoded, 0L);
          case 3 -> SirixLZ77Codec.decode(encoded, 0, encodedLength, decoded, 0L);
          default -> throw new AssertionError("Unexpected body codec " + codec);
        };
        assertEquals(raw.length, decodedLength);
        assertArrayEquals(raw, decoded.asSlice(0, raw.length).toArray(ValueLayout.JAVA_BYTE));
        return codec;
      } finally {
        sink.close();
      }
    }
  }
}
