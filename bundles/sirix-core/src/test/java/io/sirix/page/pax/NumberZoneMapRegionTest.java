/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import io.sirix.page.SirixLZ77Codec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import jdk.incubator.vector.VectorOperators;

/**
 * Tests for {@link NumberZoneMapRegion} and the promise it makes: that a range predicate can be
 * settled from the region alone, and that the bounds it reports are the same ones the number
 * region's own header would have given.
 *
 * <p>
 * The property that matters is not that the region round-trips — it is that it never disagrees with
 * the column it summarises. A zone map that is merely stale prunes a page that contains matches,
 * and the query returns a wrong count with nothing failing.
 */
@DisplayName("NumberZoneMapRegion")
final class NumberZoneMapRegionTest {

  /**
   * The zone map's wire form follows the number region's: with the per-tag election on it is the
   * varint V2 form, off it is the fixed-width V1 form. The cases below that pin V1 byte counts or its
   * compression envelope set the switch, and must not leak it into the next case.
   */
  @AfterEach
  void clearEncoderOverrides() {
    NumberRegion.clearPerTagWidthOverride();
  }


  private static NumberRegion.Header numberHeader(final long[] values, final int tagCount) {
    final int[] tags = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      tags[i] = i % tagCount;
    }
    return new NumberRegion.Header().parseInto(PaxTestSegments.of(NumberRegion.encode(values, tags, values.length)));
  }

  @Test
  @DisplayName("reports exactly the bounds the number region's own header carries")
  void agreesWithTheNumberRegionItSummarises() {
    final Random rng = new Random(2026);
    for (final int tagCount : new int[] {1, 3, 7}) {
      final long[] values = new long[500];
      for (int i = 0; i < values.length; i++) {
        values[i] = 100_000 + rng.nextInt(50_000);
      }
      final NumberRegion.Header source = numberHeader(values, tagCount);
      final byte[] encoded = NumberZoneMapRegion.encode(source);
      assertNotNull(encoded, "a zone-mapped number region must produce a zone map");
      // encodedSize is the FIXED form's exact size and the varint form's bound: the varint form is
      // elected only when it is strictly smaller, which is what keeps one buffer sufficient for both.
      assertTrue(encoded.length <= NumberZoneMapRegion.encodedSize(source.dictSize),
          "the elected form must fit the fixed form's buffer");
      assertEquals(NumberZoneMapRegion.VERSION_V2, encoded[0],
          "these bounds are small enough that the varint form must win");
      assertTrue(encoded.length < NumberZoneMapRegion.encodedSize(source.dictSize),
          "the varint form must be strictly smaller here, else it would not have been elected");

      final NumberZoneMapRegion.Header z = new NumberZoneMapRegion.Header().parseInto(PaxTestSegments.of(encoded));
      assertNotNull(z);
      assertEquals(source.tagKind, z.tagKind);
      assertEquals(source.valueMin, z.valueMin);
      assertEquals(source.valueMax, z.valueMax);
      assertEquals(source.dictSize, z.dictSize);
      for (int tag = 0; tag < source.dictSize; tag++) {
        assertEquals(source.dict[tag], z.dict[tag], "dict entry " + tag);
        assertEquals(source.tagCount[tag], z.tagCount[tag], "tagCount " + tag);
        assertEquals(source.tagMin[tag], z.tagMin[tag], "tagMin " + tag);
        assertEquals(source.tagMax[tag], z.tagMax[tag], "tagMax " + tag);
        // The bounds must also be true of the values, not merely copied faithfully.
        assertEquals(tag, NumberZoneMapRegion.lookupTag(z, z.dict[tag]), "lookupTag " + tag);
      }

      // Every value must lie inside the bounds its tag advertises.
      final long[] all = new long[source.count];
      NumberRegion.decodeAllValues(
          PaxTestSegments.of(NumberRegion.encode(values, tagsFor(values.length, tagCount), values.length)), source,
          all);
      for (int tag = 0; tag < source.dictSize; tag++) {
        final int start = source.tagStart[tag];
        for (int i = start; i < start + source.tagCount[tag]; i++) {
          assertTrue(all[i] >= z.tagMin[tag] && all[i] <= z.tagMax[tag],
              "value " + all[i] + " outside advertised bounds of tag " + tag);
        }
      }
    }
  }

  private static int[] tagsFor(final int n, final int tagCount) {
    final int[] tags = new int[n];
    for (int i = 0; i < n; i++) {
      tags[i] = i % tagCount;
    }
    return tags;
  }

  @Test
  @DisplayName("pruning from the region agrees with counting the column")
  void pruningAgreesWithScanning() {
    final Random rng = new Random(7);
    final long[] values = new long[400];
    for (int i = 0; i < values.length; i++) {
      values[i] = 1_000 + rng.nextInt(500);
    }
    final NumberRegion.Header source = numberHeader(values, 2);
    final MemorySegment payload =
        PaxTestSegments.of(NumberRegion.encode(values, tagsFor(values.length, 2), values.length));
    final NumberZoneMapRegion.Header z =
        new NumberZoneMapRegion.Header().parseInto(PaxTestSegments.of(NumberZoneMapRegion.encode(source)));

    for (int tag = 0; tag < z.dictSize; tag++) {
      final int start = source.tagStart[tag];
      final int end = start + source.tagCount[tag];
      final long lo = z.tagMin[tag];
      final long hi = z.tagMax[tag];
      // Bounds chosen to hit all three outcomes: entirely below, entirely inside, and straddling.
      for (final long[] range : new long[][] {{Long.MIN_VALUE, lo - 1}, // nothing can match
          {lo, hi}, // everything must match
          {hi + 1, Long.MAX_VALUE}, // nothing can match
          {lo + (hi - lo) / 2, hi}, // straddles: must decline
      }) {
        final long pruned =
            NumberRegionSimd.pruneCount(z.tagMin[tag], z.tagMax[tag], range[0], range[1], z.tagCount[tag]);
        final long scanned = NumberRegionSimd.countMatchingRange(payload, source, start, end, VectorOperators.GE,
            range[0], VectorOperators.LE, range[1]);
        if (pruned != NumberRegionSimd.PRUNE_UNKNOWN) {
          assertEquals(scanned, pruned,
              "prune disagreed with the scan for tag " + tag + " over [" + range[0] + ", " + range[1] + "]");
        }
      }
    }
  }

  @Test
  @DisplayName("declines rather than guessing on absent, truncated or future payloads")
  void declinesRatherThanGuessing() {
    final NumberZoneMapRegion.Header scratch = new NumberZoneMapRegion.Header();
    assertNull(scratch.parseInto(null), "absent region");
    assertNull(scratch.parseInto(PaxTestSegments.of(new byte[0])), "empty region");
    assertNull(scratch.parseInto(PaxTestSegments.of(new byte[8])), "truncated below the fixed header");

    final NumberRegion.Header source = numberHeader(new long[] {1, 2, 3, 4}, 1);
    final byte[] good = NumberZoneMapRegion.encode(source);
    assertNotNull(good);

    final byte[] futureVersion = good.clone();
    futureVersion[0] = 99;
    assertNull(scratch.parseInto(PaxTestSegments.of(futureVersion)), "a future version must decline, not misparse");

    final byte[] truncated = new byte[good.length - 4];
    System.arraycopy(good, 0, truncated, 0, truncated.length);
    assertNull(scratch.parseInto(PaxTestSegments.of(truncated)), "a payload shorter than its own dictSize claims");
  }

  @Test
  @DisplayName("a legacy number region with no per-tag bounds produces no zone map")
  void noZoneMapWithoutPerTagBounds() {
    final NumberRegion.Header legacy = new NumberRegion.Header();
    legacy.encodingKind = NumberRegion.ENC_PLAIN_LONG; // pre-zone-map encoding
    legacy.dictSize = 2;
    legacy.tagMin = null;
    legacy.tagMax = null;
    assertNull(NumberZoneMapRegion.encode(legacy), "without per-tag bounds there is nothing to summarise");
    assertNull(NumberZoneMapRegion.encode(null));
  }

  @Test
  @DisplayName("encodeInto is wire-identical across maximum/small scratch reuse")
  void encodeIntoIsWireIdenticalAcrossScratchReuse() {
    final byte[] reusable = new byte[NumberZoneMapRegion.encodedSize(1024)];
    for (final int dictSize : new int[] {1024, 1, 73, 1024}) {
      final long[] values = new long[1024];
      for (int i = 0; i < values.length; i++) {
        values[i] = 50_000L + i * 17L;
      }
      final NumberRegion.Header source = numberHeader(values, dictSize);
      final byte[] expected = NumberZoneMapRegion.encode(source);
      Arrays.fill(reusable, (byte) 0x5A);

      final int length = NumberZoneMapRegion.encodeInto(source, reusable);

      assertEquals(expected.length, length);
      assertArrayEquals(expected, Arrays.copyOf(reusable, length), "dictSize=" + dictSize + " after scratch reuse");
      final NumberZoneMapRegion.Header decoded =
          new NumberZoneMapRegion.Header().parseInto(PaxTestSegments.of(Arrays.copyOf(reusable, length)));
      assertNotNull(decoded);
      assertEquals(dictSize, decoded.dictSize);
    }
  }

  @Test
  @DisplayName("encodeInto retains the exact V1 little-endian wire layout")
  void encodeIntoMatchesFixedWireFixture() {
    NumberRegion.setPerTagWidthEnabled(false);
    final NumberRegion.Header source = new NumberRegion.Header();
    source.tagKind = NumberRegion.TAG_KIND_PATH_NODE;
    source.valueMin = 0x0102030405060708L;
    source.valueMax = -2L;
    source.dictSize = 1;
    source.dict = new int[] {0x11223344};
    source.tagCount = new int[] {0x01020304};
    source.tagMin = new long[] {Long.MIN_VALUE};
    source.tagMax = new long[] {0x0A0B0C0D0E0F1011L};
    final byte[] expected = {1, 1, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, (byte) 0xFE, (byte) 0xFF,
        (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF, 0x01, 0x00, 0x00, 0x00, 0x44,
        0x33, 0x22, 0x11, 0x04, 0x03, 0x02, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, (byte) 0x80, 0x11, 0x10,
        0x0F, 0x0E, 0x0D, 0x0C, 0x0B, 0x0A};
    final byte[] reusable = new byte[expected.length];

    final int length = NumberZoneMapRegion.encodeInto(source, reusable);

    assertEquals(expected.length, length);
    assertArrayEquals(expected, reusable);
  }

  @Test
  @DisplayName("encodeInto preserves absence and validates caller-owned capacity")
  void encodeIntoRefusalAndCapacityValidation() {
    final NumberRegion.Header legacy = new NumberRegion.Header();
    legacy.dictSize = 1;
    assertEquals(NumberZoneMapRegion.ENCODE_FAILED,
        NumberZoneMapRegion.encodeInto(legacy, new byte[NumberZoneMapRegion.encodedSize(1)]));
    assertEquals(NumberZoneMapRegion.ENCODE_FAILED,
        NumberZoneMapRegion.encodeInto(null, new byte[NumberZoneMapRegion.encodedSize(1)]));
    final NumberRegion.Header empty = new NumberRegion.Header();
    empty.dictSize = 0;
    empty.tagMin = new long[0];
    empty.tagMax = new long[0];
    assertNull(NumberZoneMapRegion.encode(empty), "the legacy encoder refuses an empty dictionary");

    final NumberRegion.Header source = numberHeader(new long[] {1, 2, 3, 4}, 2);
    assertThrows(IllegalArgumentException.class,
        () -> NumberZoneMapRegion.encodeInto(source, new byte[NumberZoneMapRegion.encodedSize(source.dictSize) - 1]));
  }

  // ───────────────────────────────────────────────── region table integration

  @Test
  @DisplayName("a small zone map stays raw and survives a region-table round trip")
  void smallMapRoundTripsThroughTheRegionTableUncompressed() {
    final NumberRegion.Header source = numberHeader(bigValues(), 8);
    final byte[] zoneMap = NumberZoneMapRegion.encode(source);
    assertNotNull(zoneMap);
    assertTrue(zoneMap.length > 64, "this fixture must exceed the compression threshold, else it proves nothing");

    try (RegionTable table = new RegionTable()) {
      table.set(RegionTable.KIND_NUMBER_ZONEMAP, zoneMap);

      final BytesOut<MemorySegment> out = Bytes.elasticHeapByteBuffer();
      table.write(out, true);
      assertEquals(Integer.BYTES + 1 + 1 + Integer.BYTES + zoneMap.length, out.writePosition(),
          "a small pruning summary must stay raw");
      try (RegionTable back = RegionTable.read(out.bytesForRead())) {
        final NumberZoneMapRegion.Header z =
            new NumberZoneMapRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER_ZONEMAP));
        assertNotNull(z, "zone map must survive the round trip");
        assertEquals(source.dictSize, z.dictSize);
        for (int tag = 0; tag < source.dictSize; tag++) {
          assertEquals(source.tagMin[tag], z.tagMin[tag]);
          assertEquals(source.tagMax[tag], z.tagMax[tag]);
        }
      }
    }
  }

  @Test
  @DisplayName("a wide zone map compresses independently and preserves every bound")
  void wideMapCompressesIndependently() {
    // Pinned to the fixed-width form: its 24 bytes per tag are what makes a wide map large and
    // repetitive enough to elect LZ77. The varint form of the same map is a third of the size and
    // stays raw — cheaper still, and covered by NumberRegionPerTagForTest.
    NumberRegion.setPerTagWidthEnabled(false);
    final int dictSize = 73;
    final NumberRegion.Header source = repetitiveHeader(dictSize);
    final byte[] zoneMap = NumberZoneMapRegion.encode(source);
    assertNotNull(zoneMap);

    try (RegionTable table = new RegionTable()) {
      table.set(RegionTable.KIND_NUMBER_ZONEMAP, zoneMap);
      final BytesOut<MemorySegment> out = Bytes.elasticHeapByteBuffer();
      table.write(out, true);

      final long rawFrameLength = Integer.BYTES + 1L + 1L + Integer.BYTES + zoneMap.length;
      assertTrue(out.writePosition() < rawFrameLength, "the repetitive wide summary must elect LZ77");
      try (RegionTable back = RegionTable.read(out.bytesForRead())) {
        final NumberZoneMapRegion.Header decoded =
            new NumberZoneMapRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER_ZONEMAP));
        assertNotNull(decoded);
        assertEquals(source.dictSize, decoded.dictSize);
        assertArrayEquals(source.dict, decoded.dict);
        assertArrayEquals(source.tagCount, decoded.tagCount);
        assertArrayEquals(source.tagMin, decoded.tagMin);
        assertArrayEquals(source.tagMax, decoded.tagMax);
      }
    }
  }

  @Test
  @DisplayName("wide-zone-map compression starts at the 512-byte accelerator threshold")
  void wideMapCompressionThresholdIsExact() {
    // The subject is RegionTable's 512-byte accelerator threshold, measured on the fixed-width
    // form whose size per tag is exact.
    NumberRegion.setPerTagWidthEnabled(false);
    final byte[] belowThreshold = NumberZoneMapRegion.encode(repetitiveHeader(20));
    final byte[] atOrAboveThreshold = NumberZoneMapRegion.encode(repetitiveHeader(21));
    assertNotNull(belowThreshold);
    assertNotNull(atOrAboveThreshold);
    assertEquals(502, belowThreshold.length);
    assertEquals(526, atOrAboveThreshold.length);

    assertEquals(0, zoneMapCodec(writeZoneMap(belowThreshold)), "a sub-512-byte accelerator must stay raw");
    assertEquals(3, zoneMapCodec(writeZoneMap(atOrAboveThreshold)),
        "a compressible accelerator at or above 512 bytes must elect LZ77");
  }

  @Test
  @DisplayName("complete-wire ties and marginal savings keep the accelerator raw")
  void marginalCompleteWireSavingsStayRaw() {
    final byte[] equalFramePayload = marginalPayload(13);
    final byte[] oneByteSavingPayload = marginalPayload(14);
    final byte[] encoded = new byte[SirixLZ77Codec.maxEncodedSize(equalFramePayload.length)];

    assertEquals(508,
        SirixLZ77Codec.encode(PaxTestSegments.of(equalFramePayload), 0L, equalFramePayload.length, encoded, 0),
        "encoded payload is four bytes smaller, making the complete RAW and LZ77 entries equal");
    assertEquals(507,
        SirixLZ77Codec.encode(PaxTestSegments.of(oneByteSavingPayload), 0L, oneByteSavingPayload.length, encoded, 0),
        "complete LZ77 entry saves exactly one byte");
    assertEquals(0, zoneMapCodec(writeZoneMap(equalFramePayload)), "a complete-wire tie must stay RAW");
    assertEquals(0, zoneMapCodec(writeZoneMap(oneByteSavingPayload)),
        "one byte does not justify decoding a pruning accelerator");
  }

  @Test
  @DisplayName("zone-map compression clears the documented 20-percent complete-wire gate")
  void zoneMapCompressionSavingBoundaryIsExact() {
    assertTrue(RegionTable.compressionPays(RegionTable.KIND_NUMBER_ZONEMAP, 512, 404),
        "414-byte LZ77 entry is at least 20% smaller than the 518-byte RAW entry");
    assertFalse(RegionTable.compressionPays(RegionTable.KIND_NUMBER_ZONEMAP, 512, 405),
        "415-byte LZ77 entry is less than 20% smaller than the 518-byte RAW entry");
  }

  @Test
  @DisplayName("an incompressible wide zone-map payload falls back to the exact raw bytes")
  void incompressibleWideMapFallsBackToRaw() {
    final byte[] payload = new byte[2_048];
    new Random(0x5eed_2048L).nextBytes(payload);
    final byte[] wire = writeZoneMap(payload);

    assertEquals(0, zoneMapCodec(wire), "compression must not be retained when its complete frame is not smaller");
    try (RegionTable back = RegionTable.read(Bytes.wrapForRead(wire))) {
      assertArrayEquals(payload, PaxTestSegments.bytes(back.payload(RegionTable.KIND_NUMBER_ZONEMAP)));
    }
  }

  @Test
  @DisplayName("asking for the number column also brings its directory; asking for neither skips both")
  void theNumberColumnAndItsDirectoryAreReadTogether() {
    final NumberRegion.Header source = numberHeader(bigValues(), 4);
    final byte[] numbers = NumberRegion.encode(bigValues(), tagsFor(bigValues().length, 4), bigValues().length);
    try (RegionTable table = new RegionTable()) {
      table.set(RegionTable.KIND_NUMBER, numbers);
      table.set(RegionTable.KIND_NUMBER_ZONEMAP, NumberZoneMapRegion.encode(source));
      table.set(RegionTable.KIND_STRING, new byte[] {1, 2, 3, 4});

      final BytesOut<MemorySegment> out = Bytes.elasticHeapByteBuffer();
      table.write(out, true);

      // The summary carries the number column's per-tag directory, so a request for the values is a
      // request for both — enforced by the reader rather than by every caller remembering. A caller
      // that forgot would not get a wrong answer, it would silently lose columnar serving.
      try (RegionTable back = RegionTable.read(out.bytesForRead(), RegionTable.maskOf(RegionTable.KIND_NUMBER))) {
        assertNotNull(back.payload(RegionTable.KIND_NUMBER_ZONEMAP), "the directory comes with its column");
        final NumberRegion.Header h = new NumberRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER),
            back.payload(RegionTable.KIND_NUMBER_ZONEMAP));
        assertNotNull(h);
        assertEquals(source.count, h.count);
        assertEquals(source.dictSize, h.dictSize);
      }

      // A reader that wants neither still steps over both by their length prefixes — the path a
      // reader predating either region takes, which is what this stands in for.
      final BytesOut<MemorySegment> again = Bytes.elasticHeapByteBuffer();
      table.write(again, true);
      try (RegionTable back = RegionTable.read(again.bytesForRead(), RegionTable.maskOf(RegionTable.KIND_STRING))) {
        assertNull(back.payload(RegionTable.KIND_NUMBER_ZONEMAP), "not requested, must be absent");
        assertNull(back.payload(RegionTable.KIND_NUMBER), "not requested, must be absent");
        assertArrayEquals(new byte[] {1, 2, 3, 4}, PaxTestSegments.bytes(back.payload(RegionTable.KIND_STRING)),
            "stepping over both must not disturb what follows them");
      }
    }
  }

  @Test
  @DisplayName("the zone map can be read without materializing the number column")
  void readsWithoutMaterializingTheColumn() {
    // Pinned to the fixed-width form so the map is large enough to be LZ77-framed: the property
    // under test is that even a COMPRESSED summary is read without materializing the column.
    NumberRegion.setPerTagWidthEnabled(false);
    final int tagCount = 73;
    final long[] values = wideValues(tagCount);
    final NumberRegion.Header source = numberHeader(values, tagCount);
    try (RegionTable table = new RegionTable()) {
      table.set(RegionTable.KIND_NUMBER, NumberRegion.encode(values, tagsFor(values.length, tagCount), values.length));
      table.set(RegionTable.KIND_NUMBER_ZONEMAP, NumberZoneMapRegion.encode(source));

      final BytesOut<MemorySegment> out = Bytes.elasticHeapByteBuffer();
      table.write(out, true);
      final byte[] wire = out.bytesForRead().toByteArray();
      assertEquals(3, zoneMapCodec(wire), "this test must exercise an independently compressed zone map");

      // Number column deferred, zone map materialized — the shape the count path reads in.
      try (RegionTable back = RegionTable.read(Bytes.wrapForRead(wire),
          RegionTable.maskOf(RegionTable.KIND_NUMBER) | RegionTable.maskOf(RegionTable.KIND_NUMBER_ZONEMAP),
          RegionTable.maskOf(RegionTable.KIND_NUMBER))) {
        assertTrue(back.hasRegion(RegionTable.KIND_NUMBER), "the column must be present, just deferred");
        final int retainedBeforePruning = back.retainedBytes();
        final NumberZoneMapRegion.Header z =
            new NumberZoneMapRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER_ZONEMAP));
        assertNotNull(z, "the bounds must be readable while the column is still compressed");
        assertEquals(source.dictSize, z.dictSize);
        assertEquals(0L,
            NumberRegionSimd.pruneCount(z.tagMin[0], z.tagMax[0], Long.MIN_VALUE, z.tagMin[0] - 1L, z.tagCount[0]),
            "the compressed summary must settle an out-of-range predicate");
        assertEquals(retainedBeforePruning, back.retainedBytes(),
            "settling the predicate from the zone map must leave the number column deferred");

        // And the column is still correct once someone does ask for it.
        final NumberRegion.Header h = new NumberRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER));
        assertEquals(source.count, h.count);
      }
    }
  }

  private static NumberRegion.Header repetitiveHeader(final int dictSize) {
    final NumberRegion.Header source = new NumberRegion.Header();
    source.encodingKind = NumberRegion.ENC_BIT_PACKED_ZM;
    source.tagKind = NumberRegion.TAG_KIND_PATH_NODE;
    source.valueMin = 100L;
    source.valueMax = 10_000L;
    source.dictSize = dictSize;
    source.dict = new int[dictSize];
    source.tagCount = new int[dictSize];
    source.tagMin = new long[dictSize];
    source.tagMax = new long[dictSize];
    for (int tag = 0; tag < dictSize; tag++) {
      source.dict[tag] = 1_000 + tag;
      source.tagCount[tag] = 10;
      source.tagMin[tag] = 100L;
      source.tagMax[tag] = 10_000L;
    }
    return source;
  }

  private static byte[] writeZoneMap(final byte[] payload) {
    try (RegionTable table = new RegionTable()) {
      table.set(RegionTable.KIND_NUMBER_ZONEMAP, payload);
      final BytesOut<MemorySegment> out = Bytes.elasticHeapByteBuffer();
      table.write(out, true);
      return out.bytesForRead().toByteArray();
    }
  }

  private static byte[] marginalPayload(final int repeatedTailBytes) {
    final byte[] payload = new byte[512];
    new Random(0L).nextBytes(payload);
    System.arraycopy(payload, 0, payload, payload.length - repeatedTailBytes, repeatedTailBytes);
    return payload;
  }

  private static int zoneMapCodec(final byte[] wire) {
    assertEquals(RegionTable.KIND_NUMBER_ZONEMAP, wire[Integer.BYTES], "zone map must be first in write order");
    return wire[Integer.BYTES + Byte.BYTES] & 0xff;
  }

  private static long[] wideValues(final int tagCount) {
    final long[] values = new long[tagCount * 10];
    for (int i = 0; i < values.length; i++) {
      values[i] = 100L + i / tagCount;
    }
    return values;
  }

  /** Enough tags that the zone map exceeds the compression threshold. */
  private static long[] bigValues() {
    final long[] values = new long[800];
    final Random rng = new Random(31);
    for (int i = 0; i < values.length; i++) {
      values[i] = 500_000 + rng.nextInt(90_000);
    }
    return values;
  }
}
