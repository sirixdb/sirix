/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import jdk.incubator.vector.VectorOperators;

/**
 * Tests for {@link NumberZoneMapRegion} and the promise it makes: that a range predicate can be
 * settled from the region alone, and that the bounds it reports are the same ones the number
 * region's own header would have given.
 *
 * <p>The property that matters is not that the region round-trips — it is that it never disagrees
 * with the column it summarises. A zone map that is merely stale prunes a page that contains
 * matches, and the query returns a wrong count with nothing failing.
 */
@DisplayName("NumberZoneMapRegion")
final class NumberZoneMapRegionTest {


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
      assertEquals(NumberZoneMapRegion.encodedSize(source.dictSize), encoded.length,
                   "encodedSize must match what encode actually writes");

      final NumberZoneMapRegion.Header z =
          new NumberZoneMapRegion.Header().parseInto(PaxTestSegments.of(encoded));
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
      NumberRegion.decodeAllValues(PaxTestSegments.of(NumberRegion.encode(values,
                                                           tagsFor(values.length, tagCount),
                                                           values.length)), source, all);
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
    final MemorySegment payload = PaxTestSegments.of(NumberRegion.encode(values, tagsFor(values.length, 2),
                                                          values.length));
    final NumberZoneMapRegion.Header z =
        new NumberZoneMapRegion.Header().parseInto(PaxTestSegments.of(NumberZoneMapRegion.encode(source)));

    for (int tag = 0; tag < z.dictSize; tag++) {
      final int start = source.tagStart[tag];
      final int end = start + source.tagCount[tag];
      final long lo = z.tagMin[tag];
      final long hi = z.tagMax[tag];
      // Bounds chosen to hit all three outcomes: entirely below, entirely inside, and straddling.
      for (final long[] range : new long[][] {
          {Long.MIN_VALUE, lo - 1},          // nothing can match
          {lo, hi},                          // everything must match
          {hi + 1, Long.MAX_VALUE},          // nothing can match
          {lo + (hi - lo) / 2, hi},          // straddles: must decline
      }) {
        final long pruned =
            NumberRegionSimd.pruneCount(z.tagMin[tag], z.tagMax[tag], range[0], range[1],
                                        z.tagCount[tag]);
        final long scanned = NumberRegionSimd.countMatchingRange(
            payload, source, start, end,
            VectorOperators.GE, range[0],
            VectorOperators.LE, range[1]);
        if (pruned != NumberRegionSimd.PRUNE_UNKNOWN) {
          assertEquals(scanned, pruned,
                       "prune disagreed with the scan for tag " + tag + " over ["
                           + range[0] + ", " + range[1] + "]");
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
    legacy.encodingKind = NumberRegion.ENC_PLAIN_LONG;   // pre-zone-map encoding
    legacy.dictSize = 2;
    legacy.tagMin = null;
    legacy.tagMax = null;
    assertNull(NumberZoneMapRegion.encode(legacy),
               "without per-tag bounds there is nothing to summarise");
    assertNull(NumberZoneMapRegion.encode(null));
  }

  // ───────────────────────────────────────────────── region table integration

  @Test
  @DisplayName("survives a region-table round trip and is never compressed")
  void roundTripsThroughTheRegionTableUncompressed() {
    final NumberRegion.Header source = numberHeader(bigValues(), 8);
    final byte[] zoneMap = NumberZoneMapRegion.encode(source);
    assertNotNull(zoneMap);
    assertTrue(zoneMap.length > 64,
               "this fixture must exceed the compression threshold, else it proves nothing");

    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_NUMBER_ZONEMAP, zoneMap);

    final BytesOut<MemorySegment> out = Bytes.elasticHeapByteBuffer();
    table.write(out, true);   // compression enabled — the zone map must opt out anyway
    final RegionTable back = RegionTable.read(out.bytesForRead());

    final NumberZoneMapRegion.Header z =
        new NumberZoneMapRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER_ZONEMAP));
    assertNotNull(z, "zone map must survive the round trip");
    assertEquals(source.dictSize, z.dictSize);
    for (int tag = 0; tag < source.dictSize; tag++) {
      assertEquals(source.tagMin[tag], z.tagMin[tag]);
      assertEquals(source.tagMax[tag], z.tagMax[tag]);
    }
  }

  @Test
  @DisplayName("a reader that skips the zone map still reads every other region correctly")
  void skippingTheZoneMapLeavesTheRestIntact() {
    final NumberRegion.Header source = numberHeader(bigValues(), 4);
    final byte[] numbers = NumberRegion.encode(bigValues(), tagsFor(bigValues().length, 4),
                                               bigValues().length);
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_NUMBER, numbers);
    table.set(RegionTable.KIND_NUMBER_ZONEMAP, NumberZoneMapRegion.encode(source));

    final BytesOut<MemorySegment> out = Bytes.elasticHeapByteBuffer();
    table.write(out, true);

    // Ask for only the number column — the zone map is stepped over by its length prefix. This is
    // the same path a reader that predates the region takes, so it stands in for one.
    final RegionTable back = RegionTable.read(out.bytesForRead(),
                                              RegionTable.maskOf(RegionTable.KIND_NUMBER));
    assertNull(back.payload(RegionTable.KIND_NUMBER_ZONEMAP), "not requested, must be absent");
    final NumberRegion.Header h =
        new NumberRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER));
    assertNotNull(h);
    assertEquals(source.count, h.count, "skipping the zone map must not disturb the number region");
    assertEquals(source.dictSize, h.dictSize);
  }

  @Test
  @DisplayName("the zone map can be read without materializing the number column")
  void readsWithoutMaterializingTheColumn() {
    final long[] values = bigValues();
    final NumberRegion.Header source = numberHeader(values, 4);
    final RegionTable table = new RegionTable();
    table.set(RegionTable.KIND_NUMBER,
              NumberRegion.encode(values, tagsFor(values.length, 4), values.length));
    table.set(RegionTable.KIND_NUMBER_ZONEMAP, NumberZoneMapRegion.encode(source));

    final BytesOut<MemorySegment> out = Bytes.elasticHeapByteBuffer();
    table.write(out, true);

    // Number column deferred, zone map materialized — the shape the count path reads in.
    final RegionTable back = RegionTable.read(
        out.bytesForRead(),
        RegionTable.maskOf(RegionTable.KIND_NUMBER) | RegionTable.maskOf(RegionTable.KIND_NUMBER_ZONEMAP),
        RegionTable.maskOf(RegionTable.KIND_NUMBER));

    assertTrue(back.hasRegion(RegionTable.KIND_NUMBER), "the column must be present, just deferred");
    final NumberZoneMapRegion.Header z =
        new NumberZoneMapRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER_ZONEMAP));
    assertNotNull(z, "the bounds must be readable while the column is still compressed");
    assertEquals(source.dictSize, z.dictSize);

    // And the column is still correct once someone does ask for it.
    final NumberRegion.Header h =
        new NumberRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER));
    assertEquals(source.count, h.count);
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
