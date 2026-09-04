/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import jdk.incubator.vector.VectorOperators;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The per-tag frame of reference: one width per tag rather than one per page, and a zone map that
 * is the frame of reference rather than a second copy of it.
 *
 * <h2>What the fixtures are</h2>
 *
 * <p>
 * Two shapes, both drawn from what a record-shaped corpus actually puts on a leaf. The
 * <em>mixed</em> one is the smallest fixture that breaks the page-wide width: an 8-bit field, a
 * 16-bit field, a constant field and a 64-bit hash, where the hash's spread alone forced every
 * other tag onto plain longs. The <em>wide</em> one is a page of 60 numeric fields over 10 rows —
 * the shape where per-tag framing has to pay for itself against the values it frames.
 *
 * <h2>What the assertions are</h2>
 *
 * <p>
 * Ground truth is the input {@code long[]}, never a second decoder. Every region-only door — scalar
 * decode, bulk decode, count, masked count, aggregate, selection, selection bitmap, zone-map prune
 * — is asserted against a hand-written loop over those values, with the vector path forced on and
 * off, because the kernels hold two implementations of every predicate.
 *
 * <p>
 * The size claims are pinned as bytes, not as ratios of themselves: the mutation is the kill
 * switch, which restores one width for the whole page, and it must cost at least four times the
 * bytes on the wide fixture. The kill switch's own output is pinned to a digest recorded from
 * HEAD-compiled classes, so "byte-identical to the old encoder" means the old encoder, not this one
 * agreeing with itself.
 */
@DisplayName("number region, per-tag frame of reference")
final class NumberRegionPerTagForTest {

  /**
   * SHA-256 of the mixed fixture's number region as the pre-per-tag encoder wrote it.
   *
   * <p>
   * Recorded by compiling {@code git show HEAD:.../NumberRegion.java} into a separate output
   * directory and hashing this exact fixture there — see the class comment on why a pin taken from
   * the new encoder would prove only that it agrees with itself.
   */
  private static final String HEAD_MIXED_NUMBER_SHA256 =
      "3af0fc96116f7fe6a43e05ea8db926153d8d8fa1a77def8804b719c4c4f1130b";

  /** SHA-256 of the mixed fixture's zone map as the pre-per-tag encoder wrote it. */
  private static final String HEAD_MIXED_ZONEMAP_SHA256 =
      "5b024de760a196ab6b250ce7f7c226809dae20180f922f4b526c1186ef65e4c8";

  /** SHA-256 of the wide fixture's number region as the pre-per-tag encoder wrote it. */
  private static final String HEAD_WIDE_NUMBER_SHA256 =
      "ec78f6d5335bfa90f60b3c655b8f58da7c80cd44ad68fd6e6c9f3d9d9997f977";

  @AfterEach
  void restoreDefaults() {
    NumberRegion.clearPerTagWidthOverride();
    NumberRegion.clearExternalHeaderOverride();
    BitUnpackSimd.resetWarmupForTesting();
  }

  // ────────────────────────────────────────────────────────────────── fixtures

  /** Values and tags of the mixed fixture, interleaved the way a record writer stages them. */
  private static final class Column {
    final long[] values;
    final int[] tags;
    final int count;

    Column(final long[] values, final int[] tags, final int count) {
      this.values = values;
      this.tags = tags;
      this.count = count;
    }
  }

  /** 8-bit, 16-bit, constant and 64-bit-hash tags — the four widths in one region. */
  private static Column mixed() {
    final int perTag = 60;
    final long[] values = new long[4 * perTag];
    final int[] tags = new int[4 * perTag];
    final Random rnd = new Random(42);
    int n = 0;
    for (int i = 0; i < perTag; i++) {
      values[n] = 100 + (i % 256);
      tags[n++] = 10;
      values[n] = 40000 + (i * 977 % 65536);
      tags[n++] = 20;
      values[n] = -7L;
      tags[n++] = 30;
      values[n] = rnd.nextLong();
      tags[n++] = 40;
    }
    return new Column(values, tags, n);
  }

  /** A record-shaped page: 60 numeric fields over 10 rows, most of them narrow. */
  private static Column wide() {
    final long[] values = new long[600];
    final int[] tags = new int[600];
    final Random rnd = new Random(7);
    int n = 0;
    for (int row = 0; row < 10; row++) {
      for (int field = 0; field < 60; field++) {
        final long v;
        if (field < 40) {
          v = 1 + rnd.nextInt(200);
        } else if (field < 50) {
          v = 1_600_000_000L + rnd.nextInt(1 << 20);
        } else if (field < 56) {
          v = 3L;
        } else {
          v = rnd.nextLong();
        }
        values[n] = v;
        tags[n++] = 1000 + field;
      }
    }
    return new Column(values, tags, n);
  }

  private static byte[] encode(final Column column, final boolean perTag) {
    NumberRegion.setPerTagWidthEnabled(perTag);
    try {
      return NumberRegion.encode(column.values, column.tags, column.count, NumberRegion.TAG_KIND_PATH_NODE);
    } finally {
      NumberRegion.clearPerTagWidthOverride();
    }
  }

  /** The column's values in region order: grouped by tag, input order within a tag. */
  private static long[] inRegionOrder(final Column column, final NumberRegion.Header h) {
    final long[] out = new long[column.count];
    final int[] cursor = new int[h.dictSize];
    for (int i = 0; i < column.count; i++) {
      final int tag = NumberRegion.lookupTag(h, column.tags[i]);
      out[h.tagStart[tag] + cursor[tag]++] = column.values[i];
    }
    return out;
  }

  private static String sha256(final byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (final NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  /** Bytes one region occupies on the wire, its codec election included. */
  private static int wireBytes(final byte kind, final byte[] payload) {
    try (RegionTable table = new RegionTable()) {
      table.set(kind, payload);
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      table.write(sink, true);
      return (int) sink.writePosition();
    }
  }

  // ───────────────────────────────────────────────────── the size claim + mutation

  @Test
  @DisplayName("one width for the whole page costs over four times the bytes of one width per tag")
  void oneWidthForEveryTagIsTheMutation() {
    final Column wide = wide();
    final byte[] perTag = encode(wide, true);
    final byte[] pageWide = encode(wide, false);

    final NumberRegion.Header ph = new NumberRegion.Header().parseInto(PaxTestSegments.of(perTag));
    final NumberRegion.Header lh = new NumberRegion.Header().parseInto(PaxTestSegments.of(pageWide));
    assertEquals(NumberRegion.ENC_PER_TAG_FOR, ph.encodingKind, "the per-tag layout must win this shape");
    assertEquals(NumberRegion.ENC_PLAIN_LONG_ZM, lh.encodingKind,
        "the mutation is the page-wide frame, which one 64-bit hash field forces to plain longs");

    assertTrue(pageWide.length >= 4 * perTag.length,
        "one width for all tags must cost at least 4x: page-wide=" + pageWide.length + " per-tag=" + perTag.length);
    // The absolute bound, so a later change that quietly inflates the framing is caught even if the
    // ratio still holds: 600 values over 60 tags, of which 40 are 8-bit, 10 are 20-bit, 6 constant
    // and 4 are 64-bit hashes.
    assertTrue(perTag.length <= 1500,
        "the per-tag region must stay under 1,500 bytes for this page, was " + perTag.length);
    assertEquals(HEAD_WIDE_NUMBER_SHA256, sha256(pageWide),
        "the kill switch must reproduce the pre-per-tag encoder's bytes for the wide fixture");
  }

  @Test
  @DisplayName("the kill switch reproduces the pre-per-tag bytes of both regions")
  void killSwitchIsByteIdenticalToTheReferenceEncoder() {
    final Column mixed = mixed();
    final byte[] pageWide = encode(mixed, false);
    assertEquals(HEAD_MIXED_NUMBER_SHA256, sha256(pageWide), "number region under the kill switch");

    NumberRegion.setPerTagWidthEnabled(false);
    final NumberRegion.Header lh = new NumberRegion.Header().parseInto(PaxTestSegments.of(pageWide));
    final byte[] zoneMap = NumberZoneMapRegion.encode(lh);
    assertEquals(NumberZoneMapRegion.VERSION_V1, zoneMap[0], "the switch pins the zone map to its fixed form too");
    assertEquals(HEAD_MIXED_ZONEMAP_SHA256, sha256(zoneMap), "zone map under the kill switch");
  }

  @Test
  @DisplayName("the region and its zone map both shrink, and the packed form stops needing LZ77")
  void bytesOnTheWire() {
    final Column wide = wide();
    final byte[] perTag = encode(wide, true);
    final byte[] pageWide = encode(wide, false);
    final NumberRegion.Header ph = new NumberRegion.Header().parseInto(PaxTestSegments.of(perTag));
    final NumberRegion.Header lh = new NumberRegion.Header().parseInto(PaxTestSegments.of(pageWide));

    NumberRegion.setPerTagWidthEnabled(true);
    final byte[] zoneV2 = NumberZoneMapRegion.encode(ph);
    NumberRegion.setPerTagWidthEnabled(false);
    final byte[] zoneV1 = NumberZoneMapRegion.encode(lh);
    NumberRegion.clearPerTagWidthOverride();

    assertEquals(NumberZoneMapRegion.VERSION_V2, zoneV2[0]);
    assertTrue(zoneV2.length * 3 < zoneV1.length,
        "the varint zone map must be well under a third of the fixed one: " + zoneV2.length + " vs " + zoneV1.length);

    // Both regions together, as the region table writes them.
    final int perTagWire =
        wireBytes(RegionTable.KIND_NUMBER, perTag) + wireBytes(RegionTable.KIND_NUMBER_ZONEMAP, zoneV2);
    final int pageWideWire =
        wireBytes(RegionTable.KIND_NUMBER, pageWide) + wireBytes(RegionTable.KIND_NUMBER_ZONEMAP, zoneV1);
    assertTrue(perTagWire * 2 < pageWideWire,
        "the per-tag pair must more than halve the wire: " + perTagWire + " vs " + pageWideWire);

    // The packed payload has almost nothing left for LZ77 to remove — the per-tag width did that
    // work — where the page-wide payload's 8-byte lanes of leading zeros gave it a third of the
    // bytes for free. The election is KEPT rather than dropped precisely because "almost" is
    // measured per page: it still wins here, by under a twentieth of what it won before.
    final int perTagRawFrame = perTag.length + 1 + 1 + Integer.BYTES;
    final int pageWideRawFrame = pageWide.length + 1 + 1 + Integer.BYTES;
    final int perTagSaving = perTagRawFrame - wireBytes(RegionTable.KIND_NUMBER, perTag);
    final int pageWideSaving = pageWideRawFrame - wireBytes(RegionTable.KIND_NUMBER, pageWide);
    assertTrue(perTagSaving * 20 < perTag.length,
        "LZ77 must save under 5% of a packed per-tag payload, saved " + perTagSaving + " of " + perTag.length);
    assertTrue(pageWideSaving * 4 > pageWide.length,
        "and over 25% of the page-wide one, saved " + pageWideSaving + " of " + pageWide.length);
  }

  // ────────────────────────────────────────────────────────── the reader contract

  @Test
  @DisplayName("every region-only door agrees with the record path, vector and scalar")
  void everyDoorAgreesWithTheRecordPath() {
    for (final boolean vector : new boolean[] {true, false}) {
      BitUnpackSimd.setWarmupRemainingForTesting(vector
          ? 0
          : Integer.MAX_VALUE);
      for (final Column column : new Column[] {mixed(), wide()}) {
        assertDoorsAgree(column, vector);
      }
    }
  }

  private static void assertDoorsAgree(final Column column, final boolean vector) {
    final byte[] wire = encode(column, true);
    final MemorySegment payload = PaxTestSegments.of(wire);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(payload);
    assertEquals(NumberRegion.ENC_PER_TAG_FOR, h.encodingKind);
    assertEquals(column.count, h.count, "count is derived from the per-tag counts");
    final long[] expected = inRegionOrder(column, h);
    final String path = vector
        ? " [vector]"
        : " [scalar]";

    for (int i = 0; i < column.count; i++) {
      assertEquals(expected[i], NumberRegion.decodeValueAt(payload, h, i), "decodeValueAt " + i + path);
    }
    final long[] bulk = new long[column.count];
    NumberRegion.decodeAllValues(payload, h, bulk);
    assertArrayEquals(expected, bulk, "decodeAllValues" + path);

    final long[] aggregate = new long[3];
    final int[] selection = new int[column.count];
    for (int tag = 0; tag < h.dictSize; tag++) {
      final int start = h.tagStart[tag];
      final int end = start + h.tagCount[tag];
      assertEquals(tag, NumberRegion.tagOfIndex(h, start), "tagOfIndex at the tag's first value" + path);
      assertEquals(tag, NumberRegion.tagOfIndex(h, end - 1), "tagOfIndex at the tag's last value" + path);
      for (int i = start; i < end; i++) {
        assertEquals(expected[i], NumberRegion.decodeValueInTag(payload, h, tag, i - start),
            "decodeValueInTag " + i + path);
      }

      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      long sum = 0L;
      for (int i = start; i < end; i++) {
        min = Math.min(min, expected[i]);
        max = Math.max(max, expected[i]);
        sum += expected[i];
      }
      assertEquals(min, h.tagMin[tag], "the zone map's lower bound IS the frame of reference" + path);
      assertEquals(max, h.tagMax[tag], "the zone map's upper bound is min + the stored spread" + path);

      assertTrue(NumberRegionSimd.aggregateRange(payload, h, start, end, aggregate), "aggregate declined" + path);
      assertEquals(sum, aggregate[0], "sum of tag " + tag + path);
      assertEquals(min, aggregate[1], "min of tag " + tag + path);
      assertEquals(max, aggregate[2], "max of tag " + tag + path);

      final long lo = min;
      final long hi = min + ((max - min) >>> 1);
      long wanted = 0;
      int wantedSelection = 0;
      final int[] wantIdx = new int[end - start];
      for (int i = start; i < end; i++) {
        if (expected[i] >= lo && expected[i] <= hi) {
          wanted++;
          wantIdx[wantedSelection++] = i;
        }
      }
      assertEquals(wanted,
          NumberRegionSimd.countMatchingRange(payload, h, start, end, VectorOperators.GE, lo, VectorOperators.LE, hi),
          "range count of tag " + tag + path);
      assertEquals(wanted,
          NumberRegionSimd.countMatching(payload, h, start, end, VectorOperators.LE, hi)
              - NumberRegionSimd.countMatching(payload, h, start, end, VectorOperators.LT, lo),
          "one-sided counts of tag " + tag + path);

      final int produced = NumberRegionSimd.selectMatching(payload, h, start, end, VectorOperators.GE, lo,
          VectorOperators.LE, hi, selection);
      assertEquals(wantedSelection, produced, "selection cardinality of tag " + tag + path);
      for (int k = 0; k < produced; k++) {
        assertEquals(wantIdx[k], selection[k], "selection index " + k + " of tag " + tag + path);
      }

      final int n = end - start;
      final long[] rowBits = new long[(n + 63) >>> 6];
      assertEquals(wantedSelection, NumberRegionSimd.selectRangeInto(payload, h, start, n, VectorOperators.GE, lo,
          VectorOperators.LE, hi, rowBits), "selection bitmap of tag " + tag + path);
      for (int k = 0; k < wantedSelection; k++) {
        final int local = wantIdx[k] - start;
        assertTrue((rowBits[local >>> 6] & (1L << (local & 63))) != 0L, "bitmap bit " + k + path);
      }

      // Masked count: a versioned merge reads a fragment with some values shadowed.
      final long[] live = new long[(n + 63) >>> 6];
      long wantedLive = 0;
      for (int i = 0; i < n; i++) {
        if ((i & 1) == 0) {
          live[i >>> 6] |= 1L << (i & 63);
          if (expected[start + i] >= lo && expected[start + i] <= hi) {
            wantedLive++;
          }
        }
      }
      assertEquals(wantedLive, NumberRegionSimd.countMatchingRangeMasked(payload, h, start, end, VectorOperators.GE, lo,
          VectorOperators.LE, hi, live), "masked count of tag " + tag + path);

      // The prune must agree with the scan wherever it commits to an answer.
      for (final long[] range : new long[][] {{Long.MIN_VALUE, min - 1}, {min, max}, {max + 1, Long.MAX_VALUE},
          {lo, hi}}) {
        final long pruned =
            NumberRegionSimd.pruneCount(h.tagMin[tag], h.tagMax[tag], range[0], range[1], h.tagCount[tag]);
        if (pruned != NumberRegionSimd.PRUNE_UNKNOWN) {
          assertEquals(NumberRegionSimd.countMatchingRange(payload, h, start, end, VectorOperators.GE, range[0],
              VectorOperators.LE, range[1]), pruned, "prune disagreed with the scan for tag " + tag + path);
        }
      }
    }
  }

  @Test
  @DisplayName("a window spanning several tags folds their frames; a masked one declines")
  void windowsSpanningTags() {
    BitUnpackSimd.setWarmupRemainingForTesting(0);
    final Column column = mixed();
    final byte[] wire = encode(column, true);
    final MemorySegment payload = PaxTestSegments.of(wire);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(payload);
    final long[] expected = inRegionOrder(column, h);

    final long lo = 0L;
    final long hi = 100_000L;
    long wanted = 0;
    long sum = 0L;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    final int[] wantIdx = new int[column.count];
    int wantedSelection = 0;
    for (int i = 0; i < column.count; i++) {
      if (expected[i] >= lo && expected[i] <= hi) {
        wanted++;
        wantIdx[wantedSelection++] = i;
      }
      sum += expected[i];
      min = Math.min(min, expected[i]);
      max = Math.max(max, expected[i]);
    }
    assertEquals(wanted, NumberRegionSimd.countMatchingRange(payload, h, 0, column.count, VectorOperators.GE, lo,
        VectorOperators.LE, hi), "a count across every tag folds the per-tag frames");

    final long[] aggregate = new long[3];
    assertTrue(NumberRegionSimd.aggregateRange(payload, h, 0, column.count, aggregate));
    assertEquals(sum, aggregate[0]);
    assertEquals(min, aggregate[1]);
    assertEquals(max, aggregate[2]);
    assertEquals(min, h.valueMin, "the page-global bounds are the fold of the per-tag ones");
    assertEquals(max, h.valueMax);

    final int[] selection = new int[column.count];
    assertEquals(wantedSelection, NumberRegionSimd.selectMatching(payload, h, 0, column.count, VectorOperators.GE, lo,
        VectorOperators.LE, hi, selection), "selection across tags stays ascending and region-absolute");
    for (int k = 0; k < wantedSelection; k++) {
      assertEquals(wantIdx[k], selection[k], "cross-tag selection index " + k);
    }

    // A liveness bitmap is indexed relative to the window's start and its lane groups must not
    // straddle a word, neither of which survives re-basing per tag — so this is declined, loudly,
    // rather than answered against a shifted bitmap.
    final long[] live = new long[(column.count + 63) >>> 6];
    Arrays.fill(live, ~0L);
    assertEquals(-1L, NumberRegionSimd.countMatchingRangeMasked(payload, h, 0, column.count, VectorOperators.GE, lo,
        VectorOperators.LE, hi, live), "a masked count spanning tags must decline");
  }

  // ─────────────────────────────────────────────────────────── width boundaries

  @Test
  @DisplayName("every width the encoder can choose round-trips, the ceiling and the wrap included")
  void widthBoundaries() {
    BitUnpackSimd.setWarmupRemainingForTesting(0);
    final long[][] shapes = {{0L, 0L}, // constant, width 0
        {5L, 6L}, // width 1
        {-3L, 252L}, // width 8, negative base
        {1L, (1L << 55)}, // width 56
        {1L, (1L << 56)}, // spread past the packer's ceiling -> 64
        {Long.MIN_VALUE, Long.MAX_VALUE}, // the spread that wraps the signed range
        {Long.MAX_VALUE - 3, Long.MAX_VALUE}, // width 2 at the top of the range
    };
    for (final long[] shape : shapes) {
      final int n = 40;
      final long[] values = new long[n];
      final int[] tags = new int[n];
      for (int i = 0; i < n; i++) {
        values[i] = (i & 1) == 0
            ? shape[0]
            : shape[1];
        tags[i] = 3;
      }
      final byte[] wire = NumberRegion.encode(values, tags, n, NumberRegion.TAG_KIND_NAME);
      final MemorySegment payload = PaxTestSegments.of(wire);
      final NumberRegion.Header h = new NumberRegion.Header().parseInto(payload);
      final String what = "[" + shape[0] + ", " + shape[1] + "] kind=" + h.encodingKind;
      if (h.encodingKind != NumberRegion.ENC_PER_TAG_FOR) {
        // A shape the older layouts encode at least as well keeps them; nothing to check here.
        continue;
      }
      final int width = h.tagWidth[0] & 0xFF;
      assertTrue(width == 0 || (width >= 1 && width <= BitUnpackSimd.MAX_BIT_WIDTH) || width == 64,
          "width must be 0, packable, or a raw 64: " + width + " for " + what);
      assertEquals(Math.min(shape[0], shape[1]), h.tagMin[0], "tagMin for " + what);
      assertEquals(Math.max(shape[0], shape[1]), h.tagMax[0], "tagMax for " + what);
      for (int i = 0; i < n; i++) {
        assertEquals(values[i], NumberRegion.decodeValueAt(payload, h, i), "value " + i + " of " + what);
      }
      final long[] bulk = new long[n];
      NumberRegion.decodeAllValues(payload, h, bulk);
      assertArrayEquals(values, bulk, "bulk decode of " + what);
      long wanted = 0;
      for (final long v : values) {
        if (v >= shape[0] && v <= shape[1]) {
          wanted++;
        }
      }
      final long counted = NumberRegionSimd.countMatchingRange(payload, h, 0, n, VectorOperators.GE, shape[0],
          VectorOperators.LE, shape[1]);
      assertTrue(counted < 0 || counted == wanted, "count of " + what + " was " + counted);
    }
  }

  @Test
  @DisplayName("a one-value tag, an empty region and a single constant tag all behave")
  void degenerateShapes() {
    final byte[] empty = NumberRegion.encode(new long[0], new int[0], 0);
    final NumberRegion.Header eh = new NumberRegion.Header().parseInto(PaxTestSegments.of(empty));
    assertEquals(0, eh.count);
    assertEquals(0, eh.dictSize);
    assertEquals(-1, NumberRegion.tagOfIndex(eh, 0), "no index exists in an empty region");

    final byte[] one = NumberRegion.encode(new long[] {42L}, new int[] {7}, 1);
    final NumberRegion.Header oh = new NumberRegion.Header().parseInto(PaxTestSegments.of(one));
    assertEquals(1, oh.count);
    assertEquals(42L, NumberRegion.decodeValueAt(PaxTestSegments.of(one), oh, 0));
    assertEquals(42L, oh.tagMin[0]);
    assertEquals(42L, oh.tagMax[0]);
    assertEquals(-1, NumberRegion.tagOfIndex(oh, 1), "one past the end has no tag");

    final long[] values = new long[64];
    final int[] tags = new int[64];
    Arrays.fill(values, -99L);
    Arrays.fill(tags, 5);
    final byte[] constant = NumberRegion.encode(values, tags, values.length);
    final MemorySegment payload = PaxTestSegments.of(constant);
    final NumberRegion.Header ch = new NumberRegion.Header().parseInto(payload);
    if (ch.encodingKind == NumberRegion.ENC_PER_TAG_FOR) {
      assertEquals(0, ch.tagWidth[0], "a constant tag stores no values at all");
      assertEquals(0, ch.valueBytesLength, "and therefore no value bytes");
    }
    final long[] bulk = new long[values.length];
    NumberRegion.decodeAllValues(payload, ch, bulk);
    assertArrayEquals(values, bulk);
    assertEquals(64L, NumberRegionSimd.countMatching(payload, ch, 0, 64, VectorOperators.EQ, -99L),
        "a constant tag is answered from its header");
    assertEquals(0L, NumberRegionSimd.countMatching(payload, ch, 0, 64, VectorOperators.EQ, -98L));
  }

  // ────────────────────────────────────────────────────────────── the zone map

  @Test
  @DisplayName("the varint zone map prunes while the number column stays deferred")
  void zoneMapPrunesWithoutMaterializingTheColumn() {
    final Column wide = wide();
    final byte[] number = encode(wide, true);
    final NumberRegion.Header source = new NumberRegion.Header().parseInto(PaxTestSegments.of(number));
    NumberRegion.setPerTagWidthEnabled(true);
    final byte[] zoneMap = NumberZoneMapRegion.encode(source);
    NumberRegion.clearPerTagWidthOverride();
    assertNotNull(zoneMap);
    assertEquals(NumberZoneMapRegion.VERSION_V2, zoneMap[0]);

    try (RegionTable table = new RegionTable()) {
      table.set(RegionTable.KIND_NUMBER, number);
      table.set(RegionTable.KIND_NUMBER_ZONEMAP, zoneMap);
      final BytesOut<MemorySegment> out = Bytes.elasticHeapByteBuffer();
      table.write(out, true);
      final byte[] wire = out.bytesForRead().toByteArray();

      try (RegionTable back = RegionTable.read(Bytes.wrapForRead(wire),
          RegionTable.maskOf(RegionTable.KIND_NUMBER) | RegionTable.maskOf(RegionTable.KIND_NUMBER_ZONEMAP),
          RegionTable.maskOf(RegionTable.KIND_NUMBER))) {
        assertTrue(back.hasRegion(RegionTable.KIND_NUMBER), "the column must be present, just deferred");
        final int retainedBefore = back.retainedBytes();
        final NumberZoneMapRegion.Header z =
            new NumberZoneMapRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER_ZONEMAP));
        assertNotNull(z, "the summary must be readable while the column is untouched");
        assertEquals(source.dictSize, z.dictSize);
        for (int tag = 0; tag < source.dictSize; tag++) {
          assertEquals(source.dict[tag], z.dict[tag], "dict " + tag);
          assertEquals(source.tagCount[tag], z.tagCount[tag], "tagCount " + tag);
          assertEquals(source.tagMin[tag], z.tagMin[tag], "tagMin " + tag);
          assertEquals(source.tagMax[tag], z.tagMax[tag], "tagMax " + tag);
          assertEquals(tag, NumberZoneMapRegion.lookupTag(z, z.dict[tag]), "lookupTag " + tag);
        }
        assertEquals(source.valueMin, z.valueMin, "the page-global bounds are folded, not stored");
        assertEquals(source.valueMax, z.valueMax);
        assertEquals(0L,
            NumberRegionSimd.pruneCount(z.tagMin[0], z.tagMax[0], Long.MIN_VALUE, z.tagMin[0] - 1L, z.tagCount[0]),
            "an out-of-range predicate must be settled from the summary alone");
        assertEquals(retainedBefore, back.retainedBytes(), "settling it must leave the number column deferred");
      }
    }
  }

  @Test
  @DisplayName("a malformed varint zone map declines rather than pruning against a mixture")
  void malformedZoneMapDeclines() {
    final Column mixed = mixed();
    final byte[] number = encode(mixed, true);
    final NumberRegion.Header source = new NumberRegion.Header().parseInto(PaxTestSegments.of(number));
    NumberRegion.setPerTagWidthEnabled(true);
    final byte[] zoneMap = NumberZoneMapRegion.encode(source);
    NumberRegion.clearPerTagWidthOverride();

    final NumberZoneMapRegion.Header scratch = new NumberZoneMapRegion.Header();
    assertNotNull(scratch.parseInto(PaxTestSegments.of(zoneMap)), "the good payload parses");

    // Truncated mid-entry: the varint runs off the end.
    assertNull(scratch.parseInto(PaxTestSegments.of(Arrays.copyOf(zoneMap, zoneMap.length - 1))));
    assertEquals(0, scratch.dictSize, "a declined parse must leave nothing to prune against");

    // A continuation byte that never terminates.
    final byte[] corrupt = zoneMap.clone();
    for (int i = 2; i < corrupt.length; i++) {
      corrupt[i] = (byte) 0x80;
    }
    assertNull(scratch.parseInto(PaxTestSegments.of(corrupt)));
    assertEquals(0, scratch.dictSize);
  }

  private static void assertNull(final Object value) {
    assertTrue(value == null, "expected null, was " + value);
  }

  // ────────────────────────────────────────────────── the folded directory

  /** Encode the wide fixture, with the directory inside the values or published as the zone map. */
  private static byte[] encodeWide(final boolean directoryIsExternal) {
    final Column wide = wide();
    final NumberRegion.Encoder encoder = new NumberRegion.Encoder(1024);
    final int length =
        encoder.encodeInto(wide.values, wide.tags, wide.count, NumberRegion.TAG_KIND_PATH_NODE, directoryIsExternal);
    return Arrays.copyOf(encoder.output(), length);
  }

  /** The zone map the writer publishes beside {@code encodeWide}'s region. */
  private static byte[] wideDirectory(final boolean directoryIsExternal) {
    final Column wide = wide();
    final NumberRegion.Encoder encoder = new NumberRegion.Encoder(1024);
    encoder.encodeInto(wide.values, wide.tags, wide.count, NumberRegion.TAG_KIND_PATH_NODE, directoryIsExternal);
    final NumberRegion.Header directory = new NumberRegion.Header();
    encoder.directoryInto(directory);
    return NumberZoneMapRegion.encode(directory);
  }

  @Test
  @DisplayName("the per-tag directory is published once, as the zone map, and the values keep only their prefix")
  void theDirectoryIsPublishedOnceAsTheZoneMap() {
    final byte[] inline = encodeWide(false);
    final byte[] folded = encodeWide(true);
    final byte[] directory = wideDirectory(true);

    final MemorySegment inlineSegment = PaxTestSegments.of(inline);
    final MemorySegment foldedSegment = PaxTestSegments.of(folded);
    final NumberRegion.Header ih = new NumberRegion.Header().parseInto(inlineSegment);
    final NumberRegion.Header fh = new NumberRegion.Header().parseInto(foldedSegment, PaxTestSegments.of(directory));

    assertEquals(NumberRegion.ENC_PER_TAG_FOR, ih.encodingKind);
    assertEquals(NumberRegion.ENC_PER_TAG_FOR_EXTERNAL, fh.encodingKind);
    // What leaves the value region is exactly the directory the summary already carried: two bytes
    // of prefix plus the packed runs is all that is left.
    assertEquals(2 + fh.valueBytesLength, folded.length, "the folded region is its prefix and its values");
    assertEquals(inline.length - folded.length, ih.tagValueOffset[0] - fh.tagValueOffset[0],
        "the bytes saved are precisely the directory the values used to repeat");
    assertTrue(inline.length - folded.length > 400,
        "on a 60-tag page that directory is over 400 bytes, was " + (inline.length - folded.length));
    // And the summary is unchanged in size: it was always carrying this.
    assertArrayEquals(wideDirectory(false), directory, "the summary is the same either way");

    assertEquals(ih.count, fh.count);
    assertEquals(ih.dictSize, fh.dictSize);
    assertEquals(ih.tagKind, fh.tagKind);
    assertEquals(ih.valueMin, fh.valueMin);
    assertEquals(ih.valueMax, fh.valueMax);
    for (int tag = 0; tag < fh.dictSize; tag++) {
      assertEquals(ih.dict[tag], fh.dict[tag], "dict " + tag);
      assertEquals(ih.tagStart[tag], fh.tagStart[tag], "tagStart " + tag);
      assertEquals(ih.tagCount[tag], fh.tagCount[tag], "tagCount " + tag);
      assertEquals(ih.tagMin[tag], fh.tagMin[tag], "tagMin " + tag);
      assertEquals(ih.tagMax[tag], fh.tagMax[tag], "tagMax " + tag);
      assertEquals(ih.tagWidth[tag], fh.tagWidth[tag], "width " + tag);
      assertEquals(ih.tagDecodeBase[tag], fh.tagDecodeBase[tag], "base " + tag);
    }
  }

  @Test
  @DisplayName("every door reads the folded region exactly as it reads the self-contained one")
  void everyDoorAgreesAcrossTheFold() {
    BitUnpackSimd.setWarmupRemainingForTesting(0);
    final MemorySegment inline = PaxTestSegments.of(encodeWide(false));
    final MemorySegment folded = PaxTestSegments.of(encodeWide(true));
    final NumberRegion.Header ih = new NumberRegion.Header().parseInto(inline);
    final NumberRegion.Header fh = new NumberRegion.Header().parseInto(folded, PaxTestSegments.of(wideDirectory(true)));

    final long[] inlineAll = new long[ih.count];
    final long[] foldedAll = new long[fh.count];
    NumberRegion.decodeAllValues(inline, ih, inlineAll);
    NumberRegion.decodeAllValues(folded, fh, foldedAll);
    assertArrayEquals(inlineAll, foldedAll, "bulk decode");

    final long[] inlineAggregate = new long[3];
    final long[] foldedAggregate = new long[3];
    final int[] inlineSelection = new int[ih.count];
    final int[] foldedSelection = new int[fh.count];
    for (int tag = 0; tag < fh.dictSize; tag++) {
      final int start = fh.tagStart[tag];
      final int end = start + fh.tagCount[tag];
      for (int i = start; i < end; i++) {
        assertEquals(NumberRegion.decodeValueAt(inline, ih, i), NumberRegion.decodeValueAt(folded, fh, i),
            "value " + i);
        assertEquals(NumberRegion.decodeValueInTag(inline, ih, tag, i - start),
            NumberRegion.decodeValueInTag(folded, fh, tag, i - start), "per-tag value " + i);
      }
      final long lo = fh.tagMin[tag];
      final long hi = lo + ((fh.tagMax[tag] - lo) >>> 1);
      assertEquals(
          NumberRegionSimd.countMatchingRange(inline, ih, start, end, VectorOperators.GE, lo, VectorOperators.LE, hi),
          NumberRegionSimd.countMatchingRange(folded, fh, start, end, VectorOperators.GE, lo, VectorOperators.LE, hi),
          "count of tag " + tag);
      assertTrue(NumberRegionSimd.aggregateRange(inline, ih, start, end, inlineAggregate));
      assertTrue(NumberRegionSimd.aggregateRange(folded, fh, start, end, foldedAggregate));
      assertArrayEquals(inlineAggregate, foldedAggregate, "aggregate of tag " + tag);
      final int inlineHits = NumberRegionSimd.selectMatching(inline, ih, start, end, VectorOperators.GE, lo,
          VectorOperators.LE, hi, inlineSelection);
      final int foldedHits = NumberRegionSimd.selectMatching(folded, fh, start, end, VectorOperators.GE, lo,
          VectorOperators.LE, hi, foldedSelection);
      assertEquals(inlineHits, foldedHits, "selection cardinality of tag " + tag);
      for (int k = 0; k < foldedHits; k++) {
        assertEquals(inlineSelection[k], foldedSelection[k], "selection index " + k);
      }
    }
  }

  @Test
  @DisplayName("the summary alone settles a prune, and a request for the values brings it along")
  void theSummaryPrunesAndTravelsWithItsColumn() {
    final byte[] folded = encodeWide(true);
    final byte[] directory = wideDirectory(true);
    final NumberRegion.Header fh =
        new NumberRegion.Header().parseInto(PaxTestSegments.of(folded), PaxTestSegments.of(directory));

    final NumberZoneMapRegion.Header z = new NumberZoneMapRegion.Header().parseInto(PaxTestSegments.of(directory));
    assertNotNull(z);
    for (int tag = 0; tag < fh.dictSize; tag++) {
      assertEquals(tag, NumberZoneMapRegion.lookupTag(z, fh.dict[tag]), "lookupTag " + tag);
      assertEquals(fh.tagMin[tag], z.tagMin[tag], "tagMin " + tag);
      assertEquals(fh.tagMax[tag], z.tagMax[tag], "tagMax " + tag);
      assertEquals(fh.tagCount[tag], z.tagCount[tag], "tagCount " + tag);
      // Settled from the summary alone: no byte of the value region is read to answer this.
      assertEquals(0L, NumberRegionSimd.pruneCount(z.tagMin[tag], z.tagMax[tag], Long.MIN_VALUE, z.tagMin[tag] - 1L,
          z.tagCount[tag]), "prune of tag " + tag);
    }

    // A reader that asks for the values gets the directory too — the pairing is the reader's rule,
    // not something each call site has to remember.
    try (RegionTable table = new RegionTable()) {
      table.set(RegionTable.KIND_NUMBER, folded);
      table.set(RegionTable.KIND_NUMBER_ZONEMAP, directory);
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      table.write(sink, true);
      try (RegionTable back = RegionTable.read(sink.bytesForRead(), RegionTable.maskOf(RegionTable.KIND_NUMBER))) {
        assertNotNull(back.payload(RegionTable.KIND_NUMBER_ZONEMAP), "the directory travels with its column");
        final NumberRegion.Header read = new NumberRegion.Header().parseInto(back.payload(RegionTable.KIND_NUMBER),
            back.payload(RegionTable.KIND_NUMBER_ZONEMAP));
        assertEquals(fh.count, read.count);
        assertEquals(fh.dictSize, read.dictSize);
      }
    }
  }

  @Test
  @DisplayName("reading folded values without their directory is refused, not guessed at")
  void readingTheValuesWithoutTheirDirectoryIsRefused() {
    final MemorySegment folded = PaxTestSegments.of(encodeWide(true));
    final MemorySegment inline = PaxTestSegments.of(encodeWide(false));
    assertTrue(NumberRegion.needsExternalDirectory(folded));
    assertFalse(NumberRegion.needsExternalDirectory(inline));
    // Silently decoding packed bytes against a directory the reader does not have would be a page of
    // plausible wrong numbers; refusing is the only safe reading of it.
    assertThrows(IllegalArgumentException.class, () -> new NumberRegion.Header().parseInto(folded));
    assertThrows(IllegalArgumentException.class, () -> new NumberRegion.Header().parseInto(folded, null));

    // A directory that describes a different column is refused too: the two regions must agree about
    // what their tags mean, or a prune and a scan would read different columns.
    final NumberRegion.Encoder encoder = new NumberRegion.Encoder(64);
    final Column wide = wide();
    encoder.encodeInto(wide.values, wide.tags, wide.count, NumberRegion.TAG_KIND_NAME, true);
    final NumberRegion.Header nameTagged = new NumberRegion.Header();
    encoder.directoryInto(nameTagged);
    final byte[] mismatched = NumberZoneMapRegion.encode(nameTagged);
    assertThrows(IllegalArgumentException.class,
        () -> new NumberRegion.Header().parseInto(folded, PaxTestSegments.of(mismatched)));
  }

  @Test
  @DisplayName("the fold's kill switch keeps the directory inside the values, byte for byte")
  void foldKillSwitchKeepsTheDirectoryInline() {
    final byte[] inline = encodeWide(false);
    NumberRegion.setExternalHeaderEnabled(false);
    try {
      // Even asked to fold, the encoder writes the self-contained form — which is also the fallback
      // a writer that cannot publish a summary takes, so a page is never undecodable.
      assertArrayEquals(inline, encodeWide(true), "the kill switch must reproduce the self-contained bytes");
      final NumberRegion.Header h = new NumberRegion.Header().parseInto(PaxTestSegments.of(encodeWide(true)));
      assertEquals(NumberRegion.ENC_PER_TAG_FOR, h.encodingKind);
    } finally {
      NumberRegion.clearExternalHeaderOverride();
    }
  }

  @Test
  @DisplayName("varints are canonical, so a header advances by arithmetic instead of a second scan")
  void varintsAreCanonical() {
    for (final long value : new long[] {0L, 1L, 127L, 128L, 16383L, 16384L, Integer.MAX_VALUE, Long.MAX_VALUE, -1L,
        Long.MIN_VALUE}) {
      final byte[] out = new byte[VarInt.MAX_BYTES];
      final int end = VarInt.writeUnsigned(out, 0, value);
      assertEquals(VarInt.sizeOfUnsigned(value), end, "sizeOfUnsigned must be what writeUnsigned writes: " + value);
      assertEquals(value, VarInt.readUnsigned(PaxTestSegments.of(Arrays.copyOf(out, end)), 0), "round trip " + value);

      final int signedEnd = VarInt.writeSigned(out, 0, value);
      assertEquals(VarInt.sizeOfSigned(value), signedEnd, "sizeOfSigned must be what writeSigned writes: " + value);
      assertEquals(value, VarInt.readSigned(PaxTestSegments.of(Arrays.copyOf(out, signedEnd)), 0),
          "signed round trip " + value);
    }
    // A small value padded into two bytes decodes to the same number but must be refused, or the
    // cursor and the value would disagree about how far to advance.
    assertFalse(canRead(new byte[] {(byte) 0x80, 0x00}), "a non-canonical zero must be refused");
    assertFalse(canRead(new byte[] {(byte) 0x81, 0x00}), "a non-canonical one must be refused");
    assertTrue(canRead(new byte[] {0x00}), "the canonical zero must be accepted");
  }

  private static boolean canRead(final byte[] bytes) {
    try {
      VarInt.readUnsigned(PaxTestSegments.of(bytes), 0);
      return true;
    } catch (final IllegalArgumentException refused) {
      return false;
    }
  }
}
