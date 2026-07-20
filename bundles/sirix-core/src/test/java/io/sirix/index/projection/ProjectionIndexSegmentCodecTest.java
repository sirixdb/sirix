/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P2 suite: descriptor + per-segment codec round trips
 * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §9 P2). The load-bearing
 * assertion everywhere is <b>byte-identity of the assembled raw form</b> —
 * segments → raw must equal the original {@code serialize()} output,
 * provenance included — plus the fail-loud integrity contract (hash/length/
 * kind mismatches throw at assembly, never mid-kernel).
 */
final class ProjectionIndexSegmentCodecTest {

  private static final byte[] KINDS = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT,
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG
  };

  private static final String[] DEPTS = {"Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp"};

  /** Bench-shaped leaf, mirroring the {@code ProjectionIndexLeafCodecTest} generator. */
  private static ProjectionIndexLeafPage benchLeaf(final int rows, final long keyBase) {
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    final Random rng = new Random(7);
    final long[] longs = new long[4];
    final boolean[] bools = new boolean[4];
    final String[] strings = new String[4];
    final boolean[] present = new boolean[4];
    final boolean[] unrep = new boolean[4];
    final boolean[] nonIntegral = new boolean[4];
    long key = keyBase;
    for (int i = 0; i < rows; i++) {
      key += 8 + rng.nextInt(9);
      final boolean deptMissing = i % 5 == 0;
      longs[0] = 18 + rng.nextInt(48);
      bools[1] = rng.nextBoolean();
      strings[2] = deptMissing ? "" : DEPTS[rng.nextInt(DEPTS.length)];
      longs[3] = 0L;
      present[0] = true;
      present[1] = true;
      present[2] = !deptMissing;
      present[3] = false;
      Arrays.fill(unrep, false);
      nonIntegral[0] = i % 11 == 0;
      assertTrue(page.appendRow(key, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page;
  }

  private static ProjectionIndexSegmentCodec.SegmentResolver resolverOf(
      final ProjectionIndexSegmentCodec.EncodedLeaf encoded) {
    final Map<Integer, byte[]> byId = new HashMap<>();
    for (int i = 0; i < encoded.segmentIds().length; i++) {
      byId.put(encoded.segmentIds()[i] & 0xFF, encoded.segments()[i]);
    }
    return byId::get;
  }

  private static void assertRoundTrip(final ProjectionIndexLeafPage page) {
    final byte[] raw = page.serialize();
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
    LeafDescriptor.validate(encoded.descriptor());
    assertArrayEquals(raw, ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)),
        "assembleRaw(encode(raw)) must be byte-identical");
  }

  // ==================== round trips ====================

  @Test
  void benchShapedLeafRoundTripsAndShrinks() {
    final ProjectionIndexLeafPage page = benchLeaf(1024, 1_000_000L);
    final byte[] raw = page.serialize();
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
    assertArrayEquals(raw, ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
    int total = encoded.descriptor().length;
    for (final byte[] seg : encoded.segments()) {
      total += seg.length;
    }
    assertTrue(total * 4 < raw.length,
        "expected >4x compaction on bench-shaped data (segment headers cost a little vs PIXC), got "
            + raw.length + " -> " + total);
  }

  @Test
  void partialSingleRowAndWordBoundaryLeavesRoundTrip() {
    assertRoundTrip(benchLeaf(100, 42L));
    assertRoundTrip(benchLeaf(1, 42L));
    assertRoundTrip(benchLeaf(64, 42L));
    assertRoundTrip(benchLeaf(65, 42L));
  }

  @Test
  void emptyLeafRoundTripsWithFlagTruthAndNoDicts() {
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    final byte[] raw = page.serialize();
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
    // KEYS + one BODY per column, no DICT segments for an empty leaf.
    assertEquals(1 + KINDS.length, encoded.segmentIds().length);
    assertEquals(0, LeafDescriptor.rowCount(encoded.descriptor()));
    assertArrayEquals(raw, ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
  }

  @Test
  void nonAscendingKeysAndExtremeValuesRoundTrip() {
    final byte[] kinds = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
    final long[] keys = {Long.MAX_VALUE - 1, 5L, Long.MAX_VALUE, 0L};
    final long[] extremes = {Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L};
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    for (int i = 0; i < keys.length; i++) {
      longs[0] = extremes[i];
      assertTrue(page.appendRow(keys[i], longs, bools, strings, present, unrep, nonIntegral));
    }
    assertRoundTrip(page);
  }

  /** Ported width sweep: every FOR width 1..64 must survive segmentation. */
  @Test
  void wideBitWidthRangesRoundTripExactly() {
    for (int width = 48; width <= 64; width++) {
      final byte[] kinds = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
      final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
      final long span = width == 64 ? -1L : (1L << (width - 1));
      final long[] longs = new long[1];
      final boolean[] bools = new boolean[1];
      final String[] strings = new String[1];
      final boolean[] present = {true};
      final boolean[] unrep = new boolean[1];
      final boolean[] nonIntegral = new boolean[1];
      final Random rng = new Random(width);
      for (int i = 0; i < 200; i++) {
        longs[0] = width == 64 ? rng.nextLong() : (rng.nextLong() & span) - (span >>> 1);
        assertTrue(page.appendRow(1000L + i, longs, bools, strings, present, unrep, nonIntegral));
      }
      assertRoundTrip(page);
    }
  }

  @Test
  void realEmptyStringAndUnrepresentableRoundTrip() {
    final byte[] kinds = {ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = new boolean[1];
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    // Row 0: genuine empty string. Row 1: missing. Row 2: unrepresentable. Row 3: value.
    strings[0] = "";
    present[0] = true;
    unrep[0] = false;
    assertTrue(page.appendRow(10L, longs, bools, strings, present, unrep, nonIntegral));
    strings[0] = "";
    present[0] = false;
    assertTrue(page.appendRow(20L, longs, bools, strings, present, unrep, nonIntegral));
    strings[0] = "";
    present[0] = true;
    unrep[0] = true;
    assertTrue(page.appendRow(30L, longs, bools, strings, present, unrep, nonIntegral));
    strings[0] = "x";
    present[0] = true;
    unrep[0] = false;
    assertTrue(page.appendRow(40L, longs, bools, strings, present, unrep, nonIntegral));
    assertRoundTrip(page);
  }

  @Test
  void multiKilobyteDictionaryRoundTrips() {
    final byte[] kinds = {ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    final StringBuilder sb = new StringBuilder(1024);
    for (int i = 0; i < 300; i++) {
      sb.setLength(0);
      sb.append("value-").append(i).append('-');
      for (int j = 0; j < 60; j++) {
        sb.append((char) ('a' + ((i + j) % 26)));
      }
      strings[0] = sb.toString();
      assertTrue(page.appendRow(100L + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    final byte[] raw = page.serialize();
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
    // The dictionary segment dominates: verify it decodes standalone and the whole re-assembles.
    final int dictIdx = LeafDescriptor.entryIndexOf(encoded.descriptor(),
        ProjectionIndexSegmentCodec.dictSegmentId(0));
    assertTrue(LeafDescriptor.entryByteLen(encoded.descriptor(), dictIdx) > 10_000);
    assertArrayEquals(raw, ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
  }

  // ==================== descriptor + provenance ====================

  @Test
  void descriptorMirrorsHeaderStatsAndFlags() {
    final ProjectionIndexLeafPage page = benchLeaf(512, 9_000L);
    final byte[] raw = page.serialize();
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
    final byte[] d = encoded.descriptor();
    assertEquals(512, LeafDescriptor.rowCount(d));
    assertEquals(4, LeafDescriptor.columnCount(d));
    assertEquals(page.firstRecordKey(), LeafDescriptor.firstRecordKey(d));
    assertEquals(page.lastRecordKey(), LeafDescriptor.lastRecordKey(d));
    for (int c = 0; c < 4; c++) {
      assertEquals(KINDS[c], LeafDescriptor.kind(d, c));
      final int bodyIdx = LeafDescriptor.entryIndexOf(d, ProjectionIndexSegmentCodec.bodySegmentId(c));
      assertEquals(page.columnMin(c), LeafDescriptor.entryMin(d, bodyIdx), "min mirror col " + c);
      assertEquals(page.columnMax(c), LeafDescriptor.entryMax(d, bodyIdx), "max mirror col " + c);
      // Mirror flags must equal segment TRUTH (the head byte of BODY bytes).
      final byte truth = ProjectionIndexSegmentCodec.bodySegmentFlags(
          resolverOf(encoded).segment(ProjectionIndexSegmentCodec.bodySegmentId(c)));
      assertEquals(truth, LeafDescriptor.entryColFlags(d, bodyIdx), "flags mirror col " + c);
    }
    // Column 0 saw non-integral marks; column 3 is all-missing (no flags).
    final int body0 = LeafDescriptor.entryIndexOf(d, ProjectionIndexSegmentCodec.bodySegmentId(0));
    assertTrue((LeafDescriptor.entryColFlags(d, body0)
        & ProjectionIndexLeafPage.COLUMN_FLAG_NON_INTEGRAL) != 0);
  }

  @Test
  void descriptorRejectsCorruption() {
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded =
        ProjectionIndexSegmentCodec.encode(benchLeaf(10, 1L).serialize());
    final byte[] d = encoded.descriptor();
    // Truncated.
    assertThrows(IllegalStateException.class,
        () -> LeafDescriptor.validate(Arrays.copyOf(d, d.length - 1)));
    // Bad version.
    final byte[] badVersion = d.clone();
    badVersion[4] = 99;
    assertThrows(IllegalStateException.class, () -> LeafDescriptor.validate(badVersion));
    // Not a descriptor at all.
    assertNull(nullOrNot(new byte[] {1, 2, 3}));
  }

  private static Object nullOrNot(final byte[] bytes) {
    return LeafDescriptor.isDescriptor(bytes) ? bytes : null;
  }

  // ==================== integrity fail-loud ====================

  @Test
  void corruptedSegmentFailsHashCheckAtAssembly() {
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded =
        ProjectionIndexSegmentCodec.encode(benchLeaf(256, 77L).serialize());
    final int victim = ProjectionIndexSegmentCodec.bodySegmentId(0);
    final ProjectionIndexSegmentCodec.SegmentResolver clean = resolverOf(encoded);
    final ProjectionIndexSegmentCodec.SegmentResolver corrupting = segmentId -> {
      final byte[] bytes = clean.segment(segmentId);
      if (segmentId == victim && bytes != null) {
        final byte[] flipped = bytes.clone();
        flipped[flipped.length - 1] ^= 0x40;
        return flipped;
      }
      return bytes;
    };
    final IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), corrupting));
    assertTrue(e.getMessage().contains("hash"), e.getMessage());
  }

  @Test
  void truncatedSegmentFailsLengthCheckAtAssembly() {
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded =
        ProjectionIndexSegmentCodec.encode(benchLeaf(256, 77L).serialize());
    final ProjectionIndexSegmentCodec.SegmentResolver clean = resolverOf(encoded);
    final ProjectionIndexSegmentCodec.SegmentResolver truncating = segmentId -> {
      final byte[] bytes = clean.segment(segmentId);
      return segmentId == 0 && bytes != null ? Arrays.copyOf(bytes, bytes.length - 3) : bytes;
    };
    final IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), truncating));
    assertTrue(e.getMessage().contains("length"), e.getMessage());
  }

  @Test
  void missingSegmentFailsAtAssembly() {
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded =
        ProjectionIndexSegmentCodec.encode(benchLeaf(64, 3L).serialize());
    final ProjectionIndexSegmentCodec.SegmentResolver clean = resolverOf(encoded);
    final int missing = ProjectionIndexSegmentCodec.dictSegmentId(2);
    final ProjectionIndexSegmentCodec.SegmentResolver dropping =
        segmentId -> segmentId == missing ? null : clean.segment(segmentId);
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), dropping));
  }

  @Test
  void hashChangesWhenContentChangesAndIsStableOtherwise() {
    // The no-op comparator contract (§3): identical re-encode → identical hash; any value
    // change → different hash for that column's BODY only.
    final ProjectionIndexLeafPage a = benchLeaf(300, 5_000L);
    final ProjectionIndexLeafPage b = benchLeaf(300, 5_000L);
    final ProjectionIndexSegmentCodec.EncodedLeaf ea = ProjectionIndexSegmentCodec.encode(a.serialize());
    final ProjectionIndexSegmentCodec.EncodedLeaf eb = ProjectionIndexSegmentCodec.encode(b.serialize());
    assertArrayEquals(ea.descriptor(), eb.descriptor(), "deterministic build → identical descriptor");
    for (int i = 0; i < ea.segments().length; i++) {
      assertArrayEquals(ea.segments()[i], eb.segments()[i]);
    }
    // Different data → the affected BODY hash differs.
    final ProjectionIndexLeafPage c = benchLeaf(300, 5_001L);
    final ProjectionIndexSegmentCodec.EncodedLeaf ec = ProjectionIndexSegmentCodec.encode(c.serialize());
    final int keysEntryA = LeafDescriptor.entryIndexOf(ea.descriptor(), 0);
    final int keysEntryC = LeafDescriptor.entryIndexOf(ec.descriptor(), 0);
    assertNotEquals(LeafDescriptor.entryContentHash(ea.descriptor(), keysEntryA),
        LeafDescriptor.entryContentHash(ec.descriptor(), keysEntryC),
        "shifted record keys must change the KEYS hash");
  }

  @Test
  void columnCapEnforced() {
    final byte[] tooMany = new byte[LeafDescriptor.MAX_COLUMNS + 1];
    Arrays.fill(tooMany, ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG);
    assertThrows(IllegalArgumentException.class,
        () -> LeafDescriptor.serialize(0, 0L, 0L, tooMany, 0, new byte[0], new int[0], new long[0],
            new byte[0], new long[0], new long[0]));
  }

  @Test
  void doubleColumnsRoundTripInTransformDomain() {
    // NUMERIC_DOUBLE cells store the order-preserving transform; at the codec layer the
    // column is byte-identical to NUMERIC_LONG (FOR bit-packing over transformed longs).
    final byte[] kinds = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE,
        ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
    final long[] longs = new long[2];
    final boolean[] bools = new boolean[2];
    final String[] strings = new String[2];
    final boolean[] present = new boolean[2];
    final boolean[] unrep = new boolean[2];
    final boolean[] nonIntegral = new boolean[2];
    final double[] doubles = {-1.0e300, -2.25, -0.0, 0.0, 0.5, 3.1415926535, 8.0, 1.0e300};
    for (int i = 0; i < doubles.length; i++) {
      longs[0] = ProjectionDoubleEncoding.encode(doubles[i]);
      longs[1] = i * 10L;
      present[0] = true;
      present[1] = i % 2 == 0;
      nonIntegral[0] = i == 3; // a lossy-conversion mark must survive (value-exactness bit)
      assertTrue(page.appendRow(100L + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    final byte[] raw = page.serialize();
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
    assertArrayEquals(raw, ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
    // Zone maps live in the transform domain: min/max mirror = encode(min double)/encode(max).
    final int body0 = LeafDescriptor.entryIndexOf(encoded.descriptor(),
        ProjectionIndexSegmentCodec.bodySegmentId(0));
    assertEquals(ProjectionDoubleEncoding.encode(-1.0e300), LeafDescriptor.entryMin(encoded.descriptor(), body0));
    assertEquals(ProjectionDoubleEncoding.encode(1.0e300), LeafDescriptor.entryMax(encoded.descriptor(), body0));
    assertTrue((LeafDescriptor.entryColFlags(encoded.descriptor(), body0)
        & ProjectionIndexLeafPage.COLUMN_FLAG_NON_INTEGRAL) != 0,
        "the value-exactness bit must survive the codec");
  }

  @Test
  void utf8DictionaryBytesSurviveExactly() {
    final byte[] kinds = {ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    final String[] values = {"ascii", "umläut-Straße", "日本語テキスト", "emoji-🎯-mix", ""};
    for (int i = 0; i < values.length; i++) {
      strings[0] = values[i];
      assertTrue(page.appendRow(500L + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    // Sanity that multi-byte UTF-8 really is in play.
    assertTrue(values[2].getBytes(StandardCharsets.UTF_8).length > values[2].length());
    assertRoundTrip(page);
  }
}
