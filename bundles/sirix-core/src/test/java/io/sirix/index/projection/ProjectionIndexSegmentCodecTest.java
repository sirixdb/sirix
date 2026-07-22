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
    // Post-P7 the repetitive multi-KB dictionary FSST-compresses; the segment must still be
    // substantial (hundreds of entries) but far below the ~20KB raw dictionary bytes.
    assertTrue(LeafDescriptor.entryByteLen(encoded.descriptor(), dictIdx) > 1_000,
        "dict segment implausibly small: " + LeafDescriptor.entryByteLen(encoded.descriptor(), dictIdx));
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
    // A "missing segment" is only meaningful for a REFERENCED segment (an inline segment's bytes
    // live in the descriptor and cannot go missing) — force all-referenced so the dropped DICT is
    // genuinely resolved through the page resolver.
    final int savedMax = ProjectionIndexSegmentCodec.inlineMaxSegmentBytes;
    ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = 0;
    try {
      final ProjectionIndexSegmentCodec.EncodedLeaf encoded =
          ProjectionIndexSegmentCodec.encode(benchLeaf(64, 3L).serialize());
      final ProjectionIndexSegmentCodec.SegmentResolver clean = resolverOf(encoded);
      final int missing = ProjectionIndexSegmentCodec.dictSegmentId(2);
      final ProjectionIndexSegmentCodec.SegmentResolver dropping =
          segmentId -> segmentId == missing ? null : clean.segment(segmentId);
      assertThrows(IllegalStateException.class,
          () -> ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), dropping));
    } finally {
      ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = savedMax;
    }
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

  /** Build a single-string-column leaf whose dictionary holds {@code values}. */
  private static ProjectionIndexLeafPage stringLeaf(final String[] values) {
    final byte[] kinds = {ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    for (int i = 0; i < values.length; i++) {
      strings[0] = values[i];
      assertTrue(page.appendRow(1000L + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page;
  }

  @Test
  void fsstCompressedDictionaryRoundTripsAndShrinks() {
    // High-cardinality repetitive-prefix dictionary — the FSST target shape (P7, doc §2.7).
    final String[] values = new String[600];
    for (int i = 0; i < values.length; i++) {
      values[i] = "https://sirix.example.com/api/v1/resources/customer-records/region-europe/"
          + "tenant-" + (i % 37) + "/entity-" + i;
    }
    final ProjectionIndexLeafPage page = stringLeaf(values);
    final byte[] raw = page.serialize();
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
    // Byte-identity is the load-bearing contract — FSST must be invisible above the codec.
    assertArrayEquals(raw, ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
    // And it must actually compress: the DICT segment holds ~48KB of URLs.
    final int dictIdx = LeafDescriptor.entryIndexOf(encoded.descriptor(),
        ProjectionIndexSegmentCodec.dictSegmentId(0));
    final int dictLen = LeafDescriptor.entryByteLen(encoded.descriptor(), dictIdx);
    int rawDictBytes = 0;
    for (final String v : values) {
      rawDictBytes += v.getBytes(StandardCharsets.UTF_8).length;
    }
    assertTrue(dictLen * 2 < rawDictBytes,
        "FSST should compress repetitive URLs >2x, got " + rawDictBytes + " -> " + dictLen);
  }

  @Test
  void fsstEncodingIsDeterministicAcrossIdenticalReencodes() {
    // The write-path no-op comparator hashes segment bytes: identical dictionaries must
    // encode to identical bytes (deterministic training over interning order) — 5.2-n.
    final String[] values = new String[300];
    for (int i = 0; i < values.length; i++) {
      values[i] = "prefix-common-part-shared/suffix-" + i + "/tail-" + (i % 7);
    }
    final ProjectionIndexSegmentCodec.EncodedLeaf a =
        ProjectionIndexSegmentCodec.encode(stringLeaf(values).serialize());
    final ProjectionIndexSegmentCodec.EncodedLeaf b =
        ProjectionIndexSegmentCodec.encode(stringLeaf(values).serialize());
    assertArrayEquals(a.descriptor(), b.descriptor());
    for (int i = 0; i < a.segments().length; i++) {
      assertArrayEquals(a.segments()[i], b.segments()[i]);
    }
  }

  @Test
  void escapeHeavyAndSmallDictionariesTakeTheRawPathAndRoundTrip() {
    // Escape-heavy: bytes ≥ 0x80 and 0xFF everywhere — FSST's escape coding worst case; the
    // beneficial-gate must refuse and fall back to RAW, and either way bytes round-trip.
    final String[] hostile = new String[80];
    for (int i = 0; i < hostile.length; i++) {
      hostile[i] = "\u00ff\u00fe\u30c6\u30b9\u30c8-" + i + "-\u00ff\u00ff";
    }
    assertRoundTrip(stringLeaf(hostile));
    // Small dictionary: below the table gates — RAW mode, still byte-identical.
    assertRoundTrip(stringLeaf(new String[] {"a", "b", "c"}));
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

  // ==================== hybrid inline / referenced storage ====================

  /** Serves ONLY referenced segments; returns {@code null} for inline ids so a test proves the
   *  inline bytes were self-resolved from the descriptor, never fetched as a page. */
  private static ProjectionIndexSegmentCodec.SegmentResolver referencedOnlyResolver(
      final ProjectionIndexSegmentCodec.EncodedLeaf encoded) {
    final byte[] d = encoded.descriptor();
    final Map<Integer, byte[]> byId = new HashMap<>();
    for (int i = 0; i < encoded.segmentIds().length; i++) {
      final int segId = encoded.segmentIds()[i] & 0xFF;
      final int entry = LeafDescriptor.entryIndexOf(d, segId);
      if (entry >= 0 && !LeafDescriptor.entryIsInline(d, entry)) {
        byId.put(segId, encoded.segments()[i]);
      }
    }
    return byId::get;
  }

  private static int inlineCount(final byte[] descriptor) {
    int n = 0;
    final int segCount = LeafDescriptor.segCount(descriptor);
    for (int i = 0; i < segCount; i++) {
      if (LeafDescriptor.entryIsInline(descriptor, i)) {
        n++;
      }
    }
    return n;
  }

  @Test
  void allInlineLeafAssemblesWithoutAnyResolver() {
    // Force every segment inline, then assemble with a resolver that can serve NOTHING — the raw
    // form must still reconstruct byte-identically purely from the descriptor's inline region.
    final int savedMax = ProjectionIndexSegmentCodec.inlineMaxSegmentBytes;
    final int savedTot = ProjectionIndexSegmentCodec.inlineMaxTotalBytes;
    ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = 1 << 20;
    ProjectionIndexSegmentCodec.inlineMaxTotalBytes = 1 << 24;
    try {
      final byte[] raw = benchLeaf(200, 9L).serialize();
      final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
      LeafDescriptor.validate(encoded.descriptor());
      assertEquals(LeafDescriptor.segCount(encoded.descriptor()), inlineCount(encoded.descriptor()),
          "every segment should be inline under an unbounded threshold");
      assertArrayEquals(raw,
          ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), segmentId -> null),
          "all-inline leaf must assemble with no page resolver");
    } finally {
      ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = savedMax;
      ProjectionIndexSegmentCodec.inlineMaxTotalBytes = savedTot;
    }
  }

  @Test
  void inlineAndReferencedMixSelfResolvesInlineFromDescriptor() {
    // A full 1024-row leaf yields both tiny (inline) and large (referenced) segments under the
    // default thresholds. A resolver that refuses to serve inline ids must still assemble the raw
    // form byte-identically — proving inline bytes come from the descriptor, referenced from pages.
    final byte[] raw = benchLeaf(1024, 1_000L).serialize();
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
    final int inlined = inlineCount(encoded.descriptor());
    final int total = LeafDescriptor.segCount(encoded.descriptor());
    assertTrue(inlined > 0 && inlined < total,
        "expected a mix of inline and referenced segments, got " + inlined + "/" + total);
    assertArrayEquals(raw,
        ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), referencedOnlyResolver(encoded)));
  }

  @Test
  void referencedOnlyModeProducesNoInlineRegion() {
    final int savedMax = ProjectionIndexSegmentCodec.inlineMaxSegmentBytes;
    ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = 0;
    try {
      final byte[] raw = benchLeaf(300, 5L).serialize();
      final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
      assertEquals(0, inlineCount(encoded.descriptor()), "threshold 0 → no inline entries");
      // No inline region → the descriptor is exactly the entry table (the pre-hybrid length rule).
      LeafDescriptor.validate(encoded.descriptor());
      assertArrayEquals(raw,
          ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), resolverOf(encoded)));
    } finally {
      ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = savedMax;
    }
  }

  @Test
  void classifyInlineIsSmallestFirstUnderBudget() {
    // Eligibility = byteLen <= 192; over-threshold entries never inline regardless of budget.
    final int savedMax = ProjectionIndexSegmentCodec.inlineMaxSegmentBytes;
    final int savedTot = ProjectionIndexSegmentCodec.inlineMaxTotalBytes;
    ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = 192;
    ProjectionIndexSegmentCodec.inlineMaxTotalBytes = 512;
    try {
      final boolean[] r = ProjectionIndexSegmentCodec.classifyInline(new int[] {100, 300, 50, 150, 200}, 5);
      assertArrayEquals(new boolean[] {true, false, true, true, false}, r,
          "300 and 200 exceed the per-segment ceiling; 100+50+150=300 fits the budget");
      // Budget spill: six 100-byte eligible segments, budget 512 → smallest-first inlines five.
      final boolean[] spill = ProjectionIndexSegmentCodec.classifyInline(new int[] {100, 100, 100, 100, 100, 100}, 6);
      assertArrayEquals(new boolean[] {true, true, true, true, true, false}, spill);
      // Escape hatch: threshold 0 → nothing inline.
      ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = 0;
      assertArrayEquals(new boolean[] {false, false}, ProjectionIndexSegmentCodec.classifyInline(new int[] {1, 2}, 2));
    } finally {
      ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = savedMax;
      ProjectionIndexSegmentCodec.inlineMaxTotalBytes = savedTot;
    }
  }

  @Test
  void corruptInlineSegmentBytesFailAtAssembly() {
    final int savedMax = ProjectionIndexSegmentCodec.inlineMaxSegmentBytes;
    final int savedTot = ProjectionIndexSegmentCodec.inlineMaxTotalBytes;
    ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = 1 << 20;
    ProjectionIndexSegmentCodec.inlineMaxTotalBytes = 1 << 24;
    try {
      final ProjectionIndexSegmentCodec.EncodedLeaf encoded =
          ProjectionIndexSegmentCodec.encode(benchLeaf(120, 7L).serialize());
      final byte[] d = encoded.descriptor().clone();
      // The last byte lies in the trailing inline region (all segments inline) — flip it so the
      // length still validates but the inline segment's content hash no longer matches.
      d[d.length - 1] ^= 0x20;
      final IllegalStateException e = assertThrows(IllegalStateException.class,
          () -> ProjectionIndexSegmentCodec.assembleRaw(d, segmentId -> null));
      assertTrue(e.getMessage().contains("hash"), e.getMessage());
    } finally {
      ProjectionIndexSegmentCodec.inlineMaxSegmentBytes = savedMax;
      ProjectionIndexSegmentCodec.inlineMaxTotalBytes = savedTot;
    }
  }

  @Test
  void descriptorInlineReadersRoundTrip() {
    final byte[] inlineSeg = {10, 11, 12, 13, 14, 15, 16};   // 7 bytes, stands in for a segment
    final int refLen = 900;
    final byte[] d = LeafDescriptor.serialize(3, 100L, 200L, new byte[0], 2,
        new byte[] {0, 1}, new int[] {inlineSeg.length, refLen},
        new long[] {0xABCDL, 0x1234L}, new byte[] {0, 0}, new long[] {1, 2}, new long[] {3, 4},
        new boolean[] {true, false}, new byte[][] {inlineSeg, null});
    LeafDescriptor.validate(d);
    assertTrue(LeafDescriptor.entryIsInline(d, 0));
    assertTrue(!LeafDescriptor.entryIsInline(d, 1));
    // byteLen readers mask the storage-class flag off → true lengths for both classes.
    assertEquals(inlineSeg.length, LeafDescriptor.entryByteLen(d, 0));
    assertEquals(refLen, LeafDescriptor.entryByteLen(d, 1));
    assertArrayEquals(inlineSeg, LeafDescriptor.inlineSegmentBytes(d, 0));
    // Referenced entry keeps its hash intact (not masked).
    assertEquals(0x1234L, LeafDescriptor.entryContentHash(d, 1));
  }

  @Test
  void descriptorRejectsInlineRegionOverTheSlotValueLimit() {
    // An out-of-band inline segment that would push the descriptor past the HOT slot-value u16
    // limit must fail specifically at serialize, not deep in the slot writer.
    final byte[] tooBig = new byte[LeafDescriptor.MAX_SLOT_VALUE_BYTES + 1];
    final IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> LeafDescriptor.serialize(1, 0L, 0L, new byte[0], 1, new byte[] {0},
            new int[] {tooBig.length}, new long[] {0L}, new byte[] {0}, new long[] {0}, new long[] {0},
            new boolean[] {true}, new byte[][] {tooBig}));
    assertTrue(e.getMessage().contains("slot value limit"), e.getMessage());
  }

  @Test
  void overflowPageRejectsOversizedDataAtConstruction() {
    // Write-time guard (review): a segment/record over MAX_PAGE_BYTES must throw when the page is
    // built, never persist as a committed-but-unreadable page.
    assertThrows(IllegalArgumentException.class,
        () -> new io.sirix.page.OverflowPage(new byte[io.sirix.page.OverflowPage.MAX_PAGE_BYTES + 1]));
    // A normal-sized page still constructs fine.
    assertEquals(16, new io.sirix.page.OverflowPage(new byte[16]).getDataBytes().length);
  }
}
