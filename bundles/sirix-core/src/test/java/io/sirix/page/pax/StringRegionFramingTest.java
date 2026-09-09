/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import io.sirix.node.Bytes;
import io.sirix.node.BytesOut;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
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
 * The string region's framing: varint per-tag headers, a length table as wide as its tag needs, and
 * a plain lane for the tags whose values are all distinct.
 *
 * <h2>What is being claimed</h2>
 *
 * <p>
 * That the region costs less to frame and reads exactly the same. The dictionary form spent sixteen
 * bytes per tag and four per entry, which on a leaf of a wide record-shaped corpus — a few rows of
 * thirty-odd fields — was most of what the region held that was not a value. The claim is measured
 * on two fixtures: the brief's smallest one (a tag whose ten values are all distinct beside a tag
 * with three), and a record-shaped page of thirty-four string fields over ten rows.
 *
 * <h2>Why "all distinct" is a precondition and not a heuristic</h2>
 *
 * <p>
 * On the plain lane a value's rank within its tag IS its dictionary id. That is a bijection only
 * while the tag's values are distinct; with a repeat, an equality count would answer for one
 * occurrence of the value rather than all of them. {@link #aRepeatedValueKeepsItsDictionary} is the
 * mutation for that: it asserts the three-valued tag stays on the dictionary lane AND that its
 * equality count is the number of occurrences.
 */
@DisplayName("string region, record framing")
final class StringRegionFramingTest {

  /** SHA-256 of the small fixture's region as the pre-framing encoder wrote it. */
  private static final String HEAD_SMALL_SHA256 = "1bc670d6bf6957beaebe57f6855a1fa4bb2d482803b70c33a69014cd4b9dc075";

  /** SHA-256 of the record-shaped fixture's region as the pre-framing encoder wrote it. */
  private static final String HEAD_WIDE_SHA256 = "2ff5de380f8e4b4b4f508284c72f9e8d206ce4aeceb886ab4ae94631b087bd11";

  private static final int DISTINCT_TAG = 100;
  private static final int REPEATED_TAG = 200;
  private static final int ROWS = 10;
  private static final String[] REPEATED = {"Firefox", "Chrome", "Safari"};

  @AfterEach
  void restoreDefaults() {
    StringRegion.clearPlainLaneOverride();
  }

  private static byte[] url(final int row) {
    return ("https://example.org/catalog/products/item-" + row + "/details?locale=en-US&campaign=" + row).getBytes(
        StandardCharsets.UTF_8);
  }

  private static byte[] repeated(final int row) {
    return REPEATED[row % REPEATED.length].getBytes(StandardCharsets.UTF_8);
  }

  private static byte[] expected(final int tag, final int row) {
    return tag == DISTINCT_TAG
        ? url(row)
        : repeated(row);
  }

  /** One all-distinct tag beside one three-valued tag — the fixture the brief names. */
  private static byte[] small(final boolean framed) {
    StringRegion.setPlainLaneEnabled(framed);
    try {
      final StringRegion.Encoder encoder = new StringRegion.Encoder();
      for (int row = 0; row < ROWS; row++) {
        encoder.addValue(DISTINCT_TAG, url(row));
        encoder.addValue(REPEATED_TAG, repeated(row));
      }
      return encoder.finish(StringRegion.TAG_KIND_PATH_NODE);
    } finally {
      StringRegion.clearPlainLaneOverride();
    }
  }

  /** A record-shaped page: 34 string fields over 10 rows, four of them fat and all-distinct. */
  private static byte[] wide(final boolean framed) {
    StringRegion.setPlainLaneEnabled(framed);
    try {
      final StringRegion.Encoder encoder = new StringRegion.Encoder();
      final Random rnd = new Random(11);
      final String[] codes = {"DE", "US", "FR", "JP", "BR", "IN"};
      for (int row = 0; row < ROWS; row++) {
        for (int field = 0; field < 34; field++) {
          final byte[] value;
          if (field < 4) {
            value = ("https://example.org/section/" + field + "/item-" + row + "?utm=" + rnd.nextInt(1 << 20)
                + "&ref=catalogue").getBytes(StandardCharsets.UTF_8);
          } else if (field < 10) {
            value = (codes[rnd.nextInt(codes.length)] + "-" + field).getBytes(StandardCharsets.UTF_8);
          } else {
            value = codes[rnd.nextInt(codes.length)].getBytes(StandardCharsets.UTF_8);
          }
          encoder.addValue(1000 + field, value);
        }
      }
      return encoder.finish(StringRegion.TAG_KIND_PATH_NODE);
    } finally {
      StringRegion.clearPlainLaneOverride();
    }
  }

  private static String sha256(final byte[] bytes) {
    try {
      return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    } catch (final NoSuchAlgorithmException impossible) {
      throw new IllegalStateException(impossible);
    }
  }

  private static int wireBytes(final byte[] payload) {
    try (RegionTable table = new RegionTable()) {
      table.set(RegionTable.KIND_STRING, payload);
      final BytesOut<?> sink = Bytes.elasticOffHeapByteBuffer();
      table.write(sink, true);
      return (int) sink.writePosition();
    }
  }

  private static byte[] entryBytes(final MemorySegment payload, final StringRegion.Header h, final int tag,
      final int dictId) {
    final int off = StringRegion.decodeStringOffset(payload, h, tag, dictId);
    final int len = StringRegion.decodeStringLength(payload, h, tag, dictId);
    final byte[] out = new byte[len];
    for (int i = 0; i < len; i++) {
      out[i] = payload.get(ValueLayout.JAVA_BYTE, off + i);
    }
    return out;
  }

  // ─────────────────────────────────────────────────────── the size claim + pins

  @Test
  @DisplayName("the dictionary framing is the mutation, and costs a quarter more on a record page")
  void framingIsMeasuredAgainstTheDictionaryLayout() {
    final byte[] framedSmall = small(true);
    final byte[] legacySmall = small(false);
    final byte[] framedWide = wide(true);
    final byte[] legacyWide = wide(false);

    assertEquals(HEAD_SMALL_SHA256, sha256(legacySmall), "the kill switch must reproduce the pre-framing bytes");
    assertEquals(HEAD_WIDE_SHA256, sha256(legacyWide), "the kill switch must reproduce the pre-framing bytes");

    assertTrue(framedSmall.length < legacySmall.length,
        "small fixture: " + framedSmall.length + " vs " + legacySmall.length);
    // The record-shaped page is where the framing was the cost: 34 tags x 16 bytes of per-tag arrays
    // and 4 bytes per dictionary entry, for values most of which are under 128 bytes.
    assertTrue(framedWide.length * 5 < legacyWide.length * 4,
        "record page must lose at least a fifth: " + framedWide.length + " vs " + legacyWide.length);
    assertTrue(wireBytes(framedWide) * 5 < wireBytes(legacyWide) * 4,
        "and the same on the wire: " + wireBytes(framedWide) + " vs " + wireBytes(legacyWide));
  }

  @Test
  @DisplayName("an all-distinct tag takes the plain lane and a three-valued one keeps its dictionary")
  void laneIsChosenPerTag() {
    final MemorySegment payload = PaxTestSegments.of(small(true));
    final StringRegion.Header h = new StringRegion.Header().parseInto(payload);
    final int distinct = StringRegion.lookupTag(h, DISTINCT_TAG);
    final int repeatedTag = StringRegion.lookupTag(h, REPEATED_TAG);
    assertTrue(distinct >= 0 && repeatedTag >= 0);
    assertTrue(h.tagPlainLane[distinct], "ten distinct values write no dict ids at all");
    assertFalse(h.tagPlainLane[repeatedTag], "three values under ten records still pay for their dictionary");
    assertFalse(h.idLaneIsAbsolute, "with a plain tag present the lane is no longer the absolute index");
    assertEquals(ROWS, h.tagStringDictSize[distinct], "the plain lane's dictionary size IS its value count");
    assertEquals(3, h.tagStringDictSize[repeatedTag]);
    assertEquals(1, h.tagLengthWidth[distinct], "values under 128 bytes need one byte of length");
    assertEquals(2, h.valueBitWidthEff, "the width is derived from the DICT-lane dictionaries alone");
  }

  @Test
  @DisplayName("a repeated value keeps its dictionary, so an equality count answers for every occurrence")
  void aRepeatedValueKeepsItsDictionary() {
    final MemorySegment payload = PaxTestSegments.of(small(true));
    final StringRegion.Header h = new StringRegion.Header().parseInto(payload);
    final int tag = StringRegion.lookupTag(h, REPEATED_TAG);
    final int id = StringRegion.findDictId(payload, h, tag, repeated(0), null);
    assertTrue(id >= 0);
    // "Firefox" occurs on rows 0, 3, 6 and 9. Had the tag taken the plain lane, rank would not be a
    // bijection onto ids and this would answer 1.
    assertEquals(4, StringRegion.countDictId(payload, h, h.tagStart[tag], h.tagCount[tag], id),
        "every occurrence must be counted");
  }

  // ───────────────────────────────────────────────────────── the reader contract

  @Test
  @DisplayName("every string door agrees with the dictionary layout, value for value")
  void everyDoorAgreesAcrossLanes() {
    final byte[] framedBytes = small(true);
    final byte[] legacyBytes = small(false);
    final MemorySegment framed = PaxTestSegments.of(framedBytes);
    final MemorySegment legacy = PaxTestSegments.of(legacyBytes);
    final StringRegion.Header fh = new StringRegion.Header().parseInto(framed);
    final StringRegion.Header lh = new StringRegion.Header().parseInto(legacy);

    assertEquals(lh.count, fh.count);
    assertEquals(lh.parentDictSize, fh.parentDictSize);
    assertEquals(lh.encodingKind, fh.encodingKind, "the element-staging promise survives the reframing");
    for (int t = 0; t < fh.parentDictSize; t++) {
      assertEquals(lh.parentDict[t], fh.parentDict[t], "parentDict " + t);
      assertEquals(lh.tagStart[t], fh.tagStart[t], "tagStart " + t);
      assertEquals(lh.tagCount[t], fh.tagCount[t], "tagCount " + t);
      assertEquals(lh.tagStringDictSize[t], fh.tagStringDictSize[t], "dictSize " + t);

      final int tagValue = fh.parentDict[t];
      final int start = fh.tagStart[t];
      final int n = fh.tagCount[t];
      for (int row = 0; row < n; row++) {
        final byte[] want = expected(tagValue, row);
        final int id = StringRegion.decodeDictIdAt(framed, fh, start + row);
        assertArrayEquals(want, entryBytes(framed, fh, t, id), "value " + row + " of tag " + tagValue);
        assertFalse(StringRegion.isEntryCompressed(framed, fh, t, id), "entry " + id + " is raw");

        final int found = StringRegion.findDictId(framed, fh, t, want, null);
        assertTrue(found >= 0, "findDictId " + row);
        assertArrayEquals(want, entryBytes(framed, fh, t, found), "findDictId points at the value");

        long occurrences = 0;
        for (int other = 0; other < n; other++) {
          if (Arrays.equals(expected(tagValue, other), want)) {
            occurrences++;
          }
        }
        assertEquals(occurrences, StringRegion.countDictId(framed, fh, start, n, found), "countDictId " + row);
        assertEquals(StringRegion.countDictId(legacy, lh, start, n, StringRegion.findDictId(legacy, lh, t, want, null)),
            StringRegion.countDictId(framed, fh, start, n, found), "countDictId parity " + row);

        final long[] rowBits = new long[(n + 63) >>> 6];
        assertEquals(occurrences, StringRegion.selectDictIdInto(framed, fh, start, n, found, rowBits),
            "selectDictIdInto " + row);
        for (int other = 0; other < n; other++) {
          assertEquals(Arrays.equals(expected(tagValue, other), want),
              (rowBits[other >>> 6] & (1L << (other & 63))) != 0L, "selection bit " + other);
        }

        final long[] live = new long[(n + 63) >>> 6];
        long liveOccurrences = 0;
        for (int other = 0; other < n; other++) {
          if ((other & 1) == 0) {
            live[other >>> 6] |= 1L << (other & 63);
            if (Arrays.equals(expected(tagValue, other), want)) {
              liveOccurrences++;
            }
          }
        }
        assertEquals(liveOccurrences, StringRegion.countDictIdMasked(framed, fh, start, n, found, live),
            "countDictIdMasked " + row);
      }

      final long[] counts = new long[fh.tagStringDictSize[t] + 1];
      StringRegion.countDictIds(framed, fh, start, n, counts);
      long total = 0;
      for (final long c : counts) {
        total += c;
      }
      assertEquals(n, total, "the histogram must account for every value of tag " + tagValue);
      for (int id = 0; id < fh.tagStringDictSize[t]; id++) {
        final byte[] entry = entryBytes(framed, fh, t, id);
        long occurrences = 0;
        for (int row = 0; row < n; row++) {
          if (Arrays.equals(expected(tagValue, row), entry)) {
            occurrences++;
          }
        }
        assertEquals(occurrences, counts[id], "histogram of id " + id + " under tag " + tagValue);
      }

      final long[] idSet = new long[(fh.tagStringDictSize[t] + 63) >>> 6];
      Arrays.fill(idSet, ~0L);
      assertEquals(n, StringRegion.countDictIdSet(framed, fh, start, n, idSet, fh.tagStringDictSize[t]),
          "every id accepted");
      Arrays.fill(idSet, 0L);
      assertEquals(0, StringRegion.countDictIdSet(framed, fh, start, n, idSet, fh.tagStringDictSize[t]),
          "no id accepted");
    }
  }

  @Test
  @DisplayName("a dict-id window that crosses a tag is refused rather than answered")
  void windowsMustLieWithinOneTag() {
    final MemorySegment payload = PaxTestSegments.of(small(true));
    final StringRegion.Header h = new StringRegion.Header().parseInto(payload);
    // Ids are tag-local, so a window spanning tags never had a meaning; on the plain lane it would
    // silently answer with another tag's ranks.
    assertThrows(IllegalArgumentException.class, () -> StringRegion.countDictId(payload, h, 0, h.count, 0));
    assertThrows(IllegalArgumentException.class, () -> StringRegion.countDictIds(payload, h, 0, h.count, new long[64]));
  }

  // ────────────────────────────────────────────────────────── length-table widths

  @Test
  @DisplayName("a tag's length table is as wide as that tag needs, and the sign still means FSST")
  void lengthWidthsAndTheCompressedFlag() {
    final int[] lengths = {5, 127, 128, 32_767, 32_768};
    for (final int length : lengths) {
      final byte[] value = new byte[length];
      Arrays.fill(value, (byte) 'x');
      final byte[] shortValue = "ab".getBytes(StandardCharsets.UTF_8);
      final StringRegion.Encoder encoder = new StringRegion.Encoder();
      // Two entries under one tag, one of them flagged FSST-encoded, so the sign and the magnitude
      // are both exercised at whatever width the tag lands on.
      encoder.addValue(7, value, false);
      encoder.addValue(7, shortValue, true);
      encoder.addValue(7, value, false);
      final byte[] wire = encoder.finish(StringRegion.TAG_KIND_NAME);
      final MemorySegment payload = PaxTestSegments.of(wire);
      final StringRegion.Header h = new StringRegion.Header().parseInto(payload);

      final int expectedWidth = length <= Byte.MAX_VALUE
          ? 1
          : (length <= Short.MAX_VALUE
              ? 2
              : 4);
      assertEquals(expectedWidth, h.tagLengthWidth[0], "length width for a " + length + "-byte value");
      assertFalse(h.tagPlainLane[0], "a repeated value keeps the dictionary at every width");
      assertEquals(length, StringRegion.decodeStringLength(payload, h, 0, 0), "length of entry 0");
      assertFalse(StringRegion.isEntryCompressed(payload, h, 0, 0), "entry 0 is raw");
      assertEquals(2, StringRegion.decodeStringLength(payload, h, 0, 1), "length of entry 1");
      assertTrue(StringRegion.isEntryCompressed(payload, h, 0, 1), "entry 1 is FSST-flagged");
      assertArrayEquals(value, entryBytes(payload, h, 0, 0), "bytes of entry 0");
      assertArrayEquals(shortValue, entryBytes(payload, h, 0, 1), "bytes of entry 1");
      assertEquals(3, h.tagCount[0]);
      assertEquals(2, h.tagStringDictSize[0]);
    }
  }

  @Test
  @DisplayName("a suppressed tag is still named, and still costs nothing when there is none")
  void suppressedTagsSurviveTheReframing() {
    final StringRegion.Encoder encoder = new StringRegion.Encoder();
    for (int row = 0; row < ROWS; row++) {
      encoder.addValue(DISTINCT_TAG, url(row));
      encoder.addValue(REPEATED_TAG, repeated(row));
    }
    encoder.suppressTag(300);
    encoder.addValue(300, "oversized".getBytes(StandardCharsets.UTF_8));
    final MemorySegment payload = PaxTestSegments.of(encoder.finish(StringRegion.TAG_KIND_PATH_NODE));
    final StringRegion.Header h = new StringRegion.Header().parseInto(payload);

    assertEquals(1, h.suppressedTagCount);
    assertTrue(StringRegion.isTagSuppressed(h, 300), "the suppressed tag must be named");
    assertFalse(StringRegion.isTagSuppressed(h, DISTINCT_TAG));
    assertEquals(-1, StringRegion.lookupTag(h, 300), "and must not be in the region's dictionary");
    assertEquals(2 * ROWS, h.count, "its values must not be in the region either");
    assertEquals(0, new StringRegion.Header().parseInto(PaxTestSegments.of(small(true))).suppressedTagCount,
        "a page without an oversized string names no absences");
  }

  // ───────────────────────────────────────────────────────────────── the sketch

  @Test
  @DisplayName("the sketch is built over both lanes and answers for every value")
  void sketchCoversBothLanes() {
    for (final boolean framed : new boolean[] {true, false}) {
      final byte[] region = small(framed);
      final StringRegion.Header h = new StringRegion.Header().parseInto(PaxTestSegments.of(region));
      final byte[] sketch = StringDictSketch.encodeFromStringRegion(region, h);
      assertNotNull(sketch, "framed=" + framed);
      final MemorySegment sketchSegment = PaxTestSegments.of(sketch);
      for (int row = 0; row < ROWS; row++) {
        assertTrue(StringDictSketch.mayContain(sketchSegment, url(row)),
            "the sketch must not miss a value it holds (framed=" + framed + ", row " + row + ")");
      }
      for (final String value : REPEATED) {
        assertTrue(StringDictSketch.mayContain(sketchSegment, value.getBytes(StandardCharsets.UTF_8)),
            "framed=" + framed + " " + value);
      }
    }
  }
}
