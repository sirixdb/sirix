package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the tag-sorted {@link NumberRegion} codec. Covers encode/decode
 * round-trips across PLAIN_LONG and BIT_PACKED paths, per-tag range lookup, and
 * a randomized stress that validates every value round-trips to its sorted index.
 */
@DisplayName("NumberRegion")
final class NumberRegionTest {

  @Test
  @DisplayName("empty encoding round-trips")
  void emptyRoundTrip() {
    final byte[] wire = NumberRegion.encode(new long[0], new int[0], 0);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);
    assertEquals(0, h.count);
    assertEquals(0, h.dictSize);
  }

  @Test
  @DisplayName("single-tag values pick BIT_PACKED and occupy full range")
  void singleTagBitPacked() {
    final long[] values = { 18, 42, 66, 30, 55, 20 };
    final int[] tags = { 7, 7, 7, 7, 7, 7 };
    final byte[] wire = NumberRegion.encode(values, tags, values.length);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);
    assertEquals(NumberRegion.ENC_BIT_PACKED_ZM, h.encodingKind);
    assertTrue(NumberRegion.isBitPacked(h.encodingKind));
    assertTrue(NumberRegion.hasZoneMap(h.encodingKind));
    assertEquals(6, h.valueBitWidth);
    assertEquals(1, h.dictSize);
    assertEquals(7, h.dict[0]);
    assertEquals(0, h.tagStart[0]);
    assertEquals(6, h.tagCount[0]);
    // Order preserved within a single tag
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], NumberRegion.decodeValueAt(wire, h, i));
    }
  }

  @Test
  @DisplayName("multiple tags are grouped contiguously")
  void multipleTagsGrouped() {
    final long[] values = { 18, 100, 30, 200, 42 };
    final int[] tags = { 11, 22, 11, 22, 11 };
    final byte[] wire = NumberRegion.encode(values, tags, values.length);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);
    assertEquals(2, h.dictSize);

    final int tag11 = NumberRegion.lookupTag(h, 11);
    final int tag22 = NumberRegion.lookupTag(h, 22);
    assertEquals(3, h.tagCount[tag11]);
    assertEquals(2, h.tagCount[tag22]);

    // Tag-11 values (in original order) = {18, 30, 42}
    final long[] tag11Values = new long[h.tagCount[tag11]];
    for (int i = 0; i < tag11Values.length; i++) {
      tag11Values[i] = NumberRegion.decodeValueAt(wire, h, h.tagStart[tag11] + i);
    }
    Arrays.sort(tag11Values);
    assertEquals(18, tag11Values[0]);
    assertEquals(30, tag11Values[1]);
    assertEquals(42, tag11Values[2]);

    // Tag-22 values = {100, 200}
    final long[] tag22Values = new long[h.tagCount[tag22]];
    for (int i = 0; i < tag22Values.length; i++) {
      tag22Values[i] = NumberRegion.decodeValueAt(wire, h, h.tagStart[tag22] + i);
    }
    Arrays.sort(tag22Values);
    assertEquals(100, tag22Values[0]);
    assertEquals(200, tag22Values[1]);

    // Per-tag zone maps mirror the per-tag extents, not the page-global ones.
    assertEquals(18, h.tagMinOrGlobal(tag11));
    assertEquals(42, h.tagMaxOrGlobal(tag11));
    assertEquals(100, h.tagMinOrGlobal(tag22));
    assertEquals(200, h.tagMaxOrGlobal(tag22));
  }

  @Test
  @DisplayName("per-tag zone maps track min/max independently of global range")
  void perTagZoneMaps() {
    // Deliberately mix small and huge values across tags so the global range
    // overlaps but the per-tag ranges are tight.
    final long[] values = { 10, 1_000_000, 20, 2_000_000, 15, 1_500_000 };
    final int[] tags = { 1, 2, 1, 2, 1, 2 };
    final byte[] wire = NumberRegion.encode(values, tags, values.length);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);

    assertTrue(NumberRegion.hasZoneMap(h.encodingKind));
    // Global min/max spans both tags.
    assertEquals(10, h.valueMin);
    assertEquals(2_000_000, h.valueMax);

    final int tag1 = NumberRegion.lookupTag(h, 1);
    final int tag2 = NumberRegion.lookupTag(h, 2);
    // Per-tag min/max is tight — this is what the zone-map skip relies on.
    assertEquals(10, h.tagMin[tag1]);
    assertEquals(20, h.tagMax[tag1]);
    assertEquals(1_000_000, h.tagMin[tag2]);
    assertEquals(2_000_000, h.tagMax[tag2]);
    // A query for "tag1 > 100" can safely skip: global max says 2M but tag max is 20.
  }

  @Test
  @DisplayName("wide-range longs fall back to PLAIN_LONG")
  void wideRangeUsesPlainLong() {
    final long[] values = { 0L, Long.MAX_VALUE, -1L, 42L };
    final int[] tags = { 3, 3, 3, 3 };
    final byte[] wire = NumberRegion.encode(values, tags, values.length);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);
    assertEquals(NumberRegion.ENC_PLAIN_LONG_ZM, h.encodingKind);
    assertFalse(NumberRegion.isBitPacked(h.encodingKind));
    assertTrue(NumberRegion.hasZoneMap(h.encodingKind));
    assertEquals(64, h.valueBitWidth);
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], NumberRegion.decodeValueAt(wire, h, i));
    }
  }

  @Test
  @DisplayName("missing nameKey returns -1")
  void missingTagReturnsMinusOne() {
    final byte[] wire = NumberRegion.encode(new long[] { 42 }, new int[] { 7 }, 1);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);
    assertEquals(-1, NumberRegion.lookupTag(h, 99));
  }

  @Test
  @DisplayName("randomized stress: every value is retrievable via its tag range")
  void randomStress() {
    final SplittableRandom rng = new SplittableRandom(0xBE1ABE11L);
    for (int trial = 0; trial < 20; trial++) {
      final int n = 1 + rng.nextInt(300);
      final long base = rng.nextLong(-1_000_000L, 1_000_000L);
      final long spread = 1L << rng.nextInt(40);
      final int dictSize = 1 + rng.nextInt(16);
      final int[] nameKeys = new int[dictSize];
      for (int i = 0; i < dictSize; i++) {
        nameKeys[i] = rng.nextInt(0, 100_000);
      }
      final long[] values = new long[n];
      final int[] tags = new int[n];
      for (int i = 0; i < n; i++) {
        values[i] = base + (spread == 0 ? 0 : rng.nextLong(0, spread));
        tags[i] = nameKeys[rng.nextInt(dictSize)];
      }
      final byte[] wire = NumberRegion.encode(values, tags, n);
      final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);

      // For each unique name key, collect values originally tagged with it,
      // and compare against the tag's range in the region.
      for (int nk : nameKeys) {
        final int tagId = NumberRegion.lookupTag(h, nk);
        if (tagId < 0) continue;
        final long[] expected = collectByTag(values, tags, nk);
        final long[] actual = new long[h.tagCount[tagId]];
        for (int i = 0; i < actual.length; i++) {
          actual[i] = NumberRegion.decodeValueAt(wire, h, h.tagStart[tagId] + i);
        }
        assertEquals(expected.length, actual.length, "trial " + trial + " tag " + nk + " count");
        Arrays.sort(expected);
        Arrays.sort(actual);
        assertTrue(Arrays.equals(expected, actual),
            "trial " + trial + " tag " + nk + " mismatch");
      }
    }
  }

  private static long[] collectByTag(final long[] values, final int[] tags, final int target) {
    int count = 0;
    for (int t : tags) if (t == target) count++;
    final long[] out = new long[count];
    int i = 0;
    for (int k = 0; k < values.length; k++) {
      if (tags[k] == target) out[i++] = values[k];
    }
    return out;
  }
}
