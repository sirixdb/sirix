/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lossless-ness and compaction of {@link ProjectionIndexRowGroupCodec}: for every
 * leaf shape, {@code decode(encode(raw))} must reproduce the raw payload
 * BYTE-IDENTICALLY (presence, unrepresentable and integrality flags
 * included), and the compact form must actually be smaller on
 * representative analytical data.
 */
public final class ProjectionIndexRowGroupCodecTest {

  private static final byte[] KINDS = {
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG
  };

  private static final String[] DEPTS = {"Eng", "Sales", "Mkt", "Ops", "HR", "Finance", "Legal", "Supp"};

  /**
   * Representative bench-shaped leaf: ascending record keys with jittered
   * spacing, small-range ages, 8-value dict, an all-missing numeric column
   * (the "amount" pattern), sparse dept rows, non-integral marks.
   */
  private static ProjectionIndexRowGroupPage benchLeaf(final int rows, final long keyBase) {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
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
      longs[3] = 0L;                       // all-missing "amount" column
      present[0] = true;
      present[1] = true;
      present[2] = !deptMissing;
      present[3] = false;
      Arrays.fill(unrep, false);
      nonIntegral[0] = i % 11 == 0;        // occasional truncated double
      assertTrue(page.appendRow(key, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page;
  }

  private static void assertRoundTrip(final ProjectionIndexRowGroupPage page) {
    final byte[] raw = page.serialize();
    final byte[] compact = ProjectionIndexRowGroupCodec.encode(raw);
    assertArrayEquals(raw, ProjectionIndexRowGroupCodec.decode(compact),
        "decode(encode(raw)) must be byte-identical");
  }

  private static boolean appendOrderAwareRow(final ProjectionIndexRowGroupPage page, final long recordKey,
      final long value, final boolean orderException) {
    final byte[][] stringUtf8 = new byte[KINDS.length][];
    stringUtf8[2] = "x".getBytes(StandardCharsets.UTF_8);
    final int[] stringUtf8Lengths = new int[KINDS.length];
    stringUtf8Lengths[2] = stringUtf8[2].length;
    return page.appendExtractedUtf8Row(recordKey, new long[] {value, 0L, 0L, value}, new boolean[KINDS.length],
        stringUtf8, stringUtf8Lengths, new String[KINDS.length][], new boolean[] {true, true, true, true},
        new boolean[KINDS.length], new boolean[KINDS.length], new boolean[KINDS.length], orderException);
  }

  // ==================== round trips ====================

  @Test
  void benchShapedLeafRoundTripsAndShrinks() {
    final ProjectionIndexRowGroupPage page = benchLeaf(1024, 1_000_000L);
    final byte[] raw = page.serialize();
    final byte[] compact = ProjectionIndexRowGroupCodec.encode(raw);
    assertArrayEquals(raw, ProjectionIndexRowGroupCodec.decode(compact));
    assertTrue(compact.length * 5 < raw.length,
        "expected >5x compaction on bench-shaped data, got " + raw.length + " -> " + compact.length);
  }

  @Test
  void partialLeafAndSingleRowRoundTrip() {
    assertRoundTrip(benchLeaf(100, 42L));
    assertRoundTrip(benchLeaf(1, 42L));
    assertRoundTrip(benchLeaf(64, 42L));  // exact word boundary
    assertRoundTrip(benchLeaf(65, 42L));
  }

  @Test
  void emptyLeafRoundTrips() {
    assertRoundTrip(new ProjectionIndexRowGroupPage(KINDS));
  }

  @Test
  void nonAscendingDocumentKeysRoundTripWhenExceptionsAreMarked() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final long[] keys = {900L, 5L, 12_345_678_901L, 3L};
    final long[] extremes = {Long.MIN_VALUE, Long.MAX_VALUE, -1L, 0L};
    for (int i = 0; i < keys.length; i++) {
      assertTrue(appendOrderAwareRow(page, keys[i], extremes[i], i == 1 || i == 3));
    }
    assertRoundTrip(page);
    final ProjectionIndexRowGroupPage restored = ProjectionIndexRowGroupPage.deserialize(
        ProjectionIndexRowGroupCodec.decode(ProjectionIndexRowGroupCodec.encode(page.serialize())));
    assertArrayEquals(keys, Arrays.copyOf(restored.recordKeys(), keys.length));
    assertFalse(restored.orderExceptionAt(0));
    assertTrue(restored.orderExceptionAt(1));
    assertFalse(restored.orderExceptionAt(2));
    assertTrue(restored.orderExceptionAt(3));
    assertEquals(900L, restored.firstRecordKey());
    assertEquals(12_345_678_901L, restored.lastRecordKey());
  }

  @Test
  void v0NoneAndDenseOrderMetadataRoundTripByteIdentically() {
    final ProjectionIndexRowGroupPage none = new ProjectionIndexRowGroupPage(KINDS);
    assertTrue(appendOrderAwareRow(none, 2L, 10L, false));
    assertTrue(appendOrderAwareRow(none, 5L, 20L, false));
    assertTrue(appendOrderAwareRow(none, 8L, 30L, false));

    final ProjectionIndexRowGroupPage dense = new ProjectionIndexRowGroupPage(KINDS);
    assertTrue(appendOrderAwareRow(dense, 2L, 10L, false));
    assertTrue(appendOrderAwareRow(dense, 100L, 20L, true));
    assertTrue(appendOrderAwareRow(dense, 5L, 30L, false));
    assertTrue(appendOrderAwareRow(dense, 8L, 40L, false));

    for (final ProjectionIndexRowGroupPage page : List.of(none, dense)) {
      assertRoundTrip(page);
    }
    final ProjectionIndexRowGroupPage restoredNone = ProjectionIndexRowGroupPage.deserialize(
        ProjectionIndexRowGroupCodec.decode(ProjectionIndexRowGroupCodec.encode(none.serialize())));
    assertFalse(restoredNone.hasOrderExceptions());
    assertArrayEquals(new long[] {2L, 5L, 8L}, Arrays.copyOf(restoredNone.recordKeys(), 3));

    final ProjectionIndexRowGroupPage restoredDense = ProjectionIndexRowGroupPage.deserialize(
        ProjectionIndexRowGroupCodec.decode(ProjectionIndexRowGroupCodec.encode(dense.serialize())));
    assertTrue(restoredDense.hasOrderExceptions());
    assertArrayEquals(new long[] {2L, 100L, 5L, 8L}, Arrays.copyOf(restoredDense.recordKeys(), 4));
    assertTrue(restoredDense.orderExceptionAt(1));
    assertEquals(2L, restoredDense.firstRecordKey());
    assertEquals(8L, restoredDense.lastRecordKey());
  }

  @Test
  void realEmptyStringAndUnrepresentableRoundTrip() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final boolean[] present = {true, true, true, true};
    page.appendRow(1L, new long[] {7, 0, 0, 0}, new boolean[] {false, true, false, false},
        new String[] {"", "", "", ""}, present, new boolean[] {false, false, false, true});
    page.appendRow(2L, new long[] {8, 0, 0, 0}, new boolean[4],
        new String[] {"", "", "Eng", ""}, present, new boolean[4]);
    assertRoundTrip(page);
  }

  @Test
  void singleValueDictCollapsesToZeroWidth() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final boolean[] present = {true, true, true, true};
    final boolean[] unrep = new boolean[4];
    for (int i = 0; i < 256; i++) {
      page.appendRow(1000 + i, new long[] {5, 0, 0, 0}, new boolean[4],
          new String[] {"", "", "OnlyValue", ""}, present, unrep);
    }
    final byte[] raw = page.serialize();
    final byte[] compact = ProjectionIndexRowGroupCodec.encode(raw);
    assertArrayEquals(raw, ProjectionIndexRowGroupCodec.decode(compact));
    // Constant columns (age=5, single dict value, amount=0) all collapse:
    // the compact form must be a small fraction of raw.
    assertTrue(compact.length * 10 < raw.length,
        "constant columns should collapse, got " + raw.length + " -> " + compact.length);
  }

  @Test
  void wideBitWidthRangesRoundTripExactly() {
    // Regression: pack widths 57-63 used to lose bits in the byte-at-a-time
    // reader (a byte shifted past bit 63); such widths now take the raw
    // 64-bit path. Cover every width from 48 to 64 with adversarial values.
    for (int width = 48; width <= 64; width++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
      final boolean[] present = {true, true, true, true};
      final boolean[] unrep = new boolean[4];
      final Random rng = new Random(width);
      final long span = width >= 64 ? Long.MAX_VALUE : (1L << (width - 1)) + 7;
      for (int i = 0; i < 100; i++) {
        final long v = i % 3 == 0 ? 0L : i % 3 == 1 ? span - i : Math.floorMod(rng.nextLong(), span);
        page.appendRow(1_000_000L + i * 3L, new long[] {v, 0, 0, 0}, new boolean[4],
            new String[] {"", "", "x", ""}, present, unrep);
      }
      final byte[] raw = page.serialize();
      assertArrayEquals(raw, ProjectionIndexRowGroupCodec.decode(ProjectionIndexRowGroupCodec.encode(raw)),
          "width " + width + " must round-trip byte-identically");
    }
  }

  @Test
  void hugeAscendingKeyDeltasRoundTrip() {
    // Delta-FOR mode with deltas needing >56 bits must clamp to the raw path.
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS);
    final boolean[] present = {true, true, true, true};
    final boolean[] unrep = new boolean[4];
    long key = 10L;
    for (int i = 0; i < 10; i++) {
      page.appendRow(key, new long[] {i, 0, 0, 0}, new boolean[4],
          new String[] {"", "", "x", ""}, present, unrep);
      key += 1L << 58;
    }
    assertRoundTrip(page);
  }

  // ==================== passthrough & flags ====================

  @Test
  void decodePassesRawPayloadsThrough() {
    final byte[] raw = benchLeaf(100, 42L).serialize();
    assertSame(raw, ProjectionIndexRowGroupCodec.decode(raw), "raw payloads must pass through untouched");
  }

  @Test
  void provenanceSurvivesTheCodec() {
    final byte[] raw = benchLeaf(200, 42L).serialize();
    final ProjectionIndexRowGroupPage back =
        ProjectionIndexRowGroupPage.deserialize(ProjectionIndexRowGroupCodec.decode(ProjectionIndexRowGroupCodec.encode(raw)));
    assertTrue(back.columnNumericNonIntegral(0), "integrality provenance must survive");
    assertEquals(200, back.getRowCount());
    // The all-missing amount column: presence bits all clear.
    final long[] pres = back.presenceColumnBits(3);
    for (final long w : pres) {
      assertEquals(0L, w, "all-missing column must stay all-missing");
    }
    // Probes over the decoded payload behave exactly as over the original.
    final byte[] decoded = ProjectionIndexRowGroupCodec.decode(ProjectionIndexRowGroupCodec.encode(raw));
    assertArrayEquals(ProjectionIndexByteScan.probeSparseEvidence(List.of(raw)),
        ProjectionIndexByteScan.probeSparseEvidence(List.of(decoded)));
    assertArrayEquals(ProjectionIndexByteScan.probeNumericNonIntegral(List.of(raw)),
        ProjectionIndexByteScan.probeNumericNonIntegral(List.of(decoded)));
  }
}
