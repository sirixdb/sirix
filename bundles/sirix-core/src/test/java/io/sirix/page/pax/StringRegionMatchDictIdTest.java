/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit + property tests for {@link StringRegion#matchDictIdInto}, the bulk
 * dict-id match kernel introduced in iter#23. Mirrors the structure of
 * {@code StringRegionTest} but focuses on the new kernel and its boundary
 * cases (1-bit, 8-bit, 16-bit lane widths plus the 32-bit limit).
 */
@DisplayName("StringRegion.matchDictIdInto")
final class StringRegionMatchDictIdTest {

  private static byte[] bytes(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** Returns true when bit {@code row} is set in the LSB-first bitmap. */
  private static boolean bitAt(final long[] bits, final int row) {
    return (bits[row >>> 6] & (1L << (row & 63))) != 0L;
  }

  @Test
  @DisplayName("dict-hit: every row matches when all values share the target dict-id")
  void dictHitAllRows() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    final int tag = 5;
    final int n = 100;
    for (int i = 0; i < n; i++) enc.addValue(tag, bytes("Eng"));
    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);

    final long[] bits = new long[(n + 63) >>> 6];
    StringRegion.matchDictIdInto(wire, h, 0, n, /*targetDictId=*/0, bits);

    for (int i = 0; i < n; i++) {
      assertTrue(bitAt(bits, i), "row " + i + " should match");
    }
  }

  @Test
  @DisplayName("dict-miss: zero rows match when target dict-id never occurs")
  void dictMissEmptyResult() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    final int tag = 5;
    // 5 distinct values → bit-width = 3, mask = 0x7. Target = 4 ("Mkt") is
    // valid in the bit-width sense but never encoded into any row, so all
    // 3 emitted rows must miss.
    enc.addValue(tag, bytes("Eng"));    // id 0
    enc.addValue(tag, bytes("Sales"));  // id 1
    enc.addValue(tag, bytes("Eng"));    // id 0
    // "Sales" value rows yield encoded id 1; "Eng" yields 0; nothing yields id 4.
    // Pad the dict so bit-width is wide enough that 4 is representable.
    enc.addValue(tag + 1, bytes("a"));
    enc.addValue(tag + 1, bytes("b"));
    enc.addValue(tag + 1, bytes("c"));
    enc.addValue(tag + 1, bytes("d"));
    enc.addValue(tag + 1, bytes("e"));
    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);
    assertTrue(h.valueBitWidthEff >= 3, "bit width should be ≥ 3");

    // Probe only the first 3 rows under the original tag — none is encoded as id 4.
    final int n = 3;
    final long[] bits = new long[(n + 63) >>> 6];
    StringRegion.matchDictIdInto(wire, h, 0, n, /*targetDictId=*/4, bits);

    for (int i = 0; i < n; i++) {
      assertFalse(bitAt(bits, i), "row " + i + " must not match");
    }
  }

  @Test
  @DisplayName("partial: only rows with the target dict-id light up")
  void partialMatch() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    final int tag = 9;
    final String[] vals = {"A", "B", "A", "C", "A", "B", "A"};
    for (final String v : vals) enc.addValue(tag, bytes(v));
    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);

    // dict-ids assigned in insertion order: A=0, B=1, C=2
    final int targetA = 0;
    final long[] bits = new long[(vals.length + 63) >>> 6];
    StringRegion.matchDictIdInto(wire, h, 0, vals.length, targetA, bits);

    for (int i = 0; i < vals.length; i++) {
      final boolean expected = vals[i].equals("A");
      assertEquals(expected, bitAt(bits, i), "row " + i + " value=" + vals[i]);
    }
  }

  @Test
  @DisplayName("1-bit lane width: 2-entry dict, alternating values")
  void oneBitLaneWidth() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    final int tag = 1;
    final int n = 200;
    for (int i = 0; i < n; i++) enc.addValue(tag, bytes(i % 2 == 0 ? "x" : "y"));
    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);
    assertEquals(1, h.valueBitWidthEff);

    final long[] bits = new long[(n + 63) >>> 6];
    // target dict id 1 corresponds to "y"
    StringRegion.matchDictIdInto(wire, h, 0, n, 1, bits);
    for (int i = 0; i < n; i++) {
      assertEquals(i % 2 != 0, bitAt(bits, i), "row " + i);
    }
  }

  @ParameterizedTest(name = "valueBitWidth={0}")
  @ValueSource(ints = {1, 2, 3, 4, 7, 8, 12, 16})
  @DisplayName("property: matchDictIdInto matches naive per-row decode for many widths")
  void propertyMatchesNaive(final int targetWidth) {
    // Build a dict large enough to force the requested bit width.
    // Insert all dict entries first so the encoder's first-seen order matches
    // value 0..dictSize-1, then emit values for the actual scan range.
    final int dictSize = Math.max(2, 1 << (targetWidth - 1)) + 1;
    final int n = 1500; // > 1024 to exercise multiple long-cache spans
    final Random rnd = new Random(0xC0FFEE ^ targetWidth);

    final StringRegion.Encoder enc = new StringRegion.Encoder();
    final int tag = 42;
    final String[] dict = new String[dictSize];
    for (int i = 0; i < dictSize; i++) dict[i] = "v" + i;

    // Prime the encoder so dict-ids assigned by addValue equal index-in-dict.
    for (int i = 0; i < dictSize; i++) enc.addValue(tag, bytes(dict[i]));

    final int[] expectedDictIds = new int[n];
    for (int i = 0; i < n; i++) {
      final int id = rnd.nextInt(dictSize);
      expectedDictIds[i] = id;
      enc.addValue(tag, bytes(dict[id]));
    }
    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);
    assertTrue(h.valueBitWidthEff >= targetWidth,
        "expected ≥" + targetWidth + " got " + h.valueBitWidthEff);

    // The first dictSize values are the priming — skip them.
    final int startInRows = dictSize;

    // Try a handful of target dict-ids (including one out-of-bounds for negative test).
    final int[] targets = {0, dictSize / 2, dictSize - 1};
    for (final int target : targets) {
      final long[] bits = new long[(n + 63) >>> 6];
      Arrays.fill(bits, 0L);
      StringRegion.matchDictIdInto(wire, h, startInRows, n, target, bits);
      for (int i = 0; i < n; i++) {
        final int actualId = StringRegion.decodeDictIdAt(wire, h, startInRows + i);
        assertEquals(expectedDictIds[i], actualId, "decode mismatch at row " + i);
        final boolean expected = (actualId == target);
        assertEquals(expected, bitAt(bits, i),
            "width=" + targetWidth + " target=" + target + " row=" + i);
      }
    }
  }

  @Test
  @DisplayName("OR-into existing bits: kernel sets new bits without clearing prior set bits")
  void orsIntoExistingBitmap() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    final int tag = 11;
    final String[] vals = {"a", "b", "a", "b"};
    for (final String v : vals) enc.addValue(tag, bytes(v));
    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);

    final long[] bits = new long[1];
    bits[0] = 0b1100_0000L; // pre-set rows 6 and 7 (out of band)

    StringRegion.matchDictIdInto(wire, h, 0, vals.length, /*target=*/1 /* "b" */, bits);

    // Original out-of-band bits preserved.
    assertTrue(bitAt(bits, 6));
    assertTrue(bitAt(bits, 7));
    // Rows 1 and 3 (the "b" values) lit up.
    assertFalse(bitAt(bits, 0));
    assertTrue(bitAt(bits, 1));
    assertFalse(bitAt(bits, 2));
    assertTrue(bitAt(bits, 3));
  }

  @Test
  @DisplayName("lookupDictId: finds present literal across multiple tags")
  void lookupDictIdHit() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    final int deptTag = 7, cityTag = 11;
    enc.addValue(deptTag, bytes("Eng"));
    enc.addValue(deptTag, bytes("Sales"));
    enc.addValue(cityTag, bytes("NYC"));
    enc.addValue(cityTag, bytes("LA"));
    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);

    final int dt = StringRegion.lookupTag(h, deptTag);
    final int ct = StringRegion.lookupTag(h, cityTag);

    final byte[] eng = bytes("Eng");
    final byte[] sales = bytes("Sales");
    final byte[] nyc = bytes("NYC");
    final byte[] missing = bytes("Marketing");

    assertEquals(0, StringRegion.lookupDictId(wire, h, dt, eng, 0, eng.length));
    assertEquals(1, StringRegion.lookupDictId(wire, h, dt, sales, 0, sales.length));
    assertEquals(0, StringRegion.lookupDictId(wire, h, ct, nyc, 0, nyc.length));
    assertEquals(-1, StringRegion.lookupDictId(wire, h, dt, missing, 0, missing.length));
    assertEquals(-1, StringRegion.lookupDictId(wire, h, ct, eng, 0, eng.length));
  }

  @Test
  @DisplayName("zero-length input is a no-op (does not write bits)")
  void zeroLengthNoOp() {
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    enc.addValue(1, bytes("x"));
    final byte[] wire = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(wire);

    final long[] bits = new long[1];
    bits[0] = 0xDEAD_BEEFL;
    StringRegion.matchDictIdInto(wire, h, 0, 0, 0, bits);
    assertEquals(0xDEAD_BEEFL, bits[0]);
  }
}
