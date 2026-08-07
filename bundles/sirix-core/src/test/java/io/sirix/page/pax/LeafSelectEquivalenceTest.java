package io.sirix.page.pax;

import jdk.incubator.vector.VectorOperators;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Every leaf kernel's SELECT must agree with its COUNT, bit for bit.
 *
 * <p>The companion of {@code DoubleRegionSelectEquivalenceTest}, covering the other three column
 * types: the boolean bit column, the string dictionary and the long number column. Together they
 * pin the contract the predicate evaluator rests on — a leaf answers as a bitmap, in its own
 * encoded domain, and {@code popcount(select) == count} exactly.
 *
 * <p>Exactness is the whole point rather than a nicety. A count that is off by one is a wrong
 * answer to one query; a SELECT that is off by one puts a row on the wrong side of every
 * conjunction and disjunction built on top of it, and no count-based test can see the difference.
 */
@DisplayName("leaf select/count equivalence")
final class LeafSelectEquivalenceTest {

  private static final int TAG = 5;

  private static long popcount(final long[] bits, final int n) {
    long c = 0;
    for (int i = 0; i < n; i++) {
      if ((bits[i >>> 6] & 1L << (i & 63)) != 0L) {
        c++;
      }
    }
    return c;
  }

  // ─────────────────────────────────────────────────────────────── boolean

  @Test
  @DisplayName("boolean: the bit column selects true and false without a comparison")
  void booleanSelectMatchesCount() {
    final Random rng = new Random(11);
    final int n = 700;
    final boolean[] values = new boolean[n];
    final int[] tags = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextInt(3) != 0;
      tags[i] = TAG;
    }
    final byte[] wire = BooleanRegion.encode(values, tags, n, NumberRegion.TAG_KIND_NAME);
    assertNotNull(wire, "boolean region must encode");
    final MemorySegment seg = PaxTestSegments.of(wire);
    final BooleanRegion.Header h = new BooleanRegion.Header().parseInto(seg);
    assertNotNull(h);
    final int t = BooleanRegion.lookupTag(h, TAG);
    assertTrue(t >= 0);
    final int start = h.tagStart[t];
    final int count = h.tagCount[t];

    long expectedTrue = 0;
    for (final boolean v : values) {
      if (v) {
        expectedTrue++;
      }
    }
    final long[] bits = new long[ColumnLoad.bitmapWords(count)];

    final long selTrue = BooleanRegion.selectInto(seg, h, start, count, bits, false);
    assertEquals(BooleanRegion.countTrue(seg, h, start, count), selTrue,
                 "select(true) must equal countTrue");
    assertEquals(expectedTrue, selTrue, "select(true) must equal brute force");
    assertEquals(selTrue, popcount(bits, count), "returned count must equal the bitmap's popcount");

    // Inversion is a negation answered by the leaf itself — no complement pass over a selection.
    final long selFalse = BooleanRegion.selectInto(seg, h, start, count, bits, true);
    assertEquals(count - expectedTrue, selFalse, "select(false) must be the complement");
    assertEquals(selFalse, popcount(bits, count), "inverted popcount must match");
  }

  // ─────────────────────────────────────────────────────────────── string

  @Test
  @DisplayName("string: dictionary-code selection matches the dictionary count")
  void stringSelectMatchesCount() {
    final Random rng = new Random(13);
    final int n = 600;
    final String[] pool = { "alpha", "beta", "gamma", "delta", "epsilon", "zeta" };
    final byte[][] values = new byte[n][];
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    for (int i = 0; i < n; i++) {
      values[i] = pool[rng.nextInt(pool.length)].getBytes(java.nio.charset.StandardCharsets.UTF_8);
      enc.addValue(TAG, values[i]);
    }
    final byte[] wire = enc.finish(NumberRegion.TAG_KIND_NAME);
    assertNotNull(wire, "string region must encode");
    final MemorySegment seg = PaxTestSegments.of(wire);
    final StringRegion.Header h = new StringRegion.Header().parseInto(seg);
    assertNotNull(h);
    final int t = StringRegion.lookupTag(h, TAG);
    assertTrue(t >= 0);
    final int start = h.tagStart[t];
    final int count = h.tagCount[t];
    final long[] bits = new long[ColumnLoad.bitmapWords(count)];

    for (final String probe : pool) {
      final byte[] needle = probe.getBytes(java.nio.charset.StandardCharsets.UTF_8);
      final int dictId = StringRegion.findDictId(seg, h, t, needle, null);
      assertTrue(dictId >= 0, "dictionary must contain " + probe);
      long expected = 0;
      for (final byte[] v : values) {
        if (Arrays.equals(v, needle)) {
          expected++;
        }
      }
      final int counted = StringRegion.countDictId(seg, h, start, count, dictId);
      final int selected = StringRegion.selectDictIdInto(seg, h, start, count, dictId, bits);
      assertEquals(expected, counted, "count disagrees with brute force for " + probe);
      assertEquals(expected, selected, "select disagrees with brute force for " + probe);
      assertEquals(expected, popcount(bits, count), "bitmap popcount disagrees for " + probe);
    }
  }

  // ─────────────────────────────────────────────────────────────── number

  @Test
  @DisplayName("number: bitmap selection matches the counting kernel over many windows")
  void numberSelectMatchesCount() {
    final Random rng = new Random(17);
    final int n = 800;
    final long[] values = new long[n];
    final int[] tags = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = 1900L + rng.nextInt(200);
      tags[i] = TAG;
    }
    final byte[] wire = NumberRegion.encode(values, tags, n, NumberRegion.TAG_KIND_NAME);
    assertNotNull(wire, "number region must encode");
    final MemorySegment seg = PaxTestSegments.of(wire);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(seg);
    assertNotNull(h);
    final int t = NumberRegion.lookupTag(h, TAG);
    assertTrue(t >= 0);
    final int start = h.tagStart[t];
    final int count = h.tagCount[t];
    final long[] bits = new long[ColumnLoad.bitmapWords(count)];

    for (int trial = 0; trial < 200; trial++) {
      // Bounds drawn from the data, so windows land on boundaries where an off-by-one shows.
      final long a = values[rng.nextInt(n)];
      final long b = values[rng.nextInt(n)];
      final long lo = Math.min(a, b);
      final long hi = Math.max(a, b);

      long expected = 0;
      for (final long v : values) {
        if (v >= lo && v <= hi) {
          expected++;
        }
      }
      final long counted = NumberRegionSimd.countMatchingRange(seg, h, start, start + count,
                                                               VectorOperators.GE, lo,
                                                               VectorOperators.LE, hi);
      final int selected = NumberRegionSimd.selectRangeInto(seg, h, start, count,
                                                            VectorOperators.GE, lo,
                                                            VectorOperators.LE, hi, bits);
      assertEquals(expected, counted, "count disagrees at [" + lo + ", " + hi + ']');
      assertTrue(selected >= 0, "select must not decline this shape");
      assertEquals(expected, selected, "select disagrees at [" + lo + ", " + hi + ']');
      assertEquals(expected, popcount(bits, count), "bitmap popcount disagrees at [" + lo + ", " + hi + ']');
    }
  }
}
