package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The selection kernels must agree with the counting kernels, value for value, on every encoding.
 *
 * <p>A selection bitmap is what makes an arbitrary predicate answerable WITHOUT decoding: each leaf
 * answers in its own encoded domain and the tree becomes bitmap algebra. That only holds if
 * {@code popcount(select(...)) == count(...)} exactly — a select that is merely close would put
 * rows on the wrong side of a conjunction, and no count-based test would ever see it.
 *
 * <p>Every encoding is covered deliberately, because they answer by different means:
 * {@code ENC_ALP} maps the bound into packed space (monotonic) and patches exceptions,
 * {@code ENC_DEC} compares in decimal space, {@code ENC_PLAIN} compares verbatim doubles, and
 * {@code ENC_ALP_RD} is NOT order-preserving so its select must decode locally — the one case where
 * "never decode" cannot hold, and the one most likely to be silently wrong.
 */
@DisplayName("DoubleRegion select/count equivalence")
final class DoubleRegionSelectEquivalenceTest {

  private static final int TAG = 11;

  private static MemorySegment encode(final double[] values) {
    return encode(values, null, null);
  }

  private static MemorySegment encode(final double[] values, final long[] unscaled,
      final int[] scales) {
    final int[] tags = new int[values.length];
    Arrays.fill(tags, TAG);
    final int[] ordinals = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      ordinals[i] = i;
    }
    final byte[] wire = DoubleRegion.encode(values, unscaled, scales, tags, ordinals, values.length,
                                            NumberRegion.TAG_KIND_NAME);
    assertNotNull(wire);
    return PaxTestSegments.of(wire);
  }

  /** Brute force over the ORIGINAL values — the oracle both kernels are checked against. */
  private static long bruteForce(final double[] values, final double lo, final double hi) {
    long c = 0;
    for (final double v : values) {
      if (v >= lo && v <= hi) {
        c++;
      }
    }
    return c;
  }

  private static long popcount(final long[] bits) {
    long c = 0;
    for (final long w : bits) {
      c += Long.bitCount(w);
    }
    return c;
  }

  /** select and count agree with each other AND with brute force, over many random windows. */
  private void assertEquivalent(final double[] values, final MemorySegment seg, final byte expEnc) {
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    assertNotNull(h, "header must parse");
    final int t = DoubleRegion.lookupTag(h, TAG);
    assertTrue(t >= 0, "tag must be present");
    assertEquals(expEnc, h.tagEnc[t], "unexpected encoding");

    double min = Double.POSITIVE_INFINITY;
    double max = Double.NEGATIVE_INFINITY;
    for (final double v : values) {
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    final Random rng = new Random(0xC0FFEE);
    final long[] out = new long[ColumnLoad.bitmapWords(values.length)];
    for (int trial = 0; trial < 200; trial++) {
      // Bounds drawn from the data itself, so windows land ON boundaries where an off-by-one
      // between the two kernels would show. A uniform window over [min, max] almost never does —
      // it mostly selects everything or nothing, which is exactly where both kernels agree anyway.
      final double a = trial < 8 ? min : values[rng.nextInt(values.length)];
      final double b = trial < 8 ? max : values[rng.nextInt(values.length)];
      final double lo = Math.min(a, b);
      final double hi = Math.max(a, b);

      final long expected = bruteForce(values, lo, hi);
      final long counted = DoubleRegionSimd.countTagRange(seg, h, t, lo, hi);
      Arrays.fill(out, 0L);
      final boolean answered = DoubleRegionSimd.selectTagRange(seg, h, t, lo, hi, out);

      if (expEnc == DoubleRegion.ENC_DEC) {
        // A decimal tag REFUSES a double bound in both kernels — that is the contract, not a gap.
        assertEquals(DoubleRegionSimd.REFUSED, counted, "decimal tag must refuse a double count");
        assertTrue(!answered, "decimal tag must refuse a double select");
        continue;
      }
      assertEquals(expected, counted, "count disagrees with brute force at [" + lo + ", " + hi + ']');
      assertTrue(answered, "select must answer encoding " + expEnc);
      assertEquals(expected, popcount(out),
                   "select disagrees with brute force at [" + lo + ", " + hi + ']');
    }
  }

  @Test
  @DisplayName("ALP: monotonic bound mapping and exception patching agree with counting")
  void alpSelectMatchesCount() {
    final Random rng = new Random(3);
    final double[] values = new double[600];
    for (int i = 0; i < values.length; i++) {
      values[i] = Math.round(rng.nextDouble() * 100_000) / 100.0;
    }
    // Force exceptions: values the decimal scheme cannot capture must still land on the right side.
    values[17] = Math.PI;
    values[301] = Math.E * 1e-7;
    assertEquivalent(values, encode(values), DoubleRegion.ENC_ALP);
  }

  @Test
  @DisplayName("PLAIN: verbatim doubles agree with counting")
  void plainSelectMatchesCount() {
    final Random rng = new Random(5);
    final double[] values = new double[300];
    for (int i = 0; i < values.length; i++) {
      // Random mantissa defeats ALP. For ALP-RD the left part at its widest split (rw = 56) is
      // sign + the TOP 7 exponent bits, so the exponents must span >256 biased values before more
      // than RD_MAX_DICT = 16 distinct left parts appear — a narrower spread is absorbed by the
      // dictionary and RD wins. +/-500 is comfortably past that, which forces PLAIN.
      values[i] = Math.scalb(1.0 + rng.nextDouble(), rng.nextInt(1000) - 500)
          * (rng.nextBoolean() ? 1.0 : -1.0);
    }
    final MemorySegment seg = encode(values);
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    assertNotNull(h);
    final int t = DoubleRegion.lookupTag(h, TAG);
    assertEquals(DoubleRegion.ENC_PLAIN, h.tagEnc[t],
                 "this shape must fall through to PLAIN, else the PLAIN select is untested");
    assertEquivalent(values, seg, DoubleRegion.ENC_PLAIN);
  }

  @Test
  @DisplayName("ALP-RD: a non-order-preserving tag decodes locally and still agrees")
  void alpRdSelectMatchesCount() {
    final Random rng = new Random(7);
    final double[] values = new double[400];
    for (int i = 0; i < values.length; i++) {
      // Clustered magnitudes with noisy mantissas: the shape ALP-RD is built for.
      values[i] = 1000.0 + rng.nextDouble() * 1e-9;
    }
    final MemorySegment seg = encode(values);
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    assertNotNull(h);
    final int t = DoubleRegion.lookupTag(h, TAG);
    assertEquals(DoubleRegion.ENC_ALP_RD, h.tagEnc[t],
                 "this shape must choose ALP-RD, else the decode-locally select is untested");
    assertEquivalent(values, seg, DoubleRegion.ENC_ALP_RD);
  }

  @Test
  @DisplayName("DEC: refuses double bounds, and its decimal-domain select matches its count")
  void decimalSelectMatchesCount() {
    final int n = 500;
    final double[] values = new double[n];
    final long[] unscaled = new long[n];
    final int[] scales = new int[n];
    for (int i = 0; i < n; i++) {
      // Tenths: inexact as doubles, so these can only be carried as exact decimals.
      unscaled[i] = 500L + i;
      scales[i] = 1;
      values[i] = unscaled[i] / 10.0;
    }
    final MemorySegment seg = encode(values, unscaled, scales);
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    assertNotNull(h);
    final int t = DoubleRegion.lookupTag(h, TAG);
    assertEquals(DoubleRegion.ENC_DEC, h.tagEnc[t], "all-decimal data must encode as ENC_DEC");

    // Contract: a double bound is refused by BOTH kernels.
    assertEquivalent(values, seg, DoubleRegion.ENC_DEC);

    // And the decimal-domain pair agrees, over every window, against exact decimal brute force.
    final long[] out = new long[ColumnLoad.bitmapWords(n)];
    final Random rng = new Random(0xD0D0);
    for (int trial = 0; trial < 200; trial++) {
      final long a = 480L + rng.nextInt(120);
      final long b = 480L + rng.nextInt(120);
      final long lo = Math.min(a, b);
      final long hi = Math.max(a, b);
      long expected = 0;
      for (int i = 0; i < n; i++) {
        final BigDecimal v = BigDecimal.valueOf(unscaled[i], scales[i]);
        if (v.compareTo(BigDecimal.valueOf(lo, 1)) >= 0 && v.compareTo(BigDecimal.valueOf(hi, 1)) <= 0) {
          expected++;
        }
      }
      final long counted = DoubleRegionSimd.countDecTagRangeMasked(seg, h, t, lo, hi, null);
      Arrays.fill(out, 0L);
      final boolean answered = DoubleRegionSimd.selectDecTagRange(seg, h, t, lo, hi, out);
      assertTrue(answered, "decimal select must answer a decimal-domain bound");
      assertEquals(expected, counted, "decimal count disagrees at [" + lo + ", " + hi + ']');
      assertEquals(expected, popcount(out), "decimal select disagrees at [" + lo + ", " + hi + ']');
    }
  }
}
