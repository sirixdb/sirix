package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for the ALP encoding of {@link DoubleRegion}: bit-exact round-trips, exception patching,
 * and — the part a wrong boundary would silently corrupt — the integer-space range count checked
 * differentially against a brute-force scan of the original values.
 */
@DisplayName("DoubleRegion ALP")
final class DoubleRegionAlpTest {

  private static final int TAG = 7;

  private static MemorySegment encode(final double[] values) {
    final int[] tags = new int[values.length];
    Arrays.fill(tags, TAG);
    final byte[] wire = DoubleRegion.encode(values, tags, values.length, NumberRegion.TAG_KIND_NAME);
    assertNotNull(wire);
    return PaxTestSegments.of(wire);
  }

  @Test
  @DisplayName("decimal data chooses ALP and round-trips bit-exactly")
  void decimalDataRoundTrips() {
    final Random rng = new Random(1);
    final double[] values = new double[512];
    for (int i = 0; i < values.length; i++) {
      // Two-decimal prices: the canonical ALP-friendly distribution.
      values[i] = Math.round(rng.nextDouble() * 100_000) / 100.0;
    }
    final MemorySegment seg = encode(values);
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    assertNotNull(h);
    final int t = DoubleRegion.lookupTag(h, TAG);
    assertEquals(DoubleRegion.ENC_ALP, h.tagEnc[t], "decimal data must ALP-encode");
    assertEquals(0, h.alpExceptionCount[t], "every two-decimal value round-trips");
    for (int i = 0; i < values.length; i++) {
      assertEquals(Double.doubleToLongBits(values[i]),
                   Double.doubleToLongBits(DoubleRegion.decodeValueAt(seg, h, t, i)), "value " + i);
    }
    // The compression claim, pinned: packed ints beat 8 bytes per value by a wide margin.
    final long plainBytes = (long) values.length * Double.BYTES;
    assertTrue(seg.byteSize() < plainBytes / 2,
               "ALP block not smaller than half of plain: " + seg.byteSize() + " vs " + plainBytes);
  }

  @Test
  @DisplayName("irrational values become exceptions and stay bit-exact")
  void exceptionsAreBitExact() {
    final double[] values = new double[128];
    for (int i = 0; i < values.length; i++) {
      values[i] = i + 0.5;
    }
    values[3] = Math.PI;
    values[77] = Math.E * 1e-9;
    values[120] = StrictMath.sqrt(2);
    final MemorySegment seg = encode(values);
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    final int t = DoubleRegion.lookupTag(h, TAG);
    assertEquals(DoubleRegion.ENC_ALP, h.tagEnc[t]);
    assertEquals(3, h.alpExceptionCount[t]);
    for (int i = 0; i < values.length; i++) {
      assertEquals(Double.doubleToLongBits(values[i]),
                   Double.doubleToLongBits(DoubleRegion.decodeValueAt(seg, h, t, i)), "value " + i);
    }
  }

  /**
   * Irrational data defeats decimal ALP and lands on ALP-RD: the left parts (sign, exponent, top
   * mantissa) of magnitude-clustered reals repeat heavily, so the dictionary covers them and the
   * split beats plain. Losslessness is structural — every value must come back bit-exact, and the
   * range count must agree with brute force at value boundaries, where a wrong split shows up.
   */
  @Test
  @DisplayName("irrational data lands on ALP-RD, bit-exact and smaller than plain")
  void irrationalDataUsesRd() {
    final Random rng = new Random(2);
    final double[] values = new double[512];
    for (int i = 0; i < values.length; i++) {
      values[i] = StrictMath.sqrt(rng.nextDouble() * 1e6);
    }
    final MemorySegment seg = encode(values);
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    final int t = DoubleRegion.lookupTag(h, TAG);
    assertEquals(DoubleRegion.ENC_ALP_RD, h.tagEnc[t], "clustered reals must take the RD split");
    for (int i = 0; i < values.length; i++) {
      assertEquals(Double.doubleToLongBits(values[i]),
                   Double.doubleToLongBits(DoubleRegion.decodeValueAt(seg, h, t, i)), "value " + i);
    }
    assertTrue(seg.byteSize() < (long) values.length * Double.BYTES,
               "the RD split must beat plain storage");
    // Differential range counts, boundaries included.
    for (int probe = 0; probe < 32; probe++) {
      final double pivot = values[rng.nextInt(values.length)];
      final double lo = probe % 2 == 0 ? pivot : Math.nextUp(pivot);
      final double hi = pivot + rng.nextInt(500);
      long expected = 0;
      for (final double v : values) {
        if (v >= lo && v <= hi) {
          expected++;
        }
      }
      assertEquals(expected, DoubleRegionSimd.countTagRange(seg, h, t, lo, hi), "probe " + probe);
    }
    assertEquals(values.length, DoubleRegionSimd.countTagRange(seg, h, t,
        Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY));
  }

  /**
   * Data engineered to defeat BOTH schemes stays PLAIN: random bit patterns have near-distinct
   * left parts at every split, so the dictionary covers nothing and the cost model must conclude
   * that 64 verbatim bits is the honest answer.
   */
  @Test
  @DisplayName("adversarial bit patterns stay PLAIN, still exact")
  void adversarialBitsStayPlain() {
    final Random rng = new Random(4);
    final double[] values = new double[256];
    for (int i = 0; i < values.length; i++) {
      double d;
      do {
        d = Double.longBitsToDouble(rng.nextLong());
      } while (!Double.isFinite(d));
      values[i] = d;
    }
    final MemorySegment seg = encode(values);
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    final int t = DoubleRegion.lookupTag(h, TAG);
    assertEquals(DoubleRegion.ENC_PLAIN, h.tagEnc[t],
                 "a split that saves nothing must not be chosen");
    for (int i = 0; i < values.length; i++) {
      assertEquals(Double.doubleToLongBits(values[i]),
                   Double.doubleToLongBits(DoubleRegion.decodeValueAt(seg, h, t, i)));
    }
  }

  /**
   * The range count, differentially against brute force — including bounds sitting EXACTLY on
   * stored values and one ulp to either side, which is where a mistranslated integer bound shows
   * up as an off-by-one rather than a crash.
   */
  @Test
  @DisplayName("countTagRange matches brute force at and around every boundary")
  void countMatchesBruteForce() {
    final Random rng = new Random(3);
    for (int trial = 0; trial < 40; trial++) {
      final int n = 1 + rng.nextInt(700);
      final double[] values = new double[n];
      final int decimals = rng.nextInt(4);
      final double scale = DoubleRegion.EXP10[decimals];
      for (int i = 0; i < n; i++) {
        values[i] = Math.round((rng.nextDouble() - 0.3) * 2_000_00) / scale
            + (rng.nextInt(50) == 0 ? Math.PI : 0);  // sprinkle exceptions
      }
      final MemorySegment seg = encode(values);
      final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
      final int t = DoubleRegion.lookupTag(h, TAG);

      for (int probe = 0; probe < 24; probe++) {
        final double pivot = values[rng.nextInt(n)];
        final double a;
        final double b;
        switch (probe % 4) {
          case 0 -> { a = pivot; b = values[rng.nextInt(n)]; }
          case 1 -> { a = Math.nextUp(pivot); b = pivot + rng.nextInt(1000); }
          case 2 -> { a = Math.nextDown(pivot); b = pivot; }
          default -> { a = pivot - rng.nextInt(1000); b = Math.nextDown(pivot); }
        }
        final double lo = Math.min(a, b);
        final double hi = Math.max(a, b);
        long expected = 0;
        for (final double v : values) {
          if (v >= lo && v <= hi) {
            expected++;
          }
        }
        assertEquals(expected, DoubleRegionSimd.countTagRange(seg, h, t, lo, hi),
                     "trial " + trial + " probe " + probe + " [" + lo + ", " + hi + "] enc="
                         + h.tagEnc[t]);
      }
      // Degenerates: empty, everything, single point.
      assertEquals(0, DoubleRegionSimd.countTagRange(seg, h, t, 1.0, 0.0));
      assertEquals(n, DoubleRegionSimd.countTagRange(seg, h, t, Double.NEGATIVE_INFINITY,
                                                    Double.POSITIVE_INFINITY));
    }
  }

  @Test
  @DisplayName("trailing-zero data uses the f exponent and stays exact")
  void trailingZerosUseScalingDown() {
    final double[] values = new double[256];
    for (int i = 0; i < values.length; i++) {
      values[i] = (i % 90) * 100.0;  // 0, 100, 200, ... — f scales the two zeros away
    }
    final MemorySegment seg = encode(values);
    final DoubleRegion.Header h = new DoubleRegion.Header().parseInto(seg);
    final int t = DoubleRegion.lookupTag(h, TAG);
    assertEquals(DoubleRegion.ENC_ALP, h.tagEnc[t]);
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], DoubleRegion.decodeValueAt(seg, h, t, i));
    }
    // 100..500 inclusive → multiples 100,200,300,400,500; each residue occurs ceil/floor times.
    long expected = 0;
    for (final double v : values) {
      if (v >= 100.0 && v <= 500.0) {
        expected++;
      }
    }
    assertEquals(expected, DoubleRegionSimd.countTagRange(seg, h, t, 100.0, 500.0));
  }
}
