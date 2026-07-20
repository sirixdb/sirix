/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Property tests pinning the NUMERIC_DOUBLE transform
 * (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.6): the encoding must be order-isomorphic to
 * SIGNED-long order — that is the entire justification for double columns reusing every
 * signed-long compare surface (zone maps, {@code zoneSkip}, the numeric predicate kernels,
 * FOR bit-packing) unchanged — and an involution on bit patterns.
 */
final class ProjectionDoubleEncodingTest {

  private static final double[] EDGE_CASES = {
      Double.NEGATIVE_INFINITY, -Double.MAX_VALUE, -1.0e300, -2.0, -1.5, -1.0,
      -Double.MIN_NORMAL, -Double.MIN_VALUE, -0.0, 0.0, Double.MIN_VALUE,
      Double.MIN_NORMAL, 0.5, 1.0, 1.5, 2.0, 1.0e300, Double.MAX_VALUE,
      Double.POSITIVE_INFINITY
  };

  @Test
  void strictOrderIsPreservedOnEdgeCasesAndRandomSamples() {
    final double[] values = new double[EDGE_CASES.length + 20_000];
    System.arraycopy(EDGE_CASES, 0, values, 0, EDGE_CASES.length);
    final Random rng = new Random(42);
    for (int i = EDGE_CASES.length; i < values.length; i++) {
      values[i] = switch (i % 4) {
        case 0 -> Double.longBitsToDouble(rng.nextLong() & ~0x7FF0_0000_0000_0000L
            | ((long) rng.nextInt(0x7FF) << 52)); // random finite-ish
        case 1 -> rng.nextDouble() * 2e9 - 1e9;
        case 2 -> rng.nextGaussian() * 1e-300;    // denormal territory
        default -> (double) rng.nextLong();
      };
      if (Double.isNaN(values[i])) {
        values[i] = 0.0;
      }
    }
    Arrays.sort(values);
    for (int i = 1; i < values.length; i++) {
      final double a = values[i - 1];
      final double b = values[i];
      final long ea = ProjectionDoubleEncoding.encode(a);
      final long eb = ProjectionDoubleEncoding.encode(b);
      if (a < b) {
        assertTrue(ea < eb, "strict order must be preserved: " + a + " < " + b
            + " but encoded " + ea + " >= " + eb);
      } else {
        // Equal doubles (incl. -0.0 vs 0.0, which compare equal via <): the encoding may
        // distinguish the bit patterns but must not invert the order.
        assertTrue(ea <= eb || Double.compare(a, b) > 0,
            "equal values must not invert: " + a + " vs " + b);
      }
    }
  }

  @Test
  void encodingIsAnInvolutionOnBitPatterns() {
    final Random rng = new Random(7);
    for (final double edge : EDGE_CASES) {
      assertEquals(Double.doubleToRawLongBits(edge),
          Double.doubleToRawLongBits(ProjectionDoubleEncoding.decode(ProjectionDoubleEncoding.encode(edge))),
          "decode(encode(x)) must be bit-exact for " + edge);
    }
    for (int i = 0; i < 50_000; i++) {
      final double v = Double.longBitsToDouble(rng.nextLong());
      if (Double.isNaN(v)) {
        continue; // NaN never reaches the transform (unrepresentable at extraction)
      }
      assertEquals(Double.doubleToRawLongBits(v),
          Double.doubleToRawLongBits(ProjectionDoubleEncoding.decode(ProjectionDoubleEncoding.encode(v))));
    }
  }

  @Test
  void zoneMapSentinelsAreNeverProducedByFiniteValues() {
    // min > max sentinels (Long.MAX_VALUE / Long.MIN_VALUE) must stay unambiguous: no finite
    // double (nor ±∞) may encode onto them.
    for (final double edge : EDGE_CASES) {
      final long e = ProjectionDoubleEncoding.encode(edge);
      assertTrue(e != Long.MAX_VALUE && e != Long.MIN_VALUE,
          edge + " encodes onto a zone-map sentinel");
    }
  }
}
