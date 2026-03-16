/*
 * Copyright (c) 2024, Sirix Contributors
 *
 * All rights reserved.
 */

package io.sirix.index.vector.ops;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SimdL2Distance}.
 */
@DisplayName("SimdL2Distance Tests")
class SimdL2DistanceTest {

  private static final float DELTA = 1e-4f;

  private final DistanceFunction l2 = new SimdL2Distance();

  // ---- float[] tests ----

  @Test
  @DisplayName("Known values: [1,2,3] vs [4,5,6] -> sqrt(27)")
  void testKnownValues() {
    final float[] a = {1.0f, 2.0f, 3.0f};
    final float[] b = {4.0f, 5.0f, 6.0f};
    final float expected = (float) Math.sqrt(27.0);
    assertEquals(expected, l2.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Identical vectors -> 0.0")
  void testIdenticalVectors() {
    final float[] a = {1.5f, -2.3f, 4.7f, 0.0f, -1.1f};
    assertEquals(0.0f, l2.distance(a, a.clone()), DELTA);
  }

  @Test
  @DisplayName("One element")
  void testSingleElement() {
    final float[] a = {3.0f};
    final float[] b = {7.0f};
    assertEquals(4.0f, l2.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Dimension 7 (non-SIMD-aligned)")
  void testDimension7() {
    final float[] a = {1, 2, 3, 4, 5, 6, 7};
    final float[] b = {7, 6, 5, 4, 3, 2, 1};
    // diffs: -6,-4,-2,0,2,4,6 -> squared: 36,16,4,0,4,16,36 = 112
    final float expected = (float) Math.sqrt(112.0);
    assertEquals(expected, l2.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Dimension 13 (non-SIMD-aligned)")
  void testDimension13() {
    final float[] a = new float[13];
    final float[] b = new float[13];
    float expectedSumSq = 0.0f;
    for (int i = 0; i < 13; i++) {
      a[i] = i * 0.5f;
      b[i] = i * 0.3f;
      final float diff = a[i] - b[i];
      expectedSumSq += diff * diff;
    }
    final float expected = (float) Math.sqrt(expectedSumSq);
    assertEquals(expected, l2.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("High-dimensional: dim=128")
  void testDimension128() {
    final int dim = 128;
    final float[] a = new float[dim];
    final float[] b = new float[dim];
    final Random rng = new Random(42);
    float expectedSumSq = 0.0f;
    for (int i = 0; i < dim; i++) {
      a[i] = rng.nextFloat() * 2.0f - 1.0f;
      b[i] = rng.nextFloat() * 2.0f - 1.0f;
      final float diff = a[i] - b[i];
      expectedSumSq += diff * diff;
    }
    final float expected = (float) Math.sqrt(expectedSumSq);
    assertEquals(expected, l2.distance(a, b), 0.01f);
  }

  @Test
  @DisplayName("High-dimensional: dim=1536")
  void testDimension1536() {
    final int dim = 1536;
    final float[] a = new float[dim];
    final float[] b = new float[dim];
    final Random rng = new Random(123);
    float expectedSumSq = 0.0f;
    for (int i = 0; i < dim; i++) {
      a[i] = rng.nextFloat();
      b[i] = rng.nextFloat();
      final float diff = a[i] - b[i];
      expectedSumSq += diff * diff;
    }
    final float expected = (float) Math.sqrt(expectedSumSq);
    assertEquals(expected, l2.distance(a, b), 0.05f);
  }

  @Test
  @DisplayName("Symmetry: d(a,b) == d(b,a)")
  void testSymmetry() {
    final float[] a = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
    final float[] b = {5.0f, 4.0f, 3.0f, 2.0f, 1.0f};
    assertEquals(l2.distance(a, b), l2.distance(b, a), DELTA);
  }

  @Test
  @DisplayName("Non-negativity")
  void testNonNegativity() {
    final float[] a = {-1.0f, -2.0f, -3.0f};
    final float[] b = {3.0f, 2.0f, 1.0f};
    assertTrue(l2.distance(a, b) >= 0.0f);
  }

  // ---- MemorySegment tests ----

  @Test
  @DisplayName("MemorySegment: known values match float[] result")
  void testMemorySegmentKnownValues() {
    final float[] a = {1.0f, 2.0f, 3.0f};
    final float[] b = {4.0f, 5.0f, 6.0f};

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segA = arena.allocate(ValueLayout.JAVA_FLOAT, a.length);
      final MemorySegment segB = arena.allocate(ValueLayout.JAVA_FLOAT, b.length);
      MemorySegment.copy(a, 0, segA, ValueLayout.JAVA_FLOAT, 0, a.length);
      MemorySegment.copy(b, 0, segB, ValueLayout.JAVA_FLOAT, 0, b.length);

      final float expected = l2.distance(a, b);
      final float actual = l2.distance(segA, segB, a.length);
      assertEquals(expected, actual, DELTA);
    }
  }

  @Test
  @DisplayName("MemorySegment: dim=128 matches float[] result")
  void testMemorySegmentDim128() {
    final int dim = 128;
    final float[] a = new float[dim];
    final float[] b = new float[dim];
    final Random rng = new Random(99);
    for (int i = 0; i < dim; i++) {
      a[i] = rng.nextFloat() * 2.0f - 1.0f;
      b[i] = rng.nextFloat() * 2.0f - 1.0f;
    }

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segA = arena.allocate(ValueLayout.JAVA_FLOAT, dim);
      final MemorySegment segB = arena.allocate(ValueLayout.JAVA_FLOAT, dim);
      MemorySegment.copy(a, 0, segA, ValueLayout.JAVA_FLOAT, 0, dim);
      MemorySegment.copy(b, 0, segB, ValueLayout.JAVA_FLOAT, 0, dim);

      final float expected = l2.distance(a, b);
      final float actual = l2.distance(segA, segB, dim);
      assertEquals(expected, actual, 0.01f);
    }
  }

  @Test
  @DisplayName("MemorySegment: non-aligned dim=7")
  void testMemorySegmentNonAligned() {
    final float[] a = {1, 2, 3, 4, 5, 6, 7};
    final float[] b = {7, 6, 5, 4, 3, 2, 1};

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segA = arena.allocate(ValueLayout.JAVA_FLOAT, a.length);
      final MemorySegment segB = arena.allocate(ValueLayout.JAVA_FLOAT, b.length);
      MemorySegment.copy(a, 0, segA, ValueLayout.JAVA_FLOAT, 0, a.length);
      MemorySegment.copy(b, 0, segB, ValueLayout.JAVA_FLOAT, 0, b.length);

      final float expected = l2.distance(a, b);
      final float actual = l2.distance(segA, segB, a.length);
      assertEquals(expected, actual, DELTA);
    }
  }

  // ---- VectorDistanceType integration ----

  @Test
  @DisplayName("VectorDistanceType.L2 returns working function")
  void testVectorDistanceTypeL2() {
    final DistanceFunction fn = VectorDistanceType.L2.getDistanceFunction(3);
    final float[] a = {1.0f, 2.0f, 3.0f};
    final float[] b = {4.0f, 5.0f, 6.0f};
    assertEquals((float) Math.sqrt(27.0), fn.distance(a, b), DELTA);
  }
}
