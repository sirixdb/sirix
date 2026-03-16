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
 * Tests for {@link SimdInnerProductDistance}.
 */
@DisplayName("SimdInnerProductDistance Tests")
class SimdInnerProductDistanceTest {

  private static final float DELTA = 1e-4f;

  private final DistanceFunction ip = new SimdInnerProductDistance();

  // ---- float[] tests ----

  @Test
  @DisplayName("Known dot product: [1,2,3] . [4,5,6] = 32, distance = 1 - 32 = -31")
  void testKnownDotProduct() {
    final float[] a = {1.0f, 2.0f, 3.0f};
    final float[] b = {4.0f, 5.0f, 6.0f};
    // dot = 1*4 + 2*5 + 3*6 = 4 + 10 + 18 = 32
    assertEquals(1.0f - 32.0f, ip.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Orthogonal vectors: dot = 0, distance = 1")
  void testOrthogonalVectors() {
    final float[] a = {1.0f, 0.0f, 0.0f};
    final float[] b = {0.0f, 1.0f, 0.0f};
    assertEquals(1.0f, ip.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Normalized identical unit vectors: dot = 1, distance = 0")
  void testNormalizedIdentical() {
    // Unit vector along x-axis
    final float[] a = {1.0f, 0.0f, 0.0f};
    assertEquals(0.0f, ip.distance(a, a.clone()), DELTA);
  }

  @Test
  @DisplayName("Normalized opposite vectors: dot = -1, distance = 2")
  void testNormalizedOpposite() {
    final float[] a = {1.0f, 0.0f, 0.0f};
    final float[] b = {-1.0f, 0.0f, 0.0f};
    assertEquals(2.0f, ip.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Normalized vectors: equivalent to cosine distance")
  void testNormalizedEquivalentToCosine() {
    // Normalize a and b
    final float[] raw_a = {1.0f, 2.0f, 3.0f};
    final float[] raw_b = {4.0f, 5.0f, 6.0f};
    final float[] a = normalize(raw_a);
    final float[] b = normalize(raw_b);

    final DistanceFunction cosineFn = new SimdCosineDistance();
    final float cosineResult = cosineFn.distance(a, b);
    final float ipResult = ip.distance(a, b);
    assertEquals(cosineResult, ipResult, 1e-3f);
  }

  @Test
  @DisplayName("Single element")
  void testSingleElement() {
    final float[] a = {0.5f};
    final float[] b = {0.8f};
    // dot = 0.4, distance = 0.6
    assertEquals(0.6f, ip.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Dimension 7 (non-SIMD-aligned)")
  void testDimension7() {
    final float[] a = new float[7];
    final float[] b = new float[7];
    float expectedDot = 0.0f;
    for (int i = 0; i < 7; i++) {
      a[i] = (i + 1) * 0.1f;
      b[i] = (i + 1) * 0.2f;
      expectedDot += a[i] * b[i];
    }
    assertEquals(1.0f - expectedDot, ip.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Dimension 13 (non-SIMD-aligned)")
  void testDimension13() {
    final float[] a = new float[13];
    final float[] b = new float[13];
    float expectedDot = 0.0f;
    for (int i = 0; i < 13; i++) {
      a[i] = (float) Math.sin(i);
      b[i] = (float) Math.cos(i);
      expectedDot += a[i] * b[i];
    }
    assertEquals(1.0f - expectedDot, ip.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("High-dimensional: dim=128")
  void testDimension128() {
    final int dim = 128;
    final float[] a = new float[dim];
    final float[] b = new float[dim];
    final Random rng = new Random(42);
    float expectedDot = 0.0f;
    for (int i = 0; i < dim; i++) {
      a[i] = rng.nextFloat() * 2.0f - 1.0f;
      b[i] = rng.nextFloat() * 2.0f - 1.0f;
      expectedDot += a[i] * b[i];
    }
    assertEquals(1.0f - expectedDot, ip.distance(a, b), 0.01f);
  }

  @Test
  @DisplayName("High-dimensional: dim=1536")
  void testDimension1536() {
    final int dim = 1536;
    final float[] a = new float[dim];
    final float[] b = new float[dim];
    final Random rng = new Random(123);
    float expectedDot = 0.0f;
    for (int i = 0; i < dim; i++) {
      a[i] = rng.nextFloat();
      b[i] = rng.nextFloat();
      expectedDot += a[i] * b[i];
    }
    assertEquals(1.0f - expectedDot, ip.distance(a, b), 0.1f);
  }

  @Test
  @DisplayName("Zero vectors: dot = 0, distance = 1")
  void testZeroVectors() {
    final float[] a = {0.0f, 0.0f, 0.0f};
    final float[] b = {0.0f, 0.0f, 0.0f};
    assertEquals(1.0f, ip.distance(a, b), DELTA);
  }

  // ---- MemorySegment tests ----

  @Test
  @DisplayName("MemorySegment: known dot product matches float[] result")
  void testMemorySegmentKnownDotProduct() {
    final float[] a = {1.0f, 2.0f, 3.0f};
    final float[] b = {4.0f, 5.0f, 6.0f};

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segA = arena.allocate(ValueLayout.JAVA_FLOAT, a.length);
      final MemorySegment segB = arena.allocate(ValueLayout.JAVA_FLOAT, b.length);
      MemorySegment.copy(a, 0, segA, ValueLayout.JAVA_FLOAT, 0, a.length);
      MemorySegment.copy(b, 0, segB, ValueLayout.JAVA_FLOAT, 0, b.length);

      final float expected = ip.distance(a, b);
      final float actual = ip.distance(segA, segB, a.length);
      assertEquals(expected, actual, DELTA);
    }
  }

  @Test
  @DisplayName("MemorySegment: dim=128 matches float[] result")
  void testMemorySegmentDim128() {
    final int dim = 128;
    final float[] a = new float[dim];
    final float[] b = new float[dim];
    final Random rng = new Random(55);
    for (int i = 0; i < dim; i++) {
      a[i] = rng.nextFloat() * 2.0f - 1.0f;
      b[i] = rng.nextFloat() * 2.0f - 1.0f;
    }

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segA = arena.allocate(ValueLayout.JAVA_FLOAT, dim);
      final MemorySegment segB = arena.allocate(ValueLayout.JAVA_FLOAT, dim);
      MemorySegment.copy(a, 0, segA, ValueLayout.JAVA_FLOAT, 0, dim);
      MemorySegment.copy(b, 0, segB, ValueLayout.JAVA_FLOAT, 0, dim);

      final float expected = ip.distance(a, b);
      final float actual = ip.distance(segA, segB, dim);
      assertEquals(expected, actual, 0.01f);
    }
  }

  @Test
  @DisplayName("MemorySegment: non-aligned dim=13")
  void testMemorySegmentNonAligned() {
    final float[] a = new float[13];
    final float[] b = new float[13];
    for (int i = 0; i < 13; i++) {
      a[i] = i * 0.3f;
      b[i] = i * 0.7f;
    }

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segA = arena.allocate(ValueLayout.JAVA_FLOAT, 13);
      final MemorySegment segB = arena.allocate(ValueLayout.JAVA_FLOAT, 13);
      MemorySegment.copy(a, 0, segA, ValueLayout.JAVA_FLOAT, 0, 13);
      MemorySegment.copy(b, 0, segB, ValueLayout.JAVA_FLOAT, 0, 13);

      final float expected = ip.distance(a, b);
      final float actual = ip.distance(segA, segB, 13);
      assertEquals(expected, actual, DELTA);
    }
  }

  // ---- VectorDistanceType integration ----

  @Test
  @DisplayName("VectorDistanceType.INNER_PRODUCT returns working function")
  void testVectorDistanceTypeInnerProduct() {
    final DistanceFunction fn = VectorDistanceType.INNER_PRODUCT.getDistanceFunction(3);
    final float[] a = {1.0f, 0.0f, 0.0f};
    final float[] b = {0.0f, 1.0f, 0.0f};
    assertEquals(1.0f, fn.distance(a, b), DELTA);
  }

  // ---- Helper ----

  private static float[] normalize(final float[] v) {
    float norm = 0.0f;
    for (final float f : v) {
      norm += f * f;
    }
    norm = (float) Math.sqrt(norm);
    final float[] result = new float[v.length];
    for (int i = 0; i < v.length; i++) {
      result[i] = v[i] / norm;
    }
    return result;
  }
}
