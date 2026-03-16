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
 * Tests for {@link SimdCosineDistance}.
 */
@DisplayName("SimdCosineDistance Tests")
class SimdCosineDistanceTest {

  private static final float DELTA = 1e-4f;

  private final DistanceFunction cosine = new SimdCosineDistance();

  // ---- float[] tests ----

  @Test
  @DisplayName("Identical vectors -> 0.0")
  void testIdenticalVectors() {
    final float[] a = {1.0f, 2.0f, 3.0f};
    assertEquals(0.0f, cosine.distance(a, a.clone()), DELTA);
  }

  @Test
  @DisplayName("Opposite vectors -> 2.0")
  void testOppositeVectors() {
    final float[] a = {1.0f, 0.0f, 0.0f};
    final float[] b = {-1.0f, 0.0f, 0.0f};
    assertEquals(2.0f, cosine.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Orthogonal vectors -> 1.0")
  void testOrthogonalVectors() {
    final float[] a = {1.0f, 0.0f, 0.0f};
    final float[] b = {0.0f, 1.0f, 0.0f};
    assertEquals(1.0f, cosine.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Zero vector -> 1.0 (edge case)")
  void testZeroVector() {
    final float[] a = {0.0f, 0.0f, 0.0f};
    final float[] b = {1.0f, 2.0f, 3.0f};
    assertEquals(1.0f, cosine.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Both zero vectors -> 1.0")
  void testBothZeroVectors() {
    final float[] a = {0.0f, 0.0f, 0.0f};
    assertEquals(1.0f, cosine.distance(a, a.clone()), DELTA);
  }

  @Test
  @DisplayName("Parallel vectors with different magnitude -> 0.0")
  void testParallelVectors() {
    final float[] a = {1.0f, 2.0f, 3.0f};
    final float[] b = {2.0f, 4.0f, 6.0f};
    assertEquals(0.0f, cosine.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Known angle: 45-degree vectors")
  void testKnownAngle() {
    // cos(45deg) = 1/sqrt(2) ~ 0.7071, distance ~ 0.2929
    final float[] a = {1.0f, 0.0f};
    final float[] b = {1.0f, 1.0f};
    final float expected = 1.0f - (float) (1.0 / Math.sqrt(2.0));
    assertEquals(expected, cosine.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Dimension 7 (non-SIMD-aligned)")
  void testDimension7() {
    final float[] a = {1, 0, 1, 0, 1, 0, 1};
    final float[] b = {0, 1, 0, 1, 0, 1, 0};
    // Orthogonal => distance 1.0
    assertEquals(1.0f, cosine.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("Dimension 13 (non-SIMD-aligned)")
  void testDimension13() {
    final float[] a = new float[13];
    final float[] b = new float[13];
    for (int i = 0; i < 13; i++) {
      a[i] = (i + 1) * 0.1f;
      b[i] = (i + 1) * 0.1f;
    }
    // Identical direction => distance ~ 0.0
    assertEquals(0.0f, cosine.distance(a, b), DELTA);
  }

  @Test
  @DisplayName("High-dimensional: dim=128")
  void testDimension128() {
    final int dim = 128;
    final float[] a = new float[dim];
    final float[] b = new float[dim];
    final Random rng = new Random(42);
    for (int i = 0; i < dim; i++) {
      a[i] = rng.nextFloat();
      b[i] = rng.nextFloat();
    }
    final float result = cosine.distance(a, b);
    assertTrue(result >= 0.0f && result <= 2.0f, "Cosine distance must be in [0, 2]");
  }

  @Test
  @DisplayName("High-dimensional: dim=1536")
  void testDimension1536() {
    final int dim = 1536;
    final float[] a = new float[dim];
    final float[] b = new float[dim];
    final Random rng = new Random(123);
    for (int i = 0; i < dim; i++) {
      a[i] = rng.nextFloat() * 2.0f - 1.0f;
      b[i] = rng.nextFloat() * 2.0f - 1.0f;
    }
    final float result = cosine.distance(a, b);
    assertTrue(result >= 0.0f && result <= 2.0f, "Cosine distance must be in [0, 2]");
  }

  @Test
  @DisplayName("Symmetry: d(a,b) == d(b,a)")
  void testSymmetry() {
    final float[] a = {1.0f, 3.0f, -2.0f, 5.0f};
    final float[] b = {-1.0f, 2.0f, 4.0f, -3.0f};
    assertEquals(cosine.distance(a, b), cosine.distance(b, a), DELTA);
  }

  // ---- MemorySegment tests ----

  @Test
  @DisplayName("MemorySegment: orthogonal vectors -> 1.0")
  void testMemorySegmentOrthogonal() {
    final float[] a = {1.0f, 0.0f, 0.0f};
    final float[] b = {0.0f, 1.0f, 0.0f};

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segA = arena.allocate(ValueLayout.JAVA_FLOAT, a.length);
      final MemorySegment segB = arena.allocate(ValueLayout.JAVA_FLOAT, b.length);
      MemorySegment.copy(a, 0, segA, ValueLayout.JAVA_FLOAT, 0, a.length);
      MemorySegment.copy(b, 0, segB, ValueLayout.JAVA_FLOAT, 0, b.length);

      assertEquals(1.0f, cosine.distance(segA, segB, a.length), DELTA);
    }
  }

  @Test
  @DisplayName("MemorySegment: dim=128 matches float[] result")
  void testMemorySegmentDim128() {
    final int dim = 128;
    final float[] a = new float[dim];
    final float[] b = new float[dim];
    final Random rng = new Random(77);
    for (int i = 0; i < dim; i++) {
      a[i] = rng.nextFloat() * 2.0f - 1.0f;
      b[i] = rng.nextFloat() * 2.0f - 1.0f;
    }

    try (final Arena arena = Arena.ofConfined()) {
      final MemorySegment segA = arena.allocate(ValueLayout.JAVA_FLOAT, dim);
      final MemorySegment segB = arena.allocate(ValueLayout.JAVA_FLOAT, dim);
      MemorySegment.copy(a, 0, segA, ValueLayout.JAVA_FLOAT, 0, dim);
      MemorySegment.copy(b, 0, segB, ValueLayout.JAVA_FLOAT, 0, dim);

      final float expected = cosine.distance(a, b);
      final float actual = cosine.distance(segA, segB, dim);
      assertEquals(expected, actual, DELTA);
    }
  }

  // ---- VectorDistanceType integration ----

  @Test
  @DisplayName("VectorDistanceType.COSINE returns working function")
  void testVectorDistanceTypeCosine() {
    final DistanceFunction fn = VectorDistanceType.COSINE.getDistanceFunction(3);
    final float[] a = {1.0f, 0.0f, 0.0f};
    final float[] b = {0.0f, 1.0f, 0.0f};
    assertEquals(1.0f, fn.distance(a, b), DELTA);
  }
}
