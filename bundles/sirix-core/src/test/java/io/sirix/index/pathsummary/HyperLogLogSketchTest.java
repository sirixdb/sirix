package io.sirix.index.pathsummary;

import io.sirix.index.path.summary.HyperLogLogSketch;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class HyperLogLogSketchTest {

  @Test
  void emptySketch_estimatesZero() {
    var sketch = new HyperLogLogSketch();
    assertEquals(0L, sketch.estimate());
  }

  @Test
  void smallCardinality_linearCountingIsNearExact() {
    var sketch = new HyperLogLogSketch();
    for (long i = 0; i < 10; i++) {
      sketch.add(i);
    }
    final long est = sketch.estimate();
    assertTrue(Math.abs(est - 10) <= 2, "estimate " + est + " should be near 10");
  }

  @Test
  void moderateCardinality_within5Percent() {
    var sketch = new HyperLogLogSketch();
    final int n = 10_000;
    for (long i = 0; i < n; i++) {
      sketch.add(i);
    }
    final long est = sketch.estimate();
    final double err = Math.abs(est - n) / (double) n;
    assertTrue(err < 0.05, "error " + err + " from estimate " + est);
  }

  @Test
  void largeCardinality_withinExpectedStdError() {
    var sketch = new HyperLogLogSketch();
    final int n = 1_000_000;
    final ThreadLocalRandom rng = ThreadLocalRandom.current();
    for (int i = 0; i < n; i++) {
      sketch.add(rng.nextLong());
    }
    // Expected std error ~2.3% for m=2048; tolerate ±5% (roughly 2 sigma).
    final long est = sketch.estimate();
    final double err = Math.abs(est - n) / (double) n;
    assertTrue(err < 0.05, "error " + err + " from estimate " + est);
  }

  @Test
  void duplicatesDoNotInflateEstimate() {
    var sketch = new HyperLogLogSketch();
    for (int r = 0; r < 10; r++) {
      for (long i = 0; i < 1_000; i++) {
        sketch.add(i);
      }
    }
    final long est = sketch.estimate();
    assertTrue(est > 900 && est < 1100, "estimate " + est + " not near 1000");
  }

  @Test
  void byteAddsAreStableAcrossSketches() {
    var a = new HyperLogLogSketch();
    var b = new HyperLogLogSketch();
    for (int i = 0; i < 1_000; i++) {
      final byte[] bytes = Integer.toString(i).getBytes(StandardCharsets.UTF_8);
      a.add(bytes);
      b.add(bytes);
    }
    // Identical sequence of adds → identical register state → identical estimate.
    assertEquals(a.estimate(), b.estimate());
  }

  @Test
  void unionApproximatesDisjointSum() {
    var a = new HyperLogLogSketch();
    var b = new HyperLogLogSketch();
    for (long i = 0; i < 5_000; i++) {
      a.add(i);
    }
    for (long i = 5_000; i < 10_000; i++) {
      b.add(i);
    }
    a.union(b);
    final long est = a.estimate();
    final double err = Math.abs(est - 10_000) / 10_000.0;
    assertTrue(err < 0.05, "union error " + err + " from estimate " + est);
  }

  @Test
  void unionWithSameSetIsIdempotent() {
    var a = new HyperLogLogSketch();
    var b = new HyperLogLogSketch();
    for (long i = 0; i < 5_000; i++) {
      a.add(i);
      b.add(i);
    }
    final long before = a.estimate();
    a.union(b);
    final long after = a.estimate();
    assertEquals(before, after);
  }

  @Test
  void serializeRoundTripPreservesEstimate() {
    var original = new HyperLogLogSketch();
    for (long i = 0; i < 5_000; i++) {
      original.add(i);
    }
    final byte[] bytes = original.serialize();
    assertEquals(HyperLogLogSketch.BYTES, bytes.length);
    final var restored = HyperLogLogSketch.deserialize(bytes);
    assertEquals(original.estimate(), restored.estimate());
  }

  @Test
  void deserializeRejectsWrongLength() {
    assertThrows(IllegalArgumentException.class,
        () -> HyperLogLogSketch.deserialize(new byte[HyperLogLogSketch.BYTES + 1]));
    assertThrows(IllegalArgumentException.class,
        () -> HyperLogLogSketch.deserialize(new byte[0]));
  }
}
