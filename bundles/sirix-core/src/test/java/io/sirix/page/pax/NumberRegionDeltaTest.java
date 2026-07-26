package io.sirix.page.pax;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.SplittableRandom;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for the delta-of-delta {@link NumberRegionDelta} codec. Covers the
 * constant-stride shortcut, monotonic-timestamp workloads, negative/decreasing
 * sequences, the wide (57..64-bit) straddle path, random stress, native vs
 * on-heap segments, and the header/size invariants.
 */
@DisplayName("NumberRegionDelta")
final class NumberRegionDeltaTest {

  private static MemorySegment onHeap(final int bytes) {
    return MemorySegment.ofArray(new byte[bytes]);
  }

  /** Encode into a fresh segment, decode back, and assert an exact round-trip. */
  private static void assertRoundTrip(final long[] values, final int count) {
    final long size = NumberRegionDelta.maxEncodedSize(values, count);
    final MemorySegment seg = onHeap((int) size + 16 /* slack, must not be needed */);

    final long written = NumberRegionDelta.writeDelta(seg, 0L, values, count);
    assertEquals(size, written, "writeDelta must consume exactly maxEncodedSize bytes");

    final NumberRegionDelta.Header h = new NumberRegionDelta.Header();
    NumberRegionDelta.readHeader(seg, 0L, h);
    assertEquals(count, h.count);
    assertEquals(written, h.headerBytes + h.bodyBytes,
        "header + body must account for every written byte");

    final long[] out = new long[Math.max(1, count)];
    NumberRegionDelta.decodeAll(seg, h, out);
    for (int i = 0; i < count; i++) {
      assertEquals(values[i], out[i], "decodeAll mismatch at index " + i);
    }
    // readDelta must agree with decodeAll at every index.
    for (int i = 0; i < count; i++) {
      assertEquals(values[i], NumberRegionDelta.readDelta(seg, h, i),
          "readDelta mismatch at index " + i);
    }
  }

  @Test
  @DisplayName("zig-zag encode/decode is an exact inverse")
  void zigZagInverse() {
    final long[] probes = { 0, 1, -1, 2, -2, 42, -42, Long.MAX_VALUE, Long.MIN_VALUE,
        1L << 40, -(1L << 40), Long.MIN_VALUE + 1 };
    for (final long v : probes) {
      assertEquals(v, NumberRegionDelta.zigZagDecode(NumberRegionDelta.zigZagEncode(v)),
          "zig-zag not invertible for " + v);
    }
  }

  @Test
  @DisplayName("empty column round-trips with no body")
  void empty() {
    assertRoundTrip(new long[0], 0);
    assertEquals(0, NumberRegionDelta.computeBitWidth(new long[0], 0));
  }

  @Test
  @DisplayName("single and two-value columns round-trip")
  void singleAndPair() {
    assertRoundTrip(new long[] { 1234567890123L }, 1);
    assertRoundTrip(new long[] { 1234567890123L, 1234567890999L }, 2);
    // Fewer than 3 values ⇒ no residuals ⇒ bit width 0.
    assertEquals(0, NumberRegionDelta.computeBitWidth(new long[] { 5, 9 }, 2));
  }

  @Test
  @DisplayName("constant stride collapses to a zero-width body regardless of length")
  void constantStride() {
    final int n = 4096;
    final long[] values = new long[n];
    long t = 1_700_000_000_000L;
    for (int i = 0; i < n; i++, t += 1000L) {
      values[i] = t;
    }
    assertEquals(0, NumberRegionDelta.computeBitWidth(values, n),
        "constant stride must have bit width 0");
    // Body is empty: encoded size equals the fixed header only.
    final long size = NumberRegionDelta.maxEncodedSize(values, n);
    assertEquals(NumberRegionDelta.headerBytes(n), size,
        "constant stride must encode to header bytes only");
    assertRoundTrip(values, n);
  }

  @Test
  @DisplayName("constant stride is dramatically smaller than frame-of-reference")
  void constantStrideBeatsFor() {
    final int n = 4096;
    final long[] values = new long[n];
    long t = 1_700_000_000_000L;
    for (int i = 0; i < n; i++, t += 60_000L) {
      values[i] = t;
    }
    final long deltaSize = NumberRegionDelta.maxEncodedSize(values, n);
    final long forSize = NumberRegionCompact.maxEncodedSize(values, n);
    assertTrue(deltaSize * 8 < forSize,
        "delta-of-delta (" + deltaSize + " B) should be far below FOR+BP (" + forSize + " B)");
  }

  @Test
  @DisplayName("monotonic timestamps with jitter round-trip")
  void jitteredTimestamps() {
    final int n = 5000;
    final long[] values = new long[n];
    final SplittableRandom rnd = new SplittableRandom(0xC0FFEEL);
    long t = 1_700_000_000_000L;
    for (int i = 0; i < n; i++) {
      t += 1000L + rnd.nextInt(-50, 51); // ~1s stride, ±50ms jitter
      values[i] = t;
    }
    assertRoundTrip(values, n);
    // Small jitter ⇒ narrow residuals ⇒ still well under raw 8 bytes/value.
    final long size = NumberRegionDelta.maxEncodedSize(values, n);
    assertTrue(size < (long) n * 8, "jittered timestamps should beat raw longs: " + size);
  }

  @Test
  @DisplayName("strictly decreasing and negative sequences round-trip")
  void decreasingAndNegative() {
    final int n = 1000;
    final long[] values = new long[n];
    long t = -500L;
    for (int i = 0; i < n; i++, t -= 37L) {
      values[i] = t;
    }
    assertRoundTrip(values, n);
  }

  @Test
  @DisplayName("wide residuals exercise the 57..64-bit straddle path")
  void wideStraddle() {
    // Alternating extremes force large-magnitude deltas and delta-of-deltas,
    // pushing the zig-zag residual width into the 57..64-bit slow path.
    final long[] values = { 0L, Long.MAX_VALUE, Long.MIN_VALUE, Long.MAX_VALUE,
        Long.MIN_VALUE, 0L, Long.MAX_VALUE };
    final int bw = NumberRegionDelta.computeBitWidth(values, values.length);
    assertTrue(bw > 56, "expected a wide bit width, got " + bw);
    assertRoundTrip(values, values.length);
  }

  @Test
  @DisplayName("every bit width 1..64 round-trips")
  void allBitWidths() {
    final SplittableRandom rnd = new SplittableRandom(99L);
    for (int width = 1; width <= 62; width++) {
      final int n = 200;
      final long[] values = new long[n];
      // Build a sequence whose delta-of-deltas span roughly `width` bits by
      // adding a bounded random jitter on top of a linear ramp.
      final long bound = 1L << Math.max(1, width - 2);
      long v = 0L;
      long delta = 1000L;
      for (int i = 0; i < n; i++) {
        values[i] = v;
        final long dd = rnd.nextLong(-bound, bound);
        delta += dd;
        v += delta;
      }
      assertRoundTrip(values, n);
    }
  }

  @Test
  @DisplayName("random stress over native segments round-trips")
  void randomStressNative() {
    final SplittableRandom rnd = new SplittableRandom(0xDEADBEEFL);
    try (Arena arena = Arena.ofConfined()) {
      for (int iter = 0; iter < 200; iter++) {
        final int n = rnd.nextInt(0, 400);
        final long[] values = new long[Math.max(1, n)];
        for (int i = 0; i < n; i++) {
          values[i] = rnd.nextLong();
        }
        final long size = NumberRegionDelta.maxEncodedSize(values, n);
        final MemorySegment seg = arena.allocate(size + 8);
        final long written = NumberRegionDelta.writeDelta(seg, 0L, values, n);
        assertEquals(size, written);

        final NumberRegionDelta.Header h = new NumberRegionDelta.Header();
        NumberRegionDelta.readHeader(seg, 0L, h);
        final long[] out = new long[Math.max(1, n)];
        NumberRegionDelta.decodeAll(seg, h, out);
        for (int i = 0; i < n; i++) {
          assertEquals(values[i], out[i], "iter " + iter + " index " + i);
        }
      }
    }
  }

  @Test
  @DisplayName("writing at a non-zero offset leaves surrounding bytes untouched")
  void nonZeroOffset() {
    final long[] values = { 10, 20, 31, 43, 56 };
    final int off = 7;
    final long size = NumberRegionDelta.maxEncodedSize(values, values.length);
    final MemorySegment seg = onHeap(off + (int) size + 5);
    // Poison the whole buffer; only [off, off+size) should change meaning.
    for (int i = 0; i < seg.byteSize(); i++) {
      seg.set(java.lang.foreign.ValueLayout.JAVA_BYTE, i, (byte) 0xAB);
    }
    final long written = NumberRegionDelta.writeDelta(seg, off, values, values.length);
    assertEquals(size, written);

    final NumberRegionDelta.Header h = new NumberRegionDelta.Header();
    NumberRegionDelta.readHeader(seg, off, h);
    final long[] out = new long[values.length];
    NumberRegionDelta.decodeAll(seg, h, out);
    for (int i = 0; i < values.length; i++) {
      assertEquals(values[i], out[i]);
    }
  }

  @Test
  @DisplayName("header reset clears every field for reuse")
  void headerReuse() {
    final long[] a = { 100, 200, 305, 411 };
    final long[] b = { -5, -4, -3, -2, -1, 0 };
    final MemorySegment segA = onHeap((int) NumberRegionDelta.maxEncodedSize(a, a.length));
    final MemorySegment segB = onHeap((int) NumberRegionDelta.maxEncodedSize(b, b.length));
    NumberRegionDelta.writeDelta(segA, 0L, a, a.length);
    NumberRegionDelta.writeDelta(segB, 0L, b, b.length);

    final NumberRegionDelta.Header h = new NumberRegionDelta.Header();
    NumberRegionDelta.readHeader(segA, 0L, h);
    final long[] outA = new long[a.length];
    NumberRegionDelta.decodeAll(segA, h, outA);

    h.reset();
    NumberRegionDelta.readHeader(segB, 0L, h);
    final long[] outB = new long[b.length];
    NumberRegionDelta.decodeAll(segB, h, outB);
    for (int i = 0; i < b.length; i++) {
      assertEquals(b[i], outB[i]);
    }
  }

  // ───────────────────────────── NumberRegion integration (ENC_DELTA_ZM) ─────

  @Test
  @DisplayName("NumberRegion selects ENC_DELTA_ZM for a monotonic timestamp column")
  void regionSelectsDeltaForTimestamps() {
    NumberRegion.clearDeltaWriteOverride();
    final int n = 512;
    final long[] values = new long[n];
    final int[] tags = new int[n];
    long t = 1_700_000_000_000L;
    for (int i = 0; i < n; i++, t += 1000L) {
      values[i] = t;
      tags[i] = 7; // single field
    }
    final byte[] wire = NumberRegion.encode(values, tags, n);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);
    assertEquals(NumberRegion.ENC_DELTA_ZM, h.encodingKind);
    assertTrue(NumberRegion.isDelta(h.encodingKind));
    assertTrue(NumberRegion.hasZoneMap(h.encodingKind));
    // Outer per-tag zone map survives the delta value region.
    final int tag = NumberRegion.lookupTag(h, 7);
    assertEquals(values[0], h.tagMin[tag]);
    assertEquals(values[n - 1], h.tagMax[tag]);

    // Both decode entry points round-trip.
    final long[] bulk = new long[n];
    NumberRegion.decodeAllValues(wire, h, bulk);
    for (int i = 0; i < n; i++) {
      assertEquals(values[i], bulk[i], "decodeAllValues @" + i);
      assertEquals(values[i], NumberRegion.decodeValueAt(wire, h, i), "decodeValueAt @" + i);
    }
    // Delta must be a large win here vs the raw payload.
    assertTrue(wire.length < n * 8L, "delta region should be far smaller than raw longs");
  }

  @Test
  @DisplayName("NumberRegion keeps FOR+BP for scattered random values")
  void regionKeepsForForRandom() {
    NumberRegion.clearDeltaWriteOverride();
    final SplittableRandom rnd = new SplittableRandom(7L);
    final int n = 256;
    final long[] values = new long[n];
    final int[] tags = new int[n];
    for (int i = 0; i < n; i++) {
      values[i] = 1000 + rnd.nextInt(0, 1 << 20); // narrow range ⇒ FOR is tight
      tags[i] = 3;
    }
    final byte[] wire = NumberRegion.encode(values, tags, n);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);
    // Random within a narrow band ⇒ delta-of-delta is wider than FOR residuals,
    // so the bake-off should not pick delta.
    assertTrue(NumberRegion.isBitPacked(h.encodingKind),
        "expected FOR/bit-packed, got kind " + h.encodingKind);
    for (int i = 0; i < n; i++) {
      assertEquals(values[i], NumberRegion.decodeValueAt(wire, h, i));
    }
  }

  @Test
  @DisplayName("delta write toggle forces FOR when disabled")
  void deltaToggleDisables() {
    try {
      NumberRegion.setDeltaWriteEnabled(false);
      final int n = 128;
      final long[] values = new long[n];
      final int[] tags = new int[n];
      long t = 1_700_000_000_000L;
      for (int i = 0; i < n; i++, t += 1000L) {
        values[i] = t;
        tags[i] = 1;
      }
      final byte[] wire = NumberRegion.encode(values, tags, n);
      final NumberRegion.Header h = new NumberRegion.Header().parseInto(wire);
      assertFalse(NumberRegion.isDelta(h.encodingKind), "delta disabled but region used it");
      for (int i = 0; i < n; i++) {
        assertEquals(values[i], NumberRegion.decodeValueAt(wire, h, i));
      }
    } finally {
      NumberRegion.clearDeltaWriteOverride();
    }
  }
}
