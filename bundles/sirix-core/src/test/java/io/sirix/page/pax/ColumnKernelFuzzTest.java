/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.VectorOperators;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Randomized differential test: every kernel against ground truth computed from the original
 * values, not from any codec.
 *
 * <h2>Why fuzzing rather than more fixtures</h2>
 *
 * <p>
 * The hand-written equivalence tests in {@link ColumnKernelEquivalenceTest} check the cases their
 * author thought of, and every defect this file was written in response to lived in a case nobody
 * thought of: a {@code NE} paired with a range operator, a delta residual wider than the vector
 * unpack serves, a dict id past the end of a 64-entry set word. Those are not exotic inputs — they
 * are ordinary combinations of parameters that no fixture happened to combine.
 *
 * <p>
 * So this test does not choose inputs. It draws them: value distributions across every encoding the
 * writer can pick, every operator pair including the ones that are not intervals, thresholds placed
 * on and around the actual data, ranges that start and end at arbitrary offsets, and liveness
 * bitmaps with arbitrary holes.
 *
 * <h2>Ground truth is independent</h2>
 *
 * <p>
 * The expected answer is computed by looping over the {@code long[]} that was handed to the
 * encoder, with a plain {@code if}. It never consults a header, a decoder, or a second kernel. A
 * differential test whose two sides share a decoder can only find disagreements between kernels,
 * and both sides of this package's worst bug — a scalar tail and a vector body that disagreed —
 * would have passed such a test on the half that ran.
 *
 * <h2>Both paths, every time</h2>
 *
 * <p>
 * Each case runs twice, once with the vector path forced on and once with it held off, because the
 * kernels contain two implementations of every predicate and a bug in either is a wrong answer.
 * Which one a given range takes in production depends on a warmup budget, so a test that did not
 * pin it would sample them arbitrarily.
 */
@DisplayName("column kernels, fuzzed against independent ground truth")
final class ColumnKernelFuzzTest {

  /** Cases per encoding per path. Large enough to reach odd corners, small enough to stay quick. */
  private static final int CASES = 300;

  @AfterEach
  void restoreWarmup() {
    BitUnpackSimd.resetWarmupForTesting();
  }


  /** Every comparison the kernels accept, including the one that is not an interval. */
  private static final VectorOperators.Comparison[] OPS = {VectorOperators.GT, VectorOperators.GE, VectorOperators.LT,
      VectorOperators.LE, VectorOperators.EQ, VectorOperators.NE};

  private static boolean eval(final long v, final VectorOperators.Comparison op, final long t) {
    if (op == VectorOperators.GT) {
      return v > t;
    }
    if (op == VectorOperators.GE) {
      return v >= t;
    }
    if (op == VectorOperators.LT) {
      return v < t;
    }
    if (op == VectorOperators.LE) {
      return v <= t;
    }
    if (op == VectorOperators.EQ) {
      return v == t;
    }
    return v != t;
  }

  /** Value shapes chosen so the encoder picks a different encoding for each. */
  private enum Shape {
    /** Wide random longs — stays PLAIN_LONG. */
    PLAIN,
    /** Narrow band above a large base — frame-of-reference bit-packing. */
    BIT_PACKED,
    /** Near-constant stride — delta-of-delta. */
    DELTA,
    /** Exactly constant stride — delta with no residual body at all. */
    DELTA_FLAT,
    /** Alternating huge jumps — delta with a residual width past the vector unpack's reach. */
    DELTA_WIDE,
    /** All values identical — degenerate bounds, zero-width packing. */
    CONSTANT
  }

  private static long[] draw(final Shape shape, final int n, final Random rng) {
    final long[] v = new long[n];
    switch (shape) {
      case PLAIN -> {
        for (int i = 0; i < n; i++) {
          v[i] = rng.nextLong();
        }
      }
      case BIT_PACKED -> {
        final long base = 1_000_000L + rng.nextInt(1000);
        final int span = 1 + rng.nextInt(4096);
        for (int i = 0; i < n; i++) {
          v[i] = base + rng.nextInt(span);
        }
      }
      case DELTA -> {
        long c = rng.nextInt(1_000_000);
        for (int i = 0; i < n; i++) {
          c += 100 + rng.nextInt(5);
          v[i] = c;
        }
      }
      case DELTA_FLAT -> {
        final long stride = 1 + rng.nextInt(64);
        long c = rng.nextInt(1000);
        for (int i = 0; i < n; i++) {
          v[i] = c;
          c += stride;
        }
      }
      case DELTA_WIDE -> {
        // Alternating between 0 and a huge value makes each delta-of-delta enormous, so the
        // residual width lands above what the vector unpack serves and the scalar unpacker becomes
        // the only decoder — the case that was silently wrong.
        for (int i = 0; i < n; i++) {
          v[i] = (i & 1) == 0
              ? 0L
              : (1L << 60);
        }
      }
      case CONSTANT -> {
        final long c = rng.nextLong();
        java.util.Arrays.fill(v, c);
      }
    }
    return v;
  }

  /** A threshold drawn from the data, its edges, and the extremes that overflow a bound. */
  private static long drawThreshold(final long[] values, final Random rng) {
    return switch (rng.nextInt(8)) {
      case 0 -> Long.MIN_VALUE;
      case 1 -> Long.MAX_VALUE;
      case 2 -> values[rng.nextInt(values.length)];
      case 3 -> values[rng.nextInt(values.length)] + 1;
      case 4 -> values[rng.nextInt(values.length)] - 1;
      case 5 -> 0L;
      case 6 -> rng.nextLong();
      default -> values[rng.nextInt(values.length)];
    };
  }

  // ────────────────────────────────────────────────────────────── number kernels

  @Test
  @DisplayName("count, masked count, aggregate and selection agree with a hand-written loop")
  void numberKernelsAgreeWithGroundTruth() {
    for (final boolean vector : new boolean[] {true, false}) {
      BitUnpackSimd.setWarmupRemainingForTesting(vector
          ? 0
          : Integer.MAX_VALUE);
      for (final Shape shape : Shape.values()) {
        fuzzNumbers(shape, vector);
      }
    }
  }

  private void fuzzNumbers(final Shape shape, final boolean vector) {
    final Random rng = new Random(shape.ordinal() * 7919L + (vector
        ? 1
        : 2));
    for (int c = 0; c < CASES; c++) {
      final int n = 1 + rng.nextInt(400);
      final int tagCount = 1 + rng.nextInt(3);
      final long[] values = draw(shape, n, rng);
      final int[] tags = new int[n];
      for (int i = 0; i < n; i++) {
        tags[i] = i % tagCount;
      }
      final byte[] encoded = NumberRegion.encode(values, tags, n);
      if (encoded == null || encoded.length == 0) {
        continue;
      }
      final MemorySegment payload = PaxTestSegments.of(encoded);
      final NumberRegion.Header h = new NumberRegion.Header().parseInto(payload);

      for (int tag = 0; tag < h.dictSize; tag++) {
        checkTag(shape, vector, c, payload, h, tag, values, tags, n, rng);
      }
    }
  }

  /** Every kernel over one tag, against the ground truth built from the input values. */
  private void checkTag(final Shape shape, final boolean vector, final int c, final MemorySegment payload,
      final NumberRegion.Header h, final int tag, final long[] values, final int[] tags, final int n,
      final Random rng) {
    {
      // Ground truth for this tag: the input values carrying it, in input order. The region
      // stores them contiguously in that same order, which is the only thing about the encoding
      // this test assumes.
      final int tagValue = h.dict[tag];
      final long[] expected = new long[h.tagCount[tag]];
      int e = 0;
      for (int i = 0; i < n; i++) {
        if (tags[i] == tagValue) {
          expected[e++] = values[i];
        }
      }
      assertEquals(h.tagCount[tag], e, describe(shape, vector, c) + ": tag population");

      final int start = h.tagStart[tag];
      final int end = start + h.tagCount[tag];

      // The scalar decoder must reproduce the values exactly — this is what catches a decoder
      // that drops the high bits of a wide value.
      for (int i = 0; i < expected.length; i++) {
        assertEquals(expected[i], NumberRegion.decodeValueAt(payload, h, start + i),
            describe(shape, vector, c) + ": decodeValueAt at " + i);
      }

      final VectorOperators.Comparison op1 = OPS[rng.nextInt(OPS.length)];
      final VectorOperators.Comparison op2 = OPS[rng.nextInt(OPS.length)];
      final long t1 = drawThreshold(values, rng);
      final long t2 = drawThreshold(values, rng);

      long want = 0;
      for (final long v : expected) {
        if (eval(v, op1, t1) && eval(v, op2, t2)) {
          want++;
        }
      }
      final long got = NumberRegionSimd.countMatchingRange(payload, h, start, end, op1, t1, op2, t2);
      assertTrue(got >= 0, describe(shape, vector, c) + ": kernel declined a supported shape");
      assertEquals(want, got, describe(shape, vector, c) + ": count " + op1 + " " + t1 + " AND " + op2 + " " + t2);

      // Single-predicate entry point.
      long wantOne = 0;
      for (final long v : expected) {
        if (eval(v, op1, t1)) {
          wantOne++;
        }
      }
      assertEquals(wantOne, NumberRegionSimd.countMatching(payload, h, start, end, op1, t1),
          describe(shape, vector, c) + ": single-predicate count " + op1 + " " + t1);

      // Masked count, with an arbitrary liveness pattern.
      final int len = expected.length;
      final long[] live = new long[Math.max(1, (len + 63) >>> 6)];
      long wantLive = 0;
      for (int i = 0; i < len; i++) {
        if (rng.nextBoolean()) {
          live[i >>> 6] |= 1L << (i & 63);
          if (eval(expected[i], op1, t1) && eval(expected[i], op2, t2)) {
            wantLive++;
          }
        }
      }
      assertEquals(wantLive, NumberRegionSimd.countMatchingRangeMasked(payload, h, start, end, op1, t1, op2, t2, live),
          describe(shape, vector, c) + ": masked count");

      checkAggregates(shape, vector, c, payload, h, start, end, expected);
      checkPerEncodingEntryPoints(shape, vector, c, payload, h, start, end, op1, t1, op2, t2, live, want, wantLive);
      checkSelection(shape, vector, c, payload, h, start, end, op1, t1, op2, t2, expected);
    }
  }

  /** Sum, min and max over one tag, against the ground truth. */
  private static void checkAggregates(final Shape shape, final boolean vector, final int c, final MemorySegment payload,
      final NumberRegion.Header h, final int start, final int end, final long[] expected) {
    final long[] out = new long[3];
    assertTrue(NumberRegionSimd.aggregateRange(payload, h, start, end, out),
        describe(shape, vector, c) + ": aggregate declined");
    long sum = 0;
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (final long v : expected) {
      sum += v;
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    if (expected.length > 0) {
      assertEquals(sum, out[0], describe(shape, vector, c) + ": sum");
      assertEquals(min, out[1], describe(shape, vector, c) + ": min");
      assertEquals(max, out[2], describe(shape, vector, c) + ": max");
    }
  }

  /**
   * The per-encoding entry points, called directly. These are public API and do NOT go through the
   * dispatcher, so a defect confined to one of them is invisible to every other assertion here —
   * which is exactly how a NE paired with a range operator came to silently discard the range.
   */
  private static void checkPerEncodingEntryPoints(final Shape shape, final boolean vector, final int c,
      final MemorySegment payload, final NumberRegion.Header h, final int start, final int end,
      final VectorOperators.Comparison op1, final long t1, final VectorOperators.Comparison op2, final long t2,
      final long[] live, final long want, final long wantLive) {
    if (h.isPerTag()) {
      // A per-tag payload has no page-wide frame at all: each tag's values are packed at its own
      // width in its own byte-aligned run, so the direct entry points are called with THAT frame
      // and tag-relative indices. Reaching for h.valueBytesOffset here is exactly the mistake the
      // sentinel makes loud, and this branch is the one that proves the per-tag frame is right.
      final int tag = NumberRegion.tagOfIndex(h, start);
      final int width = h.tagWidth[tag] & 0xFF;
      final int from = start - h.tagStart[tag];
      final int to = end - h.tagStart[tag];
      if (width >= 1 && width <= BitUnpackSimd.MAX_BIT_WIDTH) {
        assertEquals(want, NumberRegionSimd.countBitPackedRange(payload, h.tagValueOffset[tag], h.tagDecodeBase[tag],
            width, from, to, op1, t1, op2, t2), describe(shape, vector, c) + ": per-tag countBitPackedRange");
        assertEquals(
            wantLive, NumberRegionSimd.countBitPackedRangeMasked(payload, h.tagValueOffset[tag], h.tagDecodeBase[tag],
                width, from, to, op1, t1, op2, t2, live),
            describe(shape, vector, c) + ": per-tag countBitPackedRangeMasked");
      } else if (width == 64) {
        assertEquals(want,
            NumberRegionSimd.countPlainLongRange(payload, h.tagValueOffset[tag], from, to, op1, t1, op2, t2),
            describe(shape, vector, c) + ": per-tag countPlainLongRange");
        assertEquals(wantLive, NumberRegionSimd.countPlainLongRangeMasked(payload, h.tagValueOffset[tag], from, to, op1,
            t1, op2, t2, live), describe(shape, vector, c) + ": per-tag countPlainLongRangeMasked");
      }
      return;
    }
    if (NumberRegion.isBitPacked(h.encodingKind)) {
      assertEquals(want,
          NumberRegionSimd.countBitPackedRange(payload, h.valueBytesOffset, h.valueBase, h.valueBitWidth, start, end,
              op1, t1, op2, t2),
          describe(shape, vector, c) + ": countBitPackedRange " + op1 + " " + t1 + " AND " + op2 + " " + t2);
      assertEquals(
          wantLive, NumberRegionSimd.countBitPackedRangeMasked(payload, h.valueBytesOffset, h.valueBase,
              h.valueBitWidth, start, end, op1, t1, op2, t2, live),
          describe(shape, vector, c) + ": countBitPackedRangeMasked");
    } else if (!NumberRegion.isDelta(h.encodingKind)) {
      assertEquals(want,
          NumberRegionSimd.countPlainLongRange(payload, h.valueBytesOffset, start, end, op1, t1, op2, t2),
          describe(shape, vector, c) + ": countPlainLongRange " + op1 + " " + t1 + " AND " + op2 + " " + t2);
      assertEquals(wantLive,
          NumberRegionSimd.countPlainLongRangeMasked(payload, h.valueBytesOffset, start, end, op1, t1, op2, t2, live),
          describe(shape, vector, c) + ": countPlainLongRangeMasked");
    }
  }

  /** The selection vector, where the kernel serves it: same rows, same order. */
  private static void checkSelection(final Shape shape, final boolean vector, final int c, final MemorySegment payload,
      final NumberRegion.Header h, final int start, final int end, final VectorOperators.Comparison op1, final long t1,
      final VectorOperators.Comparison op2, final long t2, final long[] expected) {
    final int len = expected.length;
    final int[] selection = new int[Math.max(1, len)];
    final int produced = NumberRegionSimd.selectMatching(payload, h, start, end, op1, t1, op2, t2, selection);
    if (produced < 0) {
      return;
    }
    final int[] wantIdx = new int[len];
    int w = 0;
    for (int i = 0; i < len; i++) {
      if (eval(expected[i], op1, t1) && eval(expected[i], op2, t2)) {
        wantIdx[w++] = start + i;
      }
    }
    assertEquals(w, produced, describe(shape, vector, c) + ": selection cardinality");
    for (int i = 0; i < w; i++) {
      assertEquals(wantIdx[i], selection[i], describe(shape, vector, c) + ": selection " + i);
    }
  }

  private static String describe(final Shape shape, final boolean vector, final int caseIndex) {
    return shape + (vector
        ? "/vector"
        : "/scalar") + " case " + caseIndex;
  }

  // ─────────────────────────────────────────────────────── delta codec round trip

  @Test
  @DisplayName("delta round-trips at every residual width, including those the vector unpack declines")
  void deltaRoundTripsAtEveryWidth() {
    final Random rng = new Random(4242);
    for (int width = 0; width <= 64; width++) {
      for (int rep = 0; rep < 4; rep++) {
        final int n = 3 + rng.nextInt(120);
        final long[] values = new long[n];
        // Build values whose zig-zag delta-of-delta needs exactly `width` bits, by driving the
        // second difference directly.
        values[0] = rng.nextInt(1000);
        values[1] = values[0] + rng.nextInt(1000);
        final long magnitude = width == 0
            ? 0L
            : (1L << (Math.max(1, width) - 1)) / 2;
        long delta = values[1] - values[0];
        for (int i = 2; i < n; i++) {
          final long dd = magnitude == 0
              ? 0L
              : (rng.nextBoolean()
                  ? magnitude
                  : -magnitude);
          delta += dd;
          values[i] = values[i - 1] + delta;
        }

        final long size = NumberRegionDelta.maxEncodedSize(values, n);
        final MemorySegment target = Arena.ofAuto().allocate(size + 64, 8).asSlice(0, size);
        NumberRegionDelta.writeDelta(target, 0L, values, n);
        final NumberRegionDelta.Header h = new NumberRegionDelta.Header();
        NumberRegionDelta.readHeader(target, 0L, h);

        final long[] bulk = new long[n];
        NumberRegionDelta.decodeAll(target, h, bulk);
        assertArrayEquals(values, bulk, "decodeAll at requested width " + width);

        // The scalar bit unpacker is the only decoder for widths the vector path declines, so it
        // must reproduce every residual — not merely the ones that fit in one word.
        if (h.bitWidth >= 1) {
          final long mask = BitUnpackSimd.maskFor(h.bitWidth);
          for (int i = 0; i < n - 2; i++) {
            final long viaKernel = BitUnpackSimd.decodeAt(target, h.bodyOffset, h.bitWidth, mask, i);
            final long viaReference = referenceUnpack(target, h.bodyOffset, h.bitWidth, i);
            assertEquals(viaReference, viaKernel,
                "BitUnpackSimd.decodeAt disagrees with a bit-by-bit read at width " + h.bitWidth + " index " + i);
          }
        }
      }
    }
  }

  /** Bit-by-bit reference read, sharing nothing with any unpacker. */
  private static long referenceUnpack(final MemorySegment seg, final long byteOffset, final int width,
      final int index) {
    long value = 0;
    final long startBit = (long) index * width;
    for (int b = 0; b < width; b++) {
      final long bit = startBit + b;
      final long byteIndex = byteOffset + (bit >>> 3);
      final long got = byteIndex < seg.byteSize()
          ? ((seg.get(ValueLayout.JAVA_BYTE, byteIndex) >>> (int) (bit & 7)) & 1L)
          : 0L;
      value |= got << b;
    }
    return value;
  }

  // ────────────────────────────────────────────────────────────── string kernels

  @Test
  @DisplayName("dict-id equality, set membership and histogram agree with a hand-written loop")
  void stringKernelsAgreeWithGroundTruth() {
    for (final boolean vector : new boolean[] {true, false}) {
      BitUnpackSimd.setWarmupRemainingForTesting(vector
          ? 0
          : Integer.MAX_VALUE);
      final Random rng = new Random(vector
          ? 99
          : 100);
      for (int c = 0; c < CASES; c++) {
        final int width = 1 + rng.nextInt(20);
        // Deliberately allow ids beyond a 64-entry set word: a width wider than the dictionary
        // needs is a shape the membership kernel has to handle, and it is where its vector body
        // and its scalar tail used to disagree.
        final int idBound = 1 << Math.min(width, 20);
        final int dictSize = 1 + rng.nextInt(Math.min(idBound, 200));
        final int n = 1 + rng.nextInt(400);
        final byte[] payload = new byte[(n * width + 7) / 8 + 8 * Long.BYTES * 2];
        final int[] ids = new int[n];
        for (int i = 0; i < n; i++) {
          ids[i] = rng.nextInt(idBound);
          pack(payload, width, i, ids[i]);
        }
        final MemorySegment seg = PaxTestSegments.of(payload);
        checkStringKernels(seg, ids, n, width, idBound, dictSize, c, vector, rng);
      }
    }
  }

  /** Equality, set membership, masked equality and histogram over one packed dict-id column. */
  private static void checkStringKernels(final MemorySegment seg, final int[] ids, final int n, final int width,
      final int idBound, final int dictSize, final int c, final boolean vector, final Random rng) {
    final int target = rng.nextInt(idBound);
    long want = 0;
    for (final int id : ids) {
      if (id == target) {
        want++;
      }
    }
    assertEquals(want, StringRegionSimd.countDictId(seg, 0, width, 0, n, target),
        "countDictId width=" + width + " case " + c + (vector
            ? " vector"
            : " scalar"));

    // Set membership over a bitmap sized to the dictionary.
    final long[] idSet = new long[(dictSize + 63) >>> 6];
    for (int id = 0; id < dictSize; id++) {
      if (rng.nextBoolean()) {
        idSet[id >>> 6] |= 1L << (id & 63);
      }
    }
    long wantSet = 0;
    for (final int id : ids) {
      if (id < dictSize && (idSet[id >>> 6] & (1L << (id & 63))) != 0L) {
        wantSet++;
      }
    }
    assertEquals(wantSet, StringRegionSimd.countDictIdSet(seg, 0, width, 0, n, idSet, dictSize),
        "countDictIdSet width=" + width + " dictSize=" + dictSize + " case " + c);

    // Masked equality.
    final long[] live = new long[Math.max(1, (n + 63) >>> 6)];
    long wantMasked = 0;
    for (int i = 0; i < n; i++) {
      if (rng.nextBoolean()) {
        live[i >>> 6] |= 1L << (i & 63);
        if (ids[i] == target) {
          wantMasked++;
        }
      }
    }
    assertEquals(wantMasked, StringRegionSimd.countDictIdMasked(seg, 0, width, 0, n, target, live),
        "countDictIdMasked width=" + width + " case " + c);

    // Histogram.
    final long[] counts = new long[idBound];
    assertTrue(StringRegionSimd.histogramDictIds(seg, 0, width, 0, n, counts), "histogram declined width=" + width);
    final long[] wantCounts = new long[idBound];
    for (final int id : ids) {
      wantCounts[id]++;
    }
    assertArrayEquals(wantCounts, counts, "histogram width=" + width + " case " + c);
  }

  private static void pack(final byte[] buf, final int width, final int index, final long value) {
    final long startBit = (long) index * width;
    for (int b = 0; b < width; b++) {
      if (((value >>> b) & 1L) != 0L) {
        buf[(int) ((startBit + b) >>> 3)] |= (byte) (1 << (int) ((startBit + b) & 7));
      }
    }
  }

  // ───────────────────────────────────────────────────────────── boolean kernels

  @Test
  @DisplayName("boolean count, masked count and AND-into agree with a hand-written loop")
  void booleanKernelsAgreeWithGroundTruth() {
    final Random rng = new Random(31337);
    for (int c = 0; c < CASES; c++) {
      final int n = 1 + rng.nextInt(2000);
      final boolean[] values = new boolean[n];
      final int[] tags = new int[n];
      for (int i = 0; i < n; i++) {
        values[i] = rng.nextInt(3) != 0;
      }
      final byte[] encoded = BooleanRegion.encode(values, tags, n, BooleanRegion.TAG_KIND_NAME);
      if (encoded == null) {
        continue;
      }
      final MemorySegment payload = PaxTestSegments.of(encoded);
      final BooleanRegion.Header h = new BooleanRegion.Header().parseInto(payload);

      final int start = rng.nextInt(n);
      final int len = rng.nextInt(n - start + 1);

      int want = 0;
      for (int i = start; i < start + len; i++) {
        if (BooleanRegion.decodeAt(payload, h, i)) {
          want++;
        }
      }
      assertEquals(want, BooleanRegion.countTrue(payload, h, start, len),
          "countTrue start=" + start + " n=" + len + " case " + c);

      final long[] live = new long[Math.max(1, (len + 63) >>> 6)];
      int wantMasked = 0;
      for (int i = 0; i < len; i++) {
        if (rng.nextBoolean()) {
          live[i >>> 6] |= 1L << (i & 63);
          if (BooleanRegion.decodeAt(payload, h, start + i)) {
            wantMasked++;
          }
        }
      }
      assertEquals(wantMasked, BooleanRegion.countTrueMasked(payload, h, start, len, live),
          "countTrueMasked start=" + start + " n=" + len + " case " + c);

      final long[] target = live.clone();
      final long remaining = BooleanRegion.andInto(payload, h, start, len, target, false);
      assertEquals(wantMasked, remaining, "andInto cardinality case " + c);
    }
  }
}
