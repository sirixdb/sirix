/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Equivalence tests for the vectorized column kernels.
 *
 * <p>Every kernel here has a scalar twin that is obviously correct, and the only property worth
 * testing is that the fast one agrees with it — on every bit width, at every alignment, and at the
 * range sizes where the vector loop does not run at all. The interesting failures in a bit-unpack
 * are not "wrong on random data"; they are wrong on the last group before the buffer ends, wrong
 * when a value straddles a word, and wrong when a range starts at an odd phase. So the references
 * here decode bit by bit rather than sharing any helper with the code under test, and the ranges
 * are chosen to land on those boundaries rather than away from them.
 */
@DisplayName("column kernels agree with their scalar references")
final class ColumnKernelEquivalenceTest {

  /**
   * Take the vector path from the first call.
   *
   * <p>The kernels hold off on vectorizing until they have seen enough values that the JIT has
   * compiled them, which is right for latency and wrong for a test: left alone, a suite this small
   * would never spend the budget and would assert entirely against the scalar loops. The dedicated
   * scalar-path test below re-raises the budget to cover the other side.
   */
  @BeforeEach
  void takeTheVectorPath() {
    BitUnpackSimd.setWarmupRemainingForTesting(0);
  }

  @AfterEach
  void restoreWarmup() {
    BitUnpackSimd.resetWarmupForTesting();
  }

  // ────────────────────────────────────────────────────────── bit unpacking

  /** Reference unpack: one bit at a time, sharing nothing with the kernel. */
  private static long referenceUnpack(final byte[] buf, final int off, final int width,
      final int index) {
    long value = 0;
    final long startBit = (long) index * width;
    for (int b = 0; b < width; b++) {
      final long bit = startBit + b;
      final int byteIndex = off + (int) (bit >>> 3);
      final long got = byteIndex < buf.length ? ((buf[byteIndex] >>> (int) (bit & 7)) & 1L) : 0L;
      value |= got << b;
    }
    return value;
  }

  private static void referencePack(final byte[] buf, final int off, final int width,
      final int index, final long value) {
    final long startBit = (long) index * width;
    for (int b = 0; b < width; b++) {
      if (((value >>> b) & 1L) != 0L) {
        final long bit = startBit + b;
        buf[off + (int) (bit >>> 3)] |= (byte) (1 << (int) (bit & 7));
      }
    }
  }

  @Test
  @DisplayName("vector unpack matches a bit-by-bit reference at every width and byte alignment")
  void unpackMatchesReference() {
    final int lanes = BitUnpackSimd.lanes();
    final Random rng = new Random(20260805);
    for (int width = 1; width <= BitUnpackSimd.MAX_BIT_WIDTH; width++) {
      // Byte offsets deliberately include odd ones: the packed column does not begin on a word
      // boundary, and a plan that silently assumed it would still pass at offset 0 and 8.
      for (final int off : new int[] {0, 1, 3, 7, 8, 13}) {
        final int count = 400;
        final byte[] buf = new byte[off + (count * width + 7) / 8 + 2 * Long.BYTES * lanes];
        final long mask = BitUnpackSimd.maskFor(width);
        final long[] expected = new long[count];
        for (int i = 0; i < count; i++) {
          expected[i] = rng.nextLong() & mask;
          referencePack(buf, off, width, i, expected[i]);
        }
        for (int i = 0; i < count; i++) {
          assertEquals(expected[i], referenceUnpack(buf, off, width, i),
                       "reference pack/unpack disagree at width " + width);
        }

        final BitUnpackSimd.Plan plan = BitUnpackSimd.planFor(width);
        final int lastGroup = BitUnpackSimd.lastVectorGroupStart(buf.length, off, width);
        for (int i = 0; i + lanes <= count && i <= lastGroup; i++) {
          final LongVector unpacked = plan.unpack(PaxTestSegments.of(buf), off, i);
          for (int lane = 0; lane < lanes; lane++) {
            assertEquals(expected[i + lane], unpacked.lane(lane),
                         "width " + width + " offset " + off + " group " + i + " lane " + lane);
          }
        }
        for (int i = 0; i < count; i++) {
          assertEquals(expected[i], plan.decodeAt(PaxTestSegments.of(buf), off, i),
                       "scalar tail decode, width " + width + " offset " + off);
        }
      }
    }
  }

  @Test
  @DisplayName("widths past the two-load window decline rather than answering wrongly")
  void unpackDeclinesUnsupportedWidths() {
    assertFalse(BitUnpackSimd.supports(BitUnpackSimd.MAX_BIT_WIDTH + 1));
    assertFalse(BitUnpackSimd.supports(0));
    assertEquals(null, BitUnpackSimd.planFor(BitUnpackSimd.MAX_BIT_WIDTH + 1));
    assertEquals(null, BitUnpackSimd.planFor(0));
  }

  // ─────────────────────────────────────────────────────────── number region

  private static byte[] encodeNumbers(final long[] values, final int tagCount) {
    final int[] tags = new int[values.length];
    for (int i = 0; i < values.length; i++) {
      tags[i] = i % tagCount;
    }
    return NumberRegion.encode(values, tags, values.length);
  }

  private static long scalarCount(final byte[] payload, final NumberRegion.Header h,
      final int start, final int end, final long lo, final long hi) {
    long count = 0;
    final long[] all = new long[h.count];
    NumberRegion.decodeAllValues(PaxTestSegments.of(payload), h, all);
    for (int i = start; i < end; i++) {
      if (all[i] >= lo && all[i] <= hi) {
        count++;
      }
    }
    return count;
  }

  /** Runs the count / range / aggregate kernels for one value distribution against the scalar path. */
  private void assertNumberKernelsAgree(final long[] values, final String what) {
    assertNumberKernelsAgree(values, what, 3);
  }

  private void assertNumberKernelsAgree(final long[] values, final String what,
      final int tagCount) {
    final byte[] payload = encodeNumbers(values, tagCount);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(PaxTestSegments.of(payload));
    final long[] all = new long[h.count];
    NumberRegion.decodeAllValues(PaxTestSegments.of(payload), h, all);

    for (int tag = 0; tag < h.dictSize; tag++) {
      final int start = h.tagStart[tag];
      final int end = start + h.tagCount[tag];
      if (end <= start) {
        continue;
      }
      final long lo = all[start];
      final long hi = lo + 1000;

      final long expected = scalarCount(payload, h, start, end, lo, hi);
      final long counted = NumberRegionSimd.countMatchingRange(PaxTestSegments.of(payload), h, start, end,
                                                               VectorOperators.GE, lo,
                                                               VectorOperators.LE, hi);
      assertTrue(counted >= 0, what + ": kernel declined tag " + tag);
      assertEquals(expected, counted, what + ": range count, tag " + tag);

      // Single-sided predicate.
      long expectedGe = 0;
      for (int i = start; i < end; i++) {
        if (all[i] >= lo) {
          expectedGe++;
        }
      }
      assertEquals(expectedGe,
                   NumberRegionSimd.countMatching(PaxTestSegments.of(payload), h, start, end, VectorOperators.GE, lo),
                   what + ": single-sided count, tag " + tag);

      // Aggregates.
      final long[] out = new long[3];
      assertTrue(NumberRegionSimd.aggregateRange(PaxTestSegments.of(payload), h, start, end, out),
                 what + ": aggregate declined tag " + tag);
      long sum = 0;
      long min = Long.MAX_VALUE;
      long max = Long.MIN_VALUE;
      for (int i = start; i < end; i++) {
        sum += all[i];
        min = Math.min(min, all[i]);
        max = Math.max(max, all[i]);
      }
      assertEquals(sum, out[0], what + ": sum, tag " + tag);
      assertEquals(min, out[1], what + ": min, tag " + tag);
      assertEquals(max, out[2], what + ": max, tag " + tag);

      // Masked count: shadow every third value and check the kernel drops exactly those.
      final int n = end - start;
      final long[] live = new long[(n + 63) >>> 6];
      long expectedLive = 0;
      for (int i = 0; i < n; i++) {
        if (i % 3 != 0) {
          live[i >>> 6] |= 1L << (i & 63);
          if (all[start + i] >= lo && all[start + i] <= hi) {
            expectedLive++;
          }
        }
      }
      assertEquals(expectedLive,
                   NumberRegionSimd.countMatchingRangeMasked(PaxTestSegments.of(payload), h, start, end,
                                                             VectorOperators.GE, lo,
                                                             VectorOperators.LE, hi, live),
                   what + ": masked count, tag " + tag);
    }
  }

  @Test
  @DisplayName("plain-long values agree with the scalar decoder")
  void plainLongKernels() {
    final Random rng = new Random(11);
    final long[] values = new long[997];
    for (int i = 0; i < values.length; i++) {
      // Spread wide enough that the encoder cannot bit-pack them.
      values[i] = rng.nextLong();
    }
    assertNumberKernelsAgree(values, "plain-long");
  }

  @Test
  @DisplayName("bit-packed values agree with the scalar decoder")
  void bitPackedKernels() {
    final Random rng = new Random(12);
    final long[] values = new long[997];
    for (int i = 0; i < values.length; i++) {
      values[i] = 1_000_000 + rng.nextInt(4096);
    }
    assertNumberKernelsAgree(values, "bit-packed");
  }

  @Test
  @DisplayName("delta-of-delta values agree with the scalar decoder instead of declining")
  void deltaKernels() {
    // Near-constant stride: what the delta encoder is chosen for, and what the kernels used to
    // refuse outright.
    final Random rng = new Random(13);
    final long[] values = new long[997];
    long clock = 1_700_000_000_000L;
    for (int i = 0; i < values.length; i++) {
      clock += 1000 + rng.nextInt(3);
      values[i] = clock;
    }
    // One tag: the encoder computes the residual width across the whole tag-grouped sequence, so a
    // multi-tag page pays a wide residual for the backward jump at each tag boundary and delta
    // stops winning the size bake-off. A single-tag page is the shape that actually reaches it.
    final byte[] payload = encodeNumbers(values, 1);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(PaxTestSegments.of(payload));
    assertTrue(NumberRegion.isDelta(h.encodingKind),
               "this fixture is meant to exercise the delta encoding, got kind " + h.encodingKind);
    assertNumberKernelsAgree(values, "delta", 1);
  }

  @Test
  @DisplayName("exactly constant stride, where the residual body disappears entirely")
  void deltaConstantStrideKernels() {
    final long[] values = new long[512];
    for (int i = 0; i < values.length; i++) {
      values[i] = 5_000_000L + (long) i * 64L;
    }
    final NumberRegion.Header h =
        new NumberRegion.Header().parseInto(PaxTestSegments.of(encodeNumbers(values, 1)));
    assertTrue(NumberRegion.isDelta(h.encodingKind) && h.valueBitWidth == 0,
               "constant stride should encode as delta with no residual body, got kind "
                   + h.encodingKind + " width " + h.valueBitWidth);
    assertNumberKernelsAgree(values, "delta constant stride", 1);
  }

  @Test
  @DisplayName("delta kernels stay correct across the staging block boundary")
  void deltaAcrossStagingBlocks() {
    // Longer than the 512-value staging block, so the replay carries state across refills and any
    // mistake in the carry shows up as a wrong value rather than a wrong count of a few.
    final long[] values = new long[3000];
    long clock = 0;
    for (int i = 0; i < values.length; i++) {
      clock += 7 + (i % 5);
      values[i] = clock;
    }
    final int[] tags = new int[values.length];
    final byte[] payload = NumberRegion.encode(values, tags, values.length);
    final NumberRegion.Header h = new NumberRegion.Header().parseInto(PaxTestSegments.of(payload));
    final long[] all = new long[h.count];
    NumberRegion.decodeAllValues(PaxTestSegments.of(payload), h, all);
    assertArrayEquals(values, all, "bulk decode must reproduce the input");

    // A range starting well inside the column forces the replay to run before the tested window.
    final int start = 1500;
    final int end = values.length;
    long expected = 0;
    for (int i = start; i < end; i++) {
      if (values[i] >= values[1700] && values[i] <= values[2600]) {
        expected++;
      }
    }
    assertEquals(expected,
                 NumberRegionSimd.countMatchingRange(PaxTestSegments.of(payload), h, start, end,
                                                     VectorOperators.GE, values[1700],
                                                     VectorOperators.LE, values[2600]),
                 "delta range count across staging blocks");

    // Masked, with the bitmap indexed relative to a start that is not block-aligned.
    final int n = end - start;
    final long[] live = new long[(n + 63) >>> 6];
    long expectedLive = 0;
    for (int i = 0; i < n; i++) {
      if ((i & 1) == 0) {
        live[i >>> 6] |= 1L << (i & 63);
        if (values[start + i] >= values[1700] && values[start + i] <= values[2600]) {
          expectedLive++;
        }
      }
    }
    assertEquals(expectedLive,
                 NumberRegionSimd.countMatchingRangeMasked(PaxTestSegments.of(payload), h, start, end,
                                                           VectorOperators.GE, values[1700],
                                                           VectorOperators.LE, values[2600], live),
                 "delta masked count across staging blocks");
  }

  @Test
  @DisplayName("the scalar path, still held back by the warmup budget, agrees too")
  void scalarPathUnderWarmupHold() {
    // A budget nothing in this test can spend, so every kernel below runs its scalar loop.
    BitUnpackSimd.setWarmupRemainingForTesting(Integer.MAX_VALUE);
    final Random rng = new Random(77);
    final long[] packed = new long[997];
    for (int i = 0; i < packed.length; i++) {
      packed[i] = 2_000_000 + rng.nextInt(8192);
    }
    assertNumberKernelsAgree(packed, "bit-packed, scalar path");

    final long[] plain = new long[997];
    for (int i = 0; i < plain.length; i++) {
      plain[i] = rng.nextLong();
    }
    assertNumberKernelsAgree(plain, "plain-long, scalar path");

    final long[] clock = new long[997];
    long c = 1_600_000_000_000L;
    for (int i = 0; i < clock.length; i++) {
      c += 500 + rng.nextInt(3);
      clock[i] = c;
    }
    assertNumberKernelsAgree(clock, "delta, scalar path", 1);
  }

  @Test
  @DisplayName("short ranges, where the vector loop never runs, still answer correctly")
  void shortRangesTakeTheScalarPath() {
    final Random rng = new Random(14);
    for (int n = 0; n < 40; n++) {
      final long[] values = new long[Math.max(1, n)];
      for (int i = 0; i < values.length; i++) {
        values[i] = 500 + rng.nextInt(200);
      }
      final int[] tags = new int[values.length];
      final byte[] payload = NumberRegion.encode(values, tags, values.length);
      if (payload == null) {
        continue;
      }
      final NumberRegion.Header h = new NumberRegion.Header().parseInto(PaxTestSegments.of(payload));
      long expected = 0;
      for (int i = 0; i < n; i++) {
        if (values[i] >= 600) {
          expected++;
        }
      }
      assertEquals(expected,
                   NumberRegionSimd.countMatching(PaxTestSegments.of(payload), h, 0, n, VectorOperators.GE, 600L),
                   "short range of " + n);
    }
  }

  // ─────────────────────────────────────────────────────────── zone pruning

  @Test
  @DisplayName("zone-map pruning answers from min/max alone, and admits when it cannot")
  void zoneMapPruning() {
    assertEquals(0L, NumberRegionSimd.pruneCount(10, 20, 30, 40, 100), "disjoint above");
    assertEquals(0L, NumberRegionSimd.pruneCount(50, 60, 30, 40, 100), "disjoint below");
    assertEquals(100L, NumberRegionSimd.pruneCount(32, 38, 30, 40, 100), "fully contained");
    assertEquals(NumberRegionSimd.PRUNE_UNKNOWN, NumberRegionSimd.pruneCount(20, 38, 30, 40, 100),
                 "straddles the lower bound");

    assertEquals(0L, NumberRegionSimd.pruneCount(10, 20, VectorOperators.GT, 20L, 7));
    assertEquals(7L, NumberRegionSimd.pruneCount(10, 20, VectorOperators.GE, 10L, 7));
    assertEquals(0L, NumberRegionSimd.pruneCount(10, 20, VectorOperators.LT, 10L, 7));
    assertEquals(7L, NumberRegionSimd.pruneCount(10, 20, VectorOperators.LE, 20L, 7));
    assertEquals(0L, NumberRegionSimd.pruneCount(10, 20, VectorOperators.EQ, 21L, 7));
    assertEquals(NumberRegionSimd.PRUNE_UNKNOWN,
                 NumberRegionSimd.pruneCount(10, 20, VectorOperators.EQ, 15L, 7));
    assertEquals(0L, NumberRegionSimd.pruneCount(15, 15, VectorOperators.NE, 15L, 7));
    assertEquals(7L, NumberRegionSimd.pruneCount(15, 15, VectorOperators.NE, 16L, 7));
  }

  // ───────────────────────────────────────────────────────── selection output

  @Test
  @DisplayName("the selection kernel emits exactly the matching row indices, ascending")
  void selectionVectorMatchesReference() {
    for (final boolean packed : new boolean[] {true, false}) {
      final Random rng = new Random(packed ? 21 : 22);
      final long[] values = new long[777];
      for (int i = 0; i < values.length; i++) {
        values[i] = packed ? 900_000 + rng.nextInt(2048) : rng.nextLong();
      }
      final int[] tags = new int[values.length];
      final byte[] payload = NumberRegion.encode(values, tags, values.length);
      final NumberRegion.Header h = new NumberRegion.Header().parseInto(PaxTestSegments.of(payload));
      final long[] all = new long[h.count];
      NumberRegion.decodeAllValues(PaxTestSegments.of(payload), h, all);

      final long lo = all[100];
      final long hi = lo + (packed ? 500 : Long.MAX_VALUE / 4);
      final int[] expected = new int[values.length];
      int expectedCount = 0;
      for (int i = 0; i < values.length; i++) {
        if (all[i] >= lo && all[i] <= hi) {
          expected[expectedCount++] = i;
        }
      }

      final int[] selection = new int[values.length];
      final int produced = NumberRegionSimd.selectMatching(PaxTestSegments.of(payload), h, 0, values.length,
                                                           VectorOperators.GE, lo,
                                                           VectorOperators.LE, hi, selection);
      assertTrue(produced >= 0, "selection declined a supported encoding");
      assertEquals(expectedCount, produced, "selection cardinality");
      for (int i = 0; i < produced; i++) {
        assertEquals(expected[i], selection[i], "selection index " + i);
      }
      for (int i = 1; i < produced; i++) {
        assertTrue(selection[i] > selection[i - 1], "selection must be ascending");
      }
    }
  }

  // ────────────────────────────────────────────────────────── boolean region

  @Test
  @DisplayName("boolean popcount matches a per-bit reference at every start and length")
  void booleanCountMatchesReference() {
    final Random rng = new Random(31);
    final int count = 1500;
    final boolean[] values = new boolean[count];
    final int[] tags = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = rng.nextInt(3) != 0;
    }
    final byte[] payload = BooleanRegion.encode(values, tags, count, BooleanRegion.TAG_KIND_NAME);
    final BooleanRegion.Header h = new BooleanRegion.Header().parseInto(PaxTestSegments.of(payload));

    // Starts chosen to hit every alignment relative to a 64-bit word, including ones that make the
    // head and the tail land in the same partial word.
    for (final int start : new int[] {0, 1, 7, 8, 31, 63, 64, 65, 127, 128, 511, 1000}) {
      for (final int n : new int[] {0, 1, 2, 7, 8, 63, 64, 65, 127, 128, 255, 400}) {
        if (start + n > count) {
          continue;
        }
        int expected = 0;
        for (int i = start; i < start + n; i++) {
          if (BooleanRegion.decodeAt(PaxTestSegments.of(payload), h, i)) {
            expected++;
          }
        }
        assertEquals(expected, BooleanRegion.countTrue(PaxTestSegments.of(payload), h, start, n),
                     "countTrue start=" + start + " n=" + n);
      }
    }
  }

  @Test
  @DisplayName("masked boolean count and AND-into agree with a per-bit reference")
  void booleanMaskedAndFused() {
    final Random rng = new Random(32);
    final int count = 1000;
    final boolean[] values = new boolean[count];
    final int[] tags = new int[count];
    for (int i = 0; i < count; i++) {
      values[i] = rng.nextBoolean();
    }
    final byte[] payload = BooleanRegion.encode(values, tags, count, BooleanRegion.TAG_KIND_NAME);
    final BooleanRegion.Header h = new BooleanRegion.Header().parseInto(PaxTestSegments.of(payload));

    for (final int start : new int[] {0, 3, 64, 130}) {
      final int n = 500;
      final long[] live = new long[(n + 63) >>> 6];
      for (int i = 0; i < n; i++) {
        if (rng.nextInt(4) != 0) {
          live[i >>> 6] |= 1L << (i & 63);
        }
      }
      int expected = 0;
      for (int i = 0; i < n; i++) {
        final boolean isLive = (live[i >>> 6] & (1L << (i & 63))) != 0L;
        if (isLive && BooleanRegion.decodeAt(PaxTestSegments.of(payload), h, start + i)) {
          expected++;
        }
      }
      assertEquals(expected, BooleanRegion.countTrueMasked(PaxTestSegments.of(payload), h, start, n, live),
                   "countTrueMasked start=" + start);

      // andInto must leave exactly the intersection, and report its cardinality.
      final long[] target = live.clone();
      final long remaining = BooleanRegion.andInto(PaxTestSegments.of(payload), h, start, n, target, false);
      assertEquals(expected, remaining, "andInto cardinality start=" + start);
      for (int i = 0; i < n; i++) {
        final boolean wasLive = (live[i >>> 6] & (1L << (i & 63))) != 0L;
        final boolean expectedBit = wasLive && BooleanRegion.decodeAt(PaxTestSegments.of(payload), h, start + i);
        final boolean actualBit = (target[i >>> 6] & (1L << (i & 63))) != 0L;
        assertEquals(expectedBit, actualBit, "andInto bit " + i + " start=" + start);
      }

      // Inverted, the two halves must partition the live set.
      final long[] inverted = live.clone();
      final long invRemaining = BooleanRegion.andInto(PaxTestSegments.of(payload), h, start, n, inverted, true);
      long liveCount = 0;
      for (int i = 0; i < n; i++) {
        if ((live[i >>> 6] & (1L << (i & 63))) != 0L) {
          liveCount++;
        }
      }
      assertEquals(liveCount, remaining + invRemaining,
                   "field and NOT field must partition the live set, start=" + start);
    }
  }

  // ─────────────────────────────────────────────────────────── string region

  @Test
  @DisplayName("dict-id equality, set membership and histogram agree with a per-value reference")
  void stringDictKernels() {
    final int lanes = BitUnpackSimd.lanes();
    final Random rng = new Random(41);
    for (final int width : new int[] {1, 2, 3, 5, 8, 11, 16, 20}) {
      final int dictSize = Math.min(1 << width, 40);
      final int count = 700;
      final byte[] payload =
          new byte[(count * width + 7) / 8 + 2 * Long.BYTES * lanes];
      final int[] ids = new int[count];
      for (int i = 0; i < count; i++) {
        ids[i] = rng.nextInt(dictSize);
        referencePack(payload, 0, width, i, ids[i]);
      }

      for (int target = 0; target < Math.min(dictSize, 4); target++) {
        long expected = 0;
        for (int i = 0; i < count; i++) {
          if (ids[i] == target) {
            expected++;
          }
        }
        assertEquals(expected,
                     StringRegionSimd.countDictId(PaxTestSegments.of(payload), 0, width, 0, count, target),
                     "countDictId width=" + width + " id=" + target);
      }

      // Set membership: accept every third dictionary entry.
      final long[] idSet = new long[(Math.max(dictSize, 1) + 63) >>> 6];
      long expectedSet = 0;
      for (int id = 0; id < dictSize; id += 3) {
        idSet[id >>> 6] |= 1L << (id & 63);
      }
      for (int i = 0; i < count; i++) {
        if ((idSet[ids[i] >>> 6] & (1L << (ids[i] & 63))) != 0L) {
          expectedSet++;
        }
      }
      assertEquals(expectedSet,
                   StringRegionSimd.countDictIdSet(PaxTestSegments.of(payload), 0, width, 0, count, idSet, dictSize),
                   "countDictIdSet width=" + width);

      // An empty set must answer zero without reading anything.
      assertEquals(0L, StringRegionSimd.countDictIdSet(PaxTestSegments.of(payload), 0, width, 0, count,
                                                       new long[idSet.length], dictSize),
                   "empty set width=" + width);

      // Histogram.
      final long[] counts = new long[Math.max(dictSize, 1 << Math.min(width, 20))];
      assertTrue(StringRegionSimd.histogramDictIds(PaxTestSegments.of(payload), 0, width, 0, count, counts),
                 "histogram declined width=" + width);
      final long[] expectedCounts = new long[counts.length];
      for (int i = 0; i < count; i++) {
        expectedCounts[ids[i]]++;
      }
      assertArrayEquals(expectedCounts, counts, "histogram width=" + width);

      // Masked equality.
      final long[] live = new long[(count + 63) >>> 6];
      long expectedMasked = 0;
      for (int i = 0; i < count; i++) {
        if (i % 5 != 0) {
          live[i >>> 6] |= 1L << (i & 63);
          if (ids[i] == 0) {
            expectedMasked++;
          }
        }
      }
      assertEquals(expectedMasked,
                   StringRegionSimd.countDictIdMasked(PaxTestSegments.of(payload), 0, width, 0, count, 0, live),
                   "countDictIdMasked width=" + width);
    }
  }


}
