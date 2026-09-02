/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P5b stage 3 parity oracle: the column-sliced kernels ({@link ProjectionColumnScan}) must agree
 * with the whole-leaf byte kernels ({@link ProjectionIndexScan} / {@link ProjectionIndexByteScan})
 * on EVERY count, aggregate, and matched-value stream, over randomized stores covering sparse
 * presence, unrepresentable cells, empty leaves, negative values, ±0.0 doubles, and every supported
 * predicate op. The byte kernels are the differential-suite-pinned truth; any divergence here is a
 * column-kernel bug by definition.
 */
final class ProjectionColumnScanParityTest {

  /** Columns: 0 = long, 1 = double, 2 = boolean, 3 = string (never sliced, predicate-only). */
  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
          ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,};

  private record Fixture(ProjectionColumnStore store, List<byte[]> rawLeaves, ColumnSegmentFetcher fetcher) {
  }

  /** Build a randomized multi-leaf store + the equivalent raw payload list. */
  private static Fixture buildFixture(final long seed, final int leaves, final boolean withEmptyLeaf) {
    return buildFixture(seed, leaves, withEmptyLeaf, false);
  }

  /**
   * {@code allPresent = true} makes every cell present and clean, so mask words are almost always
   * fully set — the fused kernels' DENSE fast paths carry the whole evaluation (the ~10% sparse
   * default rarely yields a fully-set 64-row word).
   */
  private static Fixture buildFixture(final long seed, final int leaves, final boolean withEmptyLeaf,
      final boolean allPresent) {
    return buildFixture(seed, leaves, withEmptyLeaf, allPresent, false);
  }

  /**
   * {@code bandedLongs = true} gives leaf {@code L} long values in the band
   * {@code [L*10_000, L*10_000 + rows)} — ascending, disjoint per leaf — so an ascending top-K's heap
   * threshold provably prunes every leaf after the first, exercising the zone-pruned selection path
   * deterministically.
   */
  private static Fixture buildFixture(final long seed, final int leaves, final boolean withEmptyLeaf,
      final boolean allPresent, final boolean bandedLongs) {
    final Random rnd = new Random(seed);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(leaves);
    final List<byte[]> rawLeaves = new ArrayList<>(leaves);
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      final boolean empty = withEmptyLeaf && leaf == leaves / 2;
      final int rows = empty
          ? 0
          : 1 + rnd.nextInt(ProjectionIndexRowGroupPage.MAX_ROWS);
      final long[] longs = new long[KINDS.length];
      final boolean[] bools = new boolean[KINDS.length];
      final String[] strings = new String[KINDS.length];
      final boolean[] present = new boolean[KINDS.length];
      final boolean[] unrep = new boolean[KINDS.length];
      final boolean[] nonIntegral = new boolean[KINDS.length];
      final boolean[] nonDoubleSource = new boolean[KINDS.length];
      long recordKey = leaf * 100_000L + 1;
      for (int r = 0; r < rows; r++) {
        strings[3] = "s" + rnd.nextInt(6);
        longs[0] = bandedLongs
            ? leaf * 10_000L + r
            : rnd.nextLong(-1_000, 1_000);
        final double d = switch (rnd.nextInt(6)) {
          case 0 -> -0.0;
          case 1 -> 0.0;
          case 2 -> rnd.nextDouble() * 200 - 100;
          case 3 -> rnd.nextInt(50) / 10.0;
          default -> (double) rnd.nextInt(100);
        };
        longs[1] = ProjectionDoubleEncoding.encode(d);
        bools[2] = rnd.nextBoolean();
        for (int c = 0; c < KINDS.length; c++) {
          present[c] = allPresent || rnd.nextInt(10) != 0; // ~10% missing unless dense mode
          unrep[c] = !allPresent && present[c] && rnd.nextInt(40) == 0; // rare poison
          nonIntegral[c] = false;
          nonDoubleSource[c] = false; // pure doubles
        }
        page.appendRow(recordKey, longs, bools, strings, present, unrep, nonIntegral, nonDoubleSource);
        recordKey += 1 + rnd.nextInt(3);
      }
      final byte[] raw = page.serialize();
      rawLeaves.add(raw);
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
      final int columnSegmentCount = encoded.columnSegmentIds().length;
      final int[] ids = new int[columnSegmentCount];
      final long[] offsets = new long[columnSegmentCount];
      for (int i = 0; i < columnSegmentCount; i++) {
        ids[i] = encoded.columnSegmentIds()[i];
        offsets[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets,
          new byte[ids.length][]));
    }
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    };
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    return new Fixture(store, rawLeaves, fetcher);
  }

  private static List<ColumnPredicate[]> predicateShapes() {
    final List<ColumnPredicate[]> shapes = new ArrayList<>();
    shapes.add(new ColumnPredicate[0]);
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, 0L)});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.LE, -100L)});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.EQ, 42L)});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numericBetween(0, Op.GE, -500L, Op.LT, 500L)});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.booleanEq(2, true)});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.booleanEq(2, false)});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, -200L), ColumnPredicate.booleanEq(2, true)});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numeric(1, Op.GT, ProjectionDoubleEncoding.encode(10.0))});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numeric(1, Op.LT, ProjectionDoubleEncoding.encode(-0.0)),
        ColumnPredicate.booleanEq(2, false)});
    // Zone-prunable extremes.
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, Long.MAX_VALUE - 1)});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.LT, Long.MIN_VALUE + 1)});
    // String equality: a hit that exists on most leaves, a literal absent from EVERY leaf's
    // dictionary, and both mixed into conjunctions with the other kinds.
    shapes.add(new ColumnPredicate[] {ColumnPredicate.stringEq(3, "s1".getBytes(StandardCharsets.UTF_8))});
    shapes.add(
        new ColumnPredicate[] {ColumnPredicate.stringEq(3, "absent-everywhere".getBytes(StandardCharsets.UTF_8))});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, -200L),
        ColumnPredicate.stringEq(3, "s2".getBytes(StandardCharsets.UTF_8))});
    shapes.add(new ColumnPredicate[] {ColumnPredicate.stringEq(3, "s3".getBytes(StandardCharsets.UTF_8)),
        ColumnPredicate.booleanEq(2, true)});
    return shapes;
  }

  @Test
  void countsAgreeAcrossRandomizedStores() {
    for (final long seed : new long[] {1, 7, 42, 20260721}) {
      final Fixture fx = buildFixture(seed, 7, seed % 2 == 0);
      for (final ColumnPredicate[] preds : predicateShapes()) {
        if (preds.length == 0) {
          long rows = 0;
          for (int leaf = 0; leaf < fx.store().rowGroupCount(); leaf++) {
            rows += fx.store().rowCount(leaf);
          }
          assertEquals(ProjectionIndexScan.countRows(fx.rawLeaves()), rows, "countRows seed=" + seed);
          continue;
        }
        assertEquals(ProjectionIndexScan.conjunctiveCount(fx.rawLeaves(), preds),
            ProjectionColumnScan.conjunctiveCount(fx.store(), preds, fx.fetcher()),
            "count parity seed=" + seed + " preds=" + preds.length + " op=" + preds[0].op);
      }
    }
  }

  /**
   * All-present fixtures make every full 64-row word dense ({@code m == -1L}), so the string shapes
   * here drive the vector {@code equalsIdWord} DENSE arm — which the randomized fixtures (~10%
   * missing) reach with probability {@code 0.9^64} per word, i.e. essentially never — against the
   * byte-kernel count oracle.
   */
  @Test
  void stringDenseArmCountsAgreeOnAllPresentStores() {
    for (final long seed : new long[] {3, 97}) {
      final Fixture fx = buildFixture(seed, 6, false, true);
      for (final ColumnPredicate[] preds : predicateShapes()) {
        if (preds.length == 0) {
          continue;
        }
        assertEquals(ProjectionIndexScan.conjunctiveCount(fx.rawLeaves(), preds),
            ProjectionColumnScan.conjunctiveCount(fx.store(), preds, fx.fetcher()),
            "dense count parity seed=" + seed + " preds=" + preds.length + " op=" + preds[0].op);
      }
    }
  }

  @Test
  void longAggregatesAgreeAcrossRandomizedStores() {
    for (final long seed : new long[] {3, 11, 99, 314159}) {
      final Fixture fx = buildFixture(seed, 5, true);
      for (final ColumnPredicate[] preds : predicateShapes()) {
        final long[] expected = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionIndexByteScan.conjunctiveAggregateNumeric(fx.rawLeaves(), preds, 0, expected);
        final long[] actual = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, actual, fx.fetcher());
        for (int i = 0; i < 4; i++) {
          assertEquals(expected[i], actual[i], "long agg[" + i + "] seed=" + seed);
        }
      }
    }
  }

  @Test
  void fusedFoldKernelsAgreeAcrossRandomizedStores() {
    // P5b stage 4: the fold-during-decode kernels (straight from segment bytes, no slice
    // arrays) must match the byte kernels wherever the eligibility gate admits them; the
    // ineligible shapes (ALP-escaped double streams) are covered by the dedicated gate test.
    for (final long seed : new long[] {1, 7, 42, 20260721, 3, 11, 99}) {
      final Fixture fx = buildFixture(seed, 7, seed % 2 == 0);
      final int rowGroupCount = fx.store().rowGroupCount();
      for (final ColumnPredicate[] preds : predicateShapes()) {
        if (!ProjectionColumnSegmentFoldScan.eligible(fx.store(), preds, 0, fx.fetcher())) {
          continue;
        }
        final long expectedCount = preds.length == 0
            ? ProjectionIndexScan.countRows(fx.rawLeaves())
            : ProjectionIndexScan.conjunctiveCount(fx.rawLeaves(), preds);
        assertEquals(expectedCount, ProjectionColumnSegmentFoldScan.conjunctiveCount(fx.store(), preds, fx.fetcher()),
            "fused count parity seed=" + seed + " preds=" + preds.length);
        // Ranged split — the executor's chunked parallel dispatch shape.
        final int mid = rowGroupCount / 2;
        assertEquals(expectedCount,
            ProjectionColumnSegmentFoldScan.conjunctiveCount(fx.store(), preds, 0, mid, fx.fetcher())
                + ProjectionColumnSegmentFoldScan.conjunctiveCount(fx.store(), preds, mid, rowGroupCount, fx.fetcher()),
            "fused ranged count parity seed=" + seed);
        final long[] expected = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionIndexByteScan.conjunctiveAggregateNumeric(fx.rawLeaves(), preds, 0, expected);
        final long[] actual = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, actual, fx.fetcher());
        for (int i = 0; i < 4; i++) {
          assertEquals(expected[i], actual[i], "fused long agg[" + i + "] seed=" + seed);
        }
        final long[] left = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, left, 0, mid, fx.fetcher());
        final long[] right = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, right, mid, rowGroupCount,
            fx.fetcher());
        assertEquals(expected[0], left[0] + right[0], "fused ranged agg count seed=" + seed);
        assertEquals(expected[1], left[1] + right[1], "fused ranged agg sum seed=" + seed);
        assertEquals(expected[2], Math.min(left[2], right[2]), "fused ranged agg min seed=" + seed);
        assertEquals(expected[3], Math.max(left[3], right[3]), "fused ranged agg max seed=" + seed);
        // Masked fold: count and sum must be bit-identical to the full fold, and the extrema
        // slots must come back exactly as initialised. A sum-only fold that quietly disagreed on
        // a count, or that wrote a partial extremum, would corrupt a chunked merge — the merge
        // takes min/max across per-thread accumulators, so a stray write is indistinguishable
        // from a real extremum.
        final long[] sumOnly = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, sumOnly, fx.fetcher(),
            ProjectionColumnSegmentFoldScan.AGG_COUNT | ProjectionColumnSegmentFoldScan.AGG_SUM);
        assertEquals(expected[0], sumOnly[0], "masked fold count seed=" + seed);
        assertEquals(expected[1], sumOnly[1], "masked fold sum seed=" + seed);
        assertEquals(Long.MAX_VALUE, sumOnly[2], "masked fold wrote min seed=" + seed);
        assertEquals(Long.MIN_VALUE, sumOnly[3], "masked fold wrote max seed=" + seed);
        // One extremum: min(x) and max(x) are separate queries, so asking for one must fold one.
        // count, sum and the requested extremum match the full fold; the OTHER extremum stays at
        // the caller's identity, same contract as the sum-only arm.
        final long[] withMin = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, withMin, fx.fetcher(),
            ProjectionColumnSegmentFoldScan.AGG_COUNT | ProjectionColumnSegmentFoldScan.AGG_MIN);
        assertEquals(expected[0], withMin[0], "min-only fold count seed=" + seed);
        assertEquals(expected[1], withMin[1], "min-only fold sum seed=" + seed);
        assertEquals(expected[2], withMin[2], "min-only fold min seed=" + seed);
        assertEquals(Long.MIN_VALUE, withMin[3], "min-only fold wrote max seed=" + seed);
        final long[] withMax = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, withMax, fx.fetcher(),
            ProjectionColumnSegmentFoldScan.AGG_COUNT | ProjectionColumnSegmentFoldScan.AGG_MAX);
        assertEquals(expected[0], withMax[0], "max-only fold count seed=" + seed);
        assertEquals(expected[1], withMax[1], "max-only fold sum seed=" + seed);
        assertEquals(expected[3], withMax[3], "max-only fold max seed=" + seed);
        assertEquals(Long.MAX_VALUE, withMax[2], "max-only fold wrote min seed=" + seed);
        // Both extrema still take the full path and must reproduce every slot.
        final long[] withBoth = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, withBoth, fx.fetcher(),
            ProjectionColumnSegmentFoldScan.AGG_ALL);
        for (int i = 0; i < 4; i++) {
          assertEquals(expected[i], withBoth[i], "full-mask fold agg[" + i + "] seed=" + seed);
        }
        // Ranged + masked, the shape the parallel dispatch actually uses.
        final long[] mLeft = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        final long[] mRight = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        final int sumMask = ProjectionColumnSegmentFoldScan.AGG_COUNT | ProjectionColumnSegmentFoldScan.AGG_SUM;
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, mLeft, 0, mid, fx.fetcher(),
            sumMask);
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, mRight, mid, rowGroupCount,
            fx.fetcher(), sumMask);
        assertEquals(expected[0], mLeft[0] + mRight[0], "masked ranged count seed=" + seed);
        assertEquals(expected[1], mLeft[1] + mRight[1], "masked ranged sum seed=" + seed);
      }
    }
  }

  @Test
  void fusedDenseFastPathsAgreeOnAllPresentStores() {
    // All-present fixtures make every mask word fully set, so the DENSE fast paths (linear
    // 64-value compare + fold, no per-bit walk) carry the evaluation — pinned against the
    // byte kernels over every predicate shape, full and ranged.
    for (final long seed : new long[] {4, 21, 1234}) {
      final Fixture fx = buildFixture(seed, 6, false, true);
      final int rowGroupCount = fx.store().rowGroupCount();
      for (final ColumnPredicate[] preds : predicateShapes()) {
        if (!ProjectionColumnSegmentFoldScan.eligible(fx.store(), preds, 0, fx.fetcher())) {
          continue;
        }
        final long expectedCount = preds.length == 0
            ? ProjectionIndexScan.countRows(fx.rawLeaves())
            : ProjectionIndexScan.conjunctiveCount(fx.rawLeaves(), preds);
        assertEquals(expectedCount, ProjectionColumnSegmentFoldScan.conjunctiveCount(fx.store(), preds, fx.fetcher()),
            "dense count parity seed=" + seed + " preds=" + preds.length);
        final long[] expected = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionIndexByteScan.conjunctiveAggregateNumeric(fx.rawLeaves(), preds, 0, expected);
        final long[] actual = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, actual, fx.fetcher());
        for (int i = 0; i < 4; i++) {
          assertEquals(expected[i], actual[i], "dense long agg[" + i + "] seed=" + seed);
        }
        final int mid = rowGroupCount / 2;
        assertEquals(expectedCount,
            ProjectionColumnSegmentFoldScan.conjunctiveCount(fx.store(), preds, 0, mid, fx.fetcher())
                + ProjectionColumnSegmentFoldScan.conjunctiveCount(fx.store(), preds, mid, rowGroupCount, fx.fetcher()),
            "dense ranged count parity seed=" + seed);
      }
    }
  }

  /** {@code [count, sum, min, max]} at the fold identities every aggregate kernel starts from. */
  private static long[] newAgg() {
    return new long[] {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
  }

  private static final ColumnPredicate[] NO_PREDICATES = new ColumnPredicate[0];

  /**
   * A sum that would leave the {@code long} range is DECLINED, never wrapped.
   *
   * <p>
   * {@code xs:integer} is arbitrary precision and the interpreter behind these kernels promotes an
   * overflowing total to exact decimal, so a wrapped accumulator is a wrong answer that looks like a
   * fast one — a projection over a column of 1e18-scale ids turned {@code avg} from 5.7e17 into
   * 3.6e13. Every kernel therefore throws {@link ArithmeticException}, which every caller treats as a
   * fallback signal.
   *
   * <p>
   * The values here have a NARROW frame-of-reference range and a huge magnitude on purpose: that
   * bit-packs to a plain width, which is what makes the SIMD fold kernel eligible, so the assertion
   * exercises its pre-flight zone-map bound rather than the scalar kernels' exact adds.
   */
  @Test
  void hugeMagnitudeSumsDeclineInsteadOfWrapping() {
    final long[] huge = new long[64];
    for (int i = 0; i < huge.length; i++) {
      huge[i] = 4_600_000_000_000_000_000L + i;
    }
    final Fixture fx = buildLongColumnFixture(huge);
    assertTrue(ProjectionColumnSegmentFoldScan.eligible(fx.store(), NO_PREDICATES, 0, fx.fetcher()),
        "the fold kernel must be the one under test — otherwise the zone-map bound is never reached");
    assertThrows(ArithmeticException.class,
        () -> ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), NO_PREDICATES, 0, newAgg(),
            fx.fetcher()),
        "the fold kernel must decline a sum it cannot hold");
    // The scalar kernels reach the same verdict — exactly, per add, rather than by a bound.
    assertThrows(ArithmeticException.class,
        () -> ProjectionColumnScan.conjunctiveAggregateNumeric(fx.store(), NO_PREDICATES, 0, newAgg(), fx.fetcher()),
        "the slice kernel must decline a sum it cannot hold");
    assertThrows(ArithmeticException.class,
        () -> ProjectionIndexByteScan.conjunctiveAggregateNumeric(fx.rawLeaves(), NO_PREDICATES, 0, newAgg()),
        "the byte kernel must decline a sum it cannot hold");

    // count/min/max do not depend on the sum lane, so an extrema query keeps serving.
    final long[] extrema = newAgg();
    ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), NO_PREDICATES, 0, extrema, fx.fetcher(),
        ProjectionColumnSegmentFoldScan.AGG_COUNT | ProjectionColumnSegmentFoldScan.AGG_MIN
            | ProjectionColumnSegmentFoldScan.AGG_MAX);
    assertEquals(huge.length, extrema[0], "extrema-only fold must still count");
    assertEquals(huge[0], extrema[2]);
    assertEquals(huge[huge.length - 1], extrema[3]);

    // The gate is a BOUND, not a blanket refusal of large values: two values that together still
    // fit must fold, and fold exactly.
    final long[] fits = {4_600_000_000_000_000_000L, 4_600_000_000_000_000_010L};
    final Fixture ok = buildLongColumnFixture(fits);
    final long[] acc = newAgg();
    ProjectionColumnSegmentFoldScan.conjunctiveAggregateNumeric(ok.store(), NO_PREDICATES, 0, acc, ok.fetcher());
    assertEquals(2L, acc[0]);
    assertEquals(9_200_000_000_000_000_010L, acc[1]);
    assertEquals(fits[0], acc[2]);
    assertEquals(fits[1], acc[3]);
  }

  /** One leaf, one all-present NUMERIC_LONG column carrying exactly {@code values}. */
  private static Fixture buildLongColumnFixture(final long[] values) {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    final long[] longs = new long[1];
    for (int r = 0; r < values.length; r++) {
      longs[0] = values[r];
      page.appendRow(r + 1L, longs, new boolean[1], new String[1]);
    }
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final int[] ids = new int[encoded.columnSegmentIds().length];
    final long[] offsets = new long[ids.length];
    long nextOffset = 1_000;
    for (int i = 0; i < ids.length; i++) {
      ids[i] = encoded.columnSegmentIds()[i];
      offsets[i] = nextOffset;
      segmentsByOffset.put(nextOffset, encoded.segments()[i]);
      nextOffset += 1 + encoded.segments()[i].length;
    }
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    };
    final ProjectionColumnStore store =
        new ProjectionColumnStore(List.of(new RowGroupDirectory(1, encoded.descriptor(), ids, offsets,
            new byte[ids.length][])));
    return new Fixture(store, List.of(raw), fetcher);
  }

  @Test
  void predicateTreeKernelsAgreeWithRowWiseOracle() {
    // P5b stage 6: arbitrary AND/OR trees. Oracle = a row-at-a-time evaluator over the
    // decoded slices (independent of the mask algebra under test); fused tree kernels must
    // match on count AND aggregate for randomized trees over sparse randomized stores.
    for (final long seed : new long[] {6, 19, 404, 20260721}) {
      final Random rnd = new Random(seed * 31 + 7);
      final Fixture fx = buildFixture(seed, 6, seed % 2 == 0);
      for (int trial = 0; trial < 12; trial++) {
        final ProjectionIndexScan.PredicateTree tree = randomTree(rnd);
        if (!ProjectionColumnSegmentFoldScan.eligibleTree(fx.store(), tree, 0, fx.fetcher())) {
          continue;
        }
        final long expectedCount = naiveTreeCount(fx.store(), tree, fx.fetcher());
        assertEquals(expectedCount, ProjectionColumnSegmentFoldScan.treeCount(fx.store(), tree, fx.fetcher()),
            "tree count parity seed=" + seed + " trial=" + trial);
        final int rowGroupCount = fx.store().rowGroupCount();
        final int mid = rowGroupCount / 2;
        assertEquals(expectedCount,
            ProjectionColumnSegmentFoldScan.treeCount(fx.store(), tree, 0, mid, fx.fetcher())
                + ProjectionColumnSegmentFoldScan.treeCount(fx.store(), tree, mid, rowGroupCount, fx.fetcher()),
            "tree ranged count parity seed=" + seed + " trial=" + trial);
        final long[] expected = naiveTreeAggregate(fx.store(), tree, 0, fx.fetcher());
        final long[] actual = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
        ProjectionColumnSegmentFoldScan.treeAggregateNumeric(fx.store(), tree, 0, actual, fx.fetcher());
        for (int i = 0; i < 4; i++) {
          assertEquals(expected[i], actual[i], "tree agg[" + i + "] seed=" + seed + " trial=" + trial);
        }
      }
    }
    // Malformed programs must be rejected at construction.
    final ColumnPredicate leaf = ColumnPredicate.numeric(0, Op.GT, 10L);
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionIndexScan.PredicateTree.of(new ColumnPredicate[] {leaf}, new byte[] {0, 0}),
        "must end at depth 1");
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionIndexScan.PredicateTree.of(new ColumnPredicate[] {leaf},
            new byte[] {0, ProjectionIndexScan.PredicateTree.OP_OR}),
        "combinator underflow");
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionIndexScan.PredicateTree.of(new ColumnPredicate[] {leaf}, new byte[] {1}),
        "leaf index out of range");
  }

  /** Random 2-4 leaf AND/OR tree over the long/double/boolean columns (never strings). */
  private static ProjectionIndexScan.PredicateTree randomTree(final Random rnd) {
    final int n = 2 + rnd.nextInt(3);
    final ColumnPredicate[] leaves = new ColumnPredicate[n];
    final byte[] program = new byte[2 * n - 1];
    for (int i = 0; i < n; i++) {
      leaves[i] = switch (rnd.nextInt(5)) {
        case 0 -> ColumnPredicate.numeric(0, Op.GT, rnd.nextInt(2001) - 1000L);
        case 1 -> ColumnPredicate.numeric(0, Op.LE, rnd.nextInt(2001) - 1000L);
        case 2 -> ColumnPredicate.numeric(1, Op.GT, ProjectionDoubleEncoding.encode(rnd.nextDouble() * 100 - 50));
        case 3 -> ColumnPredicate.booleanEq(2, rnd.nextBoolean());
        default -> ColumnPredicate.numericBetween(0, Op.GE, rnd.nextInt(1000) - 500L, Op.LE, rnd.nextInt(1000));
      };
    }
    // Left-deep postfix: leaf0, then (leaf_i, combinator) pairs — always well-formed.
    int p = 0;
    program[p++] = 0;
    for (int i = 1; i < n; i++) {
      program[p++] = (byte) i;
      program[p++] = rnd.nextBoolean()
          ? ProjectionIndexScan.PredicateTree.OP_AND
          : ProjectionIndexScan.PredicateTree.OP_OR;
    }
    return ProjectionIndexScan.PredicateTree.of(leaves, program);
  }

  /** Row-at-a-time oracle over decoded slices — independent of the mask algebra. */
  private static long naiveTreeCount(final ProjectionColumnStore store, final ProjectionIndexScan.PredicateTree tree,
      final ColumnSegmentFetcher fetcher) {
    long total = 0;
    for (int leaf = 0; leaf < store.rowGroupCount(); leaf++) {
      final int rows = store.rowCount(leaf);
      for (int r = 0; r < rows; r++) {
        if (naiveTreeRow(store, tree, leaf, r, fetcher)) {
          total++;
        }
      }
    }
    return total;
  }

  private static long[] naiveTreeAggregate(final ProjectionColumnStore store,
      final ProjectionIndexScan.PredicateTree tree, final int aggCol, final ColumnSegmentFetcher fetcher) {
    final long[] acc = {0, 0, Long.MAX_VALUE, Long.MIN_VALUE};
    for (int leaf = 0; leaf < store.rowGroupCount(); leaf++) {
      final int rows = store.rowCount(leaf);
      final ProjectionColumnStore.ColumnSlice agg = store.column(aggCol, fetcher)[leaf];
      for (int r = 0; r < rows; r++) {
        if (!naiveTreeRow(store, tree, leaf, r, fetcher)) {
          continue;
        }
        if ((agg.presenceWords()[r >>> 6] & (1L << (r & 63))) == 0) {
          continue;
        }
        final long v = agg.numericValues()[r];
        acc[0]++;
        acc[1] += v;
        if (v < acc[2])
          acc[2] = v;
        if (v > acc[3])
          acc[3] = v;
      }
    }
    return acc;
  }

  private static boolean naiveTreeRow(final ProjectionColumnStore store, final ProjectionIndexScan.PredicateTree tree,
      final int leaf, final int r, final ColumnSegmentFetcher fetcher) {
    final boolean[] stack = new boolean[ProjectionIndexScan.PredicateTree.MAX_LEAVES];
    int depth = 0;
    for (final byte insn : tree.program) {
      if (insn >= 0) {
        stack[depth++] = naiveLeafRow(store, tree.leaves[insn], leaf, r, fetcher);
      } else if (insn == ProjectionIndexScan.PredicateTree.OP_AND) {
        depth--;
        stack[depth - 1] = stack[depth - 1] && stack[depth];
      } else {
        depth--;
        stack[depth - 1] = stack[depth - 1] || stack[depth];
      }
    }
    return stack[0];
  }

  private static boolean naiveLeafRow(final ProjectionColumnStore store, final ColumnPredicate p, final int leaf,
      final int r, final ColumnSegmentFetcher fetcher) {
    final ProjectionColumnStore.ColumnSlice slice = store.column(p.column, fetcher)[leaf];
    if ((slice.presenceWords()[r >>> 6] & (1L << (r & 63))) == 0) {
      return false; // missing ⇒ predicate false, the interpreter's general-comparison rule
    }
    if (slice.boolWords() != null) {
      final boolean v = (slice.boolWords()[r >>> 6] & (1L << (r & 63))) != 0;
      return v == p.boolLit;
    }
    final long v = slice.numericValues()[r];
    return switch (p.op) {
      case GT -> v > p.longLit;
      case LT -> v < p.longLit;
      case GE -> v >= p.longLit;
      case LE -> v <= p.longLit;
      case EQ -> v == p.longLit;
      case NE -> v != p.longLit;
      case BETWEEN_GT_LT -> v > p.longLit && v < p.highLit;
      case BETWEEN_GT_LE -> v > p.longLit && v <= p.highLit;
      case BETWEEN_GE_LT -> v >= p.longLit && v < p.highLit;
      case BETWEEN_GE_LE -> v >= p.longLit && v <= p.highLit;
      // This oracle reads numericValues(); the string ops never apply to it.
      case STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS ->
        throw new IllegalStateException("string op in the numeric oracle: " + p.op);
    };
  }

  @Test
  void alpDoubleStreamsRouteAwayFromFusedKernels() {
    // Deterministic ALP: one-decimal values compress via the ALP digits stream, so the
    // double column's BODY carries the width escape — the fused gate must decline while
    // long/boolean shapes stay eligible and the slice kernels still serve the ALP column.
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
    final long[] longs = new long[KINDS.length];
    final boolean[] bools = new boolean[KINDS.length];
    final String[] strings = new String[KINDS.length];
    final boolean[] present = new boolean[KINDS.length];
    final boolean[] unrep = new boolean[KINDS.length];
    final boolean[] nonIntegral = new boolean[KINDS.length];
    final boolean[] nonDoubleSource = new boolean[KINDS.length];
    Arrays.fill(present, true);
    for (int r = 0; r < 512; r++) {
      longs[0] = r;
      longs[1] = ProjectionDoubleEncoding.encode(r / 10.0);
      bools[2] = (r & 1) == 0;
      strings[3] = "s" + (r % 4);
      page.appendRow(r + 1, longs, bools, strings, present, unrep, nonIntegral, nonDoubleSource);
    }
    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final int columnSegmentCount = encoded.columnSegmentIds().length;
    final int[] ids = new int[columnSegmentCount];
    final long[] offsets = new long[columnSegmentCount];
    long nextOffset = 1_000;
    for (int i = 0; i < columnSegmentCount; i++) {
      ids[i] = encoded.columnSegmentIds()[i];
      offsets[i] = nextOffset;
      segmentsByOffset.put(nextOffset, encoded.segments()[i]);
      nextOffset += 1 + encoded.segments()[i].length;
    }
    final ProjectionColumnStore store =
        new ProjectionColumnStore(List.of(new RowGroupDirectory(1, encoded.descriptor(), ids, offsets,
            new byte[ids.length][])));
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    };
    final ColumnPredicate[] longAndBool = {ColumnPredicate.numeric(0, Op.GT, 99L), ColumnPredicate.booleanEq(2, true)};
    assertTrue(ProjectionColumnSegmentFoldScan.eligible(store, longAndBool, 0, fetcher),
        "plain-FOR long/boolean streams must be fold-eligible");
    final ColumnPredicate[] doublePred = {ColumnPredicate.numeric(1, Op.GT, ProjectionDoubleEncoding.encode(25.0))};
    assertFalse(ProjectionColumnSegmentFoldScan.eligible(store, doublePred, -1, fetcher),
        "an ALP-escaped double stream must route to the slice kernels");
    assertEquals(ProjectionIndexScan.conjunctiveCount(List.of(raw), doublePred),
        ProjectionColumnScan.conjunctiveCount(store, doublePred, fetcher),
        "the slice path must still serve the ALP column exactly");
    assertEquals(ProjectionIndexScan.conjunctiveCount(List.of(raw), longAndBool),
        ProjectionColumnSegmentFoldScan.conjunctiveCount(store, longAndBool, fetcher),
        "the fused path must serve the eligible shape exactly");
  }

  @Test
  void doubleAggregatesAndCursorsAgreeAcrossRandomizedStores() {
    for (final long seed : new long[] {5, 23, 777}) {
      final Fixture fx = buildFixture(seed, 5, seed == 23);
      for (final ColumnPredicate[] preds : predicateShapes()) {
        final double[] expected = {0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
        ProjectionIndexByteScan.conjunctiveAggregateNumericDouble(fx.rawLeaves(), preds, 1, expected);
        final double[] actual = {0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};
        ProjectionColumnScan.conjunctiveAggregateNumericDouble(fx.store(), preds, 1, actual, fx.fetcher());
        for (int i = 0; i < 4; i++) {
          assertEquals(Double.doubleToRawLongBits(expected[i]), Double.doubleToRawLongBits(actual[i]),
              "double agg[" + i + "] seed=" + seed + " (bitwise, ±0.0 included)");
        }
        // Matched-value stream parity — bit-exact, in document order (feeds the seed-first
        // served-sum fold, so ORDER is part of the contract).
        final ProjectionIndexByteScan.MatchingDoubleCursor byteCursor =
            new ProjectionIndexByteScan.MatchingDoubleCursor(fx.rawLeaves(), preds, 1);
        final ProjectionColumnScan.MatchingDoubleCursor colCursor =
            new ProjectionColumnScan.MatchingDoubleCursor(fx.store(), preds, 1, fx.fetcher());
        while (true) {
          final boolean a = byteCursor.advance();
          final boolean b = colCursor.advance();
          assertEquals(a, b, "cursor exhaustion parity seed=" + seed);
          if (!a) {
            break;
          }
          assertEquals(Double.doubleToRawLongBits(byteCursor.value()), Double.doubleToRawLongBits(colCursor.value()),
              "cursor value parity seed=" + seed);
        }
      }
    }
  }

  @Test
  void directAssemblyMatchesPageSerialization() {
    // CI pin for the writeRawDirect ↔ reconstruct().serialize() byte identity: with the
    // cross-verifier forced on, every assembly self-checks and throws on divergence.
    ProjectionIndexColumnSegmentCodec.verifyDirectAssembly = true;
    try {
      for (final long seed : new long[] {2, 13, 4242}) {
        final Fixture fx = buildFixture(seed, 4, seed == 13);
        for (int leaf = 0; leaf < fx.rawLeaves().size(); leaf++) {
          final byte[] raw = fx.rawLeaves().get(leaf);
          final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
              ProjectionIndexColumnSegmentCodec.encode(raw);
          final java.util.Map<Integer, byte[]> byId = new HashMap<>();
          for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
            byId.put(encoded.columnSegmentIds()[i], encoded.segments()[i]);
          }
          final byte[] assembled = ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), byId::get);
          assertEquals(raw.length, assembled.length, "assembly length seed=" + seed);
          org.junit.jupiter.api.Assertions.assertArrayEquals(raw, assembled,
              "assembly bytes seed=" + seed + " leaf=" + leaf);
        }
      }
    } finally {
      ProjectionIndexColumnSegmentCodec.verifyDirectAssembly =
          Boolean.getBoolean("sirix.projection.verifyDirectAssembly");
    }
  }

  @Test
  void stringColumnsSliceAndMalformedStringShapesAreRejected() {
    final Fixture fx = buildFixture(17, 3, false);
    // The string column slices: dict-ids from the BODY chain, entries from the DICT chain.
    final ProjectionColumnStore.ColumnSlice[] slices = fx.store().column(3, fx.fetcher());
    assertEquals(fx.store().rowGroupCount(), slices.length);
    for (int leaf = 0; leaf < slices.length; leaf++) {
      final ProjectionColumnStore.ColumnSlice s = slices[leaf];
      if (s.rowCount() == 0) {
        continue;
      }
      assertTrue(s.stringDictIds() != null && s.stringDictIds().length == s.rowCount(),
          "leaf " + leaf + " must carry one dict-id per row");
      assertTrue(s.dictBytes() != null && s.dictOffsets() != null && s.dictSize() > 0,
          "leaf " + leaf + " must carry its decoded dictionary");
    }
    // The shapes the string kernel does NOT serve stay loud: a string literal against a
    // non-string column, and a string column without a literal.
    assertThrows(IllegalStateException.class,
        () -> ProjectionColumnScan.conjunctiveCount(fx.store(),
            new ColumnPredicate[] {ColumnPredicate.stringEq(0, "s1".getBytes(StandardCharsets.UTF_8))}, fx.fetcher()),
        "a string literal against a numeric column must be rejected loudly");
    assertThrows(IllegalStateException.class,
        () -> ProjectionColumnScan.conjunctiveCount(fx.store(),
            new ColumnPredicate[] {ColumnPredicate.numeric(3, Op.GT, 0L)}, fx.fetcher()),
        "a non-equality predicate on a string column must be rejected loudly");
  }

  @Test
  void sortedCollectionAndTopKAgreeWithByteKernels() {
    final int[] sortCols = {0};
    for (final long seed : new long[] {5, 23, 777, 424242}) {
      final Fixture fx = buildFixture(seed, 7, seed % 2 == 0);
      for (final ColumnPredicate[] preds : predicateShapes()) {
        final LongArrayList bv = new LongArrayList();
        final LongArrayList bk = new LongArrayList();
        final LongArrayList bm = new LongArrayList();
        ProjectionIndexByteScan.collectMatchingSortTuples(fx.rawLeaves(), preds, sortCols, bv, bk, bm);
        final LongArrayList sv = new LongArrayList();
        final LongArrayList sk = new LongArrayList();
        final LongArrayList sm = new LongArrayList();
        ProjectionColumnScan.collectMatchingSortTuples(fx.store(), preds, sortCols, sv, sk, sm, fx.fetcher());
        assertEquals(bv, sv, "sort-tuple parity seed=" + seed);
        assertEquals(bk, sk, "sort-key parity seed=" + seed);
        assertEquals(bm, sm, "missing-key parity seed=" + seed);
        for (final boolean desc : new boolean[] {false, true}) {
          for (final int k : new int[] {0, 1, 3, 17, 100_000}) {
            final long[] top =
                ProjectionColumnScan.topKRecordKeys(fx.store(), preds, sortCols, new boolean[] {desc}, k, fx.fetcher());
            if (!bm.isEmpty() && k > 0) {
              assertTrue(top == null, "a matching row without an order key must decline top-K");
              continue;
            }
            assertTrue(top != null, "top-K must serve when every matching row has its order key");
            assertArrayEquals(topKOracle(bv, bk, desc, k), top,
                "top-K parity seed=" + seed + " desc=" + desc + " k=" + k);
          }
        }
      }
    }
  }

  @Test
  void topKZonePruneStaysExactOnBandedLeaves() {
    // Banded, all-present leaves: ascending top-K's worst kept row settles inside leaf 0, so
    // every later leaf's min beats nothing and the zone prune skips it — the result must
    // still be exactly the oracle's, both directions, with and without predicates.
    final Fixture fx = buildFixture(97, 9, false, true, true);
    final ColumnPredicate[][] shapes =
        {new ColumnPredicate[0], new ColumnPredicate[] {ColumnPredicate.booleanEq(2, true)},
            new ColumnPredicate[] {ColumnPredicate.stringEq(3, "s1".getBytes(StandardCharsets.UTF_8))},};
    final int[] sortCols = {0};
    for (final ColumnPredicate[] preds : shapes) {
      final LongArrayList bv = new LongArrayList();
      final LongArrayList bk = new LongArrayList();
      final LongArrayList bm = new LongArrayList();
      ProjectionIndexByteScan.collectMatchingSortTuples(fx.rawLeaves(), preds, sortCols, bv, bk, bm);
      assertTrue(bm.isEmpty(), "banded fixture is all-present");
      for (final boolean desc : new boolean[] {false, true}) {
        for (final int k : new int[] {1, 5, 64}) {
          assertArrayEquals(topKOracle(bv, bk, desc, k),
              ProjectionColumnScan.topKRecordKeys(fx.store(), preds, sortCols, new boolean[] {desc}, k, fx.fetcher()),
              "pruned top-K parity desc=" + desc + " k=" + k);
        }
      }
    }
  }

  @Test
  void topKBestFirstStaysExactOnScatteredTieHeavyLeaves() {
    // The banded fixture above cannot catch a visitation-order bug: there the leaf minima ascend
    // with document order, so best-first visits leaves in document order anyway. This one is
    // adversarial on both counts — all-present (so best-first is admissible) with keys scattered
    // uniformly, so the best leaf is almost never leaf 0, and heavily tied (2000 distinct values
    // over ~6000 rows), so the k-boundary is decided by the DOCUMENT-ORDER rank of rows the walk
    // reached out of order. The oracle is the full stable sort in document order.
    for (final long seed : new long[] {11, 404, 90210}) {
      final Fixture fx = buildFixture(seed, 6, false, true);
      final int[] sortCols = {0};
      final ColumnPredicate[][] shapes =
          {new ColumnPredicate[0], new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, 0L)},
              new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, 990L)}, // rare
              new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, Long.MAX_VALUE - 1)}, // none
          };
      assertFalse(leafMinimaFollowDocumentOrder(fx),
          "seed " + seed + " must scatter the leaf minima, or best-first is never exercised");
      for (final ColumnPredicate[] preds : shapes) {
        final LongArrayList bv = new LongArrayList();
        final LongArrayList bk = new LongArrayList();
        final LongArrayList bm = new LongArrayList();
        ProjectionIndexByteScan.collectMatchingSortTuples(fx.rawLeaves(), preds, sortCols, bv, bk, bm);
        assertTrue(bm.isEmpty(), "an all-present fixture has no missing order cells");
        for (final boolean desc : new boolean[] {false, true}) {
          for (final int k : new int[] {1, 2, 10, 64, 5_000}) {
            final long skippedBefore = ProjectionColumnScan.topKLeavesSkippedCount();
            final long[] top =
                ProjectionColumnScan.topKRecordKeys(fx.store(), preds, sortCols, new boolean[] {desc}, k, fx.fetcher());
            assertArrayEquals(topKOracle(bv, bk, desc, k), top,
                "best-first top-K parity seed=" + seed + " desc=" + desc + " k=" + k);
            if (k == 1 && preds.length == 0) {
              assertTrue(ProjectionColumnScan.topKLeavesSkippedCount() > skippedBefore,
                  "k=1 over six scattered leaves must skip at least one leaf, or the plan never engaged");
            }
          }
        }
      }
    }
  }

  @Test
  void topKBestFirstStaysExactOnAStringOrderKey() {
    // A STRING first key: the leaf extrema are dict IDS in the slice, so the plan has to read the
    // column's value extrema instead — and with six distinct values over thousands of rows nearly
    // every row ties, making the document-order rank the only thing separating the k-boundary.
    for (final long seed : new long[] {7, 1234}) {
      final Fixture fx = buildFixture(seed, 6, false, true);
      final int[] sortCols = {3};
      for (final long filterGt : new long[] {Long.MIN_VALUE, 0L, 990L, Long.MAX_VALUE - 1}) {
        final ColumnPredicate[] preds = filterGt == Long.MIN_VALUE
            ? new ColumnPredicate[0]
            : new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, filterGt)};
        for (final boolean desc : new boolean[] {false, true}) {
          for (final int k : new int[] {1, 3, 25, 5_000}) {
            assertArrayEquals(stringTopKOracle(fx, 3, filterGt, desc, k),
                ProjectionColumnScan.topKRecordKeys(fx.store(), preds, sortCols, new boolean[] {desc}, k, fx.fetcher()),
                "string best-first top-K parity seed=" + seed + " gt=" + filterGt + " desc=" + desc + " k=" + k);
          }
        }
      }
    }
  }

  @Test
  void topKAbandonsTheBestFirstPlanWhenEveryLeafOffersTheSameKey() {
    // The Q25 shape: a low-cardinality string column whose smallest value occurs in every leaf. No
    // leaf can ever be pruned, so best-first can only cost the sequential slice access the
    // document-order walk has — the plan must hand back. Both halves are asserted, because the
    // ANSWER is identical either way and equality alone would pass whatever the plan decided.
    final Fixture fx = buildFixture(31, 6, false, true);
    final int[] tiedKey = {3}; // "s0".."s5" over ~1024 rows a leaf: every leaf holds an "s0"
    final int[] scatteredKey = {0};
    final ColumnPredicate[] none = new ColumnPredicate[0];

    final long tiedBefore = ProjectionColumnScan.topKPlanTiedCount();
    final long[] tiedTop =
        ProjectionColumnScan.topKRecordKeys(fx.store(), none, tiedKey, new boolean[] {false}, 5, fx.fetcher());
    assertEquals(1L, ProjectionColumnScan.topKPlanTiedCount() - tiedBefore,
        "every leaf's smallest string is the same, so the best-first plan must be abandoned");
    assertArrayEquals(stringTopKOracle(fx, 3, Long.MIN_VALUE, false, 5), tiedTop,
        "abandoning the plan must not change the answer");

    // Control on the SAME store: the scattered long key does NOT tie, so the plan must be kept.
    final long keptBefore = ProjectionColumnScan.topKPlanTiedCount();
    ProjectionColumnScan.topKRecordKeys(fx.store(), none, scatteredKey, new boolean[] {false}, 5, fx.fetcher());
    assertEquals(0L, ProjectionColumnScan.topKPlanTiedCount() - keptBefore,
        "a scattered key must KEEP the best-first plan — otherwise the tie test is over-firing");
  }

  /**
   * A fetcher that answers {@link ColumnSegmentFetcher#rangedFetchIsConcurrent()} — the catalog
   * fetcher's shape — so the top-k walk fans its chunks out over slabs instead of taking them on the
   * calling thread.
   */
  private static ColumnSegmentFetcher concurrentFetcher(final ColumnSegmentFetcher plain) {
    return new ColumnSegmentFetcher() {
      @Override
      public byte @Nullable [] @Nullable [] fetchAll(final long[] offsets) {
        return plain.fetchAll(offsets);
      }

      @Override
      public boolean rangedFetchIsConcurrent() {
        return true;
      }
    };
  }

  @Test
  void topKParallelSlabsAgreeWithTheSerialWalkAndTheOracle() {
    // The catalog's fetcher tolerates concurrent ranged fetches, so the walk splits every chunk into
    // slabs with a heap each — skipping on the frozen global heap AND on the slab's own — and merges
    // them after the chunk; a plain fetcher takes the same chunks on the calling thread. Both must be
    // the oracle and each other: a lost slab heap, a merge admitting by the wrong comparison, or a
    // slab-local skip that is not strict shows up as a wrong prefix. 40 scattered all-present leaves
    // give chunks of 1, 2, 4, 8, 16 and 9 leaves, so up to eight slabs share one chunk.
    for (final long seed : new long[] {5, 61}) {
      final Fixture fx = buildFixture(seed, 40, false, true);
      assertFalse(leafMinimaFollowDocumentOrder(fx), "seed " + seed + " must scatter the leaf minima");
      final ColumnSegmentFetcher concurrent = concurrentFetcher(fx.fetcher());
      final ColumnPredicate[][] shapes =
          {new ColumnPredicate[0], new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, 0L)},
              new ColumnPredicate[] {ColumnPredicate.booleanEq(2, true)},};
      final int[] longKey = {0};
      for (final ColumnPredicate[] preds : shapes) {
        final LongArrayList bv = new LongArrayList();
        final LongArrayList bk = new LongArrayList();
        final LongArrayList bm = new LongArrayList();
        ProjectionIndexByteScan.collectMatchingSortTuples(fx.rawLeaves(), preds, longKey, bv, bk, bm);
        assertTrue(bm.isEmpty(), "an all-present fixture has no missing order cells");
        for (final boolean desc : new boolean[] {false, true}) {
          for (final int k : new int[] {1, 3, 17, 300, 100_000}) {
            final long skippedBefore = ProjectionColumnScan.topKLeavesSkippedCount();
            final long[] parallel = ProjectionColumnScan.topKRecordKeys(fx.store(), preds, longKey,
                new boolean[] {desc}, k, concurrent);
            final long skippedParallel = ProjectionColumnScan.topKLeavesSkippedCount() - skippedBefore;
            final long[] serial =
                ProjectionColumnScan.topKRecordKeys(fx.store(), preds, longKey, new boolean[] {desc}, k, fx.fetcher());
            final String label = "seed=" + seed + " preds=" + preds.length + " desc=" + desc + " k=" + k;
            assertArrayEquals(topKOracle(bv, bk, desc, k), parallel, "parallel slabs vs oracle " + label);
            assertArrayEquals(serial, parallel, "serial walk vs parallel slabs " + label);
            if (k <= 3 && preds.length == 0) {
              assertTrue(skippedParallel > 0L,
                  "k=" + k + " over 40 scattered leaves must skip leaves on the parallel walk " + label);
            }
          }
        }
      }
      // A string first key and a (string, long) key pair through the same slabs, against a
      // slice-level oracle that shares no ordering code with the kernel.
      for (final boolean desc : new boolean[] {false, true}) {
        for (final int k : new int[] {1, 7, 250}) {
          final String label = "seed=" + seed + " desc=" + desc + " k=" + k;
          assertArrayEquals(stringTopKOracle(fx, 3, Long.MIN_VALUE, desc, k),
              ProjectionColumnScan.topKRecordKeys(fx.store(), new ColumnPredicate[0], new int[] {3},
                  new boolean[] {desc}, k, concurrent),
              "string key, parallel slabs vs oracle " + label);
          final long[] pair = ProjectionColumnScan.topKRecordKeys(fx.store(), new ColumnPredicate[0], new int[] {3, 0},
              new boolean[] {desc, !desc}, k, concurrent);
          assertArrayEquals(stringLongTopKOracle(fx, desc, !desc, k), pair, "(string, long) keys vs oracle " + label);
          assertArrayEquals(ProjectionColumnScan.topKRecordKeys(fx.store(), new ColumnPredicate[0], new int[] {3, 0},
              new boolean[] {desc, !desc}, k, fx.fetcher()), pair, "(string, long) keys, serial vs parallel " + label);
        }
      }
    }
  }

  @Test
  void topKDeclinesExactlyWhenAMatchingRowMissesAnOrderKey() {
    // Sparse presence, no predicate: some matching row misses its order key on nearly every leaf, and
    // the interpreter's empty-least/greatest placement is not this scan's to make — it declines.
    // The same store under a predicate on the order column has no such row (every op is
    // missing ⇒ false), so it must answer, and answer the oracle.
    final Fixture fx = buildFixture(19, 12, true, false);
    final ColumnSegmentFetcher concurrent = concurrentFetcher(fx.fetcher());
    final int[] longKey = {0};
    final LongArrayList bv = new LongArrayList();
    final LongArrayList bk = new LongArrayList();
    final LongArrayList bm = new LongArrayList();
    ProjectionIndexByteScan.collectMatchingSortTuples(fx.rawLeaves(), new ColumnPredicate[0], longKey, bv, bk, bm);
    assertFalse(bm.isEmpty(), "the sparse fixture must hold rows without an order key");
    for (final ColumnSegmentFetcher fetcher : new ColumnSegmentFetcher[] {fx.fetcher(), concurrent}) {
      assertEquals(null,
          ProjectionColumnScan.topKRecordKeys(fx.store(), new ColumnPredicate[0], longKey, new boolean[] {false}, 5,
              fetcher),
          "a matching row without an order key must decline");
    }
    final ColumnPredicate[] onKey = {ColumnPredicate.numeric(0, Op.GE, -1_000L)};
    bv.clear();
    bk.clear();
    bm.clear();
    ProjectionIndexByteScan.collectMatchingSortTuples(fx.rawLeaves(), onKey, longKey, bv, bk, bm);
    assertTrue(bm.isEmpty(), "a predicate on the order column admits no row without it");
    for (final boolean desc : new boolean[] {false, true}) {
      for (final int k : new int[] {1, 9, 4_000}) {
        for (final ColumnSegmentFetcher fetcher : new ColumnSegmentFetcher[] {fx.fetcher(), concurrent}) {
          assertArrayEquals(topKOracle(bv, bk, desc, k),
              ProjectionColumnScan.topKRecordKeys(fx.store(), onKey, longKey, new boolean[] {desc}, k, fetcher),
              "predicate-guaranteed presence desc=" + desc + " k=" + k);
        }
      }
    }
  }

  /**
   * Slice-level oracle for the key pair (string column 3, long column 0): every row in document
   * order, stably sorted by entry bytes then the long, each with its own direction.
   */
  private static long[] stringLongTopKOracle(final Fixture fx, final boolean descString, final boolean descLong,
      final int k) {
    final ProjectionColumnStore.ColumnSlice[] stringSlices = fx.store().column(3, fx.fetcher());
    final ProjectionColumnStore.ColumnSlice[] longSlices = fx.store().column(0, fx.fetcher());
    final long[][] recordKeys = fx.store().recordKeys(fx.fetcher());
    final List<byte[]> values = new ArrayList<>();
    final LongArrayList longs = new LongArrayList();
    final LongArrayList keys = new LongArrayList();
    for (int leaf = 0; leaf < fx.store().rowGroupCount(); leaf++) {
      final int rows = fx.store().rowCount(leaf);
      final ProjectionColumnStore.ColumnSlice slice = stringSlices[leaf];
      for (int r = 0; r < rows; r++) {
        final int dictId = slice.stringDictIds()[r];
        values.add(Arrays.copyOfRange(slice.dictBytes(), slice.dictOffset(dictId),
            slice.dictOffset(dictId) + slice.dictLength(dictId)));
        longs.add(longSlices[leaf].numericValues()[r]);
        keys.add(recordKeys[leaf][r]);
      }
    }
    final int n = keys.size();
    final Integer[] order = new Integer[n];
    for (int i = 0; i < n; i++) {
      order[i] = i;
    }
    Arrays.sort(order, (a, b) -> {
      int cmp = ProjectionColumnScan.compareDictEntries(values.get(a), values.get(b));
      if (cmp != 0) {
        return descString
            ? -cmp
            : cmp;
      }
      cmp = Long.compare(longs.getLong(a), longs.getLong(b));
      if (cmp != 0) {
        return descLong
            ? -cmp
            : cmp;
      }
      return Integer.compare(a, b);
    });
    final int take = Math.min(k, n);
    final long[] out = new long[take];
    for (int i = 0; i < take; i++) {
      out[i] = keys.getLong(order[i]);
    }
    return out;
  }

  /** Whether the leaves' minimum long key already ascends with leaf index (a banded corpus). */
  private static boolean leafMinimaFollowDocumentOrder(final Fixture fx) {
    long previous = Long.MIN_VALUE;
    for (final ProjectionColumnStore.ColumnSlice slice : fx.store().column(0, fx.fetcher())) {
      if (slice.rowCount() <= 0) {
        continue;
      }
      if (slice.min() < previous) {
        return false;
      }
      previous = slice.min();
    }
    return true;
  }

  /**
   * Independent oracle for a STRING order key: every row in DOCUMENT order, filtered by
   * {@code col 0 > filterGt} ({@link Long#MIN_VALUE} = unfiltered), stably sorted by entry bytes with
   * a document-order tiebreak. Reads only slice accessors — it shares no ordering code with the
   * kernel under test.
   */
  private static long[] stringTopKOracle(final Fixture fx, final int sortCol, final long filterGt, final boolean desc,
      final int k) {
    final ProjectionColumnStore.ColumnSlice[] sortSlices = fx.store().column(sortCol, fx.fetcher());
    final ProjectionColumnStore.ColumnSlice[] filterSlices = fx.store().column(0, fx.fetcher());
    final long[][] recordKeys = fx.store().recordKeys(fx.fetcher());
    final List<byte[]> values = new ArrayList<>();
    final LongArrayList keys = new LongArrayList();
    final LongArrayList ranks = new LongArrayList();
    long rank = 0;
    for (int leaf = 0; leaf < fx.store().rowGroupCount(); leaf++) {
      final int rows = fx.store().rowCount(leaf);
      final ProjectionColumnStore.ColumnSlice slice = sortSlices[leaf];
      for (int r = 0; r < rows; r++, rank++) {
        if (filterGt != Long.MIN_VALUE && filterSlices[leaf].numericValues()[r] <= filterGt) {
          continue;
        }
        final int dictId = slice.stringDictIds()[r];
        values.add(Arrays.copyOfRange(slice.dictBytes(), slice.dictOffset(dictId),
            slice.dictOffset(dictId) + slice.dictLength(dictId)));
        keys.add(recordKeys[leaf][r]);
        ranks.add(rank);
      }
    }
    final int n = keys.size();
    final Integer[] order = new Integer[n];
    for (int i = 0; i < n; i++) {
      order[i] = i;
    }
    Arrays.sort(order, (a, b) -> {
      final int cmp = ProjectionColumnScan.compareDictEntries(values.get(a), values.get(b));
      if (cmp != 0) {
        return desc
            ? -cmp
            : cmp;
      }
      return Long.compare(ranks.getLong(a), ranks.getLong(b));
    });
    final int take = Math.min(k, n);
    final long[] out = new long[take];
    for (int i = 0; i < take; i++) {
      out[i] = keys.getLong(order[i]);
    }
    return out;
  }

  /** Full stable sort of the collected (value, key) pairs, truncated to {@code k} keys. */
  private static long[] topKOracle(final LongArrayList values, final LongArrayList keys, final boolean desc,
      final int k) {
    final int n = keys.size();
    final Integer[] order = new Integer[n];
    for (int i = 0; i < n; i++) {
      order[i] = i;
    }
    Arrays.sort(order, (a, b) -> {
      final int cmp = Long.compare(values.getLong(a), values.getLong(b));
      if (cmp != 0) {
        return desc
            ? -cmp
            : cmp;
      }
      return Integer.compare(a, b);
    });
    final int take = Math.min(k, n);
    final long[] out = new long[take];
    for (int i = 0; i < take; i++) {
      out[i] = keys.getLong(order[i]);
    }
    return out;
  }

  @Test
  void corruptSegmentFailsLoudly() {
    final Fixture good = buildFixture(29, 3, false);
    // Rebuild with a fetcher that corrupts one BODY byte — hash verification must throw.
    final Fixture tampered = buildFixture(29, 3, false);
    final ProjectionColumnStore corrupt = new ProjectionColumnStore(directoriesOf(tampered));
    // A fetcher that corrupts one BODY byte — hash verification must throw when threaded in.
    final ColumnSegmentFetcher corruptFetcher = offsets -> {
      final byte[][] out = new byte[offsets.length][];
      for (int i = 0; i < offsets.length; i++) {
        final byte[] viaGood = fetchFrom(good, offsets[i]);
        out[i] = viaGood == null
            ? null
            : viaGood.clone();
        if (i == 1 && out[i] != null && out[i].length > 8) {
          out[i][8] ^= 0x40;
        }
      }
      return out;
    };
    assertThrows(IllegalStateException.class, () -> corrupt.column(0, corruptFetcher),
        "hash mismatch must reject, never serve tampered bytes");
    assertThrows(IllegalStateException.class, () -> corrupt.columnBytes(0, corruptFetcher),
        "the byte-level cache must reject tampered bytes identically");
    assertThrows(IllegalStateException.class,
        () -> ProjectionColumnSegmentFoldScan.conjunctiveCount(corrupt,
            new ColumnPredicate[] {ColumnPredicate.numeric(0, Op.GT, -1L)}, corruptFetcher),
        "the fused kernels must never fold unverified bytes");
  }

  // The store keeps its directories private; rebuild identical fixtures from the same seed
  // instead of reaching into internals (deterministic by construction).
  private static List<RowGroupDirectory> directoriesOf(final Fixture fx) {
    final List<RowGroupDirectory> dirs = new ArrayList<>();
    rebuildInto(fx, dirs, null);
    return dirs;
  }

  private static byte[] fetchFrom(final Fixture fx, final long offset) {
    final Map<Long, byte[]> map = new HashMap<>();
    rebuildInto(fx, null, map);
    return map.get(offset);
  }

  private static void rebuildInto(final Fixture fx, final List<RowGroupDirectory> dirsOut,
      final Map<Long, byte[]> segsOut) {
    long nextOffset = 1_000;
    int leaf = 0;
    for (final byte[] raw : fx.rawLeaves()) {
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
      final int columnSegmentCount = encoded.columnSegmentIds().length;
      final int[] ids = new int[columnSegmentCount];
      final long[] offsets = new long[columnSegmentCount];
      for (int i = 0; i < columnSegmentCount; i++) {
        ids[i] = encoded.columnSegmentIds()[i];
        offsets[i] = nextOffset;
        if (segsOut != null) {
          segsOut.put(nextOffset, encoded.segments()[i]);
        }
        nextOffset += 1 + encoded.segments()[i].length;
      }
      if (dirsOut != null) {
        dirsOut.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets,
            new byte[ids.length][]));
      }
      leaf++;
    }
  }

  @Test
  void maskedFillOfAColumnWhoseBytesAreRetainedIsPricedIncrementally() {
    // q19 at 100M/8 GB inside a leg: the predicate column's body was already retained, the residency
    // decision priced it at zero, and the masked fetch then charged its masked projection against a
    // budget the column's own body already filled — "masked slice fill adds 117 MB beside 2,118 MB
    // already retained", declined on every try. A masked fill of a retained column adds nothing.
    final Fixture fx = buildFixture(7L, 12, false);
    final int col = 0;
    fx.store().column(col, fx.fetcher());
    final long retained = fx.store().retainedFillBytes();
    assertTrue(retained > 0L, "the plain fill must retain bytes");
    final long prior = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(retained); // zero headroom
    try {
      final long[] keep = new long[(fx.store().leafCount() + 63) >>> 6];
      keep[0] = 1L; // leaf 0 only
      final ProjectionColumnStore.ColumnSlice[] masked = fx.store().columnMasked(col, fx.fetcher(), keep);
      assertEquals(fx.store().leafCount(), masked.length);
      assertTrue(masked[0].rowCount() > 0, "the kept leaf decodes");
      assertEquals(0, masked[1].rowCount(), "a dropped leaf is the pruned sentinel");
      assertEquals(retained, fx.store().retainedFillBytes(), "a masked fill retains nothing");
      final ProjectionColumnStore.ColumnSlice[] view = fx.store().columnMaskedView(col, fx.fetcher(), keep);
      assertSame(fx.store().column(col, fx.fetcher())[0], view[0], "the view hands out the resident slice");
      assertEquals(0, view[1].rowCount(), "the view prunes the dropped leaf");
    } finally {
      ProjectionColumnStore.setColumnFillBudgetBytesForTesting(prior);
    }
  }
}
