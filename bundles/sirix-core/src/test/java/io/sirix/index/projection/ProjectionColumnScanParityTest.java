/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexHOTStorage.LeafDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * P5b stage 3 parity oracle: the column-sliced kernels ({@link ProjectionColumnScan}) must
 * agree with the whole-leaf byte kernels ({@link ProjectionIndexScan} /
 * {@link ProjectionIndexByteScan}) on EVERY count, aggregate, and matched-value stream, over
 * randomized stores covering sparse presence, unrepresentable cells, empty leaves, negative
 * values, ±0.0 doubles, and every supported predicate op. The byte kernels are the
 * differential-suite-pinned truth; any divergence here is a column-kernel bug by definition.
 */
final class ProjectionColumnScanParityTest {

  /** Columns: 0 = long, 1 = double, 2 = boolean, 3 = string (never sliced, predicate-only). */
  private static final byte[] KINDS = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT,
  };

  private record Fixture(ProjectionColumnStore store, List<byte[]> rawLeaves) {
  }

  /** Build a randomized multi-leaf store + the equivalent raw payload list. */
  private static Fixture buildFixture(final long seed, final int leaves, final boolean withEmptyLeaf) {
    return buildFixture(seed, leaves, withEmptyLeaf, false);
  }

  /**
   * {@code allPresent = true} makes every cell present and clean, so mask words are almost
   * always fully set — the fused kernels' DENSE fast paths carry the whole evaluation
   * (the ~10% sparse default rarely yields a fully-set 64-row word).
   */
  private static Fixture buildFixture(final long seed, final int leaves, final boolean withEmptyLeaf,
      final boolean allPresent) {
    final Random rnd = new Random(seed);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<LeafDirectory> directories = new ArrayList<>(leaves);
    final List<byte[]> rawLeaves = new ArrayList<>(leaves);
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS.clone());
      final boolean empty = withEmptyLeaf && leaf == leaves / 2;
      final int rows = empty ? 0 : 1 + rnd.nextInt(ProjectionIndexLeafPage.MAX_ROWS);
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
        longs[0] = rnd.nextLong(-1_000, 1_000);
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
          nonDoubleSource[c] = false;                     // pure doubles
        }
        page.appendRow(recordKey, longs, bools, strings, present, unrep, nonIntegral,
            nonDoubleSource);
        recordKey += 1 + rnd.nextInt(3);
      }
      final byte[] raw = page.serialize();
      rawLeaves.add(raw);
      final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
      final int segCount = encoded.segmentIds().length;
      final int[] ids = new int[segCount];
      final long[] offsets = new long[segCount];
      for (int i = 0; i < segCount; i++) {
        ids[i] = encoded.segmentIds()[i] & 0xFF;
        offsets[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new LeafDirectory(leaf + 1, encoded.descriptor(), ids, offsets));
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(directories, wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    });
    return new Fixture(store, rawLeaves);
  }

  private static List<ColumnPredicate[]> predicateShapes() {
    final List<ColumnPredicate[]> shapes = new ArrayList<>();
    shapes.add(new ColumnPredicate[0]);
    shapes.add(new ColumnPredicate[] { ColumnPredicate.numeric(0, Op.GT, 0L) });
    shapes.add(new ColumnPredicate[] { ColumnPredicate.numeric(0, Op.LE, -100L) });
    shapes.add(new ColumnPredicate[] { ColumnPredicate.numeric(0, Op.EQ, 42L) });
    shapes.add(new ColumnPredicate[] {
        ColumnPredicate.numericBetween(0, Op.GE, -500L, Op.LT, 500L) });
    shapes.add(new ColumnPredicate[] { ColumnPredicate.booleanEq(2, true) });
    shapes.add(new ColumnPredicate[] { ColumnPredicate.booleanEq(2, false) });
    shapes.add(new ColumnPredicate[] {
        ColumnPredicate.numeric(0, Op.GT, -200L), ColumnPredicate.booleanEq(2, true) });
    shapes.add(new ColumnPredicate[] {
        ColumnPredicate.numeric(1, Op.GT, ProjectionDoubleEncoding.encode(10.0)) });
    shapes.add(new ColumnPredicate[] {
        ColumnPredicate.numeric(1, Op.LT, ProjectionDoubleEncoding.encode(-0.0)),
        ColumnPredicate.booleanEq(2, false) });
    // Zone-prunable extremes.
    shapes.add(new ColumnPredicate[] { ColumnPredicate.numeric(0, Op.GT, Long.MAX_VALUE - 1) });
    shapes.add(new ColumnPredicate[] { ColumnPredicate.numeric(0, Op.LT, Long.MIN_VALUE + 1) });
    return shapes;
  }

  @Test
  void countsAgreeAcrossRandomizedStores() {
    for (final long seed : new long[] { 1, 7, 42, 20260721 }) {
      final Fixture fx = buildFixture(seed, 7, seed % 2 == 0);
      for (final ColumnPredicate[] preds : predicateShapes()) {
        if (preds.length == 0) {
          long rows = 0;
          for (int leaf = 0; leaf < fx.store().leafCount(); leaf++) {
            rows += fx.store().rowCount(leaf);
          }
          assertEquals(ProjectionIndexScan.countRows(fx.rawLeaves()), rows, "countRows seed=" + seed);
          continue;
        }
        assertEquals(ProjectionIndexScan.conjunctiveCount(fx.rawLeaves(), preds),
            ProjectionColumnScan.conjunctiveCount(fx.store(), preds),
            "count parity seed=" + seed + " preds=" + preds.length + " op=" + preds[0].op);
      }
    }
  }

  @Test
  void longAggregatesAgreeAcrossRandomizedStores() {
    for (final long seed : new long[] { 3, 11, 99, 314159 }) {
      final Fixture fx = buildFixture(seed, 5, true);
      for (final ColumnPredicate[] preds : predicateShapes()) {
        final long[] expected = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionIndexByteScan.conjunctiveAggregateNumeric(fx.rawLeaves(), preds, 0, expected);
        final long[] actual = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionColumnScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, actual);
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
    for (final long seed : new long[] { 1, 7, 42, 20260721, 3, 11, 99 }) {
      final Fixture fx = buildFixture(seed, 7, seed % 2 == 0);
      final int leafCount = fx.store().leafCount();
      for (final ColumnPredicate[] preds : predicateShapes()) {
        if (!ProjectionSegmentFoldScan.eligible(fx.store(), preds, 0)) {
          continue;
        }
        final long expectedCount = preds.length == 0
            ? ProjectionIndexScan.countRows(fx.rawLeaves())
            : ProjectionIndexScan.conjunctiveCount(fx.rawLeaves(), preds);
        assertEquals(expectedCount, ProjectionSegmentFoldScan.conjunctiveCount(fx.store(), preds),
            "fused count parity seed=" + seed + " preds=" + preds.length);
        // Ranged split — the executor's chunked parallel dispatch shape.
        final int mid = leafCount / 2;
        assertEquals(expectedCount,
            ProjectionSegmentFoldScan.conjunctiveCount(fx.store(), preds, 0, mid)
                + ProjectionSegmentFoldScan.conjunctiveCount(fx.store(), preds, mid, leafCount),
            "fused ranged count parity seed=" + seed);
        final long[] expected = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionIndexByteScan.conjunctiveAggregateNumeric(fx.rawLeaves(), preds, 0, expected);
        final long[] actual = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, actual);
        for (int i = 0; i < 4; i++) {
          assertEquals(expected[i], actual[i], "fused long agg[" + i + "] seed=" + seed);
        }
        final long[] left = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, left, 0, mid);
        final long[] right = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, right, mid, leafCount);
        assertEquals(expected[0], left[0] + right[0], "fused ranged agg count seed=" + seed);
        assertEquals(expected[1], left[1] + right[1], "fused ranged agg sum seed=" + seed);
        assertEquals(expected[2], Math.min(left[2], right[2]), "fused ranged agg min seed=" + seed);
        assertEquals(expected[3], Math.max(left[3], right[3]), "fused ranged agg max seed=" + seed);
      }
    }
  }

  @Test
  void fusedDenseFastPathsAgreeOnAllPresentStores() {
    // All-present fixtures make every mask word fully set, so the DENSE fast paths (linear
    // 64-value compare + fold, no per-bit walk) carry the evaluation — pinned against the
    // byte kernels over every predicate shape, full and ranged.
    for (final long seed : new long[] { 4, 21, 1234 }) {
      final Fixture fx = buildFixture(seed, 6, false, true);
      final int leafCount = fx.store().leafCount();
      for (final ColumnPredicate[] preds : predicateShapes()) {
        if (!ProjectionSegmentFoldScan.eligible(fx.store(), preds, 0)) {
          continue;
        }
        final long expectedCount = preds.length == 0
            ? ProjectionIndexScan.countRows(fx.rawLeaves())
            : ProjectionIndexScan.conjunctiveCount(fx.rawLeaves(), preds);
        assertEquals(expectedCount, ProjectionSegmentFoldScan.conjunctiveCount(fx.store(), preds),
            "dense count parity seed=" + seed + " preds=" + preds.length);
        final long[] expected = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionIndexByteScan.conjunctiveAggregateNumeric(fx.rawLeaves(), preds, 0, expected);
        final long[] actual = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionSegmentFoldScan.conjunctiveAggregateNumeric(fx.store(), preds, 0, actual);
        for (int i = 0; i < 4; i++) {
          assertEquals(expected[i], actual[i], "dense long agg[" + i + "] seed=" + seed);
        }
        final int mid = leafCount / 2;
        assertEquals(expectedCount,
            ProjectionSegmentFoldScan.conjunctiveCount(fx.store(), preds, 0, mid)
                + ProjectionSegmentFoldScan.conjunctiveCount(fx.store(), preds, mid, leafCount),
            "dense ranged count parity seed=" + seed);
      }
    }
  }

  @Test
  void predicateTreeKernelsAgreeWithRowWiseOracle() {
    // P5b stage 6: arbitrary AND/OR trees. Oracle = a row-at-a-time evaluator over the
    // decoded slices (independent of the mask algebra under test); fused tree kernels must
    // match on count AND aggregate for randomized trees over sparse randomized stores.
    for (final long seed : new long[] { 6, 19, 404, 20260721 }) {
      final Random rnd = new Random(seed * 31 + 7);
      final Fixture fx = buildFixture(seed, 6, seed % 2 == 0);
      for (int trial = 0; trial < 12; trial++) {
        final ProjectionIndexScan.PredicateTree tree = randomTree(rnd);
        if (!ProjectionSegmentFoldScan.eligibleTree(fx.store(), tree, 0)) {
          continue;
        }
        final long expectedCount = naiveTreeCount(fx.store(), tree);
        assertEquals(expectedCount, ProjectionSegmentFoldScan.treeCount(fx.store(), tree),
            "tree count parity seed=" + seed + " trial=" + trial);
        final int leafCount = fx.store().leafCount();
        final int mid = leafCount / 2;
        assertEquals(expectedCount,
            ProjectionSegmentFoldScan.treeCount(fx.store(), tree, 0, mid)
                + ProjectionSegmentFoldScan.treeCount(fx.store(), tree, mid, leafCount),
            "tree ranged count parity seed=" + seed + " trial=" + trial);
        final long[] expected = naiveTreeAggregate(fx.store(), tree, 0);
        final long[] actual = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
        ProjectionSegmentFoldScan.treeAggregateNumeric(fx.store(), tree, 0, actual);
        for (int i = 0; i < 4; i++) {
          assertEquals(expected[i], actual[i],
              "tree agg[" + i + "] seed=" + seed + " trial=" + trial);
        }
      }
    }
    // Malformed programs must be rejected at construction.
    final ColumnPredicate leaf = ColumnPredicate.numeric(0, Op.GT, 10L);
    assertThrows(IllegalArgumentException.class, () -> ProjectionIndexScan.PredicateTree.of(
        new ColumnPredicate[] { leaf }, new byte[] { 0, 0 }), "must end at depth 1");
    assertThrows(IllegalArgumentException.class, () -> ProjectionIndexScan.PredicateTree.of(
        new ColumnPredicate[] { leaf }, new byte[] { 0, ProjectionIndexScan.PredicateTree.OP_OR }),
        "combinator underflow");
    assertThrows(IllegalArgumentException.class, () -> ProjectionIndexScan.PredicateTree.of(
        new ColumnPredicate[] { leaf }, new byte[] { 1 }), "leaf index out of range");
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
        case 2 -> ColumnPredicate.numeric(1, Op.GT,
            ProjectionDoubleEncoding.encode(rnd.nextDouble() * 100 - 50));
        case 3 -> ColumnPredicate.booleanEq(2, rnd.nextBoolean());
        default -> ColumnPredicate.numericBetween(0, Op.GE, rnd.nextInt(1000) - 500L,
            Op.LE, rnd.nextInt(1000));
      };
    }
    // Left-deep postfix: leaf0, then (leaf_i, combinator) pairs — always well-formed.
    int p = 0;
    program[p++] = 0;
    for (int i = 1; i < n; i++) {
      program[p++] = (byte) i;
      program[p++] = rnd.nextBoolean() ? ProjectionIndexScan.PredicateTree.OP_AND
          : ProjectionIndexScan.PredicateTree.OP_OR;
    }
    return ProjectionIndexScan.PredicateTree.of(leaves, program);
  }

  /** Row-at-a-time oracle over decoded slices — independent of the mask algebra. */
  private static long naiveTreeCount(final ProjectionColumnStore store,
      final ProjectionIndexScan.PredicateTree tree) {
    long total = 0;
    for (int leaf = 0; leaf < store.leafCount(); leaf++) {
      final int rows = store.rowCount(leaf);
      for (int r = 0; r < rows; r++) {
        if (naiveTreeRow(store, tree, leaf, r)) {
          total++;
        }
      }
    }
    return total;
  }

  private static long[] naiveTreeAggregate(final ProjectionColumnStore store,
      final ProjectionIndexScan.PredicateTree tree, final int aggCol) {
    final long[] acc = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
    for (int leaf = 0; leaf < store.leafCount(); leaf++) {
      final int rows = store.rowCount(leaf);
      final ProjectionColumnStore.ColumnSlice agg = store.column(aggCol)[leaf];
      for (int r = 0; r < rows; r++) {
        if (!naiveTreeRow(store, tree, leaf, r)) {
          continue;
        }
        if ((agg.presenceWords()[r >>> 6] & (1L << (r & 63))) == 0) {
          continue;
        }
        final long v = agg.numericValues()[r];
        acc[0]++;
        acc[1] += v;
        if (v < acc[2]) acc[2] = v;
        if (v > acc[3]) acc[3] = v;
      }
    }
    return acc;
  }

  private static boolean naiveTreeRow(final ProjectionColumnStore store,
      final ProjectionIndexScan.PredicateTree tree, final int leaf, final int r) {
    final boolean[] stack = new boolean[ProjectionIndexScan.PredicateTree.MAX_LEAVES];
    int depth = 0;
    for (final byte insn : tree.program) {
      if (insn >= 0) {
        stack[depth++] = naiveLeafRow(store, tree.leaves[insn], leaf, r);
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

  private static boolean naiveLeafRow(final ProjectionColumnStore store, final ColumnPredicate p,
      final int leaf, final int r) {
    final ProjectionColumnStore.ColumnSlice slice = store.column(p.column)[leaf];
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
      case BETWEEN_GT_LT -> v > p.longLit && v < p.highLit;
      case BETWEEN_GT_LE -> v > p.longLit && v <= p.highLit;
      case BETWEEN_GE_LT -> v >= p.longLit && v < p.highLit;
      case BETWEEN_GE_LE -> v >= p.longLit && v <= p.highLit;
    };
  }

  @Test
  void alpDoubleStreamsRouteAwayFromFusedKernels() {
    // Deterministic ALP: one-decimal values compress via the ALP digits stream, so the
    // double column's BODY carries the width escape — the fused gate must decline while
    // long/boolean shapes stay eligible and the slice kernels still serve the ALP column.
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS.clone());
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
    final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final int segCount = encoded.segmentIds().length;
    final int[] ids = new int[segCount];
    final long[] offsets = new long[segCount];
    long nextOffset = 1_000;
    for (int i = 0; i < segCount; i++) {
      ids[i] = encoded.segmentIds()[i] & 0xFF;
      offsets[i] = nextOffset;
      segmentsByOffset.put(nextOffset, encoded.segments()[i]);
      nextOffset += 1 + encoded.segments()[i].length;
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(
        List.of(new LeafDirectory(1, encoded.descriptor(), ids, offsets)), wanted -> {
          final byte[][] out = new byte[wanted.length][];
          for (int i = 0; i < wanted.length; i++) {
            out[i] = segmentsByOffset.get(wanted[i]);
          }
          return out;
        });
    final ColumnPredicate[] longAndBool =
        { ColumnPredicate.numeric(0, Op.GT, 99L), ColumnPredicate.booleanEq(2, true) };
    assertTrue(ProjectionSegmentFoldScan.eligible(store, longAndBool, 0),
        "plain-FOR long/boolean streams must be fold-eligible");
    final ColumnPredicate[] doublePred =
        { ColumnPredicate.numeric(1, Op.GT, ProjectionDoubleEncoding.encode(25.0)) };
    assertFalse(ProjectionSegmentFoldScan.eligible(store, doublePred, -1),
        "an ALP-escaped double stream must route to the slice kernels");
    assertEquals(ProjectionIndexScan.conjunctiveCount(List.of(raw), doublePred),
        ProjectionColumnScan.conjunctiveCount(store, doublePred),
        "the slice path must still serve the ALP column exactly");
    assertEquals(ProjectionIndexScan.conjunctiveCount(List.of(raw), longAndBool),
        ProjectionSegmentFoldScan.conjunctiveCount(store, longAndBool),
        "the fused path must serve the eligible shape exactly");
  }

  @Test
  void doubleAggregatesAndCursorsAgreeAcrossRandomizedStores() {
    for (final long seed : new long[] { 5, 23, 777 }) {
      final Fixture fx = buildFixture(seed, 5, seed == 23);
      for (final ColumnPredicate[] preds : predicateShapes()) {
        final double[] expected = { 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
        ProjectionIndexByteScan.conjunctiveAggregateNumericDouble(fx.rawLeaves(), preds, 1, expected);
        final double[] actual = { 0, 0, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY };
        ProjectionColumnScan.conjunctiveAggregateNumericDouble(fx.store(), preds, 1, actual);
        for (int i = 0; i < 4; i++) {
          assertEquals(Double.doubleToRawLongBits(expected[i]), Double.doubleToRawLongBits(actual[i]),
              "double agg[" + i + "] seed=" + seed + " (bitwise, ±0.0 included)");
        }
        // Matched-value stream parity — bit-exact, in document order (feeds the seed-first
        // served-sum fold, so ORDER is part of the contract).
        final ProjectionIndexByteScan.MatchingDoubleCursor byteCursor =
            new ProjectionIndexByteScan.MatchingDoubleCursor(fx.rawLeaves(), preds, 1);
        final ProjectionColumnScan.MatchingDoubleCursor colCursor =
            new ProjectionColumnScan.MatchingDoubleCursor(fx.store(), preds, 1);
        while (true) {
          final boolean a = byteCursor.advance();
          final boolean b = colCursor.advance();
          assertEquals(a, b, "cursor exhaustion parity seed=" + seed);
          if (!a) {
            break;
          }
          assertEquals(Double.doubleToRawLongBits(byteCursor.value()),
              Double.doubleToRawLongBits(colCursor.value()), "cursor value parity seed=" + seed);
        }
      }
    }
  }

  @Test
  void directAssemblyMatchesPageSerialization() {
    // CI pin for the writeRawDirect ↔ reconstruct().serialize() byte identity: with the
    // cross-verifier forced on, every assembly self-checks and throws on divergence.
    ProjectionIndexSegmentCodec.verifyDirectAssembly = true;
    try {
      for (final long seed : new long[] { 2, 13, 4242 }) {
        final Fixture fx = buildFixture(seed, 4, seed == 13);
        for (int leaf = 0; leaf < fx.rawLeaves().size(); leaf++) {
          final byte[] raw = fx.rawLeaves().get(leaf);
          final ProjectionIndexSegmentCodec.EncodedLeaf encoded =
              ProjectionIndexSegmentCodec.encode(raw);
          final java.util.Map<Integer, byte[]> byId = new HashMap<>();
          for (int i = 0; i < encoded.segmentIds().length; i++) {
            byId.put(encoded.segmentIds()[i] & 0xFF, encoded.segments()[i]);
          }
          final byte[] assembled =
              ProjectionIndexSegmentCodec.assembleRaw(encoded.descriptor(), byId::get);
          assertEquals(raw.length, assembled.length, "assembly length seed=" + seed);
          org.junit.jupiter.api.Assertions.assertArrayEquals(raw, assembled,
              "assembly bytes seed=" + seed + " leaf=" + leaf);
        }
      }
    } finally {
      ProjectionIndexSegmentCodec.verifyDirectAssembly =
          Boolean.getBoolean("sirix.projection.verifyDirectAssembly");
    }
  }

  @Test
  void stringPredicatesAndColumnsAreRejected() {
    final Fixture fx = buildFixture(17, 3, false);
    assertThrows(IllegalStateException.class, () -> fx.store().column(3),
        "string columns must not slice");
    final ColumnPredicate[] stringPred =
        { ColumnPredicate.stringEq(3, "s1".getBytes(StandardCharsets.UTF_8)) };
    assertThrows(IllegalStateException.class,
        () -> ProjectionColumnScan.conjunctiveCount(fx.store(), stringPred),
        "string predicates must be rejected loudly (callers gate and fall back)");
  }

  @Test
  void corruptSegmentFailsLoudly() {
    final Fixture good = buildFixture(29, 3, false);
    // Rebuild with a fetcher that corrupts one BODY byte — hash verification must throw.
    final Fixture tampered = buildFixture(29, 3, false);
    final ProjectionColumnStore corrupt = new ProjectionColumnStore(
        directoriesOf(tampered), offsets -> {
          final byte[][] out = new byte[offsets.length][];
          for (int i = 0; i < offsets.length; i++) {
            final byte[] viaGood = fetchFrom(good, offsets[i]);
            out[i] = viaGood == null ? null : viaGood.clone();
            if (i == 1 && out[i] != null && out[i].length > 8) {
              out[i][8] ^= 0x40;
            }
          }
          return out;
        });
    assertThrows(IllegalStateException.class, () -> corrupt.column(0),
        "hash mismatch must reject, never serve tampered bytes");
    assertThrows(IllegalStateException.class, () -> corrupt.columnBytes(0),
        "the byte-level cache must reject tampered bytes identically");
    assertThrows(IllegalStateException.class,
        () -> ProjectionSegmentFoldScan.conjunctiveCount(corrupt,
            new ColumnPredicate[] { ColumnPredicate.numeric(0, Op.GT, -1L) }),
        "the fused kernels must never fold unverified bytes");
  }

  // The store keeps its directories private; rebuild identical fixtures from the same seed
  // instead of reaching into internals (deterministic by construction).
  private static List<LeafDirectory> directoriesOf(final Fixture fx) {
    final List<LeafDirectory> dirs = new ArrayList<>();
    rebuildInto(fx, dirs, null);
    return dirs;
  }

  private static byte[] fetchFrom(final Fixture fx, final long offset) {
    final Map<Long, byte[]> map = new HashMap<>();
    rebuildInto(fx, null, map);
    return map.get(offset);
  }

  private static void rebuildInto(final Fixture fx, final List<LeafDirectory> dirsOut,
      final Map<Long, byte[]> segsOut) {
    long nextOffset = 1_000;
    int leaf = 0;
    for (final byte[] raw : fx.rawLeaves()) {
      final ProjectionIndexSegmentCodec.EncodedLeaf encoded = ProjectionIndexSegmentCodec.encode(raw);
      final int segCount = encoded.segmentIds().length;
      final int[] ids = new int[segCount];
      final long[] offsets = new long[segCount];
      for (int i = 0; i < segCount; i++) {
        ids[i] = encoded.segmentIds()[i] & 0xFF;
        offsets[i] = nextOffset;
        if (segsOut != null) {
          segsOut.put(nextOffset, encoded.segments()[i]);
        }
        nextOffset += 1 + encoded.segments()[i].length;
      }
      if (dirsOut != null) {
        dirsOut.add(new LeafDirectory(leaf + 1, encoded.descriptor(), ids, offsets));
      }
      leaf++;
    }
  }
}
