/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The predicate TREE's keep mask: every leaf predicate's zone / fingerprint evidence gathered on its
 * own and combined by the program — AND intersects, OR unites, NOT keeps every leaf — so an
 * OR-bearing WHERE over a sorted key prunes exactly like the flat conjunction would. A tree used to
 * be filled FULL (q40 at 100M: one {@code IN} made the whole WHERE a tree and every leaf of every
 * column was fetched). The masked tree fill must answer byte-identically to the full one while
 * fetching only the surviving leaves.
 */
final class PredicateTreeKeepMaskTest {

  private static final byte[] KINDS = {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};

  private static final int LEAVES = 8;
  private static final int ROWS = 64;

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher, AtomicInteger fetchedSegments) {
  }

  /**
   * Column 0 is the sorted key ({@code leaf * 1000 + r}: every leaf its own zone), column 1 the
   * leaf's class ({@code leaf % 4}, constant per leaf), column 2 a per-leaf title.
   */
  private static Fixture buildFixture() {
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(LEAVES);
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      long recordKey = leaf * 100_000L + 1;
      for (int r = 0; r < ROWS; r++) {
        page.appendRow(recordKey++, new long[] {leaf * 1_000L + r, leaf % 4, 0L}, new boolean[] {false, false, false},
            new String[] {null, null, "t-" + leaf + "-" + r}, new boolean[] {true, true, true},
            new boolean[] {false, false, false}, new boolean[] {false, false, false}, new boolean[] {false, false, false});
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int[] idArr = new int[encoded.columnSegmentIds().length];
      final long[] offArr = new long[idArr.length];
      for (int i = 0; i < idArr.length; i++) {
        idArr[i] = encoded.columnSegmentIds()[i];
        offArr[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), idArr, offArr, new byte[idArr.length][]));
    }
    final AtomicInteger fetched = new AtomicInteger();
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        if (wanted[i] != Constants.NULL_ID_LONG) {
          out[i] = segmentsByOffset.get(wanted[i]);
          if (out[i] != null) {
            fetched.incrementAndGet();
          }
        }
      }
      return out;
    };
    return new Fixture(new ProjectionColumnStore(directories), fetcher, fetched);
  }

  private static final ColumnPredicate[] NO_PREDICATES = new ColumnPredicate[0];

  /** {@code col0 in leaf L}'s zone: {@code col0 >= L*1000} — a lower bound alone already isolates the leaves at or above L. */
  private static ColumnPredicate keyAtLeast(final int leaf) {
    return ColumnPredicate.numeric(0, Op.GE, leaf * 1_000L);
  }

  private static ColumnPredicate keyBelow(final int leaf) {
    return ColumnPredicate.numeric(0, Op.LT, leaf * 1_000L);
  }

  private static ColumnPredicate classIs(final int c) {
    return ColumnPredicate.numeric(1, Op.EQ, c);
  }

  private static long[] mask(final int... leaves) {
    final long[] m = new long[1];
    for (final int leaf : leaves) {
      m[0] |= 1L << leaf;
    }
    return m;
  }

  private static int kept(final long[] m) {
    return Long.bitCount(m[0]);
  }

  @Test
  @DisplayName("AND intersects, OR unites, and an evidence-less operand keeps everything")
  void programCombinesLeafEvidence() {
    final Fixture f = buildFixture();
    // (key >= 6000) OR (key < 1000): leaves 6, 7 and 0.
    final PredicateTree or = PredicateTree.of(new ColumnPredicate[] {keyAtLeast(6), keyBelow(1)},
        new byte[] {0, 1, PredicateTree.OP_OR});
    assertArrayEquals(mask(0, 6, 7), ProjectionColumnScan.predicateKeepMask(f.store(), NO_PREDICATES, or, f.fetcher()),
        "OR unites each operand's own leaf set");
    // (key >= 6000) AND (class = 3): leaves {6, 7} ∩ {3, 7} = {7}.
    final PredicateTree and = PredicateTree.of(new ColumnPredicate[] {keyAtLeast(6), classIs(3)},
        new byte[] {0, 1, PredicateTree.OP_AND});
    assertArrayEquals(mask(7), ProjectionColumnScan.predicateKeepMask(f.store(), NO_PREDICATES, and, f.fetcher()),
        "AND intersects");
    // (key >= 6000 AND class = 3) OR (key < 1000): {7} ∪ {0}.
    final PredicateTree nested = PredicateTree.of(new ColumnPredicate[] {keyAtLeast(6), classIs(3), keyBelow(1)},
        new byte[] {0, 1, PredicateTree.OP_AND, 2, PredicateTree.OP_OR});
    assertArrayEquals(mask(0, 7),
        ProjectionColumnScan.predicateKeepMask(f.store(), NO_PREDICATES, nested, f.fetcher()), "nested program");
    // An operand no leaf can satisfy prunes everything under AND ...
    final PredicateTree empty = PredicateTree.of(new ColumnPredicate[] {keyAtLeast(6), classIs(0)},
        new byte[] {0, 1, PredicateTree.OP_AND});
    assertEquals(0, kept(ProjectionColumnScan.predicateKeepMask(f.store(), NO_PREDICATES, empty, f.fetcher())),
        "{6,7} ∩ {0,4} is empty — a non-null all-zero mask, not 'nothing pruned'");
    // ... and an operand WITHOUT evidence (a CONTAINS literal has no prune rule) keeps every leaf under OR.
    final PredicateTree unknownOr = PredicateTree.of(new ColumnPredicate[] {keyAtLeast(6),
        ColumnPredicate.stringContains(2, "t-".getBytes(StandardCharsets.UTF_8))}, new byte[] {0, 1, PredicateTree.OP_OR});
    assertNull(ProjectionColumnScan.predicateKeepMask(f.store(), NO_PREDICATES, unknownOr, f.fetcher()),
        "no leaf is proven empty when one OR operand has no evidence");
    // NOT keeps everything: the operand's evidence says where the operand cannot match, nothing about its negation.
    final PredicateTree not = PredicateTree.of(new ColumnPredicate[] {keyAtLeast(6)}, new byte[] {0, PredicateTree.OP_NOT});
    assertNull(ProjectionColumnScan.predicateKeepMask(f.store(), NO_PREDICATES, not, f.fetcher()), "NOT prunes nothing");
    // A string-equality leaf brings fingerprint evidence: (class = 1) OR (title = "t-3-7") ⊇ {1, 5} ∪ {3}.
    final PredicateTree bloomOr = PredicateTree.of(new ColumnPredicate[] {classIs(1),
        ColumnPredicate.stringEq(2, "t-3-7".getBytes(StandardCharsets.UTF_8))}, new byte[] {0, 1, PredicateTree.OP_OR});
    final long[] bloom = ProjectionColumnScan.predicateKeepMask(f.store(), NO_PREDICATES, bloomOr, f.fetcher());
    assertTrue(bloom != null && (bloom[0] & mask(1, 3, 5)[0]) == mask(1, 3, 5)[0], "the true home leaves survive");
    assertTrue(kept(bloom) < LEAVES, "leaves the fingerprint rejects are dropped");
  }

  /** Rows the tree admits on {@code leaf}, evaluated over {@code treeCols}. */
  private static int rowsAdmitted(final ProjectionColumnStore store, final PredicateTree tree,
      final ColumnSlice[][] treeCols, final int leaf) {
    final long[] mask = new long[(ROWS + 63) >>> 6];
    final int rowCount = ProjectionColumnScan.evaluateMaskTree(tree, treeCols, leaf, store.rowCount(leaf), mask);
    if (rowCount <= 0) {
      return 0;
    }
    int rows = 0;
    for (final long w : mask) {
      rows += Long.bitCount(w);
    }
    return rows;
  }

  @Test
  @DisplayName("The masked tree fill answers like the full one and fetches only the surviving leaves")
  void maskedTreeFillIsExactAndFetchesLess() {
    // (key >= 6000 AND class = 3) OR (key < 1000): rows 64 (leaf 7) + 64 (leaf 0).
    final PredicateTree tree = PredicateTree.of(new ColumnPredicate[] {keyAtLeast(6), classIs(3), keyBelow(1)},
        new byte[] {0, 1, PredicateTree.OP_AND, 2, PredicateTree.OP_OR});
    final Fixture full = buildFixture();
    final ColumnSlice[][] fullCols = ProjectionColumnScan.resolveTreeColumnsShared(full.store(), tree, full.fetcher(), null);
    final Fixture masked = buildFixture();
    final long[] keep = ProjectionColumnScan.predicateKeepMask(masked.store(), NO_PREDICATES, tree, masked.fetcher());
    final ColumnSlice[][] maskedCols =
        ProjectionColumnScan.resolveTreeColumnsShared(masked.store(), tree, masked.fetcher(), keep);
    int fullRows = 0;
    int maskedRows = 0;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final int a = rowsAdmitted(full.store(), tree, fullCols, leaf);
      final int b = rowsAdmitted(masked.store(), tree, maskedCols, leaf);
      assertEquals(a, b, "leaf " + leaf + " admits the same rows either way");
      fullRows += a;
      maskedRows += b;
    }
    assertEquals(2 * ROWS, fullRows, "ground truth: leaf 7 and leaf 0");
    assertEquals(fullRows, maskedRows);
    // The full fill fetched every leaf's BODY of both numeric columns (16 segments); the masked one only
    // the two surviving leaves' (4) — anything in between means the mask never reached the fill.
    assertEquals(2 * LEAVES, full.fetchedSegments().get(), "full fill: one BODY per leaf per tree column");
    assertEquals(2 * 2, masked.fetchedSegments().get(), "masked fill: the surviving leaves only");
    for (int leaf = 1; leaf < 7; leaf++) {
      for (final ColumnSlice[] col : maskedCols) {
        assertTrue(col[leaf].rowCount() <= 0, "leaf " + leaf + " is the pruned sentinel in every tree column");
      }
    }
  }

  @Test
  @DisplayName("allPruned: exact at word boundaries and on unaligned ranges")
  void allPrunedRanges() {
    assertFalse(ProjectionColumnScan.allPruned(null, 0, 64), "no mask: nothing is pruned");
    final long[] keep = new long[4]; // 256 leaves, bits 70 and 130 kept
    keep[1] = 1L << (70 - 64);
    keep[2] = 1L << (130 - 128);
    assertTrue(ProjectionColumnScan.allPruned(keep, 0, 64));
    assertTrue(ProjectionColumnScan.allPruned(keep, 0, 70));
    assertFalse(ProjectionColumnScan.allPruned(keep, 0, 71));
    assertFalse(ProjectionColumnScan.allPruned(keep, 64, 71));
    assertFalse(ProjectionColumnScan.allPruned(keep, 70, 71));
    assertTrue(ProjectionColumnScan.allPruned(keep, 71, 130));
    assertTrue(ProjectionColumnScan.allPruned(keep, 65, 70));
    assertFalse(ProjectionColumnScan.allPruned(keep, 128, 131));
    assertTrue(ProjectionColumnScan.allPruned(keep, 131, 256));
    assertTrue(ProjectionColumnScan.allPruned(keep, 192, 256));
    assertFalse(ProjectionColumnScan.allPruned(keep, 0, 256));
    assertFalse(ProjectionColumnScan.allPruned(keep, 5, 5), "an empty range is never 'all pruned'");
    final long[] one = mask(1, 5);
    assertTrue(ProjectionColumnScan.allPruned(one, 0, 1));
    assertFalse(ProjectionColumnScan.allPruned(one, 1, 2));
    assertTrue(ProjectionColumnScan.allPruned(one, 2, 5));
    assertTrue(ProjectionColumnScan.allPruned(one, 6, 8));
    assertFalse(ProjectionColumnScan.allPruned(one, 0, 8));
  }
}
