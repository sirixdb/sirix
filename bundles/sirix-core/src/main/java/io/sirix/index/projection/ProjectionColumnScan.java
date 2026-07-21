/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;

import java.util.Arrays;

/**
 * Column-sliced scan kernels (P5b stage 3, docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-7):
 * the {@link ProjectionIndexByteScan} conjunctive semantics re-expressed over
 * {@link ProjectionColumnStore} slices, so a query touches ONLY its predicate and aggregate
 * columns' BODY segments — never the whole raw leaf.
 *
 * <p><b>Parity contract.</b> Every rule mirrors {@code evaluateLeafMask} bit for bit: numeric
 * zone-skip on segment-truth min/max (including the {@code min > max} all-missing prune and
 * the BETWEEN skip table), missing-field ⇒ predicate-false via the presence AND, boolean
 * bitmap equality, {@code Double.compare} total order for double min/max folds, and the
 * aggregate column's own presence AND before folding. Supported predicate shapes: numeric
 * compare/BETWEEN and boolean equality — string predicates never reach these kernels
 * (callers decline to the whole-leaf path). {@code ProjectionColumnScanParityTest} pins the
 * equivalence against the byte kernels over randomized stores.
 *
 * <p>Scratch is thread-local and bounded by {@link ProjectionIndexLeafPage#MAX_ROWS} —
 * per-leaf evaluation allocates nothing.
 */
public final class ProjectionColumnScan {

  private static final int MASK_WORDS = (ProjectionIndexLeafPage.MAX_ROWS + 63) >>> 6;

  private static final class Scratch {
    final long[] mask = new long[MASK_WORDS];
  }

  private static final ThreadLocal<Scratch> SCRATCH = ThreadLocal.withInitial(Scratch::new);

  private ProjectionColumnScan() {
  }

  /**
   * Conjunctive count over the store's slices. Predicates must be column-sliceable
   * (numeric/boolean) — callers gate before dispatching.
   */
  public static long conjunctiveCount(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates) {
    return conjunctiveCount(store, predicates, 0, store.leafCount());
  }

  /** Ranged variant for the executor's chunked parallel dispatch — scratch is thread-local. */
  public static long conjunctiveCount(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int fromLeaf, final int toLeaf) {
    checkPredicates(store, predicates);
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates);
    final Scratch s = SCRATCH.get();
    long total = 0;
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = evaluateMask(predicates, cols, leaf, store.rowCount(leaf), s.mask);
      if (rowCount <= 0) {
        continue;
      }
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        total += Long.bitCount(s.mask[w]);
      }
    }
    return total;
  }

  /**
   * Conjunctive numeric-long aggregate — {@code acc = [count, sum, min, max]}, initialised
   * by the caller to {@code {0, 0, Long.MAX_VALUE, Long.MIN_VALUE}}. The aggregate column's
   * presence is ANDed before folding, exactly like the byte kernel.
   */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final long[] acc) {
    conjunctiveAggregateNumeric(store, predicates, numericColumn, acc, 0, store.leafCount());
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final long[] acc,
      final int fromLeaf, final int toLeaf) {
    checkPredicates(store, predicates);
    if (store.columnKind(numericColumn) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) {
      throw new IllegalStateException("aggregate column " + numericColumn + " is not NUMERIC_LONG");
    }
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates);
    final ColumnSlice[] aggCol = store.column(numericColumn);
    final Scratch s = SCRATCH.get();
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = evaluateMask(predicates, cols, leaf, store.rowCount(leaf), s.mask);
      if (rowCount <= 0) {
        continue;
      }
      final ColumnSlice agg = aggCol[leaf];
      final long[] values = agg.numericValues();
      final long[] presence = agg.presenceWords();
      final int stride = (rowCount + 63) >>> 6;
      long count = acc[0];
      long sum = acc[1];
      long min = acc[2];
      long max = acc[3];
      for (int w = 0; w < stride; w++) {
        long word = s.mask[w] & presence[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          final long v = values[rowIdx];
          count++;
          sum += v;
          if (v < min) min = v;
          if (v > max) max = v;
        }
      }
      acc[0] = count;
      acc[1] = sum;
      acc[2] = min;
      acc[3] = max;
    }
  }

  /**
   * Conjunctive numeric-double aggregate — {@code acc = [count, sum, min, max]} as doubles,
   * initialised to {@code {0, 0, +Inf, -Inf}}. Min/max use {@code Double.compare} total
   * order (parity with the interpreter's comparator, {@code -0.0 < 0.0}); the kernel sum is
   * diagnostic only — served sums fold seed-first through {@link MatchingDoubleCursor}.
   */
  public static void conjunctiveAggregateNumericDouble(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final double[] acc) {
    conjunctiveAggregateNumericDouble(store, predicates, numericColumn, acc, 0, store.leafCount());
  }

  /** Ranged variant for chunked parallel dispatch (count/min/max are merge-order-insensitive). */
  public static void conjunctiveAggregateNumericDouble(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final double[] acc,
      final int fromLeaf, final int toLeaf) {
    checkPredicates(store, predicates);
    if (store.columnKind(numericColumn) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE) {
      throw new IllegalStateException("aggregate column " + numericColumn + " is not NUMERIC_DOUBLE");
    }
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates);
    final ColumnSlice[] aggCol = store.column(numericColumn);
    final Scratch s = SCRATCH.get();
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final int rowCount = evaluateMask(predicates, cols, leaf, store.rowCount(leaf), s.mask);
      if (rowCount <= 0) {
        continue;
      }
      final ColumnSlice agg = aggCol[leaf];
      final long[] values = agg.numericValues();
      final long[] presence = agg.presenceWords();
      final int stride = (rowCount + 63) >>> 6;
      double count = acc[0];
      double sum = acc[1];
      double min = acc[2];
      double max = acc[3];
      for (int w = 0; w < stride; w++) {
        long word = s.mask[w] & presence[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          final double v = ProjectionDoubleEncoding.decode(values[rowIdx]);
          count++;
          sum += v;
          if (Double.compare(v, min) < 0) min = v;
          if (Double.compare(v, max) > 0) max = v;
        }
      }
      acc[0] = count;
      acc[1] = sum;
      acc[2] = min;
      acc[3] = max;
    }
  }

  /**
   * Pull-cursor over the predicate-matched, presence-filtered cells of one NUMERIC_DOUBLE
   * column in document order — the column-sliced twin of
   * {@link ProjectionIndexByteScan.MatchingDoubleCursor}, feeding the executor's seed-first
   * served-sum fold. Single-threaded use (borrows the thread's scratch).
   */
  public static final class MatchingDoubleCursor {

    private final ProjectionColumnStore store;
    private final ColumnPredicate[] predicates;
    private final ColumnSlice[][] predicateCols;
    private final ColumnSlice[] aggCol;
    private final Scratch s = SCRATCH.get();

    private int leaf;
    private int rowCount;
    private long[] values;
    private int stride;
    private int wordIdx;
    private long word;
    private double current;
    private boolean leafLoaded;

    public MatchingDoubleCursor(final ProjectionColumnStore store,
        final ColumnPredicate[] predicates, final int numericColumn) {
      checkPredicates(store, predicates);
      if (store.columnKind(numericColumn) != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_DOUBLE) {
        throw new IllegalStateException("cursor column " + numericColumn + " is not NUMERIC_DOUBLE");
      }
      this.store = store;
      this.predicates = predicates;
      this.predicateCols = resolvePredicateColumns(store, predicates);
      this.aggCol = store.column(numericColumn);
    }

    /** Advance to the next matching cell; {@code false} = stream exhausted. */
    public boolean advance() {
      while (true) {
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = ((wordIdx - 1) << 6) + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          current = ProjectionDoubleEncoding.decode(values[rowIdx]);
          return true;
        }
        if (leafLoaded && wordIdx < stride) {
          final ColumnSlice agg = aggCol[leaf - 1];
          word = s.mask[wordIdx] & agg.presenceWords()[wordIdx];
          wordIdx++;
          continue;
        }
        if (leaf >= store.leafCount()) {
          return false;
        }
        rowCount = evaluateMask(predicates, predicateCols, leaf, store.rowCount(leaf), s.mask);
        leaf++;
        if (rowCount <= 0) {
          leafLoaded = false;
          continue;
        }
        final ColumnSlice agg = aggCol[leaf - 1];
        values = agg.numericValues();
        stride = (rowCount + 63) >>> 6;
        wordIdx = 0;
        word = 0L;
        leafLoaded = true;
      }
    }

    /** The matched cell decoded to its double value; valid after a true {@link #advance()}. */
    public double value() {
      return current;
    }
  }

  // ==================== shared evaluation ====================

  private static void checkPredicates(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    for (final ColumnPredicate p : predicates) {
      if (!store.columnSliceable(p.column)) {
        throw new IllegalStateException("Predicate column " + p.column + " is not sliceable");
      }
      if (p.stringLitBytes != null) {
        throw new IllegalStateException("String predicates are not column-sliceable");
      }
    }
  }

  private static ColumnSlice[][] resolvePredicateColumns(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates) {
    final ColumnSlice[][] cols = new ColumnSlice[predicates.length][];
    for (int i = 0; i < predicates.length; i++) {
      cols[i] = store.column(predicates[i].column);
    }
    return cols;
  }

  /**
   * Build the conjunctive mask for leaf {@code leaf} into {@code mask} — the slice twin of
   * {@code ProjectionIndexByteScan.evaluateLeafMask}: numeric zone-skip (segment truth,
   * {@code min > max} prunes outright), per-predicate evaluation, missing ⇒ false via the
   * presence AND. Returns the leaf's rowCount (0 = pruned/empty; the mask may still be
   * all-zero for a live rowCount).
   */
  private static int evaluateMask(final ColumnPredicate[] predicates, final ColumnSlice[][] cols,
      final int leaf, final int rowCount, final long[] mask) {
    if (rowCount <= 0) {
      return 0;
    }
    // Zone-map prune — numeric predicate columns only (byte-kernel policy).
    for (int i = 0; i < predicates.length; i++) {
      final ColumnSlice slice = cols[i][leaf];
      if (slice.numericValues() == null) {
        continue;
      }
      if (slice.min() > slice.max()) {
        return 0;
      }
      if (zoneSkip(predicates[i], slice.min(), slice.max())) {
        return 0;
      }
    }
    final int rows = rowCount;
    final int stride = (rows + 63) >>> 6;
    fillAllTrue(mask, rows, stride);
    for (int i = 0; i < predicates.length; i++) {
      final ColumnPredicate p = predicates[i];
      final ColumnSlice slice = cols[i][leaf];
      final long[] presence = slice.presenceWords();
      final long[] values = slice.numericValues();
      if (values != null) {
        evalNumeric(values, rows, p, presence, mask);
      } else {
        evalBoolean(slice.boolWords(), stride, p.boolLit, presence, mask);
      }
    }
    return rows;
  }

  /** Single zone-skip authority: the byte kernel's table (iter07-range-fusion-analysis.md). */
  private static boolean zoneSkip(final ColumnPredicate p, final long min, final long max) {
    return ProjectionIndexByteScan.zoneSkip(p, min, max);
  }

  /** Rowless leaves never reach here with {@code predicates.length == 0} callers — count paths pass ≥1. */
  private static void fillAllTrue(final long[] mask, final int rowCount, final int stride) {
    Arrays.fill(mask, 0, stride, -1L);
    final int tailBits = rowCount & 63;
    if (tailBits != 0) {
      mask[stride - 1] = (1L << tailBits) - 1L;
    }
  }

  private static void evalNumeric(final long[] values, final int rowCount, final ColumnPredicate p,
      final long[] presence, final long[] mask) {
    final int stride = (rowCount + 63) >>> 6;
    final long lit = p.longLit;
    final long high = p.highLit;
    for (int w = 0; w < stride; w++) {
      long m = mask[w] & presence[w];
      if (m == 0L) {
        mask[w] = 0L;
        continue;
      }
      long out = 0L;
      final int rowBase = w << 6;
      long candidates = m;
      while (candidates != 0L) {
        final int bit = Long.numberOfTrailingZeros(candidates);
        candidates &= candidates - 1L;
        final int rowIdx = rowBase + bit;
        if (rowIdx >= rowCount) {
          break;
        }
        final long v = values[rowIdx];
        final boolean match = switch (p.op) {
          case GT -> v > lit;
          case LT -> v < lit;
          case GE -> v >= lit;
          case LE -> v <= lit;
          case EQ -> v == lit;
          case BETWEEN_GT_LT -> v > lit && v < high;
          case BETWEEN_GT_LE -> v > lit && v <= high;
          case BETWEEN_GE_LT -> v >= lit && v < high;
          case BETWEEN_GE_LE -> v >= lit && v <= high;
        };
        if (match) {
          out |= 1L << bit;
        }
      }
      mask[w] = out;
    }
  }

  private static void evalBoolean(final long[] boolWords, final int stride, final boolean lit,
      final long[] presence, final long[] mask) {
    for (int w = 0; w < stride; w++) {
      final long match = lit ? boolWords[w] : ~boolWords[w];
      mask[w] &= match & presence[w];
    }
  }
}
