/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

/**
 * Column-sliced scan kernels (P5b stage 3, docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-7): the
 * {@link ProjectionIndexByteScan} conjunctive semantics re-expressed over
 * {@link ProjectionColumnStore} slices, so a query touches ONLY its predicate and aggregate
 * columns' BODY segments — never the whole raw leaf.
 *
 * <p>
 * <b>Parity contract.</b> Every rule mirrors {@code evaluateRowGroupMask} bit for bit: numeric
 * zone-skip on segment-truth min/max (including the {@code min > max} all-missing prune and the
 * BETWEEN skip table), missing-field ⇒ predicate-false via the presence AND, boolean bitmap
 * equality, {@code Double.compare} total order for double min/max folds, and the aggregate column's
 * own presence AND before folding. Supported predicate shapes: numeric compare/BETWEEN, boolean
 * equality, and string equality — the string leaf resolves its literal against the leaf's DICT
 * segment (absent ⇒ the leaf contributes nothing) and compares dict-ids, exactly like the byte
 * kernel, from two segments instead of a whole leaf. {@code ProjectionColumnScanParityTest} pins
 * the equivalence against the byte kernels over randomized stores.
 *
 * <p>
 * Scratch is thread-local and bounded by {@link ProjectionIndexRowGroupPage#MAX_ROWS} — per-leaf
 * evaluation allocates nothing.
 */
public final class ProjectionColumnScan {

  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");

  /**
   * Escape hatch for {@link #topKRecordKeys}'s best-first leaf visitation: set
   * {@code -Dsirix.topK.docOrder=true} to walk leaves in document order with the zone prune alone.
   * Same answer either way — this exists to A/B the plan's cost against its pruning on a real corpus.
   */
  private static final boolean TOPK_DOC_ORDER = Boolean.getBoolean("sirix.topK.docOrder");

  /**
   * Leaves the bounded top-K never opened — skipped by the best-first stop or by the document-order
   * zone prune. Test/ops observability: a pruning route that silently stops pruning is
   * indistinguishable from one that works, since the answer is identical either way.
   */
  private static final LongAdder TOPK_LEAVES_SKIPPED = new LongAdder();

  /** Test/ops observability for {@link #TOPK_LEAVES_SKIPPED}. */
  public static long topKLeavesSkippedCount() {
    return TOPK_LEAVES_SKIPPED.sum();
  }

  /**
   * Bounded top-K plans abandoned back to the document-order walk because every leaf offered the SAME
   * best first key. Observability only — the answer is identical either way, so nothing else
   * distinguishes this decision from a best-first walk that simply never got to stop.
   */
  private static final LongAdder TOPK_PLAN_TIED = new LongAdder();

  /** Test/ops observability for {@link #TOPK_PLAN_TIED}. */
  public static long topKPlanTiedCount() {
    return TOPK_PLAN_TIED.sum();
  }

  /** Sort-key kind: a raw long, compared directly. */
  private static final byte KEY_NUMERIC = 0;

  /** Sort-key kind: a packed {@code (leaf, dictId)} whose entries compare as unsigned bytes. */
  private static final byte KEY_STRING_BYTES = 1;

  /** Sort-key kind: as {@link #KEY_STRING_BYTES}, but a supplementary character forces decoding. */
  private static final byte KEY_STRING_COLLATED = 2;

  /** Sort-key kind: a resource-wide dictionary id resolved through a revision-bound read view. */
  private static final byte KEY_STRING_GLOBAL = 3;

  private static final GlobalValueDictionary.ReadView[] NO_GLOBAL_SORT_VIEWS = {};

  private static final int MASK_WORDS = (ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6;

  private static final class Scratch {
    private ColumnSlice[] leafPredicateSlices;
    private ColumnSlice[] leafSortSlices;

    /** Scratch for the per-leaf predicate slices (one per predicate), reused across leaves. */
    ColumnSlice[] leafPredicateSlices(final int predicates) {
      ColumnSlice[] a = leafPredicateSlices;
      if (a == null || a.length < predicates) {
        a = new ColumnSlice[Math.max(predicates, 4)];
        leafPredicateSlices = a;
      }
      return a;
    }

    /** Scratch for the per-leaf sort-key slices (one per key), reused across leaves. */
    ColumnSlice[] leafSortSlices(final int keys) {
      ColumnSlice[] a = leafSortSlices;
      if (a == null || a.length < keys) {
        a = new ColumnSlice[Math.max(keys, 4)];
        leafSortSlices = a;
      }
      return a;
    }
    final long[] mask = new long[MASK_WORDS];
  }

  private static final ThreadLocal<Scratch> SCRATCH = ThreadLocal.withInitial(Scratch::new);

  private ProjectionColumnScan() {}

  /**
   * Conjunctive count over the store's slices. Predicates must be column-sliceable (numeric/boolean)
   * — callers gate before dispatching.
   */
  public static long conjunctiveCount(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final ColumnSegmentFetcher fetcher) {
    return conjunctiveCount(store, predicates, 0, store.rowGroupCount(), fetcher);
  }

  /**
   * Resolve the predicate columns ONCE for a chunked parallel dispatch: the keep-mask (fingerprint +
   * descriptor-zone pruning) is computed a single time, the surviving leaves are fetched a single
   * time, and every range then shares the immutable slice arrays. Without this, each range
   * re-resolved — and the executor's cache-warming prefill filled the FULL column besides, fetching
   * the whole 70 MB dictionary chain the prune had just proven irrelevant.
   */
  public static ColumnSlice[][] resolvePredicateColumnsShared(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    return resolvePredicateColumns(store, predicates, fetcher);
  }

  /**
   * The predicates' zone-map / fingerprint keep mask ({@code null} = nothing pruned), for callers that
   * feed the kernels windowed slices instead of the shared resident fill.
   */
  public static long @Nullable [] predicateKeepMask(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    return computeKeepMask(store, predicates, fetcher);
  }

  /** Ranged variant over PRE-RESOLVED columns ({@link #resolvePredicateColumnsShared}). */
  public static long conjunctiveCount(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int fromRowGroup, final int toRowGroup, final ColumnSlice[][] cols) {
    final Scratch s = SCRATCH.get();
    long total = 0;
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
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
   * Ranged conjunctive count over a per-leaf access — the windowed twin of
   * {@link #conjunctiveCount(ProjectionColumnStore, ColumnPredicate[], int, int, ColumnSlice[][])} for the
   * caller whose predicate column the fill budget refused to retain: {@code [fromRowGroup, toRowGroup)}
   * is decoded one window at a time through {@code access}, touching the predicate columns and nothing
   * else, where the whole-leaf byte route it replaces hydrates every column of every row group. The
   * access is single-threaded by contract — one per parallel worker; a keep-masked access hands the
   * kernel the pruned sentinel for a leaf the zone/fingerprint evidence dropped, so that leaf's window
   * is never fetched.
   */
  public static long conjunctiveCount(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int fromRowGroup, final int toRowGroup, final ProjectionColumnStore.LeafColumnAccess access) {
    checkPredicates(store, predicates);
    final Scratch s = SCRATCH.get();
    final ColumnSlice[] leafSlices = s.leafPredicateSlices(predicates.length);
    long total = 0;
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
      final int rowCount = evaluateMask(predicates, access, leaf, store.rowCount(leaf), s.mask, leafSlices);
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

  /** Ranged variant for the executor's chunked parallel dispatch — scratch is thread-local. */
  public static long conjunctiveCount(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int fromRowGroup, final int toRowGroup, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates, fetcher);
    final Scratch s = SCRATCH.get();
    long total = 0;
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
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
   * Conjunctive numeric-long aggregate — {@code acc = [count, sum, min, max]}, initialised by the
   * caller to {@code {0, 0, Long.MAX_VALUE, Long.MIN_VALUE}}. The aggregate column's presence is
   * ANDed before folding, exactly like the byte kernel.
   *
   * <p>
   * Exact sum or DECLINE, parity with the byte kernel: {@code xs:integer} is arbitrary precision, so
   * a total that wraps a {@code long} is a wrong answer rather than a fast one. This is a scalar
   * walk, so the check is a per-value {@link Math#addExact} — exact, one never-taken branch — rather
   * than the conservative pre-flight zone-map bound {@code ProjectionColumnSegmentFoldScan} needs for
   * its lanewise fold.
   *
   * @throws ArithmeticException on overflow — callers treat it as a DECLINE
   */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int numericColumn, final long[] acc, final ColumnSegmentFetcher fetcher) {
    conjunctiveAggregateNumeric(store, predicates, numericColumn, acc, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int numericColumn, final long[] acc, final int fromRowGroup, final int toRowGroup,
      final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    // The temporal kinds are admitted for their ORDER (the min/max lanes), which is exactly what an
    // epoch answers; the sum lane still folds and simply is not read for such a column — the caller
    // that requests a sum over a temporal column is the one that must decline, and an overflow here
    // raises ArithmeticException, which callers already treat as a decline.
    if (!ProjectionIndexRowGroupPage.isOrderedLongKind(store.columnKind(numericColumn))) {
      throw new IllegalStateException(
          "aggregate column " + numericColumn + " is not NUMERIC_LONG or a temporal kind");
    }
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates, fetcher);
    final ColumnSlice[] aggCol = store.column(numericColumn, fetcher);
    final Scratch s = SCRATCH.get();
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
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
          sum = Math.addExact(sum, v);
          if (v < min)
            min = v;
          if (v > max)
            max = v;
        }
      }
      acc[0] = count;
      acc[1] = sum;
      acc[2] = min;
      acc[3] = max;
    }
  }

  /**
   * {@link #conjunctiveAggregateNumeric} with a 128-BIT SUM — the overflow fallback's sliced form
   * (64-bit id columns; the interpreter promotes to big-integer arithmetic). {@code acc} layout:
   * {@code [count, sumHi, sumLo(unsigned), min, max]}; carry-exact, associative merge.
   */
  public static void conjunctiveAggregateNumeric128(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final long[] acc, final int fromRowGroup,
      final int toRowGroup, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    // The temporal kinds are admitted for their ORDER (the min/max lanes), which is exactly what an
    // epoch answers; the sum lane still folds and simply is not read for such a column — the caller
    // that requests a sum over a temporal column is the one that must decline, and an overflow here
    // raises ArithmeticException, which callers already treat as a decline.
    if (!ProjectionIndexRowGroupPage.isOrderedLongKind(store.columnKind(numericColumn))) {
      throw new IllegalStateException(
          "aggregate column " + numericColumn + " is not NUMERIC_LONG or a temporal kind");
    }
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates, fetcher);
    final ColumnSlice[] aggCol = store.column(numericColumn, fetcher);
    final Scratch s = SCRATCH.get();
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
      final int rowCount = evaluateMask(predicates, cols, leaf, store.rowCount(leaf), s.mask);
      if (rowCount <= 0) {
        continue;
      }
      final ColumnSlice agg = aggCol[leaf];
      final long[] values = agg.numericValues();
      final long[] presence = agg.presenceWords();
      final int stride = (rowCount + 63) >>> 6;
      long count = acc[0];
      long sumHi = acc[1];
      long sumLo = acc[2];
      long min = acc[3];
      long max = acc[4];
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
          final long lo = sumLo + v;
          sumHi += (v >> 63) + (((sumLo & v) | ((sumLo | v) & ~lo)) >>> 63);
          sumLo = lo;
          if (v < min) {
            min = v;
          }
          if (v > max) {
            max = v;
          }
        }
      }
      acc[0] = count;
      acc[1] = sumHi;
      acc[2] = sumLo;
      acc[3] = min;
      acc[4] = max;
    }
  }

  /**
   * Conjunctive numeric-double aggregate — {@code acc = [count, sum, min, max]} as doubles,
   * initialised to {@code {0, 0, +Inf, -Inf}}. Min/max use {@code Double.compare} total order (parity
   * with the interpreter's comparator, {@code -0.0 < 0.0}); the kernel sum is diagnostic only —
   * served sums fold seed-first through {@link MatchingDoubleCursor}.
   */
  public static void conjunctiveAggregateNumericDouble(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final double[] acc,
      final ColumnSegmentFetcher fetcher) {
    conjunctiveAggregateNumericDouble(store, predicates, numericColumn, acc, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch (count/min/max are merge-order-insensitive). */
  public static void conjunctiveAggregateNumericDouble(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final double[] acc, final int fromRowGroup,
      final int toRowGroup, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    if (store.columnKind(numericColumn) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE) {
      throw new IllegalStateException("aggregate column " + numericColumn + " is not NUMERIC_DOUBLE");
    }
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates, fetcher);
    final ColumnSlice[] aggCol = store.column(numericColumn, fetcher);
    final Scratch s = SCRATCH.get();
    for (int leaf = fromRowGroup; leaf < toRowGroup; leaf++) {
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
          if (Double.compare(v, min) < 0)
            min = v;
          if (Double.compare(v, max) > 0)
            max = v;
        }
      }
      acc[0] = count;
      acc[1] = sum;
      acc[2] = min;
      acc[3] = max;
    }
  }

  /**
   * Pull-cursor over the predicate-matched, presence-filtered cells of one NUMERIC_DOUBLE column in
   * document order — the column-sliced twin of {@link ProjectionIndexByteScan.MatchingDoubleCursor},
   * feeding the executor's seed-first served-sum fold. Single-threaded use (borrows the thread's
   * scratch).
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

    public MatchingDoubleCursor(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
        final int numericColumn, final ColumnSegmentFetcher fetcher) {
      checkPredicates(store, predicates);
      if (store.columnKind(numericColumn) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE) {
        throw new IllegalStateException("cursor column " + numericColumn + " is not NUMERIC_DOUBLE");
      }
      this.store = store;
      this.predicates = predicates;
      this.predicateCols = resolvePredicateColumns(store, predicates, fetcher);
      this.aggCol = store.column(numericColumn, fetcher);
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
        if (leaf >= store.rowGroupCount()) {
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

  // ==================== sorted collection ====================

  /**
   * Column-sliced twin of {@code ProjectionIndexByteScan.collectMatchingSortTuples}: matching rows'
   * order-key tuples (row-major, stride {@code sortColumns.length}) and record keys, in document
   * order, from the predicate/order columns' slices and the KEYS chain — never a whole leaf. A
   * matching row MISSING any order key goes to {@code missingKeysOut}; the caller declines on any
   * such row (the interpreter owns empty-order-key placement).
   */
  public static void collectMatchingSortTuples(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int[] sortColumns, final LongArrayList valuesOut, final LongArrayList keysOut,
      final LongArrayList missingKeysOut, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    if (!sortColumnsOrderable(store, sortColumns)) {
      throw new IllegalStateException("a sort column stores dictionary ids, which are not an order");
    }
    final ColumnSlice[][] sortCols = resolveSortColumns(store, sortColumns, fetcher);
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates, fetcher);
    final long[][] keySlices = store.recordKeys(fetcher);
    final Scratch s = SCRATCH.get();
    final int keyCount = sortColumns.length;
    // Long tuples only — this collector reads numericValues() unconditionally, so the caller
    // gates string order keys onto the bounded heap instead.
    for (int leaf = 0; leaf < store.rowGroupCount(); leaf++) {
      final int rowCount = evaluateMask(predicates, cols, leaf, store.rowCount(leaf), s.mask);
      if (rowCount <= 0) {
        continue;
      }
      final long[] keys = keySlices[leaf];
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = s.mask[w];
        if (word == 0L) {
          continue;
        }
        // AND every order key's presence once per 64-row word; the row loop tests bits only.
        long presAll = -1L;
        for (int k = 0; k < keyCount; k++) {
          presAll &= sortCols[k][leaf].presenceWords()[w];
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          if ((presAll & (1L << bit)) == 0L) {
            missingKeysOut.add(keys[rowIdx]);
            continue;
          }
          for (int k = 0; k < keyCount; k++) {
            valuesOut.add(sortCols[k][leaf].numericValues()[rowIdx]);
          }
          keysOut.add(keys[rowIdx]);
        }
      }
    }
  }

  /**
   * Reject sort columns whose long lane is not an ORDER.
   *
   * <p>
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL} cells are dictionary ids minted in
   * first-seen order, so sorting on them sorts by when a value was first interned. The kernels here
   * decide "numeric key" by asking whether a column is NOT a per-leaf dictionary, which was
   * exhaustive until a second string kind existed and would now call an id a number. Ordering is the
   * one thing a global column cannot do without its dictionary, so it declines outright.
   *
   * @return {@code true} when every sort column can be ordered by its stored lane
   */
  private static boolean sortColumnsOrderable(final ProjectionColumnStore store, final int[] sortColumns) {
    for (final int col : sortColumns) {
      if (store.columnKind(col) == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
        return false;
      }
    }
    return true;
  }

  private static boolean topKSortColumnsOrderable(final ProjectionColumnStore store, final int[] sortColumns,
      final GlobalValueDictionary.ReadView[] globalSortViews) {
    if (globalSortViews.length != sortColumns.length && globalSortViews.length != 0) {
      return false;
    }
    for (int key = 0; key < sortColumns.length; key++) {
      if (store.columnKind(sortColumns[key]) == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
          && (globalSortViews.length == 0 || globalSortViews[key] == null)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Bounded top-{@code k} sorted scan, fused with collection — the R2 "heap over zone-map-pruned
   * leaves" shape: once the heap is full, a leaf whose FIRST order key's zone bounds cannot beat the
   * current worst kept row is skipped without evaluating its mask at all. Sound because entering the
   * heap requires beating the worst row, the first key is compared first, and the prune only fires
   * when the leaf's every row is STRICTLY worse on it — ties fall through to full evaluation. The
   * prune additionally requires the leaf's order columns to be all-present: a skipped leaf must not
   * be able to hide a matching row with a missing order key, which obliges the caller to decline
   * outright.
   *
   * <p>
   * <b>Best-first visitation.</b> Document order makes that prune a late bloomer: it can only fire
   * after enough leaves have filled the heap with good rows, so the scan pays full mask evaluation
   * over the whole prefix. When every leaf's order columns are all-present — the same precondition
   * the prune already needs — the leaves are instead visited in order of their BEST POSSIBLE first
   * key, and the walk STOPS at the first leaf that cannot beat the worst kept row: all remaining
   * leaves are ordered no better, so none can contribute. A `k` of 10 over a thousand leaves then
   * touches tens of leaves rather than all of them. Emission is unaffected — ranks are the leaf's
   * DOCUMENT-order base plus the row index, so the heap's total order (and its stable-sort tiebreak)
   * does not depend on visitation order.
   *
   * <p>
   * The plan needs a per-leaf VALUE extremum of the first key. Numeric slices carry one already
   * ({@code min()}/{@code max()}); STRING_DICT slices do NOT — theirs are dict ids — so the string
   * arm reads {@link ProjectionColumnStore#stringValueExtrema}, which is data-derived and memoized
   * per column. Any leaf without one (no dict lanes, no present value) makes the plan inadmissible
   * and the scan falls back to the document-order walk unchanged.
   *
   * @return record keys of the first {@code k} rows of the full stable sort, in emission order — or
   *         {@code null} when a matching row misses an order key, which only the generic pipeline can
   *         place correctly
   */
  public static long @Nullable [] topKRecordKeys(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int[] sortColumns, final boolean[] descending, final int k, final ColumnSegmentFetcher fetcher) {
    return topKRecordKeys(store, predicates, sortColumns, descending, k, fetcher, NO_GLOBAL_SORT_VIEWS);
  }

  /**
   * Global-string-capable twin of {@link #topKRecordKeys(ProjectionColumnStore, ColumnPredicate[], int[], boolean[],
   * int, ColumnSegmentFetcher)}. {@code globalSortViews} is aligned to {@code sortColumns}; only
   * {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL} positions require a non-null view.
   */
  public static long @Nullable [] topKRecordKeys(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int[] sortColumns, final boolean[] descending, final int k, final ColumnSegmentFetcher fetcher,
      final GlobalValueDictionary.ReadView[] globalSortViews) {
    checkPredicates(store, predicates);
    if (!topKSortColumnsOrderable(store, sortColumns, globalSortViews)) {
      return null; // the generic pipeline knows the values; this heap would only know their ids
    }
    final long tPhase0 = DIAG
        ? System.nanoTime()
        : 0L;
    validateSortColumns(store, sortColumns);
    if (k <= 0) {
      return new long[0];
    }
    final long tPhase1 = DIAG
        ? System.nanoTime()
        : 0L;
    // Zone-map pruning from the descriptors alone; then ONE leaf at a time through the access —
    // resident slices when every column fits the fill budget, decoded per window otherwise, so a
    // fat string column at 100M rows serves this exact kernel without a whole-column residency.
    final long[] keep = computeKeepMask(store, predicates, fetcher);
    final int[] needed = new int[sortColumns.length + predicates.length];
    System.arraycopy(sortColumns, 0, needed, 0, sortColumns.length);
    for (int i = 0; i < predicates.length; i++) {
      needed[sortColumns.length + i] = predicates[i].column;
    }
    final ProjectionColumnStore.LeafColumnAccess access = store.leafAccess(fetcher, keep, needed, true);
    final long tPhase2 = DIAG
        ? System.nanoTime()
        : 0L;
    final long tPhase3 = DIAG
        ? System.nanoTime()
        : 0L;
    long tMask = 0L;
    long tHeap = 0L;
    long nCand = 0L;
    long nLeavesPruned = 0L;
    final Scratch s = SCRATCH.get();
    final int keyCount = sortColumns.length;
    final long tPlan0 = DIAG
        ? System.nanoTime()
        : 0L;
    // Heap of the K best rows: tuples row-major beside keys and document-order ranks, root =
    // worst kept. The rank is the stable-sort tiebreak, so bounded selection and the full
    // stable sort agree exactly.
    final long[] heapTuple = new long[k * keyCount];
    final long[] heapKey = new long[k];
    final long[] heapRank = new long[k];
    int size = 0;
    final long[] candidate = new long[keyCount];
    final int leafCount = store.rowGroupCount();
    final int[] leafRows = new int[leafCount];
    // Document-order rank base per leaf: the tiebreak must stay document order no matter which
    // order the leaves are VISITED in, so it can never be a running counter here.
    final long[] leafRankBase = new long[leafCount];
    long rankAcc = 0;
    for (int leaf = 0; leaf < leafCount; leaf++) {
      leafRows[leaf] = store.rowCount(leaf);
      leafRankBase[leaf] = rankAcc;
      rankAcc += leafRows[leaf];
    }
    // Best-possible first key per leaf, or null when no admissible best-first plan exists. Built
    // BEFORE keyKind: its walk over the first key's dictionaries is what establishes that column's
    // collation verdict, and paying a separate sweep for it costs more than the verdict saves.
    // A windowed access visits leaves in DOCUMENT order: the best-first plan would hop across the
    // whole store and re-decode a window per hop, while the sequential walk decodes each window once
    // and the per-leaf zone prune still skips what the heap has already beaten.
    long[] leafBestOrNull = TOPK_DOC_ORDER || access.windowed()
        || store.columnKind(sortColumns[0]) == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
        ? null
        : leafBestFirstKeys(store, access, sortColumns, leafRows, descending[0], fetcher);
    // A string key's heap value is the PACKED (leaf << 32) | dictId — never comparable as a
    // long (the same value in two leaves packs differently), so every comparison resolves the
    // entry bytes through sortCols. Numeric keys stay raw longs. The string kinds split on
    // COLLATION so the comparison never has to re-derive it; an unestablished verdict takes the
    // exact per-pair path, which is what the scan did before the split existed.
    final byte[] keyKind = new byte[keyCount];
    for (int kk = 0; kk < keyCount; kk++) {
      final byte columnKind = store.columnKind(sortColumns[kk]);
      keyKind[kk] = switch (columnKind) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT ->
          store.stringDictSupplementaryMemo(sortColumns[kk]) == ProjectionColumnStore.SUPPLEMENTARY_NONE
              ? KEY_STRING_BYTES
              : KEY_STRING_COLLATED;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL -> KEY_STRING_GLOBAL;
        default -> KEY_NUMERIC;
      };
    }
    // Rowless leaves are dropped from the plan rather than ordered: they contribute nothing, and a
    // sort would have to resolve an extremum they do not have (their slice carries no dictionary).
    final int[] visit = new int[leafCount];
    int visitCount = 0;
    for (int leaf = 0; leaf < leafCount; leaf++) {
      if (leafRows[leaf] > 0) {
        visit[visitCount++] = leaf;
      }
    }
    if (leafBestOrNull != null
        && allLeafExtremaTie(leafBestOrNull, visit, visitCount, keyKind, access, sortColumns, globalSortViews)) {
      // Every leaf offers the same best key, so the worst kept row can never be beaten by it and the
      // stop can never fire — the reordering would buy nothing and cost the sequential slice access
      // the document-order walk enjoys. (Q25's shape: `WHERE SearchPhrase <> ''` excludes the empty
      // string that is every leaf's minimum, so no leaf is ever prunable. Measured 3.6 ms of masking
      // turning into 12.9 ms purely on lost locality.)
      leafBestOrNull = null;
      TOPK_PLAN_TIED.increment();
    }
    final long[] leafBest = leafBestOrNull;
    if (leafBest != null) {
      IntArrays.mergeSort(visit, 0, visitCount, (a, b) -> {
        final int cmp = compareKeyAt(leafBest[a], leafBest[b], 0, keyKind, access, sortColumns, globalSortViews);
        if (cmp != 0) {
          return descending[0]
              ? -cmp
              : cmp;
        }
        return Integer.compare(a, b);
      });
    }
    final long tPlan1 = DIAG
        ? System.nanoTime()
        : 0L;
    for (int v = 0; v < visitCount; v++) {
      final int leaf = visit[v];
      final int rows = leafRows[leaf];
      if (size == k) {
        if (leafBest != null) {
          // Leaves are ordered best-first on key 0, so the first one whose BEST possible key is
          // strictly worse than the worst kept row proves the same of every leaf after it.
          final int cmp = compareKeyAt(leafBest[leaf], heapTuple[0], 0, keyKind, access, sortColumns, globalSortViews);
          if ((descending[0]
              ? -cmp
              : cmp) > 0) {
            nLeavesPruned += visitCount - v;
            TOPK_LEAVES_SKIPPED.add(visitCount - v);
            break;
          }
        } else if (keyKind[0] == KEY_NUMERIC && leafZonePrunable(access, sortColumns, leaf, rows, descending[0], heapTuple)) {
          // NEVER zone-prune on a string first key without the value extrema above:
          // ColumnSlice.min()/max() of a STRING_DICT column are dict IDS, meaningless for value
          // order — pruning on them drops matching leaves silently (the exact hazard the STR_*
          // predicate ops were split off to avoid).
          nLeavesPruned++;
          TOPK_LEAVES_SKIPPED.increment();
          continue;
        }
      }
      final long rank = leafRankBase[leaf];
      final long tm0 = DIAG
          ? System.nanoTime()
          : 0L;
      final int rowCount = evaluateMask(predicates, access, leaf, rows, s.mask, s.leafPredicateSlices(predicates.length));
      if (DIAG) {
        tMask += System.nanoTime() - tm0;
      }
      if (rowCount <= 0) {
        continue;
      }
      final long th0 = DIAG
          ? System.nanoTime()
          : 0L;
      final long[] keys = access.recordKeys(leaf);
      final ColumnSlice[] leafSort = s.leafSortSlices(keyCount);
      for (int kk = 0; kk < keyCount; kk++) {
        leafSort[kk] = access.slice(sortColumns[kk], leaf);
      }
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = s.mask[w];
        if (word == 0L) {
          continue;
        }
        long presAll = -1L;
        for (int kk = 0; kk < keyCount; kk++) {
          presAll &= leafSort[kk].presenceWords()[w];
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          if ((presAll & (1L << bit)) == 0L) {
            return null; // a matching row without an order key — the interpreter places it
          }
          for (int kk = 0; kk < keyCount; kk++) {
            candidate[kk] = switch (keyKind[kk]) {
              case KEY_STRING_BYTES, KEY_STRING_COLLATED ->
                (long) leaf << 32 | leafSort[kk].stringDictIds()[rowIdx];
              default -> leafSort[kk].numericValues()[rowIdx];
            };
          }
          final long rowRank = rank + rowIdx;
          if (size < k) {
            final int slot = size++;
            System.arraycopy(candidate, 0, heapTuple, slot * keyCount, keyCount);
            heapKey[slot] = keys[rowIdx];
            heapRank[slot] = rowRank;
            if (size == k) {
              for (int i = (k >>> 1) - 1; i >= 0; i--) {
                siftDownWorst(heapTuple, heapKey, heapRank, i, k, keyCount, descending, access, sortColumns, keyKind,
                    globalSortViews);
              }
            }
          } else if (compareCandidate(candidate, rowRank, heapTuple, heapRank, 0, keyCount, descending, access,
              sortColumns, keyKind, globalSortViews) < 0) {
            System.arraycopy(candidate, 0, heapTuple, 0, keyCount);
            heapKey[0] = keys[rowIdx];
            heapRank[0] = rowRank;
            siftDownWorst(heapTuple, heapKey, heapRank, 0, k, keyCount, descending, access, sortColumns, keyKind,
                globalSortViews);
          }
          if (DIAG) {
            nCand++;
          }
        }
      }
      if (DIAG) {
        tHeap += System.nanoTime() - th0;
      }
    }
    if (DIAG) {
      System.err.printf(
          "[topk] sortCols=%.2fms preds=%.2fms keys=%.2fms plan=%.2fms mask=%.2fms heap=%.2fms cand=%d pruned=%d/%d%n",
          (tPhase1 - tPhase0) / 1e6, (tPhase2 - tPhase1) / 1e6, (tPhase3 - tPhase2) / 1e6, (tPlan1 - tPlan0) / 1e6,
          tMask / 1e6, tHeap / 1e6, nCand, nLeavesPruned, store.rowGroupCount());
    }
    // Emit in sort order: primitive index sort over the kept rows under the same total order.
    final int kept = size;
    final int[] order = new int[kept];
    for (int i = 0; i < kept; i++) {
      order[i] = i;
    }
    IntArrays.mergeSort(order, (a, b) ->
        compareHeapRows(heapTuple, heapRank, a, b, keyCount, descending, access, sortColumns, keyKind, globalSortViews));
    final long[] out = new long[kept];
    for (int i = 0; i < kept; i++) {
      out[i] = heapKey[order[i]];
    }
    return out;
  }

  /**
   * PREDICATE SCAN (no ordering): record keys of every matching row, in DOCUMENT order — leaf order,
   * then row order, exactly the interpreter's emission for {@code for $r in P where p
   * return $r}. Pruned/zone-skipped leaves cost nothing ({@link #evaluateMask}'s pre-checks).
   * {@code null} when more than {@code maxMatches} rows match: materializing an unbounded result
   * through per-record navigation would be slower than the row path it replaces, so the caller
   * declines instead. {@code limit >= 0} truncates to the first {@code limit} matches (a
   * sole-consumer {@code fn:subsequence} cap — document order makes the prefix exact).
   */
  public static long @Nullable [] matchingRecordKeys(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int maxMatches, final long limit, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    if (predicates.length == 0) {
      return null; // an unfiltered full-table materialization is the row path's case
    }
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates, fetcher);
    final long[][] keySlices = store.recordKeys(fetcher);
    final Scratch s = SCRATCH.get();
    final LongArrayList out = new LongArrayList(64);
    final long cap = limit >= 0
        ? Math.min(limit, maxMatches)
        : maxMatches;
    for (int leaf = 0; leaf < store.rowGroupCount(); leaf++) {
      final int leafRows = store.rowCount(leaf);
      if (leafRows <= 0) {
        continue;
      }
      final int rowCount = evaluateMask(predicates, cols, leaf, leafRows, s.mask);
      if (rowCount <= 0) {
        continue;
      }
      final long[] keys = keySlices[leaf];
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = s.mask[w];
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          if (out.size() >= cap) {
            if (limit >= 0) {
              return out.toLongArray(); // the cap IS the answer prefix under a limit
            }
            return null; // unbounded and over budget — the row path serves this better
          }
          out.add(keys[rowIdx]);
        }
      }
    }
    return out.toLongArray();
  }

  /**
   * Receives one projected column's cell for every mask-surviving row of
   * {@link #matchingFieldValues}, in document order. Exactly one accept call per emitted row; a row
   * whose cell is ABSENT is skipped without any call (the interpreter's missing deref contributes
   * nothing), unless the caller asked to decline on holes.
   */
  public interface FieldValueSink {

    /**
     * A NUMERIC_LONG value, a NUMERIC_DOUBLE value in {@link ProjectionDoubleEncoding}'s transform
     * domain, or a BOOLEAN cell as {@code 0}/{@code 1} — which one is fixed by the column's kind, known
     * to the caller before the scan starts.
     */
    void acceptLong(long raw);

    /**
     * A STRING_DICT cell as the dictionary's plain UTF-8 entry bytes (FSST lives in the persisted form
     * only), given as a RANGE because the slice holds its dictionary as one flat run. BORROWED: the
     * array belongs to the decoded slice and must not be retained or mutated.
     */
    void acceptString(byte[] utf8, int off, int len);
  }

  /** {@link #matchingFieldValues} verdict: no answer may be served from what the sink received. */
  public static final long FIELD_VALUES_DECLINED = -1L;

  /**
   * PREDICATE VALUE EMISSION: one projected column's VALUES at every row surviving the same
   * conjunctive mask {@link #matchingRecordKeys} walks, in the same DOCUMENT order (leaf order, then
   * row order) — the record materialization removed. Serves
   * {@code for $r in P where p return $r.field} without touching a single record page; the KEYS chain
   * is never fetched either, since no key leaves this kernel.
   *
   * <p>
   * <b>Absent cells.</b> A surviving row whose cell has no presence bit is SKIPPED — the interpreter
   * emits nothing for a missing deref, so the emitted sequence is exact. Under a
   * {@code fn:subsequence} cap that is not enough (the window counts items, and a skipped row shifts
   * it), so {@code missingDeclines} makes the first hole abandon the scan.
   *
   * @param maxMatches eager-buffer budget: more surviving rows than this declines, since the caller
   *        holds every emitted item in memory before its sequence returns
   * @param limit sole-consumer {@code fn:subsequence} cap, or {@code -1} for unbounded; document
   *        order makes the first {@code limit} rows the exact prefix
   * @param missingDeclines abandon the scan on the first surviving row whose cell is absent
   * @return the number of values pushed to {@code sink}, or {@link #FIELD_VALUES_DECLINED} — after
   *         which the caller must DISCARD everything the sink received
   */
  public static long matchingFieldValues(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int valueColumn, final long maxMatches, final long limit, final boolean missingDeclines,
      final ColumnSegmentFetcher fetcher, final FieldValueSink sink) {
    checkPredicates(store, predicates);
    if (predicates.length == 0) {
      return FIELD_VALUES_DECLINED; // an unfiltered full-table emission is the row path's case
    }
    if (!store.columnSliceable(valueColumn)) {
      throw new IllegalStateException("value column " + valueColumn + " is not sliceable");
    }
    final byte kind = store.columnKind(valueColumn);
    if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
      // A SET cell is a SEQUENCE of values; one accept call cannot represent it.
      throw new IllegalStateException("value column " + valueColumn + " is not emittable (kind=" + kind + ")");
    }
    // ONE keep mask for predicate and value columns alike: a leaf the fingerprint/zone evidence
    // already proved empty must not have its VALUE segments fetched either.
    final long[] keep = computeKeepMask(store, predicates, fetcher);
    // Predicate and value columns through ONE per-leaf access: resident slices when they fit the
    // fill budget together, windowed otherwise — a fat value or predicate column at 100M rows no
    // longer declines this route to the record path. The point-lookup shape that filters and emits
    // the SAME column (Q20) reads one decoded slice per leaf either way; the keep mask applies to
    // the value column too, so a leaf the evidence proved empty never has its value segments fetched.
    final int[] needed = new int[predicates.length + 1];
    for (int i = 0; i < predicates.length; i++) {
      needed[i] = predicates[i].column;
    }
    needed[predicates.length] = valueColumn;
    final ProjectionColumnStore.LeafColumnAccess access = store.leafAccess(fetcher, keep, needed, false);
    final Scratch s = SCRATCH.get();
    final ColumnSlice[] leafPredicateSlices = s.leafPredicateSlices(predicates.length);
    // Row budget vs item budget: the cap prefix is exact only because document order makes the
    // first `limit` SURVIVING ROWS the window, and a hole inside it already declined.
    final long rowCap = limit >= 0
        ? limit
        : Long.MAX_VALUE;
    long matched = 0;
    long emitted = 0;
    for (int leaf = 0; leaf < store.rowGroupCount(); leaf++) {
      final int leafRows = store.rowCount(leaf);
      if (leafRows <= 0) {
        continue;
      }
      final int rowCount = evaluateMask(predicates, access, leaf, leafRows, s.mask, leafPredicateSlices);
      if (rowCount <= 0) {
        continue;
      }
      final ColumnSlice value = access.predicateSlice(valueColumn, leaf);
      if (value.rowCount() < rowCount) {
        // The predicate columns say this leaf has rows the value column does not — mismatched
        // segment truth, never a benign skip. Fail loud into the caller's fail-soft catch.
        throw new IllegalStateException("value column " + valueColumn + " has " + value.rowCount() + " rows on leaf "
            + leaf + ", predicate columns have " + rowCount);
      }
      final long[] presence = value.presenceWords();
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = s.mask[w];
        if (word == 0L) {
          continue;
        }
        final long present = presence[w];
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          if (matched >= rowCap) {
            return emitted; // the cap IS the answer prefix under a limit
          }
          if (matched >= maxMatches) {
            return FIELD_VALUES_DECLINED; // over the eager-buffer budget
          }
          matched++;
          if ((present & 1L << bit) == 0L) {
            if (missingDeclines) {
              return FIELD_VALUES_DECLINED;
            }
            continue; // a missing deref contributes nothing — exact
          }
          emitOne(sink, kind, value, rowIdx);
          emitted++;
        }
      }
    }
    return emitted;
  }

  /** One present cell of {@code slice} to {@code sink}; the kind is loop-invariant at every call. */
  private static void emitOne(final FieldValueSink sink, final byte kind, final ColumnSlice slice, final int rowIdx) {
    switch (kind) {
      // A temporal cell is handed out as its stored EPOCH, exactly as a global id is: this kernel has
      // no business rendering text, and the caller that asked for a temporal column formats the run
      // through ProjectionTemporalCodec. The executor gates the kind before it gets here.
      case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
          ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
          ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
        sink.acceptLong(slice.numericValues()[rowIdx]);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
        sink.acceptLong((slice.boolWords()[rowIdx >>> 6] >>> (rowIdx & 63) & 1L));
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> {
        final int dictId = slice.stringDictIds()[rowIdx];
        sink.acceptString(slice.dictBytes(), slice.dictOffset(dictId), slice.dictLength(dictId));
      }
      // The cell is a resource-wide dictionary ID, and this kernel has no dictionary to resolve it
      // against — that lives in the NamePage sub-trie, one layer up. So the id is handed out as a
      // long and the caller reverse-maps the whole run in ONE batch, which is the only shape in
      // which the reverse direction is affordable. A sink that does not expect that must not have
      // been given a global column: the executor gates the kind before it gets here.
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL -> sink.acceptLong(slice.numericValues()[rowIdx]);
      default -> throw new IllegalStateException("value column kind " + kind + " is not emittable");
    }
  }

  /**
   * The BEST POSSIBLE first order key of each leaf — its minimum for an ascending scan, its maximum
   * for a descending one — in the same encoding the heap tuples use (raw long for a numeric key,
   * packed {@code (leaf << 32) | dictId} for a string one), or {@code null} when no leaf ordering may
   * be built at all.
   *
   * <p>
   * Inadmissible, and why each case must be: a leaf whose order columns are NOT all-present could
   * hide a matching row with an empty key, which the interpreter places by the empty-least/greatest
   * mode and the caller can only answer by declining — so it must never be skipped, and the
   * document-order walk (which reaches every leaf) has to run. A string leaf without a value extremum
   * has nothing sound to order or stop on.
   */
  private static long @Nullable [] leafBestFirstKeys(final ProjectionColumnStore store,
      final ProjectionColumnStore.LeafColumnAccess access,
      final int[] sortColumns, final int[] leafRows, final boolean descendingFirst,
      final ColumnSegmentFetcher fetcher) {
    final int leafCount = leafRows.length;
    final int[] extrema = store.columnKind(sortColumns[0]) == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
        ? store.stringValueExtrema(sortColumns[0], access)
        : null;
    final long[] best = new long[leafCount];
    for (int leaf = 0; leaf < leafCount; leaf++) {
      final int rows = leafRows[leaf];
      if (rows <= 0) {
        continue; // rowless leaves are skipped by the walk itself; any order key sorts them first
      }
      if (!orderColumnsAllPresent(access, sortColumns, leaf, rows)) {
        return null;
      }
      if (extrema != null) {
        final int id = extrema[2 * leaf + (descendingFirst
            ? 1
            : 0)];
        if (id < 0) {
          return null;
        }
        best[leaf] = (long) leaf << 32 | id;
      } else {
        final ColumnSlice slice = access.slice(sortColumns[0], leaf);
        best[leaf] = descendingFirst
            ? slice.max()
            : slice.min();
      }
    }
    return best;
  }

  /**
   * Whether every leaf offers the SAME best first key — the shape in which best-first visitation is
   * provably useless. Every kept row then comes from a leaf whose best key is that shared value, so
   * the worst kept row is never better than it, so the stop condition (STRICTLY worse) is never met.
   * Direction-independent: equality does not depend on which end of the range is "best".
   */
  private static boolean allLeafExtremaTie(final long[] leafBest, final int[] visit, final int visitCount,
      final byte[] keyKind, final ProjectionColumnStore.LeafColumnAccess access, final int[] sortColumns,
      final GlobalValueDictionary.ReadView[] globalSortViews) {
    if (visitCount < 2) {
      return true; // one leaf orders itself; skip the sort entirely
    }
    final long first = leafBest[visit[0]];
    for (int v = 1; v < visitCount; v++) {
      if (compareKeyAt(first, leafBest[visit[v]], 0, keyKind, access, sortColumns, globalSortViews) != 0) {
        return false;
      }
    }
    return true;
  }

  /** Every order column of {@code leaf} carries a value in every one of its {@code leafRows} rows. */
  private static boolean orderColumnsAllPresent(final ProjectionColumnStore.LeafColumnAccess access,
      final int[] sortColumns, final int leaf, final int leafRows) {
    final int presWords = (leafRows + 63) >>> 6;
    for (final int sortCol : sortColumns) {
      final long[] presence = access.slice(sortCol, leaf).presenceWords();
      if (presence == null || presence.length < presWords) {
        return false;
      }
      for (int w = 0; w < presWords; w++) {
        final int width = Math.min(64, leafRows - (w << 6));
        final long full = width >= 64
            ? -1L
            : (1L << width) - 1L;
        if ((presence[w] & full) != full) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Whether every row of {@code leaf} is STRICTLY worse than the worst kept row on the first order
   * key, with the leaf's order columns provably all-present. Zone truth: a slice's min/max fold only
   * present values, and the all-present check makes them row-complete.
   */
  private static boolean leafZonePrunable(final ProjectionColumnStore.LeafColumnAccess access,
      final int[] sortColumns, final int leaf, final int leafRows, final boolean descendingFirst,
      final long[] heapTuple) {
    final int presWords = (leafRows + 63) >>> 6;
    for (final int sortCol : sortColumns) {
      final long[] presence = access.slice(sortCol, leaf).presenceWords();
      for (int w = 0; w < presWords; w++) {
        final int width = Math.min(64, leafRows - (w << 6));
        final long full = width >= 64
            ? -1L
            : (1L << width) - 1L;
        if ((presence[w] & full) != full) {
          return false; // a missing order key could hide behind the prune — evaluate the leaf
        }
      }
    }
    final ColumnSlice first = access.slice(sortColumns[0], leaf);
    final long worstFirstKey = heapTuple[0]; // root row's first key sits at tuple offset 0
    return descendingFirst
        ? first.max() < worstFirstKey
        : first.min() > worstFirstKey;
  }

  /**
   * One key-position comparison: raw longs for a numeric key; for a string key both values are packed
   * {@code (leaf, dictId)} refs whose ENTRY BYTES resolve through {@code sortCols} and compare under
   * the interpreter's collation (unsigned UTF-8, decoded compareTo on any 4-byte lead — the same gate
   * every dict kernel uses). Which of the two string arms applies is decided ONCE per column
   * ({@link ProjectionColumnStore#stringDictHasSupplementary}) and carried in {@code keyKind}:
   * re-deriving it here rescanned both operands' bytes on every one of the millions of comparisons a
   * top-k selection makes.
   */
  private static int compareKeyAt(final long a, final long b, final int k, final byte[] keyKind,
      final ProjectionColumnStore.LeafColumnAccess access, final int[] sortColumns, final GlobalValueDictionary.ReadView[] globalSortViews) {
    final byte kind = keyKind[k];
    if (kind == KEY_NUMERIC) {
      return Long.compare(a, b);
    }
    if (kind == KEY_STRING_GLOBAL) {
      return globalSortViews[k].compareIds(Math.toIntExact(a), Math.toIntExact(b));
    }
    if (a == b) {
      return 0; // same leaf, same dict id — same value, no byte walk needed
    }
    final ColumnSlice sa = access.slice(sortColumns[k], (int) (a >>> 32));
    final ColumnSlice sb = access.slice(sortColumns[k], (int) (b >>> 32));
    final byte[] ea = sa.dictBytes();
    final byte[] eb = sb.dictBytes();
    final int aOff = sa.dictOffset((int) a);
    final int aLen = sa.dictLength((int) a);
    final int bOff = sb.dictOffset((int) b);
    final int bLen = sb.dictLength((int) b);
    if (kind == KEY_STRING_COLLATED) {
      // The authority is PER PAIR, not per column: byte order and UTF-16 order differ only when an
      // operand carries a supplementary character, and the byte kernels decode exactly then. A
      // column-wide switch to decoding would reorder clean pairs that merely SHARE a column with a
      // supplementary entry — a different answer from the byte twin. So the memo only ever buys the
      // scan's omission on a provably clean column; here it is earned back in full.
      return ProjectionIndexByteScan.compareStrSlices(ea, aOff, aLen, eb, bOff, bLen);
    }
    return Arrays.compareUnsigned(ea, aOff, aOff + aLen, eb, bOff, bOff + bLen);
  }

  /** Compare a candidate row against heap slot {@code slot} under the sort's total order. */
  private static int compareCandidate(final long[] candidate, final long candidateRank, final long[] heapTuple,
      final long[] heapRank, final int slot, final int keyCount, final boolean[] descending,
      final ProjectionColumnStore.LeafColumnAccess access, final int[] sortColumns, final byte[] keyKind,
      final GlobalValueDictionary.ReadView[] globalSortViews) {
    final int base = slot * keyCount;
    for (int k = 0; k < keyCount; k++) {
      final int cmp = compareKeyAt(candidate[k], heapTuple[base + k], k, keyKind, access, sortColumns, globalSortViews);
      if (cmp != 0) {
        return descending[k]
            ? -cmp
            : cmp;
      }
    }
    return Long.compare(candidateRank, heapRank[slot]);
  }

  /** Compare two heap slots under the sort's total order (per-key direction, rank tiebreak). */
  private static int compareHeapRows(final long[] heapTuple, final long[] heapRank, final int a, final int b,
      final int keyCount, final boolean[] descending, final ProjectionColumnStore.LeafColumnAccess access, final int[] sortColumns, final byte[] keyKind,
      final GlobalValueDictionary.ReadView[] globalSortViews) {
    final int ba = a * keyCount;
    final int bb = b * keyCount;
    for (int k = 0; k < keyCount; k++) {
      final int cmp = compareKeyAt(heapTuple[ba + k], heapTuple[bb + k], k, keyKind, access, sortColumns, globalSortViews);
      if (cmp != 0) {
        return descending[k]
            ? -cmp
            : cmp;
      }
    }
    return Long.compare(heapRank[a], heapRank[b]);
  }

  /** Max-heap sift-down (root = WORST kept row) over the parallel heap arrays. */
  private static void siftDownWorst(final long[] heapTuple, final long[] heapKey, final long[] heapRank,
      final int start, final int size, final int keyCount, final boolean[] descending, final ProjectionColumnStore.LeafColumnAccess access, final int[] sortColumns,
      final byte[] keyKind, final GlobalValueDictionary.ReadView[] globalSortViews) {
    int i = start;
    final int half = size >>> 1;
    while (i < half) {
      int child = (i << 1) + 1;
      final int right = child + 1;
      if (right < size
          && compareHeapRows(heapTuple, heapRank, right, child, keyCount, descending, access, sortColumns, keyKind,
              globalSortViews) > 0) {
        child = right;
      }
      if (compareHeapRows(heapTuple, heapRank, child, i, keyCount, descending, access, sortColumns, keyKind,
          globalSortViews) <= 0) {
        return;
      }
      swapHeapRows(heapTuple, heapKey, heapRank, i, child, keyCount);
      i = child;
    }
  }

  private static void swapHeapRows(final long[] heapTuple, final long[] heapKey, final long[] heapRank, final int a,
      final int b, final int keyCount) {
    final int ba = a * keyCount;
    final int bb = b * keyCount;
    for (int k = 0; k < keyCount; k++) {
      final long tmp = heapTuple[ba + k];
      heapTuple[ba + k] = heapTuple[bb + k];
      heapTuple[bb + k] = tmp;
    }
    long tmp = heapKey[a];
    heapKey[a] = heapKey[b];
    heapKey[b] = tmp;
    tmp = heapRank[a];
    heapRank[a] = heapRank[b];
    heapRank[b] = tmp;
  }

  /**
   * Resolve + validate the order columns: NUMERIC_LONG, STRING_DICT, or STRING_GLOBAL slices, one
   * per key. A global string's numeric lane carries its dictionary id; the bounded top-K caller
   * supplies the revision-bound read view that turns that id into an order in
   * {@link #topKSortColumnsOrderable(ProjectionColumnStore, int[], GlobalValueDictionary.ReadView[])}.
   */
  private static ColumnSlice[][] resolveSortColumns(final ProjectionColumnStore store, final int[] sortColumns,
      final ColumnSegmentFetcher fetcher) {
    validateSortColumns(store, sortColumns);
    final ColumnSlice[][] cols = new ColumnSlice[sortColumns.length][];
    for (int k = 0; k < sortColumns.length; k++) {
      cols[k] = store.column(sortColumns[k], fetcher);
    }
    return cols;
  }

  /** The sort columns' kind check, shared by the resident resolve and the per-leaf access path. */
  private static void validateSortColumns(final ProjectionColumnStore store, final int[] sortColumns) {
    if (sortColumns == null || sortColumns.length < 1) {
      throw new IllegalArgumentException("sortColumns must not be empty");
    }
    for (int k = 0; k < sortColumns.length; k++) {
      final byte kind = store.columnKind(sortColumns[k]);
      // A temporal column sorts on its epoch, which IS the text's order — the one property that
      // makes the numeric key exact for it and not for a global dictionary id.
      if (!ProjectionIndexRowGroupPage.isOrderedLongKind(kind)
          && kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
          && kind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
        throw new IllegalStateException("sortColumn " + sortColumns[k]
            + " is not NUMERIC_LONG, a temporal kind, STRING_DICT, or STRING_GLOBAL");
      }
    }
  }

  // ==================== shared evaluation ====================

  private static void checkPredicates(final ProjectionColumnStore store, final ColumnPredicate[] predicates) {
    if (predicates == null) {
      throw new IllegalArgumentException("predicates must not be null");
    }
    for (final ColumnPredicate p : predicates) {
      if (!store.columnSliceable(p.column)) {
        throw new IllegalStateException("Predicate column " + p.column + " is not sliceable");
      }
      // A SET column is a string column here too: membership takes a string literal with EQ, and
      // the evaluator picks the kernel off the slice's shape. Omitted, this threw "string literal
      // against non-string column" into the caller's fail-soft catch, and every query silently
      // took the whole-leaf byte scan instead.
      final byte columnKind = store.columnKind(p.column);
      if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        // Every per-value string op the dict evaluator serves. NE used to be wrongly excluded
        // here — the whole query silently lost the slice path and took the whole-leaf byte scan.
        if (p.stringLitBytes == null) {
          throw new IllegalStateException("String column " + p.column + " needs a string literal");
        }
        switch (p.op) {
          case EQ, NE, STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS -> {
            /* servable */ }
          default -> throw new IllegalStateException("String column " + p.column + " cannot serve op " + p.op);
        }
      } else if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET) {
        // Membership only: ordering/substring over a SEQUENCE is a different question.
        if (p.stringLitBytes == null || p.op != ProjectionIndexScan.Op.EQ) {
          throw new IllegalStateException("String-set column " + p.column + " only serves EQ membership");
        }
      } else if (columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL
          && p.globalIdVerdict != null) {
        // A pre-evaluated verdict over the resource-wide dictionary: the slice evaluator answers
        // each row with one bit test against its id — servable for every per-value string op.
        switch (p.op) {
          case EQ, NE, STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS -> {
            /* servable */ }
          default -> throw new IllegalStateException(
              "String-global column " + p.column + " cannot serve op " + p.op + " by verdict");
        }
      } else if (p.stringLitBytes != null) {
        throw new IllegalStateException("String literal against non-string column " + p.column);
      }
    }
  }

  private static ColumnSlice[][] resolvePredicateColumns(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSegmentFetcher fetcher) {
    final long[] keep = computeKeepMask(store, predicates, fetcher);
    if (DIAG) {
      int kept = 0;
      if (keep != null) {
        for (final long w : keep) {
          kept += Long.bitCount(w);
        }
      }
      System.err.println("[prune] leaves=" + store.leafCount() + " kept=" + (keep == null
          ? "ALL (no evidence dropped)"
          : kept));
    }
    final ColumnSlice[][] cols = new ColumnSlice[predicates.length][];
    for (int i = 0; i < predicates.length; i++) {
      // A resident column is masked in place — no second fetch, no second budget charge.
      cols[i] = store.columnMaskedView(predicates[i].column, fetcher, keep);
    }
    return cols;
  }

  /**
   * Leaf keep-mask from evidence the store holds BEFORE any predicate column is fetched: the
   * descriptor's numeric zone pairs, and — for string-equality literals — the per-leaf
   * {@link ProjectionIndexColumnSegmentCodec#SEG_KIND_STRING_BLOOM} fingerprints (their chain is a
   * fraction of the BODY+DICT bytes it lets the fill skip). A dropped leaf is one PROVEN to
   * contribute no row: descriptor {@code min > max} (no present value), a numeric zone the predicate
   * excludes, or a fingerprint miss (no false negatives). Leaves without evidence — pre-fingerprint
   * indexes, rowless descriptors' segments, non-EQ string ops — are kept, so behaviour degrades to
   * the unpruned fill, never past it.
   *
   * @return the bitset over leaf indices, or {@code null} when nothing was dropped (callers then use
   *         the plain cached fill)
   */
  private static long @Nullable [] computeKeepMask(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSegmentFetcher fetcher) {
    final int n = store.leafCount();
    if (n == 0) {
      return null;
    }
    final long[] keep = new long[(n + 63) >>> 6];
    Arrays.fill(keep, -1L);
    if ((n & 63) != 0) {
      keep[keep.length - 1] = (1L << (n & 63)) - 1;
    }
    boolean dropped = false;
    for (final ColumnPredicate p : predicates) {
      final byte kind = store.columnKind(p.column);
      if (ProjectionIndexRowGroupPage.isOrderedLongKind(kind) && p.stringLitBytes == null) {
        final int bodyId = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(p.column);
        for (int i = 0; i < n; i++) {
          if ((keep[i >>> 6] & 1L << (i & 63)) == 0) {
            continue;
          }
          final byte[] d = store.leafDescriptor(i);
          final int e = RowGroupDescriptor.entryIndexOf(d, bodyId);
          if (e < 0) {
            continue; // no descriptor evidence — keep
          }
          final long min = RowGroupDescriptor.entryMin(d, e);
          final long max = RowGroupDescriptor.entryMax(d, e);
          if (min > max || zoneSkip(p, min, max)) {
            keep[i >>> 6] &= ~(1L << (i & 63));
            dropped = true;
          }
        }
      } else if (p.stringLitBytes != null && p.op == ProjectionIndexScan.Op.EQ
          && kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        // Op.EQ ONLY, and load-bearing: bloom fingerprints hash WHOLE values, so pruning a leaf
        // on a CONTAINS or ordering literal is a false negative — a leaf whose every URL
        // contains "google" fingerprints none of them as the string "google". Rows would be
        // silently dropped on the one path the byte kernel does not take.
        final long literalHash = ProjectionIndexColumnSegmentCodec.bloomHash(p.stringLitBytes);
        dropped |= store.applyBloomPrune(p.column, literalHash, keep, fetcher) > 0;
      }
    }
    return dropped
        ? keep
        : null;
  }

  /**
   * Build the conjunctive mask for leaf {@code leaf} into {@code mask} — the slice twin of
   * {@code ProjectionIndexByteScan.evaluateRowGroupMask}: numeric zone-skip (segment truth,
   * {@code min > max} prunes outright), per-predicate evaluation, missing ⇒ false via the presence
   * AND. Returns the leaf's rowCount (0 = pruned/empty; the mask may still be all-zero for a live
   * rowCount).
   */
  /** {@link #evaluateMask(ColumnPredicate[], ColumnSlice[][], int, int, long[])} over a per-leaf access. */
  static int evaluateMask(final ColumnPredicate[] predicates, final ProjectionColumnStore.LeafColumnAccess access,
      final int leaf, final int rowCount, final long[] mask, final ColumnSlice[] leafSlices) {
    if (rowCount <= 0) {
      return 0;
    }
    for (int i = 0; i < predicates.length; i++) {
      leafSlices[i] = access.predicateSlice(predicates[i].column, leaf);
    }
    return evaluateMaskOnSlices(predicates, leafSlices, leaf, rowCount, mask);
  }

  static int evaluateMask(final ColumnPredicate[] predicates, final ColumnSlice[][] cols, final int leaf,
      final int rowCount, final long[] mask) {
    if (rowCount <= 0) {
      return 0;
    }
    final ColumnSlice[] leafSlices = new ColumnSlice[predicates.length];
    for (int i = 0; i < predicates.length; i++) {
      leafSlices[i] = cols[i][leaf];
    }
    return evaluateMaskOnSlices(predicates, leafSlices, leaf, rowCount, mask);
  }

  /** The mask kernel proper: {@code slices[i]} is predicate {@code i}'s slice on {@code leaf}. */
  private static int evaluateMaskOnSlices(final ColumnPredicate[] predicates, final ColumnSlice[] slices,
      final int leaf, final int rowCount, final long[] mask) {
    if (rowCount <= 0) {
      return 0;
    }
    // A predicate slice with no rows on a leaf the KEYS chain says has rows is a PRUNED slice
    // (descriptor zone or string fingerprint proved no row can match) — the conjunction is
    // false for the whole leaf. Checked before any evaluator so the pruned sentinel's absent
    // typed arrays are never touched.
    for (int i = 0; i < predicates.length; i++) {
      if (slices[i].rowCount() <= 0) {
        return 0;
      }
    }
    // Zone-map prune — numeric predicate columns only (byte-kernel policy).
    for (int i = 0; i < predicates.length; i++) {
      final ColumnSlice slice = slices[i];
      requireTranslatedLiteral(predicates[i], slice);
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
      final ColumnSlice slice = slices[i];
      final long[] presence = slice.presenceWords();
      final long[] values = slice.numericValues();
      if (values != null) {
        evalNumeric(values, rows, p, presence, mask);
      } else if (slice.setCounts() != null) {
        evalStringSetContains(slice.dictBytes(), slice.dictOffsets(), slice.setCounts(), slice.stringDictIds(), rows,
            p.stringLitBytes, presence, mask);
      } else if (slice.stringDictIds() != null) {
        evalStringDict(slice.dictBytes(), slice.dictOffsets(), slice.stringDictIds(), rows, p, presence, mask);
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

  /**
   * Rowless leaves never reach here with {@code predicates.length == 0} callers — count paths pass
   * ≥1.
   */
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
    if (p.globalIdVerdict != null) {
      // Pre-evaluated global verdict: one bit test per present row. Ids outside 1..count — the
      // missing-cell 0 included — never match, the leaf contract's missing ⇒ false.
      final long[] verdict = p.globalIdVerdict;
      final int idCount = p.globalIdVerdictCount;
      for (int w = 0; w < stride; w++) {
        long candidates = mask[w] & presence[w];
        long out = 0L;
        final int rowBase = w << 6;
        while (candidates != 0L) {
          final int bit = Long.numberOfTrailingZeros(candidates);
          candidates &= candidates - 1L;
          final int rowIdx = rowBase + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          final long id = values[rowIdx];
          if (id >= 1 && id <= idCount && (verdict[(int) (id >>> 6)] & 1L << (id & 63)) != 0L) {
            out |= 1L << bit;
          }
        }
        mask[w] = out;
      }
      return;
    }
    final long lit = p.longLit;
    final long high = p.highLit;
    for (int w = 0; w < stride; w++) {
      final long m = mask[w] & presence[w];
      if (m == 0L) {
        mask[w] = 0L;
        continue;
      }
      final int rowBase = w << 6;
      // Slice arrays are exactly rowCount long, so the branch-free vector compare (measured
      // ~20x over the walk on dense words — see ProjectionVectorKernels) serves only words
      // with a full 64-value window; the tail word takes the guarded walk below.
      if (rowBase + 64 <= rowCount) {
        if (m == -1L) {
          mask[w] = ProjectionVectorKernels.compareWord(values, rowBase, p.op, lit, high);
          continue;
        }
        if (Long.bitCount(m) > ProjectionVectorKernels.COMPARE_WALK_MAX_BITS) {
          mask[w] = m & ProjectionVectorKernels.compareWord(values, rowBase, p.op, lit, high);
          continue;
        }
      }
      long out = 0L;
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
          case NE -> v != lit;
          case BETWEEN_GT_LT -> v > lit && v < high;
          case BETWEEN_GT_LE -> v > lit && v <= high;
          case BETWEEN_GE_LT -> v >= lit && v < high;
          case BETWEEN_GE_LE -> v >= lit && v <= high;
          // Routing defect — checkPredicates admits string ops onto STRING_DICT slices only.
          case STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS ->
            throw new IllegalStateException("string op in the numeric slice kernel: " + p.op);
        };
        if (match) {
          out |= 1L << bit;
        }
      }
      mask[w] = out;
    }
  }

  private static void evalBoolean(final long[] boolWords, final int stride, final boolean lit, final long[] presence,
      final long[] mask) {
    for (int w = 0; w < stride; w++) {
      final long match = lit
          ? boolWords[w]
          : ~boolWords[w];
      mask[w] &= match & presence[w];
    }
  }

  /**
   * String equality over a leaf's dict-id slice — the byte kernel's evaluator, column-scoped: resolve
   * the literal against the leaf dictionary once, then compare ids under the surviving mask. A
   * literal the dictionary does not hold zeroes the leaf's mask outright — "not on this leaf" is
   * exact, the dictionary interns every present value.
   */
  /**
   * Count rows whose set contains {@code literal}, from the DICTIONARIES ALONE.
   *
   * <p>
   * No BODY segment is fetched, no element is visited and no mask is built: each leaf contributes the
   * row count stored beside its dictionary entry. A leaf whose dictionary lacks the literal
   * contributes nothing without any further work, which is the same pruning the scanning kernel does
   * but at a fraction of the I/O.
   *
   * <p>
   * Valid ONLY for a bare count over one set column. A conjunction needs to know WHICH rows matched
   * so later predicates can narrow them, and a per-value total cannot say — the caller's gate is what
   * keeps this method from being reached with a second predicate.
   *
   * @return matching rows, or {@code -1} when any leaf lacks counts and the caller must scan
   */
  public static long countSetMembership(final ProjectionColumnStore store, final int col, final byte[] literal,
      final ProjectionColumnStore.ColumnSegmentFetcher fetcher) {
    // Preferred: the inline SET_COUNTS segments, whose bytes ride in the descriptors this store
    // already holds — no page is read at all. The dictionary route below is the fallback for leaves
    // written before that segment existed, and it costs a page per leaf.
    final byte[][] inlineCounts = store.setCountsSegments(col, fetcher);
    if (inlineCounts != null) {
      long viaInline = 0;
      boolean complete = true;
      for (int leaf = 0; leaf < inlineCounts.length; leaf++) {
        if (store.rowCount(leaf) == 0) {
          continue;
        }
        final long n =
            ProjectionIndexColumnSegmentCodec.setCountFor(store.descriptor(leaf), inlineCounts[leaf], col, literal);
        if (n < 0) {
          complete = false;
          break;
        }
        viaInline += n;
      }
      if (complete) {
        return viaInline;
      }
    }
    final var perLeaf = store.dictRowCounts(col, fetcher);
    if (perLeaf == null) {
      return -1;
    }
    long total = 0;
    for (final var leaf : perLeaf) {
      if (leaf == null) {
        continue; // rowless leaf writes no dictionary
      }
      if (leaf.rowCounts() == null) {
        return -1; // written before the counts existed — fall back wholesale
      }
      final byte[][] dict = leaf.dict();
      for (int i = 0; i < dict.length && dict[i] != null; i++) {
        if (Arrays.equals(dict[i], literal)) {
          total += leaf.rowCounts()[i];
          break;
        }
      }
    }
    return total;
  }

  /**
   * Set membership: mark the rows whose set holds {@code literal}.
   *
   * <p>
   * The whole point of giving array-valued fields their own column kind. The literal resolves against
   * the leaf's dictionary ONCE, and a literal the dictionary does not hold rules out every row in the
   * leaf without reading a single element — the pruning a per-page storage scan cannot do, because
   * there the values of one field are mixed in with every other string on the page.
   *
   * <p>
   * Elements are laid out flat, rows consecutive, so one forward walk with a running cursor visits
   * each element exactly once. An existential stops at the first hit in a row.
   *
   * @param counts per-row element count
   * @param elems flat dict ids, rows consecutive
   */
  private static void evalStringSetContains(final byte[] dictBytes, final int[] dictOffsets, final int[] counts,
      final int[] elems, final int rowCount, final byte[] literal, final long[] presence, final long[] mask) {
    final int stride = (rowCount + 63) >>> 6;
    final int dictSize = dictOffsets.length - 1;
    int targetDictId = -1;
    for (int i = 0; i < dictSize; i++) {
      if (Arrays.equals(dictBytes, dictOffsets[i], dictOffsets[i + 1], literal, 0, literal.length)) {
        targetDictId = i;
        break;
      }
    }
    if (targetDictId < 0) {
      Arrays.fill(mask, 0, stride, 0L); // not in this leaf at all
      return;
    }
    // ONE pass, scalar, with an early exit per row. Measured against a vectorized restructuring
    // (flat SIMD compare over the elements into a bitmap, then a range test per row): that was
    // 338 -> 579 ms on the movies corpus, i.e. 1.7x SLOWER, and worse still on a rarer literal.
    //
    // The reason is element DENSITY, not the kernel. A leaf holds ~1,024 rows of ~2.5 elements, so
    // ~2,560 ints; the scalar loop exits at the first match in a row, which on a selective literal
    // is usually the first element. The vector version pays a count pass, a bitmap zeroing and a
    // range test per row to save comparisons that were never the cost. SIMD wins on this shape only
    // when a row holds many elements, and the profile's 38 %% self-time meant "this is the only work
    // left", not "this is inefficient per element".
    if (SET_SCAN_VECTORIZED) {
      evalStringSetContainsVectorized(counts, elems, rowCount, targetDictId, presence, mask);
      return;
    }
    int cursor = 0;
    for (int row = 0; row < rowCount; row++) {
      final int n = counts[row];
      final int w = row >>> 6;
      final long bit = 1L << (row & 63);
      // The cursor advances over every row's elements whether or not the row is live, or the flat
      // run desynchronises and later rows read their neighbours' elements.
      if ((mask[w] & presence[w] & bit) != 0L) {
        boolean hit = false;
        for (int k = 0; k < n; k++) {
          if (elems[cursor + k] == targetDictId) {
            hit = true;
            break;
          }
        }
        if (!hit) {
          mask[w] &= ~bit;
        }
      } else {
        mask[w] &= ~bit;
      }
      cursor += n;
    }
  }

  /**
   * Which membership kernel runs. Both live in one build so they can be INTERLEAVED on one machine
   * rather than compared across builds — a comparison of this kernel across two runs was what a
   * thermally throttled laptop turned into a 1.7x phantom regression.
   */
  public static boolean SET_SCAN_VECTORIZED = Boolean.getBoolean("sirix.projection.setScanSimd");

  /**
   * The same answer via a flat SIMD compare over the elements, then a range test per row.
   *
   * <p>
   * The hypothesis this exists to test: finding the literal among a leaf's elements has no per-row
   * structure, so it should vectorize at 64 elements per word, and attributing hits to rows should
   * then be a word-granular range test rather than a branch per element.
   */
  private static void evalStringSetContainsVectorized(final int[] counts, final int[] elems, final int rowCount,
      final int targetDictId, final long[] presence, final long[] mask) {
    int elemCount = 0;
    for (int row = 0; row < rowCount; row++) {
      elemCount += counts[row];
    }
    final int elemWords = (elemCount + 63) >>> 6;
    long[] hits = ELEMENT_HITS.get();
    if (hits.length < elemWords) {
      hits = new long[Math.max(elemWords, hits.length << 1)];
      ELEMENT_HITS.set(hits);
    }
    Arrays.fill(hits, 0, elemWords, 0L);
    int e = 0;
    for (; e + 64 <= elemCount; e += 64) {
      hits[e >>> 6] = ProjectionVectorKernels.equalsIdWord(elems, e, targetDictId);
    }
    for (; e < elemCount; e++) {
      if (elems[e] == targetDictId) {
        hits[e >>> 6] |= 1L << (e & 63);
      }
    }
    int cursor = 0;
    for (int row = 0; row < rowCount; row++) {
      final int n = counts[row];
      final int w = row >>> 6;
      final long bit = 1L << (row & 63);
      final boolean live = (mask[w] & presence[w] & bit) != 0L;
      if (!live || !anySet(hits, cursor, cursor + n)) {
        mask[w] &= ~bit;
      }
      cursor += n;
    }
  }

  /** Per-thread element-hit bitmap, grown to the widest leaf a thread has seen. */
  private static final ThreadLocal<long[]> ELEMENT_HITS = ThreadLocal.withInitial(() -> new long[64]);

  /** Whether any bit in {@code [from, to)} is set, tested a word at a time. */
  private static boolean anySet(final long[] bits, final int from, final int to) {
    if (from >= to) {
      return false;
    }
    final int firstWord = from >>> 6;
    final int lastWord = (to - 1) >>> 6;
    final long headMask = -1L << (from & 63);
    final int lastBit = (to - 1) & 63;
    final long tailMask = lastBit == 63
        ? -1L
        : (1L << (lastBit + 1)) - 1L;
    if (firstWord == lastWord) {
      return (bits[firstWord] & headMask & tailMask) != 0L;
    }
    if ((bits[firstWord] & headMask) != 0L || (bits[lastWord] & tailMask) != 0L) {
      return true;
    }
    for (int w = firstWord + 1; w < lastWord; w++) {
      if (bits[w] != 0L) {
        return true;
      }
    }
    return false;
  }

  private static void evalStringDict(final byte[] dictBytes, final int[] dictOffsets, final int[] ids,
      final int rowCount, final ColumnPredicate p, final long[] presence, final long[] mask) {
    // Two-phase like the byte kernel: the predicate evaluates once per dict entry into an id
    // bitset, the rows test one bit each. The single-matching-id EQ case keeps the SIMD
    // equalsIdWord specialization the old equality-only kernel had.
    final int dictSize = dictOffsets.length - 1;
    final long[] idBits = new long[dictSize + 63 >>> 6];
    int matches = 0;
    int lastMatch = -1;
    final byte[] lit = p.stringLitBytes;
    final boolean litHasSupplementary = ProjectionIndexScan.hasFourByteUtf8(lit, 0, lit.length);
    for (int i = 0; i < dictSize; i++) {
      if (ProjectionIndexScan.stringDictEntryMatches(dictBytes, dictOffsets[i], dictOffsets[i + 1] - dictOffsets[i],
          p.op, lit, litHasSupplementary)) {
        idBits[i >>> 6] |= 1L << (i & 63);
        matches++;
        lastMatch = i;
      }
    }
    final int stride = (rowCount + 63) >>> 6;
    if (matches == 0) {
      Arrays.fill(mask, 0, stride, 0L);
      return;
    }
    final boolean singleId = matches == 1;
    final int targetDictId = lastMatch;
    for (int w = 0; w < stride; w++) {
      final long m = mask[w] & presence[w];
      if (m == 0L) {
        mask[w] = 0L;
        continue;
      }
      final int rowBase = w << 6;
      // Same dispatch as evalNumeric, on int lanes: the branch-free id compare (measured 8x
      // over the walk on dense words — see ProjectionVectorKernels.equalsIdWord) serves full
      // 64-id windows when exactly one dict id matches; the general case and the tail word
      // take the guarded walk below.
      if (singleId && rowBase + 64 <= rowCount) {
        if (m == -1L) {
          mask[w] = ProjectionVectorKernels.equalsIdWord(ids, rowBase, targetDictId);
          continue;
        }
        if (Long.bitCount(m) > ProjectionVectorKernels.COMPARE_WALK_MAX_BITS_INT) {
          mask[w] = m & ProjectionVectorKernels.equalsIdWord(ids, rowBase, targetDictId);
          continue;
        }
      }
      long out = 0L;
      long candidates = m;
      while (candidates != 0L) {
        final int bit = Long.numberOfTrailingZeros(candidates);
        candidates &= candidates - 1L;
        final int rowIdx = rowBase + bit;
        if (rowIdx >= rowCount) {
          break;
        }
        final int id = ids[rowIdx];
        if ((idBits[id >>> 6] & 1L << (id & 63)) != 0L) {
          out |= 1L << bit;
        }
      }
      mask[w] = out;
    }
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#distinctPresentStrings}: distinct PRESENT string
   * values of a dict column over leaves {@code [fromLeaf, toLeaf)}, from the column's slices alone —
   * the payload route hydrates every column of every leaf to read one dict. Same contract:
   * sparse-clean unpredicated callers only; every non-empty dict entry was interned by a present row,
   * and only a zero-length entry needs per-row disambiguation (a MISSING row interns the "" default).
   * Content-based dedup; {@code cardLimit} exceeded, a null slice, or absent dict/id/presence lanes
   * return {@code null} — the caller falls back.
   */
  public static @Nullable ArrayList<byte[]> distinctPresentStrings(final ColumnSlice[] slices, final int fromLeaf,
      final int toLeaf, final int cardLimit) {
    final ArrayList<byte[]> distinct = new ArrayList<>(16);
    boolean emptyReal = false;
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final ColumnSlice slice = slices[leaf];
      if (slice == null) {
        return null;
      }
      final int rowCount = slice.rowCount();
      if (rowCount == 0) {
        continue;
      }
      final byte[] dictBytes = slice.dictBytes();
      final int[] dictOffsets = slice.dictOffsets();
      if (dictBytes == null || dictOffsets == null) {
        return null;
      }
      final int dictSize = dictOffsets.length - 1;
      int emptyId = -1;
      for (int i = 0; i < dictSize; i++) {
        final int off = dictOffsets[i];
        final int end = dictOffsets[i + 1];
        if (off == end) {
          emptyId = i;
          continue;
        }
        boolean present = false;
        for (int c = distinct.size() - 1; c >= 0; c--) {
          final byte[] seen = distinct.get(c);
          if (Arrays.equals(dictBytes, off, end, seen, 0, seen.length)) {
            present = true;
            break;
          }
        }
        if (!present) {
          if (distinct.size() >= cardLimit) {
            return null;
          }
          // The result OUTLIVES the slice's flat run, so this one is copied out — bounded by
          // cardLimit, unlike the per-entry arrays the flat form exists to avoid.
          distinct.add(Arrays.copyOfRange(dictBytes, off, end));
        }
      }
      if (emptyId >= 0 && !emptyReal) {
        final long[] presence = slice.presenceWords();
        final int[] ids = slice.stringDictIds();
        if (presence == null || ids == null) {
          return null;
        }
        final int presWords = (rowCount + 63) >>> 6;
        boolean allPresent = true;
        for (int w = 0; w < presWords; w++) {
          final long expect = w == presWords - 1 && (rowCount & 63) != 0
              ? (1L << (rowCount & 63)) - 1
              : -1L;
          if ((presence[w] & expect) != expect) {
            allPresent = false;
            break;
          }
        }
        if (allPresent) {
          // Every row is present, so the "" entry was interned by a present row.
          emptyReal = true;
        } else {
          for (int r = 0; r < rowCount; r++) {
            if ((presence[r >>> 6] & 1L << (r & 63)) == 0L) {
              continue;
            }
            if (ids[r] == emptyId) {
              emptyReal = true;
              break;
            }
          }
        }
      }
    }
    if (emptyReal) {
      distinct.add(new byte[0]);
    }
    return distinct;
  }

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#stringDictMinMax}: the presence-gated extremum of a
   * dict column's REFERENCED entries over leaves {@code [fromLeaf, toLeaf)} — a dictionary holds
   * PHANTOM entries interned by missing rows, so an unreferenced entry must never win. Comparison
   * authority is the byte kernel's own {@code compareStrSlices} (UTF-16 collation fallback on 4-byte
   * leads). Returns the winning entry's bytes, or {@code null} when no present value exists in the
   * range; a slice missing its dict/id lanes throws — the caller declines.
   */
  public static byte @Nullable [] stringDictMinMax(final ColumnSlice[] slices, final int fromLeaf, final int toLeaf,
      final boolean min) {
    byte[] best = null;
    long[] referenced = null;
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final ColumnSlice slice = slices[leaf];
      if (slice == null || slice.rowCount() <= 0) {
        continue;
      }
      final byte[] dictBytes = slice.dictBytes();
      final int[] dictOffsets = slice.dictOffsets();
      final int[] ids = slice.stringDictIds();
      if (dictBytes == null || dictOffsets == null || ids == null) {
        throw new IllegalStateException("string min/max slice without dict/id lanes at leaf " + leaf);
      }
      final int dictSize = dictOffsets.length - 1;
      if (dictSize == 0) {
        continue;
      }
      final long[] presence = slice.presenceWords();
      final int rowCount = slice.rowCount();
      final int refWords = (dictSize + 63) >>> 6;
      if (referenced == null || referenced.length < refWords) {
        referenced = new long[Math.max(16, refWords)];
      } else {
        Arrays.fill(referenced, 0, refWords, 0L);
      }
      for (int r = 0; r < rowCount; r++) {
        if (presence != null && (presence[r >>> 6] & 1L << (r & 63)) == 0L) {
          continue;
        }
        final int id = ids[r];
        referenced[id >>> 6] |= 1L << (id & 63);
      }
      for (int i = 0; i < dictSize; i++) {
        if ((referenced[i >>> 6] & 1L << (i & 63)) == 0L) {
          continue;
        }
        final int off = dictOffsets[i];
        final int len = dictOffsets[i + 1] - off;
        if (best == null || ProjectionIndexByteScan.compareStrSlices(dictBytes, off, len, best, 0, best.length) * (min
            ? 1
            : -1) < 0) {
          // Copied out of the flat run: the winner is returned to the caller and must not alias a
          // slice's dictionary, and at most one copy per improvement is made per leaf.
          best = Arrays.copyOfRange(dictBytes, off, off + len);
        }
      }
    }
    return best;
  }

  /** The dict-entry comparison authority, exposed for callers merging per-chunk winners. */
  public static int compareDictEntries(final byte[] a, final byte[] b) {
    return ProjectionIndexByteScan.compareStrSlices(a, 0, a.length, b, 0, b.length);
  }

  /** Per-thread tree-mask stack (MAX_LEAVES × MAX_ROWS bound — same bound the byte twin uses). */
  private static final ThreadLocal<long[][]> TREE_STACK = ThreadLocal.withInitial(
      () -> new long[ProjectionIndexScan.PredicateTree.MAX_LEAVES][(ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6]);

  /**
   * Sliced twin of the byte kernels' tree mask evaluator: a stack machine over per-leaf masks — each
   * leaf is (predicate matches AND its own column's presence), AND/OR combine, NOT is the complement
   * (missing OR present-and-failing = fn:not exactly; tail garbage clears on the final AND with the
   * tail-masked base). Zone/prune evidence stays PER LEAF (an all-zero leaf mask, sound under OR) —
   * never a row-group skip. {@code treeCols[i]} = FULL fill of leaf {@code i}'s column (keep-mask
   * pruning is conjunction-only and must not touch tree fills).
   */
  static int evaluateMaskTree(final ProjectionIndexScan.PredicateTree tree, final ColumnSlice[][] treeCols,
      final int leaf, final int rowCount, final long[] mask) {
    if (rowCount <= 0) {
      return 0;
    }
    final int stride = (rowCount + 63) >>> 6;
    fillAllTrue(mask, rowCount, stride);
    final long[][] stack = TREE_STACK.get();
    int top = 0;
    for (final byte op : tree.program) {
      if (op >= 0) {
        final long[] dst = stack[top++];
        evalLeafInto(tree.leaves[op], treeCols[op][leaf], rowCount, stride, dst);
      } else if (op == ProjectionIndexScan.PredicateTree.OP_AND) {
        final long[] b = stack[--top];
        final long[] a = stack[top - 1];
        for (int i = 0; i < stride; i++) {
          a[i] &= b[i];
        }
      } else if (op == ProjectionIndexScan.PredicateTree.OP_NOT) {
        final long[] a = stack[top - 1];
        for (int i = 0; i < stride; i++) {
          a[i] = ~a[i];
        }
      } else {
        final long[] b = stack[--top];
        final long[] a = stack[top - 1];
        for (int i = 0; i < stride; i++) {
          a[i] |= b[i];
        }
      }
    }
    final long[] result = stack[0];
    for (int i = 0; i < stride; i++) {
      mask[i] &= result[i];
    }
    return rowCount;
  }

  /**
   * A string literal must never meet a slice whose values live in the LONG lane.
   *
   * <p>
   * Both numeric columns and {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL} present
   * the same slice shape — {@code numericValues != null} — and the evaluators dispatch on that shape,
   * so nothing downstream can tell a quantity from a dictionary id. A predicate over a global string
   * column is therefore resolved to an id at plan time and arrives NUMERIC; one that still carries
   * its literal bytes has escaped that translation, and running it would compare an unset
   * {@code longLit} against ids and answer a different question with a plausible number. Loud, and
   * caught by every caller as a decline.
   */
  private static void requireTranslatedLiteral(final ColumnPredicate p, final ColumnSlice slice) {
    if (p.stringLitBytes != null && slice.numericValues() != null && p.globalIdVerdict == null) {
      // A VERDICT predicate keeps its literal by design (it also keys the zone-skip exemption);
      // only a literal with neither an id translation nor a verdict marks a routing defect.
      throw new IllegalStateException("column " + p.column + " stores values in the long lane, but the " + p.op
          + " predicate still carries a string literal — it was never resolved to a dictionary id");
    }
  }

  /**
   * One tree leaf's mask: (matches AND presence), into {@code dst} — pruned/rowless/zone-skipped
   * slices yield the all-zero mask (the correct per-leaf value under any combiner).
   */
  private static void evalLeafInto(final ColumnPredicate p, final ColumnSlice slice, final int rowCount,
      final int stride, final long[] dst) {
    if (slice == null || slice.rowCount() <= 0) {
      Arrays.fill(dst, 0, stride, 0L);
      return;
    }
    requireTranslatedLiteral(p, slice);
    final long[] values = slice.numericValues();
    if (values != null && (slice.min() > slice.max() || zoneSkip(p, slice.min(), slice.max()))) {
      Arrays.fill(dst, 0, stride, 0L);
      return;
    }
    fillAllTrue(dst, rowCount, stride);
    final long[] presence = slice.presenceWords();
    if (values != null) {
      evalNumeric(values, rowCount, p, presence, dst);
    } else if (slice.setCounts() != null) {
      evalStringSetContains(slice.dictBytes(), slice.dictOffsets(), slice.setCounts(), slice.stringDictIds(), rowCount,
          p.stringLitBytes, presence, dst);
    } else if (slice.stringDictIds() != null) {
      evalStringDict(slice.dictBytes(), slice.dictOffsets(), slice.stringDictIds(), rowCount, p, presence, dst);
    } else {
      evalBoolean(slice.boolWords(), stride, p.boolLit, presence, dst);
    }
  }

}
