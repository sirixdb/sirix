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

import java.util.Arrays;

/**
 * Column-sliced scan kernels (P5b stage 3, docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-7):
 * the {@link ProjectionIndexByteScan} conjunctive semantics re-expressed over
 * {@link ProjectionColumnStore} slices, so a query touches ONLY its predicate and aggregate
 * columns' BODY segments — never the whole raw leaf.
 *
 * <p><b>Parity contract.</b> Every rule mirrors {@code evaluateRowGroupMask} bit for bit: numeric
 * zone-skip on segment-truth min/max (including the {@code min > max} all-missing prune and
 * the BETWEEN skip table), missing-field ⇒ predicate-false via the presence AND, boolean
 * bitmap equality, {@code Double.compare} total order for double min/max folds, and the
 * aggregate column's own presence AND before folding. Supported predicate shapes: numeric
 * compare/BETWEEN, boolean equality, and string equality — the string leaf resolves its
 * literal against the leaf's DICT segment (absent ⇒ the leaf contributes nothing) and
 * compares dict-ids, exactly like the byte kernel, from two segments instead of a whole
 * leaf. {@code ProjectionColumnScanParityTest} pins the equivalence against the byte
 * kernels over randomized stores.
 *
 * <p>Scratch is thread-local and bounded by {@link ProjectionIndexRowGroupPage#MAX_ROWS} —
 * per-leaf evaluation allocates nothing.
 */
public final class ProjectionColumnScan {

  private static final int MASK_WORDS = (ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6;

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
      final ColumnPredicate[] predicates, final ColumnSegmentFetcher fetcher) {
    return conjunctiveCount(store, predicates, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for the executor's chunked parallel dispatch — scratch is thread-local. */
  public static long conjunctiveCount(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int fromRowGroup, final int toRowGroup,
      final ColumnSegmentFetcher fetcher) {
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
   * Conjunctive numeric-long aggregate — {@code acc = [count, sum, min, max]}, initialised
   * by the caller to {@code {0, 0, Long.MAX_VALUE, Long.MIN_VALUE}}. The aggregate column's
   * presence is ANDed before folding, exactly like the byte kernel.
   */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final long[] acc,
      final ColumnSegmentFetcher fetcher) {
    conjunctiveAggregateNumeric(store, predicates, numericColumn, acc, 0, store.rowGroupCount(), fetcher);
  }

  /** Ranged variant for chunked parallel dispatch. */
  public static void conjunctiveAggregateNumeric(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final long[] acc,
      final int fromRowGroup, final int toRowGroup, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    if (store.columnKind(numericColumn) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
      throw new IllegalStateException("aggregate column " + numericColumn + " is not NUMERIC_LONG");
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
      final ColumnPredicate[] predicates, final int numericColumn, final double[] acc,
      final ColumnSegmentFetcher fetcher) {
    conjunctiveAggregateNumericDouble(store, predicates, numericColumn, acc, 0, store.rowGroupCount(),
        fetcher);
  }

  /** Ranged variant for chunked parallel dispatch (count/min/max are merge-order-insensitive). */
  public static void conjunctiveAggregateNumericDouble(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int numericColumn, final double[] acc,
      final int fromRowGroup, final int toRowGroup, final ColumnSegmentFetcher fetcher) {
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
        final ColumnPredicate[] predicates, final int numericColumn, final ColumnSegmentFetcher fetcher) {
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
   * Column-sliced twin of {@code ProjectionIndexByteScan.collectMatchingSortTuples}: matching
   * rows' order-key tuples (row-major, stride {@code sortColumns.length}) and record keys, in
   * document order, from the predicate/order columns' slices and the KEYS chain — never a whole
   * leaf. A matching row MISSING any order key goes to {@code missingKeysOut}; the caller
   * declines on any such row (the interpreter owns empty-order-key placement).
   */
  public static void collectMatchingSortTuples(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int[] sortColumns,
      final LongArrayList valuesOut, final LongArrayList keysOut,
      final LongArrayList missingKeysOut, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    final ColumnSlice[][] sortCols = resolveSortColumns(store, sortColumns, fetcher);
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates, fetcher);
    final long[][] keySlices = store.recordKeys(fetcher);
    final Scratch s = SCRATCH.get();
    final int keyCount = sortColumns.length;
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
   * Bounded top-{@code k} sorted scan, fused with collection — the R2 "heap over
   * zone-map-pruned leaves" shape: once the heap is full, a leaf whose FIRST order key's
   * zone bounds cannot beat the current worst kept row is skipped without evaluating its
   * mask at all. Sound because entering the heap requires beating the worst row, the first
   * key is compared first, and the prune only fires when the leaf's every row is STRICTLY
   * worse on it — ties fall through to full evaluation. The prune additionally requires the
   * leaf's order columns to be all-present: a skipped leaf must not be able to hide a
   * matching row with a missing order key, which obliges the caller to decline outright.
   *
   * @return record keys of the first {@code k} rows of the full stable sort, in emission
   *         order — or {@code null} when a matching row misses an order key, which only the
   *         generic pipeline can place correctly
   */
  public static long @Nullable [] topKRecordKeys(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final int[] sortColumns, final boolean[] descending,
      final int k, final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    final ColumnSlice[][] sortCols = resolveSortColumns(store, sortColumns, fetcher);
    if (k <= 0) {
      return new long[0];
    }
    final ColumnSlice[][] cols = resolvePredicateColumns(store, predicates, fetcher);
    final long[][] keySlices = store.recordKeys(fetcher);
    final Scratch s = SCRATCH.get();
    final int keyCount = sortColumns.length;
    // Heap of the K best rows: tuples row-major beside keys and document-order ranks, root =
    // worst kept. The rank is the stable-sort tiebreak, so bounded selection and the full
    // stable sort agree exactly.
    final long[] heapTuple = new long[k * keyCount];
    final long[] heapKey = new long[k];
    final long[] heapRank = new long[k];
    int size = 0;
    long rank = 0;
    final long[] candidate = new long[keyCount];
    for (int leaf = 0; leaf < store.rowGroupCount(); leaf++) {
      final int leafRows = store.rowCount(leaf);
      if (leafRows <= 0) {
        continue;
      }
      if (size == k && leafZonePrunable(sortCols, leaf, leafRows, descending[0], heapTuple)) {
        rank += leafRows;  // ranks only order rows WITHIN the kept set; skipped rows never enter
        continue;
      }
      final int rowCount = evaluateMask(predicates, cols, leaf, leafRows, s.mask);
      if (rowCount <= 0) {
        rank += leafRows;
        continue;
      }
      final long[] keys = keySlices[leaf];
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = s.mask[w];
        if (word == 0L) {
          continue;
        }
        long presAll = -1L;
        for (int kk = 0; kk < keyCount; kk++) {
          presAll &= sortCols[kk][leaf].presenceWords()[w];
        }
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = (w << 6) + bit;
          if (rowIdx >= rowCount) {
            break;
          }
          if ((presAll & (1L << bit)) == 0L) {
            return null;  // a matching row without an order key — the interpreter places it
          }
          for (int kk = 0; kk < keyCount; kk++) {
            candidate[kk] = sortCols[kk][leaf].numericValues()[rowIdx];
          }
          final long rowRank = rank + rowIdx;
          if (size < k) {
            final int slot = size++;
            System.arraycopy(candidate, 0, heapTuple, slot * keyCount, keyCount);
            heapKey[slot] = keys[rowIdx];
            heapRank[slot] = rowRank;
            if (size == k) {
              for (int i = (k >>> 1) - 1; i >= 0; i--) {
                siftDownWorst(heapTuple, heapKey, heapRank, i, k, keyCount, descending);
              }
            }
          } else if (compareCandidate(candidate, rowRank, heapTuple, heapRank, 0, keyCount,
                                      descending) < 0) {
            System.arraycopy(candidate, 0, heapTuple, 0, keyCount);
            heapKey[0] = keys[rowIdx];
            heapRank[0] = rowRank;
            siftDownWorst(heapTuple, heapKey, heapRank, 0, k, keyCount, descending);
          }
        }
      }
      rank += leafRows;
    }
    // Emit in sort order: primitive index sort over the kept rows under the same total order.
    final int kept = size;
    final int[] order = new int[kept];
    for (int i = 0; i < kept; i++) {
      order[i] = i;
    }
    IntArrays.mergeSort(order, (a, b) -> compareHeapRows(heapTuple, heapRank, a, b, keyCount, descending));
    final long[] out = new long[kept];
    for (int i = 0; i < kept; i++) {
      out[i] = heapKey[order[i]];
    }
    return out;
  }

  /**
   * Whether every row of {@code leaf} is STRICTLY worse than the worst kept row on the first
   * order key, with the leaf's order columns provably all-present. Zone truth: a slice's
   * min/max fold only present values, and the all-present check makes them row-complete.
   */
  private static boolean leafZonePrunable(final ColumnSlice[][] sortCols, final int leaf,
      final int leafRows, final boolean descendingFirst, final long[] heapTuple) {
    final int presWords = (leafRows + 63) >>> 6;
    for (final ColumnSlice[] col : sortCols) {
      final long[] presence = col[leaf].presenceWords();
      for (int w = 0; w < presWords; w++) {
        final int width = Math.min(64, leafRows - (w << 6));
        final long full = width >= 64 ? -1L : (1L << width) - 1L;
        if ((presence[w] & full) != full) {
          return false;  // a missing order key could hide behind the prune — evaluate the leaf
        }
      }
    }
    final ColumnSlice first = sortCols[0][leaf];
    final long worstFirstKey = heapTuple[0];  // root row's first key sits at tuple offset 0
    return descendingFirst ? first.max() < worstFirstKey : first.min() > worstFirstKey;
  }

  /** Compare a candidate row against heap slot {@code slot} under the sort's total order. */
  private static int compareCandidate(final long[] candidate, final long candidateRank,
      final long[] heapTuple, final long[] heapRank, final int slot, final int keyCount,
      final boolean[] descending) {
    final int base = slot * keyCount;
    for (int k = 0; k < keyCount; k++) {
      final int cmp = Long.compare(candidate[k], heapTuple[base + k]);
      if (cmp != 0) {
        return descending[k] ? -cmp : cmp;
      }
    }
    return Long.compare(candidateRank, heapRank[slot]);
  }

  /** Compare two heap slots under the sort's total order (per-key direction, rank tiebreak). */
  private static int compareHeapRows(final long[] heapTuple, final long[] heapRank, final int a,
      final int b, final int keyCount, final boolean[] descending) {
    final int ba = a * keyCount;
    final int bb = b * keyCount;
    for (int k = 0; k < keyCount; k++) {
      final int cmp = Long.compare(heapTuple[ba + k], heapTuple[bb + k]);
      if (cmp != 0) {
        return descending[k] ? -cmp : cmp;
      }
    }
    return Long.compare(heapRank[a], heapRank[b]);
  }

  /** Max-heap sift-down (root = WORST kept row) over the parallel heap arrays. */
  private static void siftDownWorst(final long[] heapTuple, final long[] heapKey,
      final long[] heapRank, final int start, final int size, final int keyCount,
      final boolean[] descending) {
    int i = start;
    final int half = size >>> 1;
    while (i < half) {
      int child = (i << 1) + 1;
      final int right = child + 1;
      if (right < size
          && compareHeapRows(heapTuple, heapRank, right, child, keyCount, descending) > 0) {
        child = right;
      }
      if (compareHeapRows(heapTuple, heapRank, child, i, keyCount, descending) <= 0) {
        return;
      }
      swapHeapRows(heapTuple, heapKey, heapRank, i, child, keyCount);
      i = child;
    }
  }

  private static void swapHeapRows(final long[] heapTuple, final long[] heapKey,
      final long[] heapRank, final int a, final int b, final int keyCount) {
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

  /** Resolve + validate the order columns: NUMERIC_LONG slices, one per key. */
  private static ColumnSlice[][] resolveSortColumns(final ProjectionColumnStore store,
      final int[] sortColumns, final ColumnSegmentFetcher fetcher) {
    if (sortColumns == null || sortColumns.length < 1) {
      throw new IllegalArgumentException("sortColumns must not be empty");
    }
    final ColumnSlice[][] cols = new ColumnSlice[sortColumns.length][];
    for (int k = 0; k < sortColumns.length; k++) {
      if (store.columnKind(sortColumns[k]) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        throw new IllegalStateException("sortColumn " + sortColumns[k] + " is not NUMERIC_LONG");
      }
      cols[k] = store.column(sortColumns[k], fetcher);
    }
    return cols;
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
      final boolean stringColumn = store.columnKind(p.column)
          == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
      if (stringColumn) {
        // The one string shape these kernels serve, mirroring the byte kernel's evaluator.
        if (p.stringLitBytes == null || p.op != ProjectionIndexScan.Op.EQ) {
          throw new IllegalStateException(
              "String column " + p.column + " only serves equality with a string literal");
        }
      } else if (p.stringLitBytes != null) {
        throw new IllegalStateException(
            "String literal against non-string column " + p.column);
      }
    }
  }

  private static ColumnSlice[][] resolvePredicateColumns(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSegmentFetcher fetcher) {
    final ColumnSlice[][] cols = new ColumnSlice[predicates.length][];
    for (int i = 0; i < predicates.length; i++) {
      cols[i] = store.column(predicates[i].column, fetcher);
    }
    return cols;
  }

  /**
   * Build the conjunctive mask for leaf {@code leaf} into {@code mask} — the slice twin of
   * {@code ProjectionIndexByteScan.evaluateRowGroupMask}: numeric zone-skip (segment truth,
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
      } else if (slice.stringDictIds() != null) {
        evalStringEq(slice.stringDict(), slice.stringDictIds(), rows, p.stringLitBytes, presence,
                     mask);
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

  /**
   * String equality over a leaf's dict-id slice — the byte kernel's evaluator, column-scoped:
   * resolve the literal against the leaf dictionary once, then compare ids under the
   * surviving mask. A literal the dictionary does not hold zeroes the leaf's mask outright —
   * "not on this leaf" is exact, the dictionary interns every present value.
   */
  private static void evalStringEq(final byte[][] dict, final int[] ids, final int rowCount,
      final byte[] literal, final long[] presence, final long[] mask) {
    int targetDictId = -1;
    for (int i = 0; i < dict.length && dict[i] != null; i++) {
      if (Arrays.equals(dict[i], literal)) {
        targetDictId = i;
        break;
      }
    }
    final int stride = (rowCount + 63) >>> 6;
    if (targetDictId < 0) {
      Arrays.fill(mask, 0, stride, 0L);
      return;
    }
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
        if (ids[rowIdx] == targetDictId) {
          out |= 1L << bit;
        }
      }
      mask[w] = out;
    }
  }
}
