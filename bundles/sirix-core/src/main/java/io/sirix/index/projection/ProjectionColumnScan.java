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

  private static final int MASK_WORDS = (ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6;

  private static final class Scratch {
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
   * Bounded top-{@code k} sorted scan, fused with collection — the R2 "heap over zone-map-pruned
   * leaves" shape: once the heap is full, a leaf whose FIRST order key's zone bounds cannot beat the
   * current worst kept row is skipped without evaluating its mask at all. Sound because entering the
   * heap requires beating the worst row, the first key is compared first, and the prune only fires
   * when the leaf's every row is STRICTLY worse on it — ties fall through to full evaluation. The
   * prune additionally requires the leaf's order columns to be all-present: a skipped leaf must not
   * be able to hide a matching row with a missing order key, which obliges the caller to decline
   * outright.
   *
   * @return record keys of the first {@code k} rows of the full stable sort, in emission order — or
   *         {@code null} when a matching row misses an order key, which only the generic pipeline can
   *         place correctly
   */
  public static long @Nullable [] topKRecordKeys(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int[] sortColumns, final boolean[] descending, final int k, final ColumnSegmentFetcher fetcher) {
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
        rank += leafRows; // ranks only order rows WITHIN the kept set; skipped rows never enter
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
            return null; // a matching row without an order key — the interpreter places it
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
          } else if (compareCandidate(candidate, rowRank, heapTuple, heapRank, 0, keyCount, descending) < 0) {
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
   * Whether every row of {@code leaf} is STRICTLY worse than the worst kept row on the first order
   * key, with the leaf's order columns provably all-present. Zone truth: a slice's min/max fold only
   * present values, and the all-present check makes them row-complete.
   */
  private static boolean leafZonePrunable(final ColumnSlice[][] sortCols, final int leaf, final int leafRows,
      final boolean descendingFirst, final long[] heapTuple) {
    final int presWords = (leafRows + 63) >>> 6;
    for (final ColumnSlice[] col : sortCols) {
      final long[] presence = col[leaf].presenceWords();
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
    final ColumnSlice first = sortCols[0][leaf];
    final long worstFirstKey = heapTuple[0]; // root row's first key sits at tuple offset 0
    return descendingFirst
        ? first.max() < worstFirstKey
        : first.min() > worstFirstKey;
  }

  /** Compare a candidate row against heap slot {@code slot} under the sort's total order. */
  private static int compareCandidate(final long[] candidate, final long candidateRank, final long[] heapTuple,
      final long[] heapRank, final int slot, final int keyCount, final boolean[] descending) {
    final int base = slot * keyCount;
    for (int k = 0; k < keyCount; k++) {
      final int cmp = Long.compare(candidate[k], heapTuple[base + k]);
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
      final int keyCount, final boolean[] descending) {
    final int ba = a * keyCount;
    final int bb = b * keyCount;
    for (int k = 0; k < keyCount; k++) {
      final int cmp = Long.compare(heapTuple[ba + k], heapTuple[bb + k]);
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
      final int start, final int size, final int keyCount, final boolean[] descending) {
    int i = start;
    final int half = size >>> 1;
    while (i < half) {
      int child = (i << 1) + 1;
      final int right = child + 1;
      if (right < size && compareHeapRows(heapTuple, heapRank, right, child, keyCount, descending) > 0) {
        child = right;
      }
      if (compareHeapRows(heapTuple, heapRank, child, i, keyCount, descending) <= 0) {
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

  /** Resolve + validate the order columns: NUMERIC_LONG slices, one per key. */
  private static ColumnSlice[][] resolveSortColumns(final ProjectionColumnStore store, final int[] sortColumns,
      final ColumnSegmentFetcher fetcher) {
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
      final boolean stringColumn = columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
          || columnKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET;
      if (stringColumn) {
        // The one string shape these kernels serve, mirroring the byte kernel's evaluator.
        if (p.stringLitBytes == null || p.op != ProjectionIndexScan.Op.EQ) {
          throw new IllegalStateException("String column " + p.column + " only serves equality with a string literal");
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
      cols[i] = keep == null
          ? store.column(predicates[i].column, fetcher)
          : store.columnMasked(predicates[i].column, fetcher, keep);
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
      if (kind == ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG && p.stringLitBytes == null) {
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
  private static int evaluateMask(final ColumnPredicate[] predicates, final ColumnSlice[][] cols, final int leaf,
      final int rowCount, final long[] mask) {
    if (rowCount <= 0) {
      return 0;
    }
    // A predicate slice with no rows on a leaf the KEYS chain says has rows is a PRUNED slice
    // (descriptor zone or string fingerprint proved no row can match) — the conjunction is
    // false for the whole leaf. Checked before any evaluator so the pruned sentinel's absent
    // typed arrays are never touched.
    for (int i = 0; i < predicates.length; i++) {
      if (cols[i][leaf].rowCount() <= 0) {
        return 0;
      }
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
      } else if (slice.setCounts() != null) {
        evalStringSetContains(slice.stringDict(), slice.setCounts(), slice.stringDictIds(), rows, p.stringLitBytes,
            presence, mask);
      } else if (slice.stringDictIds() != null) {
        evalStringEq(slice.stringDict(), slice.stringDictIds(), rows, p.stringLitBytes, presence, mask);
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
  private static void evalStringSetContains(final byte[][] dict, final int[] counts, final int[] elems,
      final int rowCount, final byte[] literal, final long[] presence, final long[] mask) {
    final int stride = (rowCount + 63) >>> 6;
    int targetDictId = -1;
    for (int i = 0; i < dict.length && dict[i] != null; i++) {
      if (Arrays.equals(dict[i], literal)) {
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

  private static void evalStringEq(final byte[][] dict, final int[] ids, final int rowCount, final byte[] literal,
      final long[] presence, final long[] mask) {
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
      final long m = mask[w] & presence[w];
      if (m == 0L) {
        mask[w] = 0L;
        continue;
      }
      final int rowBase = w << 6;
      // Same dispatch as evalNumeric, on int lanes: the branch-free id compare (measured 8x
      // over the walk on dense words — see ProjectionVectorKernels.equalsIdWord) serves full
      // 64-id windows; the tail word takes the guarded walk below.
      if (rowBase + 64 <= rowCount) {
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
        if (ids[rowIdx] == targetDictId) {
          out |= 1L << bit;
        }
      }
      mask[w] = out;
    }
  }
}
