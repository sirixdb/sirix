/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.StringValueExtrema;
import io.sirix.index.projection.ProjectionColumnStore.ZoneIndex;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrays;
import net.openhft.hashing.LongTupleHashFunction;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.IntStream;

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

  /**
   * Leaves a predicate TREE's keep mask dropped (zone / fingerprint evidence combined by the tree's
   * program). Observability: a tree route that silently stops pruning answers identically and only
   * this count tells it from one that never pruned.
   */
  private static final LongAdder TREE_LEAVES_PRUNED = new LongAdder();

  /**
   * Leaves {@link #pruneLeaves} dropped on store evidence (zone or fingerprint) for a conjunctive
   * predicate, before any leaf was fetched. Test observability: a kind that "has zones" proves
   * nothing until a predicate over it is seen to drop leaves here.
   */
  private static final LongAdder LEAVES_PRUNED = new LongAdder();

  /** Test/ops observability for {@link #LEAVES_PRUNED}. */
  public static long leavesPrunedCount() {
    return LEAVES_PRUNED.sum();
  }

  /** Test/ops observability for {@link #TREE_LEAVES_PRUNED}. */
  public static long treeLeavesPrunedCount() {
    return TREE_LEAVES_PRUNED.sum();
  }

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
    return predicateKeepMask(store, predicates, null, fetcher);
  }

  /**
   * The keep mask for BOTH predicate shapes a scan can carry: the conjunctive {@code predicates}
   * narrow the mask predicate by predicate, and a {@code tree} contributes the leaf set its program
   * can still reach — every tree leaf's evidence is gathered on its own (a fresh all-kept mask, the
   * same zone / fingerprint rules), then combined by the program: AND intersects, OR unites, NOT keeps
   * every leaf (the operand's evidence bounds where the operand matches and says nothing about its
   * negation). A tree leaf whose evidence drops nothing therefore contributes an all-kept operand, so
   * an OR over one evidence-less leaf keeps everything — degrading to the unpruned fill, never past
   * it. A {@code (CounterID = c AND (src = a OR src = b))} tree over a CounterID-sorted table prunes
   * to CounterID's leaves exactly as the flat conjunction would. {@code null} = nothing pruned.
   */
  public static long @Nullable [] predicateKeepMask(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ProjectionIndexScan.@Nullable PredicateTree tree,
      final ColumnSegmentFetcher fetcher) {
    checkPredicates(store, predicates);
    long[] keep = computeKeepMask(store, predicates, fetcher);
    if (tree != null) {
      checkPredicates(store, tree.leaves);
      final long[] treeKeep = computeTreeKeepMask(store, tree, fetcher);
      if (treeKeep != null) {
        TREE_LEAVES_PRUNED.add(store.leafCount() - cardinality(treeKeep));
        if (keep == null) {
          keep = treeKeep;
        } else {
          for (int i = 0; i < keep.length; i++) {
            keep[i] &= treeKeep[i];
          }
        }
      }
    }
    if (DIAG) {
      printKeepMask(store, keep);
    }
    return keep;
  }

  /**
   * Masked views of every tree leaf's column, index-aligned with {@code tree.leaves}: a leaf the mask
   * drops is the pruned sentinel in EVERY tree column, which {@link #evaluateMaskTree} answers as "no
   * rows" without touching a slice. {@code keep == null} = the plain (cached) fills.
   */
  public static ColumnSlice[][] resolveTreeColumnsShared(final ProjectionColumnStore store,
      final ProjectionIndexScan.PredicateTree tree, final ColumnSegmentFetcher fetcher, final long @Nullable [] keep) {
    checkPredicates(store, tree.leaves);
    final ColumnSlice[][] cols = new ColumnSlice[tree.leaves.length][];
    for (int i = 0; i < tree.leaves.length; i++) {
      // A masked fill is predicate-specific and never cached, so a column two leaves share (a
      // BETWEEN's pair, an IN's alternatives) is resolved once and its immutable array shared.
      final int column = tree.leaves[i].column;
      ColumnSlice[] shared = null;
      for (int j = 0; j < i; j++) {
        if (tree.leaves[j].column == column) {
          shared = cols[j];
          break;
        }
      }
      cols[i] = shared != null
          ? shared
          : store.columnMaskedView(column, fetcher, keep);
    }
    return cols;
  }

  /** Whether every leaf in {@code [fromLeaf, toLeaf)} is dropped by {@code keep} — a morsel a pass can skip whole. */
  public static boolean allPruned(final long @Nullable [] keep, final int fromLeaf, final int toLeaf) {
    if (keep == null || fromLeaf >= toLeaf) {
      return false;
    }
    int i = fromLeaf;
    if ((i & 63) != 0) {
      final int wordEnd = Math.min(toLeaf, (i | 63) + 1);
      final long lowBits = -1L << (i & 63);
      final long highBits = (wordEnd & 63) == 0
          ? -1L
          : (1L << (wordEnd & 63)) - 1;
      if ((keep[i >>> 6] & lowBits & highBits) != 0L) {
        return false;
      }
      i = wordEnd;
    }
    for (; i + 64 <= toLeaf; i += 64) {
      if (keep[i >>> 6] != 0L) {
        return false;
      }
    }
    if (i < toLeaf) {
      final long tail = (1L << (toLeaf - i)) - 1;
      if ((keep[i >>> 6] & tail) != 0L) {
        return false;
      }
    }
    return true;
  }

  /** The {@code [prune]} line: how many leaves survive the evidence, for both fill routes. */
  private static void printKeepMask(final ProjectionColumnStore store, final long @Nullable [] keep) {
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
   * Bounded top-{@code k} sorted scan, fused with collection: the {@code k} best rows under the order
   * keys (per-key direction, then document rank — exactly the full stable sort's prefix), emitted as
   * record keys. Never fills a column for itself: whatever the store already holds resident is served
   * from it, everything else is decoded for exactly the leaves the scan visits, in one batched fetch
   * per column per slab ({@link ProjectionColumnStore#leafSetAccess}). A {@code LIMIT 10} over a fat
   * string column at 100M rows therefore touches tens to thousands of leaves, never the column.
   *
   * <p>
   * <b>The plan comes from descriptor and memo truth — zero leaf decodes.</b> Each leaf's BEST possible
   * first key is its zone bound for a numeric kind ({@link ProjectionColumnStore#zoneIndex}) and its
   * smallest / largest referenced dictionary VALUE for a per-leaf string kind
   * ({@link ProjectionColumnStore#stringValueExtrema} — dict ids are meaningless for value order). A
   * {@code <>} predicate on the first key that names that extremum moves the bound to the SECOND
   * distinct extremum (q25's shape, {@code WHERE SearchPhrase <> '' ORDER BY SearchPhrase}: the empty
   * string is every leaf's minimum, so without the refinement every leaf ties and nothing can ever
   * be skipped); a leaf without a second one holds no matching row at all and is dropped. A bound is
   * USABLE only when every matching row of the leaf is guaranteed to carry every order key: the
   * column is all-present on the leaf ({@link ProjectionColumnStore#allPresentLeaves}) or a
   * predicate names it (every predicate op is missing ⇒ false). Otherwise the leaf may hide a
   * matching row with an empty key — which the interpreter places by the empty-least/greatest mode
   * and this scan can only answer by declining — so it is visited unconditionally, and FIRST, so the
   * heap it fills bounds every leaf after it.
   *
   * <p>
   * <b>Best-first, chunked, parallel.</b> Leaves with a usable bound follow, best-first, in doubling
   * chunks (1, 2, 4, …, {@value #TOPK_CHUNK_MAX}); each chunk is split into contiguous slabs evaluated
   * in parallel, one {@link TopKHeap} per slab, merged into the global heap after the chunk. A leaf
   * whose bound is strictly worse than the worst row of a FULL heap — the global one, frozen for the
   * chunk, or the slab's own — is skipped without a decode; the walk STOPS at the first chunk whose
   * next leaf is skippable, because every leaf after it is ordered no better. Ties on the first key
   * fall through to evaluation, so the selection is exact; ranks are the leaf's document-order base
   * plus the row index, so the total order never depends on visitation. When every leaf offers the
   * same best key the reordering cannot skip anything and the leaves are walked in document order.
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
    final long tStart = DIAG
        ? System.nanoTime()
        : 0L;
    validateSortColumns(store, sortColumns);
    if (k <= 0) {
      return new long[0];
    }
    final int keyCount = sortColumns.length;
    // Zone-map pruning from the descriptors alone; a dropped leaf carries the PRUNED sentinel in every
    // predicate slice and is never visited.
    final long[] keep = computeKeepMask(store, predicates, fetcher);
    // Residency is OBSERVED, never created: a positive answer pins the lane for this query and the
    // slab accesses serve it from the retained arrays; everything else is decoded per slab. The
    // witness counts the scan as non-retaining exactly when it had to decode something itself.
    boolean resident = store.recordKeysFilled();
    for (int kk = 0; kk < keyCount; kk++) {
      resident &= store.columnFilled(sortColumns[kk]);
    }
    for (final ColumnPredicate p : predicates) {
      resident &= store.columnFilled(p.column);
    }
    if (!resident) {
      ProjectionColumnStore.noteWindowedLeafAccess();
    }
    final int leafCount = store.rowGroupCount();
    final int[] leafRows = new int[leafCount];
    // Document-order rank base per leaf: the tiebreak must stay document order no matter which order
    // the leaves are VISITED in, so it can never be a running counter here.
    final long[] leafRankBase = new long[leafCount];
    long rankAcc = 0L;
    for (int leaf = 0; leaf < leafCount; leaf++) {
      leafRows[leaf] = store.rowCount(leaf);
      leafRankBase[leaf] = rankAcc;
      rankAcc += leafRows[leaf];
    }
    final long tPlan0 = DIAG
        ? System.nanoTime()
        : 0L;
    // Built BEFORE keyKind: the extrema walk over the first key's dictionaries is what establishes
    // that column's collation verdict, and a separate sweep for it would cost more than it saves.
    final TopKPlan plan = planTopK(store, predicates, sortColumns, descending, keep, leafRows, fetcher);
    final byte[] keyKind = new byte[keyCount];
    for (int kk = 0; kk < keyCount; kk++) {
      keyKind[kk] = switch (store.columnKind(sortColumns[kk])) {
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT ->
          store.stringDictSupplementaryMemo(sortColumns[kk]) == ProjectionColumnStore.SUPPLEMENTARY_NONE
              ? TopKHeap.KEY_STRING_BYTES
              : TopKHeap.KEY_STRING_COLLATED;
        case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL -> TopKHeap.KEY_STRING_GLOBAL;
        default -> TopKHeap.KEY_NUMERIC;
      };
    }
    plan.orderVisit(keyKind[0]);
    final long tPlan1 = DIAG
        ? System.nanoTime()
        : 0L;
    // Slabs decode independently only when the fetcher tolerates concurrent ranged fetches; a plain
    // fetcher gets the same chunks, slabs and counters on the calling thread.
    final int workers = fetcher.rangedFetchIsConcurrent()
        ? TOPK_WORKERS
        : 1;
    final TopKRun run = new TopKRun(store, predicates, sortColumns, keyKind, descending, globalSortViews, k, keep,
        leafRows, leafRankBase, plan, fetcher, workers);
    final TopKHeap global = run.global;
    final int visitCount = plan.visitCount;
    int v = 0;
    int chunk = 1;
    int chunks = 0;
    while (v < visitCount) {
      final int from = v;
      final int to = Math.min(visitCount, v + chunk);
      final int len = to - from;
      final int slabs = Math.min(workers, Math.max(1, len >>> 1));
      chunks++;
      if (slabs == 1) {
        run.evaluateSlab(0, from, to);
      } else {
        final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        IntStream.range(0, slabs).parallel().forEach(slab -> {
          try {
            run.evaluateSlab(slab, from + (int) ((long) len * slab / slabs), from + (int) ((long) len * (slab + 1) / slabs));
          } catch (final RuntimeException e) {
            failure.compareAndSet(null, e);
          }
        });
        final RuntimeException failed = failure.get();
        if (failed != null) {
          throw failed;
        }
      }
      if (run.declined.get()) {
        return null; // a matching row without an order key — the interpreter places it
      }
      run.mergeLocals(slabs);
      v = to;
      if (plan.sorted && v < visitCount && global.full() && plan.strictlyWorse(global, plan.visit[v])) {
        // Known leaves are ordered best-first on key 0: the first whose BEST possible key is strictly
        // worse than the worst kept row proves the same of every leaf after it.
        TOPK_LEAVES_SKIPPED.add(visitCount - v);
        run.skipped.add(visitCount - v);
        break;
      }
      chunk = Math.min(chunk << 1, TOPK_CHUNK_MAX);
    }
    if (DIAG) {
      final long tEnd = System.nanoTime();
      System.err.printf(
          "[topk] k=%d keys=%d preds=%d order=%s leaves=%d visit=%d unknown=%d plan=%.2fms eval=%.2fms chunks=%d "
              + "evaluated=%d skipped=%d cand=%d workers=%d resident=%b%n",
          k, keyCount, predicates.length, plan.sorted
              ? "best-first"
              : "document", leafCount, visitCount, plan.unknownCount, (tPlan1 - tPlan0) / 1e6, (tEnd - tPlan1) / 1e6,
          chunks, run.evaluated.sum(), run.skipped.sum(), run.candidates.sum(), workers, resident);
    }
    return global.sortedRecordKeys();
  }

  /** Upper bound on the leaves one chunk of {@link #topKRecordKeys} evaluates before re-checking the stop rule. */
  static final int TOPK_CHUNK_MAX = 4096;

  /** Slabs per chunk of {@link #topKRecordKeys} when the fetcher tolerates concurrent ranged fetches. */
  private static final int TOPK_WORKERS =
      Math.max(1, Integer.getInteger("sirix.topK.workers", Runtime.getRuntime().availableProcessors()));

  /**
   * The visitation plan of one {@link #topKRecordKeys}: which leaves to visit, in which order, and
   * each leaf's usable lower bound on the first key ({@link #strictlyWorse}). Built from the store's
   * descriptors and memos alone.
   */
  private static final class TopKPlan {
    /** Leaves to visit: the unknown-bound ones first in leaf order, then the known ones (sorted by {@link #orderVisit}). */
    final int[] visit;
    final int visitCount;
    final int unknownCount;
    /** Per leaf: {@code -1} no usable bound; for a numeric first key {@code 0}; for a string one the extrema slot. */
    private final byte[] lbSlot;
    private final long @Nullable [] lbNumeric;
    private final @Nullable StringValueExtrema extrema;
    private final boolean descendingFirst;
    /** Whether the known leaves are ordered best-first (and the stop rule applies). */
    boolean sorted;

    TopKPlan(final int[] visit, final int visitCount, final int unknownCount, final byte[] lbSlot,
        final long @Nullable [] lbNumeric, final @Nullable StringValueExtrema extrema,
        final boolean descendingFirst) {
      this.visit = visit;
      this.visitCount = visitCount;
      this.unknownCount = unknownCount;
      this.lbSlot = lbSlot;
      this.lbNumeric = lbNumeric;
      this.extrema = extrema;
      this.descendingFirst = descendingFirst;
    }

    boolean known(final int leaf) {
      return lbSlot[leaf] >= 0;
    }

    /** Whether {@code leaf}'s every row is strictly worse on the first key than the worst row of the FULL {@code heap}. */
    boolean strictlyWorse(final TopKHeap heap, final int leaf) {
      final int slot = lbSlot[leaf];
      if (slot < 0) {
        return false;
      }
      if (extrema == null) {
        return heap.firstKeyStrictlyWorse(lbNumeric[leaf]);
      }
      return heap.firstKeyStrictlyWorse(extrema.bytes(), extrema.offset(leaf, slot), extrema.length(leaf, slot));
    }

    /**
     * Order the known leaves best-first on the first key (ties in leaf order), or leave the whole
     * visit list in document order when every known leaf offers the same best — then no leaf can ever
     * be skipped and the reordering would only cost locality — or when the escape hatch asks for it.
     */
    void orderVisit(final byte firstKeyKind) {
      final int knownCount = visitCount - unknownCount;
      final boolean tied = visitCount < 2 || (unknownCount == 0 && allKnownBestsEqual(firstKeyKind));
      if (tied && visitCount >= 2) {
        TOPK_PLAN_TIED.increment();
      }
      sorted = !TOPK_DOC_ORDER && !tied;
      if (!sorted) {
        // Document order over the admitted leaves — unknown and known interleaved as stored.
        IntArrays.quickSort(visit, 0, visitCount);
        return;
      }
      if (knownCount < 2) {
        return; // nothing to order; the unknown leaves already lead
      }
      final int from = unknownCount;
      if (extrema == null) {
        sortKnownNumeric(from, knownCount);
      } else if (firstKeyKind == TopKHeap.KEY_STRING_BYTES) {
        sortKnownStringBytes(from, knownCount);
      } else {
        sortKnownStringCollated(from, knownCount);
      }
    }

    private boolean allKnownBestsEqual(final byte firstKeyKind) {
      final int first = visit[unknownCount];
      if (extrema == null) {
        final long best = lbNumeric[first];
        for (int i = unknownCount + 1; i < visitCount; i++) {
          if (lbNumeric[visit[i]] != best) {
            return false;
          }
        }
        return true;
      }
      final byte[] bytes = extrema.bytes();
      final int off = extrema.offset(first, lbSlot[first]);
      final int len = extrema.length(first, lbSlot[first]);
      for (int i = unknownCount + 1; i < visitCount; i++) {
        final int leaf = visit[i];
        final int o = extrema.offset(leaf, lbSlot[leaf]);
        final int l = extrema.length(leaf, lbSlot[leaf]);
        if (firstKeyKind == TopKHeap.KEY_STRING_COLLATED
            ? ProjectionIndexByteScan.compareStrSlices(bytes, off, len, bytes, o, l) != 0
            : !Arrays.equals(bytes, off, off + len, bytes, o, o + l)) {
          return false;
        }
      }
      return true;
    }

    /** Stable radix sort of the known leaves by their numeric best — ascending on {@code ~best} for a descending key. */
    private void sortKnownNumeric(final int from, final int n) {
      final long[] keys = new long[n];
      final int[] perm = new int[n];
      for (int i = 0; i < n; i++) {
        final long best = lbNumeric[visit[from + i]];
        keys[i] = descendingFirst
            ? ~best
            : best;
        perm[i] = i;
      }
      LongArrays.radixSortIndirect(perm, keys, true);
      applyPermutation(from, n, perm);
    }

    /**
     * Known leaves by their string best under unsigned byte order: a stable radix sort on the first
     * eight bytes (zero-padded — consistent with the lexicographic order, a shorter string never sorts
     * after a longer one sharing its prefix), then every run of equal prefixes refined by the full
     * comparison. Three orders of magnitude fewer byte comparisons than a merge sort at 100k leaves.
     */
    private void sortKnownStringBytes(final int from, final int n) {
      final byte[] bytes = extrema.bytes();
      final long[] keys = new long[n];
      final int[] perm = new int[n];
      for (int i = 0; i < n; i++) {
        final int leaf = visit[from + i];
        final int slot = lbSlot[leaf];
        final long prefix = unsignedPrefix(bytes, extrema.offset(leaf, slot), extrema.length(leaf, slot));
        keys[i] = descendingFirst
            ? ~prefix
            : prefix;
        perm[i] = i;
      }
      LongArrays.radixSortIndirect(perm, keys, true);
      int runStart = 0;
      for (int i = 1; i <= n; i++) {
        if (i == n || keys[perm[i]] != keys[perm[runStart]]) {
          if (i - runStart > 1) {
            IntArrays.mergeSort(perm, runStart, i, (a, b) -> compareKnownBytes(visit[from + a], visit[from + b]));
          }
          runStart = i;
        }
      }
      applyPermutation(from, n, perm);
    }

    private void sortKnownStringCollated(final int from, final int n) {
      final int[] perm = new int[n];
      for (int i = 0; i < n; i++) {
        perm[i] = i;
      }
      IntArrays.mergeSort(perm, 0, n, (a, b) -> {
        final int la = visit[from + a];
        final int lb = visit[from + b];
        final byte[] bytes = extrema.bytes();
        final int cmp = ProjectionIndexByteScan.compareStrSlices(bytes, extrema.offset(la, lbSlot[la]),
            extrema.length(la, lbSlot[la]), bytes, extrema.offset(lb, lbSlot[lb]), extrema.length(lb, lbSlot[lb]));
        if (cmp != 0) {
          return descendingFirst
              ? -cmp
              : cmp;
        }
        return Integer.compare(la, lb);
      });
      applyPermutation(from, n, perm);
    }

    private int compareKnownBytes(final int la, final int lb) {
      final byte[] bytes = extrema.bytes();
      final int oa = extrema.offset(la, lbSlot[la]);
      final int ob = extrema.offset(lb, lbSlot[lb]);
      final int cmp = Arrays.compareUnsigned(bytes, oa, oa + extrema.length(la, lbSlot[la]), bytes, ob,
          ob + extrema.length(lb, lbSlot[lb]));
      if (cmp != 0) {
        return descendingFirst
            ? -cmp
            : cmp;
      }
      return Integer.compare(la, lb);
    }

    private void applyPermutation(final int from, final int n, final int[] perm) {
      final int[] sorted = new int[n];
      for (int i = 0; i < n; i++) {
        sorted[i] = visit[from + perm[i]];
      }
      System.arraycopy(sorted, 0, visit, from, n);
    }

    /** The first eight bytes as a big-endian long, zero-padded, biased so signed order is unsigned order. */
    private static long unsignedPrefix(final byte[] bytes, final int off, final int len) {
      long prefix = 0L;
      final int n = Math.min(Long.BYTES, len);
      for (int i = 0; i < n; i++) {
        prefix = (prefix << 8) | (bytes[off + i] & 0xFFL);
      }
      prefix <<= 8 * (Long.BYTES - n);
      return prefix ^ Long.MIN_VALUE;
    }
  }

  /**
   * Build the {@link TopKPlan}: admitted leaves (kept by the zone mask, rowful, not proven empty by a
   * {@code <>} on the first key), each with its usable bound. Reads descriptors and memos only.
   */
  private static TopKPlan planTopK(final ProjectionColumnStore store, final ColumnPredicate[] predicates,
      final int[] sortColumns, final boolean[] descending, final long @Nullable [] keep, final int[] leafRows,
      final ColumnSegmentFetcher fetcher) {
    final int leafCount = leafRows.length;
    final int keyCount = sortColumns.length;
    final int first = sortColumns[0];
    final byte firstKind = store.columnKind(first);
    final boolean desc = descending[0];
    final byte[] lbSlot = new byte[leafCount];
    Arrays.fill(lbSlot, (byte) -1);
    final boolean[] dropped = new boolean[leafCount];
    long[] lbNumeric = null;
    StringValueExtrema extrema = null;
    if (firstKind != ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL) {
      if (firstKind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
        extrema = store.stringValueExtrema(first, fetcher);
        final int slot1 = desc
            ? StringValueExtrema.MAX1
            : StringValueExtrema.MIN1;
        final int slot2 = desc
            ? StringValueExtrema.MAX2
            : StringValueExtrema.MIN2;
        final byte[] bytes = extrema.bytes();
        for (int leaf = 0; leaf < leafCount; leaf++) {
          if (leafRows[leaf] <= 0 || !extrema.has(leaf, slot1)) {
            continue;
          }
          int slot = slot1;
          for (final ColumnPredicate p : predicates) {
            if (p.column != first || p.op != ProjectionIndexScan.Op.NE || slot != slot1) {
              continue;
            }
            final int off = extrema.offset(leaf, slot);
            final int len = extrema.length(leaf, slot);
            if (Arrays.equals(bytes, off, off + len, p.stringLitBytes, 0, p.stringLitBytes.length)) {
              // The extremum itself is excluded: the second distinct value bounds what remains. Once
              // on the second slot the bound stays sound whatever else is excluded (the remaining
              // values are beyond it), so the refinement runs once.
              if (extrema.has(leaf, slot2)) {
                slot = slot2;
              } else {
                dropped[leaf] = true; // its only present value is excluded; missing cells never match
                break;
              }
            }
          }
          if (!dropped[leaf]) {
            lbSlot[leaf] = (byte) slot;
          }
        }
      } else {
        final ZoneIndex zone = store.zoneIndex(first);
        lbNumeric = new long[leafCount];
        for (int leaf = 0; leaf < leafCount; leaf++) {
          if (leafRows[leaf] > 0 && zone.known(leaf) && !zone.allMissing(leaf)) {
            lbNumeric[leaf] = desc
                ? zone.max(leaf)
                : zone.min(leaf);
            lbSlot[leaf] = 0;
          }
        }
      }
      // A bound is usable only where every matching row is guaranteed to carry every order key: a
      // predicate on the column (missing ⇒ false) or the column all-present on the leaf.
      for (int kk = 0; kk < keyCount; kk++) {
        if (predicateNames(predicates, sortColumns[kk])) {
          continue;
        }
        final long[] allPresent = store.allPresentLeaves(sortColumns[kk], fetcher);
        for (int leaf = 0; leaf < leafCount; leaf++) {
          if (lbSlot[leaf] >= 0 && (allPresent[leaf >>> 6] & (1L << (leaf & 63))) == 0L) {
            lbSlot[leaf] = -1;
          }
        }
      }
    }
    // Admitted leaves: unknown bounds first (in leaf order), then the known ones (in leaf order until
    // orderVisit sorts them). Rowless leaves contribute nothing and have no extremum to order on.
    final int[] visit = new int[leafCount];
    int unknownCount = 0;
    int knownCount = 0;
    for (int leaf = 0; leaf < leafCount; leaf++) {
      if (leafRows[leaf] <= 0 || dropped[leaf] || (keep != null && (keep[leaf >>> 6] & (1L << (leaf & 63))) == 0L)) {
        continue;
      }
      if (lbSlot[leaf] < 0) {
        unknownCount++;
      } else {
        knownCount++;
      }
    }
    int u = 0;
    int w = unknownCount;
    for (int leaf = 0; leaf < leafCount; leaf++) {
      if (leafRows[leaf] <= 0 || dropped[leaf] || (keep != null && (keep[leaf >>> 6] & (1L << (leaf & 63))) == 0L)) {
        continue;
      }
      if (lbSlot[leaf] < 0) {
        visit[u++] = leaf;
      } else {
        visit[w++] = leaf;
      }
    }
    return new TopKPlan(visit, unknownCount + knownCount, unknownCount, lbSlot, lbNumeric, extrema, desc);
  }

  private static boolean predicateNames(final ColumnPredicate[] predicates, final int column) {
    for (final ColumnPredicate p : predicates) {
      if (p.column == column) {
        return true;
      }
    }
    return false;
  }

  /** The shared state of one {@link #topKRecordKeys} evaluation: the global heap, one local heap per slab, counters. */
  private static final class TopKRun {
    private final ProjectionColumnStore store;
    private final ColumnPredicate[] predicates;
    private final int[] sortColumns;
    private final int keyCount;
    private final long @Nullable [] keep;
    private final int[] leafRows;
    private final long[] leafRankBase;
    private final TopKPlan plan;
    private final ColumnSegmentFetcher fetcher;
    final TopKHeap global;
    private final TopKHeap[] locals;
    private final int k;
    private final byte[] keyKind;
    private final boolean[] descending;
    private final GlobalValueDictionary.ReadView[] globalSortViews;
    final AtomicBoolean declined = new AtomicBoolean();
    final LongAdder evaluated = new LongAdder();
    final LongAdder skipped = new LongAdder();
    final LongAdder candidates = new LongAdder();

    TopKRun(final ProjectionColumnStore store, final ColumnPredicate[] predicates, final int[] sortColumns,
        final byte[] keyKind, final boolean[] descending, final GlobalValueDictionary.ReadView[] globalSortViews,
        final int k, final long @Nullable [] keep, final int[] leafRows, final long[] leafRankBase, final TopKPlan plan,
        final ColumnSegmentFetcher fetcher, final int workers) {
      this.store = store;
      this.predicates = predicates;
      this.sortColumns = sortColumns;
      this.keyCount = sortColumns.length;
      this.keyKind = keyKind;
      this.descending = descending;
      this.globalSortViews = globalSortViews;
      this.k = k;
      this.keep = keep;
      this.leafRows = leafRows;
      this.leafRankBase = leafRankBase;
      this.plan = plan;
      this.fetcher = fetcher;
      this.global = new TopKHeap(k, keyKind, descending, globalSortViews);
      this.locals = new TopKHeap[workers];
    }

    /** Fold the slabs' heaps of the chunk just evaluated into the global one (calling thread only). */
    void mergeLocals(final int slabs) {
      for (int s = 0; s < slabs; s++) {
        final TopKHeap local = locals[s];
        if (local != null && local.size() > 0) {
          global.mergeFrom(local);
          local.clear();
        }
      }
    }

    /**
     * Evaluate {@code plan.visit[from..to)} on slab {@code slab}: skip what the (frozen) global heap
     * already rules out, decode the rest through ONE leaf-set access, offer every matching row to the
     * slab's heap, and skip further leaves the slab's own full heap rules out.
     */
    void evaluateSlab(final int slab, final int from, final int to) {
      TopKHeap local = locals[slab];
      if (local == null) {
        local = new TopKHeap(k, keyKind, descending, globalSortViews);
        locals[slab] = local;
      }
      final int[] set = new int[to - from];
      int n = 0;
      final boolean globalFull = global.full();
      for (int i = from; i < to; i++) {
        final int leaf = plan.visit[i];
        if (globalFull && plan.strictlyWorse(global, leaf)) {
          TOPK_LEAVES_SKIPPED.increment();
          skipped.increment();
          continue;
        }
        set[n++] = leaf;
      }
      if (n == 0) {
        return;
      }
      final int[] ascending = Arrays.copyOf(set, n);
      IntArrays.quickSort(ascending);
      final ProjectionColumnStore.LeafColumnAccess access = store.leafSetAccess(fetcher, keep, ascending, 0, n);
      final Scratch s = SCRATCH.get();
      final ColumnSlice[] predicateSlices = s.leafPredicateSlices(predicates.length);
      final ColumnSlice[] leafSort = s.leafSortSlices(keyCount);
      long cand = 0L;
      int visited = 0;
      for (int i = 0; i < n; i++) {
        final int leaf = set[i]; // best-first within the slab, so the local heap bounds early
        if (local.full() && plan.strictlyWorse(local, leaf)) {
          TOPK_LEAVES_SKIPPED.increment();
          skipped.increment();
          continue;
        }
        if (declined.get()) {
          break;
        }
        final int rows = leafRows[leaf];
        visited++;
        if (evaluateMask(predicates, access, leaf, rows, s.mask, predicateSlices) <= 0) {
          continue;
        }
        final long[] keys = access.recordKeys(leaf);
        for (int kk = 0; kk < keyCount; kk++) {
          leafSort[kk] = access.slice(sortColumns[kk], leaf);
        }
        final long rank = leafRankBase[leaf];
        final int stride = (rows + 63) >>> 6;
        for (int w = 0; w < stride; w++) {
          long word = s.mask[w];
          if (word == 0L) {
            continue;
          }
          long presAll = -1L;
          for (int kk = 0; kk < keyCount; kk++) {
            presAll &= leafSort[kk].presenceWords()[w];
          }
          if ((word & ~presAll) != 0L) {
            declined.set(true); // a matching row without an order key — the interpreter places it
            evaluated.add(visited);
            candidates.add(cand);
            return;
          }
          while (word != 0L) {
            final int rowIdx = (w << 6) + Long.numberOfTrailingZeros(word);
            word &= word - 1L;
            local.offer(leafSort, rowIdx, keys[rowIdx], rank + rowIdx);
            cand++;
          }
        }
      }
      evaluated.add(visited);
      candidates.add(cand);
    }
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
      printKeepMask(store, keep);
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
    final long[] keep = allKept(n);
    boolean dropped = false;
    for (final ColumnPredicate p : predicates) {
      dropped |= pruneLeaves(store, p, keep, fetcher) > 0;
    }
    return dropped
        ? keep
        : null;
  }

  /**
   * The tree's keep mask: its program run over per-leaf evidence masks. Each leaf predicate is priced
   * on a FRESH all-kept mask (an OR operand must not inherit its sibling's drops), AND intersects, OR
   * unites, NOT replaces its operand by all-kept. {@code null} = the program can still reach every leaf.
   */
  private static long @Nullable [] computeTreeKeepMask(final ProjectionColumnStore store,
      final ProjectionIndexScan.PredicateTree tree, final ColumnSegmentFetcher fetcher) {
    final int n = store.leafCount();
    if (n == 0) {
      return null;
    }
    final long[][] masks = leafEvidenceMasks(store, tree.leaves, fetcher);
    // The program owns every pushed array (AND/OR fold into the left operand in place), so a leaf
    // the program references more than once pushes a COPY on every reference — the first push
    // would otherwise be narrowed in place before the second one reads it.
    final int[] refs = new int[masks.length];
    int depth = 0;
    int maxDepth = 0;
    for (final byte insn : tree.program) {
      if (insn >= 0) {
        refs[insn]++;
        maxDepth = Math.max(maxDepth, ++depth);
      } else if (insn != ProjectionIndexScan.PredicateTree.OP_NOT) {
        depth--;
      }
    }
    // The stack is as deep as the PROGRAM gets (a leaf may be referenced more than once), which
    // PredicateTree.of bounds by MAX_LEAVES, not by the leaf count.
    final long[][] stack = new long[maxDepth][];
    int top = 0;
    for (final byte insn : tree.program) {
      if (insn >= 0) {
        stack[top++] = refs[insn] > 1
            ? masks[insn].clone()
            : masks[insn];
      } else if (insn == ProjectionIndexScan.PredicateTree.OP_AND) {
        final long[] b = stack[--top];
        final long[] a = stack[top - 1];
        for (int i = 0; i < a.length; i++) {
          a[i] &= b[i];
        }
      } else if (insn == ProjectionIndexScan.PredicateTree.OP_OR) {
        final long[] b = stack[--top];
        final long[] a = stack[top - 1];
        for (int i = 0; i < a.length; i++) {
          a[i] |= b[i];
        }
      } else {
        stack[top - 1] = allKept(n); // NOT: no evidence about where the negation cannot match
      }
    }
    final long[] result = stack[0];
    final long[] full = allKept(n);
    return Arrays.equals(result, full)
        ? null
        : result;
  }

  /**
   * One fresh evidence mask per leaf predicate — all-kept, narrowed by what the predicate PROVES.
   * String equalities on the same STRING_DICT column are priced in ONE fingerprint walk
   * ({@link ProjectionColumnStore#applyBloomPruneMany}): the walk is the whole cost of a bloom prune,
   * so an OR of k literals over one column costs one walk instead of k. Everything else goes through
   * {@link #pruneLeaves} one predicate at a time.
   */
  private static long[][] leafEvidenceMasks(final ProjectionColumnStore store, final ColumnPredicate[] leaves,
      final ColumnSegmentFetcher fetcher) {
    final int n = store.leafCount();
    final long[][] masks = new long[leaves.length][];
    for (int i = 0; i < leaves.length; i++) {
      masks[i] = allKept(n);
    }
    // Batch: per column, the string-EQ leaves not yet priced.
    final boolean[] done = new boolean[leaves.length];
    for (int i = 0; i < leaves.length; i++) {
      if (done[i]) {
        continue;
      }
      final ColumnPredicate first = leaves[i];
      if (!bloomPrunable(store, first)) {
        pruneLeaves(store, first, masks[i], fetcher);
        done[i] = true;
        continue;
      }
      int members = 0;
      for (int j = i; j < leaves.length; j++) {
        if (!done[j] && leaves[j].column == first.column && bloomPrunable(store, leaves[j])) {
          members++;
        }
      }
      if (members == 1) {
        pruneLeaves(store, first, masks[i], fetcher);
        done[i] = true;
        continue;
      }
      final long[] hashes = new long[members];
      final long[][] keeps = new long[members][];
      int m = 0;
      for (int j = i; j < leaves.length; j++) {
        if (!done[j] && leaves[j].column == first.column && bloomPrunable(store, leaves[j])) {
          hashes[m] = ProjectionIndexColumnSegmentCodec.bloomHash(leaves[j].stringLitBytes);
          keeps[m] = masks[j];
          m++;
          done[j] = true;
        }
      }
      final long dropped = store.applyBloomPruneMany(first.column, hashes, keeps, fetcher);
      if (DIAG) {
        System.err.println("[prune] col=" + first.column + " kind=" + store.columnKind(first.column) + " op=EQ bloom x"
            + members + " (one walk): dropped=" + dropped);
      }
    }
    return masks;
  }

  /** Whether {@link #pruneLeaves} would price {@code p} by string fingerprint (Op.EQ on STRING_DICT). */
  private static boolean bloomPrunable(final ProjectionColumnStore store, final ColumnPredicate p) {
    return p.stringLitBytes != null && p.op == ProjectionIndexScan.Op.EQ
        && store.columnKind(p.column) == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT;
  }

  /** The all-kept mask over {@code n} leaves (tail bits clear) — the starting point of every prune. */
  public static long[] allKeptMask(final int n) {
    if (n < 0) {
      throw new IllegalArgumentException("n < 0: " + n);
    }
    return allKept(n);
  }

  /**
   * Zone stabbing for MANY values of one long-lane column ({@link #zonePrunableKind}: ordered-long,
   * or global-string whose values are dictionary ids) in ONE pass over the memoized
   * {@link ProjectionColumnStore.ZoneIndex}: SET bit {@code leaf} in {@code keeps[j]} when the leaf
   * MAY hold {@code sortedValues[j]} — its descriptor range covers the value, or the range is
   * unknown (no evidence is never a proof of absence). Containment needs no value order, so a
   * global column's ids stab exactly whatever order they were minted in. A leaf whose every cell is missing sets
   * nothing. Values must be strictly ascending; masks are OR-ed into (callers pass zeroed masks for
   * a pure answer). Per leaf the work is two binary searches over the values plus the covered span,
   * so k values cost about one {@link #pruneLeaves} walk rather than k.
   *
   * @return the number of bits set
   */
  public static long zoneStabSorted(final ProjectionColumnStore store, final int col, final long[] sortedValues,
      final long[][] keeps) {
    if (store == null || sortedValues == null || keeps == null || sortedValues.length != keeps.length) {
      throw new IllegalArgumentException("store, values and masks must pair up");
    }
    if (!zonePrunableKind(store.columnKind(col))) {
      throw new IllegalArgumentException("column " + col + " has no long-lane zone (not ordered-long or global-string)");
    }
    final int n = store.leafCount();
    final int words = (n + 63) >>> 6;
    final int k = sortedValues.length;
    for (int j = 1; j < k; j++) {
      if (sortedValues[j - 1] >= sortedValues[j]) {
        throw new IllegalArgumentException("values must be strictly ascending");
      }
    }
    for (final long[] keep : keeps) {
      if (keep == null || keep.length < words) {
        throw new IllegalArgumentException("every mask must cover " + n + " leaves");
      }
    }
    if (k == 0 || n == 0) {
      return 0L;
    }
    final ProjectionColumnStore.ZoneIndex zone = store.zoneIndex(col);
    long set = 0L;
    for (int leaf = 0; leaf < n; leaf++) {
      final int word = leaf >>> 6;
      final long bit = 1L << (leaf & 63);
      final int from;
      final int to;
      if (!zone.known(leaf)) {
        from = 0;
        to = k;
      } else {
        final long min = zone.min(leaf);
        final long max = zone.max(leaf);
        if (min > max) {
          continue;
        }
        from = lowerBound(sortedValues, min);
        to = upperBound(sortedValues, max);
      }
      for (int j = from; j < to; j++) {
        keeps[j][word] |= bit;
        set++;
      }
    }
    return set;
  }

  /** First index whose value is {@code >= key} ({@code a.length} when none). */
  private static int lowerBound(final long[] a, final long key) {
    int lo = 0;
    int hi = a.length;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (a[mid] < key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  /** First index whose value is {@code > key} ({@code a.length} when none). */
  private static int upperBound(final long[] a, final long key) {
    int lo = 0;
    int hi = a.length;
    while (lo < hi) {
      final int mid = (lo + hi) >>> 1;
      if (a[mid] <= key) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return lo;
  }

  private static int cardinality(final long[] words) {
    int bits = 0;
    for (final long w : words) {
      bits += Long.bitCount(w);
    }
    return bits;
  }

  /** The all-kept mask over {@code n} leaves (tail bits clear). */
  private static long[] allKept(final int n) {
    final long[] keep = new long[(n + 63) >>> 6];
    Arrays.fill(keep, -1L);
    if ((n & 63) != 0) {
      keep[keep.length - 1] = (1L << (n & 63)) - 1;
    }
    return keep;
  }

  /**
   * Kinds whose descriptor min/max bound the LONG lane a numeric predicate compares against: the
   * ordered-long kinds, and {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_GLOBAL}, whose
   * cells are dictionary ids. A predicate on a global column reaches the scan ALREADY TRANSLATED —
   * {@code stringLitBytes == null}, {@code longLit} = the literal's id (or the ID_ABSENT sentinels
   * {@code > Long.MAX_VALUE} / {@code >= Long.MIN_VALUE}) — so {@link #zoneSkip} tests an id against
   * an id range: EQ is containment and NE a collapsed zone, both sound whatever order the ids were
   * minted in, and the sentinels prune every leaf or none. This is the same test
   * {@link #evalLeafInto} applies to a global slice AFTER fetching it; here it runs on the memoized
   * descriptor zones BEFORE any leaf is read. Without it a global string column had no leaf pruning
   * at all — every equality over it scanned the whole store (found when SearchPhrase became global).
   */
  static boolean zonePrunableKind(final byte kind) {
    return ProjectionIndexRowGroupPage.isOrderedLongKind(kind)
        || kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL;
  }

  /**
   * Clear from {@code keep} every still-kept leaf that predicate {@code p} PROVES contributes no row:
   * descriptor {@code min > max} (no present value), a numeric zone the predicate excludes, or a
   * string-equality fingerprint miss. Returns how many leaves this predicate dropped.
   */
  private static int pruneLeaves(final ProjectionColumnStore store, final ColumnPredicate p, final long[] keep,
      final ColumnSegmentFetcher fetcher) {
    final int n = store.leafCount();
    final byte kind = store.columnKind(p.column);
    if (zonePrunableKind(kind) && p.stringLitBytes == null) {
      // The memoized zone mirrors: built once per column from the descriptors, then every predicate
      // on the column (this query's and the next's) reads leaf-indexed arrays instead of paying a
      // descriptor binary search per leaf.
      final ProjectionColumnStore.ZoneIndex zone = store.zoneIndex(p.column);
      int dropped = 0;
      int noEvidence = 0;
      for (int i = 0; i < n; i++) {
        if ((keep[i >>> 6] & 1L << (i & 63)) == 0) {
          continue;
        }
        if (!zone.known(i)) {
          noEvidence++;
          continue; // no descriptor evidence — keep
        }
        final long min = zone.min(i);
        final long max = zone.max(i);
        if (min > max || zoneSkip(p, min, max)) {
          keep[i >>> 6] &= ~(1L << (i & 63));
          dropped++;
        }
      }
      if (DIAG) {
        System.err.println("[prune] col=" + p.column + " kind=" + kind + " op=" + p.op + " zone: dropped=" + dropped
            + " noEvidence=" + noEvidence);
      }
      LEAVES_PRUNED.add(dropped);
      return dropped;
    }
    if (p.stringLitBytes != null && p.op == ProjectionIndexScan.Op.EQ
        && kind == ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT) {
      // Op.EQ ONLY, and load-bearing: bloom fingerprints hash WHOLE values, so pruning a leaf
      // on a CONTAINS or ordering literal is a false negative — a leaf whose every URL
      // contains "google" fingerprints none of them as the string "google". Rows would be
      // silently dropped on the one path the byte kernel does not take.
      final long literalHash = ProjectionIndexColumnSegmentCodec.bloomHash(p.stringLitBytes);
      final int dropped = store.applyBloomPrune(p.column, literalHash, keep, fetcher);
      if (DIAG) {
        System.err.println("[prune] col=" + p.column + " kind=" + kind + " op=EQ bloom: dropped=" + dropped);
      }
      LEAVES_PRUNED.add(dropped);
      return dropped;
    }
    if (DIAG) {
      System.err.println("[prune] col=" + p.column + " kind=" + kind + " op=" + p.op + " literal="
          + (p.stringLitBytes != null) + ": no prune rule");
    }
    return 0;
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

  /** The 128-bit domain of {@link #distinctDictUnion}: xxHash128 of the entry's UTF-8 bytes. */
  private static final LongTupleHashFunction DISTINCT_HASH = LongTupleHashFunction.xx128();
  private static final byte[] NO_BYTES = new byte[0];

  /**
   * Hashed twin of {@link #distinctPresentStrings} for ANY cardinality: the 128-bit hash of every
   * non-empty dictionary entry of dict column {@code col} over leaves {@code [fromLeaf, toLeaf)} goes
   * to the {@code sink} — one hash per (leaf, entry), never per row — and the size of the set behind
   * the sink is the distinct present count. Same contract as the content-based kernel: sparse-clean
   * unpredicated callers only, since every non-empty entry was interned by a present row and only a
   * zero-length entry (the "" default a MISSING row interns) needs per-row disambiguation. The "" hash
   * is put once, when some leaf proves a present row references it.
   *
   * <p>
   * Two 128-bit hashes of distinct values coincide with probability {@code 2^-128} per pair — below
   * the hardware error rate at any cardinality this store can hold — which is what lets the count be
   * exact without keeping a byte of any value. The {@code hash} scratch is the caller's {@code long[2]}
   * (one per worker), so the kernel allocates nothing per entry.
   * </p>
   *
   * @param access per-leaf access to the column, resident or windowed
   * @param col the dict column
   * @param fromLeaf the first leaf, inclusive
   * @param toLeaf the last leaf, exclusive
   * @param sink where the hashes go: a set, or a worker's handle on a shared one
   * @param hash a {@code long[2]} scratch for the hash halves
   * @return {@code false} when a slice is missing or lacks its dictionary, id or presence lanes — the
   *         caller declines; {@code true} when every leaf was folded in
   * @throws DistinctHash128Set.ByteBudgetExceededException when the set behind the sink is refused a growth
   */
  public static boolean distinctDictUnion(final ProjectionColumnStore.LeafColumnAccess access, final int col,
      final int fromLeaf, final int toLeaf, final DistinctHash128Sink sink, final long[] hash) {
    if (hash.length < 2) {
      throw new IllegalArgumentException("hash scratch needs two longs");
    }
    boolean emptyReal = false;
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      final ColumnSlice slice = access.slice(col, leaf);
      if (slice == null) {
        return false;
      }
      final int rowCount = slice.rowCount();
      if (rowCount == 0) {
        continue;
      }
      final byte[] dictBytes = slice.dictBytes();
      final int[] dictOffsets = slice.dictOffsets();
      if (dictBytes == null || dictOffsets == null) {
        return false;
      }
      final int dictSize = dictOffsets.length - 1;
      int emptyId = -1;
      int off = dictOffsets[0];
      for (int i = 0; i < dictSize; i++) {
        final int end = dictOffsets[i + 1];
        if (off == end) {
          emptyId = i;
        } else {
          DISTINCT_HASH.hashBytes(dictBytes, off, end - off, hash);
          sink.put(hash[0], hash[1]);
        }
        off = end;
      }
      if (emptyId >= 0 && !emptyReal) {
        final int referenced = emptyEntryReferenced(slice, emptyId, rowCount);
        if (referenced < 0) {
          return false;
        }
        emptyReal = referenced > 0;
      }
    }
    if (emptyReal) {
      DISTINCT_HASH.hashBytes(NO_BYTES, 0, 0, hash);
      sink.put(hash[0], hash[1]);
    }
    return true;
  }

  /**
   * Whether a PRESENT row of the slice references dictionary entry {@code emptyId}: {@code 1} when one
   * does (every row present, or an id scan finds one), {@code 0} when none does, {@code -1} when the
   * slice lacks its presence or id lane.
   */
  private static int emptyEntryReferenced(final ColumnSlice slice, final int emptyId, final int rowCount) {
    final long[] presence = slice.presenceWords();
    final int[] ids = slice.stringDictIds();
    if (presence == null || ids == null) {
      return -1;
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
      return 1;
    }
    for (int r = 0; r < rowCount; r++) {
      if ((presence[r >>> 6] & 1L << (r & 63)) == 0L) {
        continue;
      }
      if (ids[r] == emptyId) {
        return 1;
      }
    }
    return 0;
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
    // A leaf the keep mask dropped is the pruned sentinel in EVERY tree column (the mask is a
    // whole-leaf decision — see ProjectionColumnScan.predicateKeepMask(store, preds, tree, fetcher)),
    // and every all-zero operand combines to all-zero under AND and OR: answer "no rows" here so the
    // kernel never touches the leaf's (equally pruned) key and aggregate slices. NOT would flip an
    // operand to all-true, so a program that negates keeps the slow, exact evaluation.
    if (!tree.hasNot() && allTreeColumnsPruned(treeCols, leaf)) {
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

  /** Whether every tree column's slice on {@code leaf} is absent or rowless (the pruned sentinel). */
  private static boolean allTreeColumnsPruned(final ColumnSlice[][] treeCols, final int leaf) {
    for (final ColumnSlice[] col : treeCols) {
      final ColumnSlice slice = col[leaf];
      if (slice != null && slice.rowCount() > 0) {
        return false;
      }
    }
    return true;
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
