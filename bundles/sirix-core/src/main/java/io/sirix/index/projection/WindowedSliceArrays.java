package io.sirix.index.projection;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.LeafColumnAccess;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;
import org.jspecify.annotations.Nullable;

import java.util.Arrays;

import static java.util.Objects.requireNonNull;

/**
 * Per-worker slice arrays for the sliced group kernels, filled one sub-chunk of leaves at a time through
 * a windowed {@link LeafColumnAccess}.
 *
 * <p>
 * The sliced kernels index {@code ColumnSlice[column][leaf]} arrays and scan a leaf range; the group arms
 * hand them resident arrays shared by every worker — which the fill budget refuses for a fat column at
 * 100M rows, sending the whole query to the whole-leaf byte kernels that stream EVERY column of every row
 * group per try. This object keeps one full-length array per needed column (a few hundred KB of references
 * at 100M) and, before each sub-chunk, fills exactly its leaves from a small-cache windowed access, then
 * releases them: the kernels are unchanged, only the two to four columns a query needs are decoded, and
 * the working set is two windows per column. Single-threaded: one instance per parallel worker.
 * </p>
 *
 * <p>
 * <b>The arrays of a filled slice are valid until {@link #release}.</b> The sub-chunk constructor asks the
 * access to RECYCLE an evicted slice's presence and value arrays into the next window's decode: a pass
 * over 100M rows minted 3.2 GB of {@code long[1024]} per column set per pass otherwise, every array dead
 * two windows later. The cache holds two windows and a sub-chunk is at most one, so a slice released
 * by the kernel is evicted (and its arrays rewritten) no earlier than the fill after next; a caller
 * that reads a slice after releasing its range breaks that contract. The one-leaf emission shape keeps
 * allocating: its consumers resolve winners scattered across the store in no window order.
 * </p>
 */
public final class WindowedSliceArrays {
  private final LeafColumnAccess access;
  private final int leafCount;
  /** Longest fill range a recycling access can serve without evicting a slice of the range itself; unbounded when not recycling. */
  private final int maxFillLeaves;
  private final Int2ObjectOpenHashMap<ColumnSlice[]> arrays = new Int2ObjectOpenHashMap<>();
  private final IntArrayList filled = new IntArrayList();

  /**
   * @param store the column store
   * @param fetcher the caller's own live fetcher
   * @param keepWords the predicates' zone-map keep mask ({@code null} = every leaf); a pruned leaf holds
   *        the zero-row sentinel in every filled array, see {@link #column}
   */
  public WindowedSliceArrays(final ProjectionColumnStore store, final ProjectionColumnStore.ColumnSegmentFetcher fetcher,
      final long @Nullable [] keepWords) {
    this(store, fetcher, keepWords, ProjectionColumnStore.LEAF_ACCESS_WINDOW, 2 * ProjectionColumnStore.LEAF_ACCESS_WINDOW,
        true);
  }

  /**
   * @param windowLeaves leaves fetched and decoded per window (1 for a winner-emission access that
   *        touches a handful of leaves once)
   * @param cacheLeaves per-column leaf cache, at least one window
   */
  public WindowedSliceArrays(final ProjectionColumnStore store, final ProjectionColumnStore.ColumnSegmentFetcher fetcher,
      final long @Nullable [] keepWords, final int windowLeaves, final int cacheLeaves) {
    this(store, fetcher, keepWords, windowLeaves, cacheLeaves, false);
  }

  /**
   * @param recycleSlices recycle an evicted slice's long-lane arrays into the next decode — only for the
   *        fill / kernel / release discipline described on the class
   */
  public WindowedSliceArrays(final ProjectionColumnStore store, final ProjectionColumnStore.ColumnSegmentFetcher fetcher,
      final long @Nullable [] keepWords, final int windowLeaves, final int cacheLeaves, final boolean recycleSlices) {
    requireNonNull(store, "store");
    this.access = store.windowedLeafAccess(requireNonNull(fetcher, "fetcher"), keepWords, windowLeaves, cacheLeaves,
        recycleSlices);
    this.leafCount = store.rowGroupCount();
    // A range of L leaves touches at most ceil((L - 1) / window) + 1 windows; the cache must hold them
    // all, or filling the range's tail would evict (and rewrite) its head before the kernel runs.
    this.maxFillLeaves = recycleSlices
        ? cacheLeaves - windowLeaves + 1
        : Integer.MAX_VALUE;
  }

  private ColumnSlice[] arrayFor(final int col) {
    ColumnSlice[] a = arrays.get(col);
    if (a == null) {
      a = new ColumnSlice[leafCount];
      arrays.put(col, a);
    }
    return a;
  }

  private void checkRange(final int from, final int to) {
    if (from < 0 || to > leafCount || from > to) {
      throw new IndexOutOfBoundsException("leaves [" + from + ", " + to + ") of " + leafCount);
    }
    if (to - from > maxFillLeaves) {
      throw new IllegalArgumentException("fill of " + (to - from) + " leaves exceeds the " + maxFillLeaves
          + " a recycling access can hold at once");
    }
  }

  /**
   * The array of {@code col} with {@code [from, to)} filled (keys, aggregates, tree leaves). A leaf the
   * keep mask pruned holds the store's zero-row sentinel instead of a decoded slice: the group kernels
   * evaluate the predicate mask first and skip such a leaf, and the unmasked pre-passes that do read
   * every leaf in range (the exact-sum bound, string length modes) see no rows, which is exactly the
   * set of rows the kernel folds. One array serves both roles, so a column that is a predicate AND an
   * aggregate (q: {@code WHERE amount >= x ... SUM(amount)}) reads consistently from either.
   */
  public ColumnSlice[] column(final int col, final int from, final int to) {
    checkRange(from, to);
    final ColumnSlice[] a = arrayFor(col);
    for (int leaf = from; leaf < to; leaf++) {
      a[leaf] = access.predicateSlice(col, leaf);
    }
    filled.add(col);
    return a;
  }

  /** {@link #columns} for an array with {@code -1} holes (a key without a condition column): a hole maps to {@code null}. */
  public ColumnSlice[][] columnsNullable(final int[] cols, final int from, final int to) {
    final ColumnSlice[][] out = new ColumnSlice[cols.length][];
    for (int i = 0; i < cols.length; i++) {
      out[i] = cols[i] >= 0
          ? column(cols[i], from, to)
          : null;
    }
    return out;
  }

  /** One array per column of {@code cols}, each with {@code [from, to)} filled. */
  public ColumnSlice[][] columns(final int[] cols, final int from, final int to) {
    final ColumnSlice[][] out = new ColumnSlice[cols.length][];
    for (int i = 0; i < cols.length; i++) {
      out[i] = column(cols[i], from, to);
    }
    return out;
  }

  /** One array per predicate, keep-masked, each with {@code [from, to)} filled. */
  public ColumnSlice[][] predicateColumns(final ColumnPredicate[] predicates, final int from, final int to) {
    checkRange(from, to);
    final ColumnSlice[][] out = new ColumnSlice[predicates.length][];
    for (int i = 0; i < predicates.length; i++) {
      out[i] = column(predicates[i].column, from, to);
    }
    return out;
  }

  /**
   * One array per tree leaf (index-aligned with {@code tree.leaves}), keep-masked like every other
   * column here: the access's mask is the TREE'S mask when the scan carries a tree
   * ({@code ProjectionColumnScan.predicateKeepMask(store, preds, tree, fetcher)}), so a dropped leaf is
   * the pruned sentinel in every tree column and the tree evaluator answers it as "no rows".
   */
  public ColumnSlice[][] treeColumns(final PredicateTree tree, final int from, final int to) {
    final ColumnSlice[][] out = new ColumnSlice[tree.leaves.length][];
    for (int i = 0; i < tree.leaves.length; i++) {
      out[i] = column(tree.leaves[i].column, from, to);
    }
    return out;
  }

  /** Drop the references of {@code [from, to)} in every array filled since the last release. */
  public void release(final int from, final int to) {
    checkRange(from, to);
    for (int i = 0; i < filled.size(); i++) {
      Arrays.fill(arrays.get(filled.getInt(i)), from, to, null);
    }
    filled.clear();
  }
}
