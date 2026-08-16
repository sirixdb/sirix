package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;

/**
 * GROUP-AGGREGATE kernels over {@link ColumnSlice}s — the column-sliced twins of
 * {@link ProjectionIndexByteScan}'s flat group kernels. The byte kernels consume WHOLE-LEAF
 * payloads, which forces the executor to assemble every leaf (decode + re-serialize of all
 * columns — the cold-start whale); these read only the queried columns' decoded slices.
 *
 * <p>
 * <b>Identity parity is structural, not replicated:</b> group hashes go through the ONE
 * {@link ProjectionIndexByteScan#fnv1a64} both kernel families share, and the payload's dict
 * entry bytes are written from the same decoded DICT segment a slice exposes — both sides hash
 * identical byte content. Aux references use ABSOLUTE leaf indices ({@code (long) leaf << 20 |
 * rowIdx}), exactly what the executor's winner decoders already assume.
 *
 * <p>
 * V1 scope: the NUMERIC single-key flat kernel (conjunctive predicates; COUNT(DISTINCT) lane
 * included; strlen aggregate mode and predicate TREES stay on the whole-leaf path — the caller
 * gates). Missing-key semantics mirror the byte kernel exactly: a row without the group field
 * folds into {@code missingAcc} with a first-seen ordinal stamp; group value {@code 0} takes the
 * table's zero side slot.
 */
public final class ProjectionColumnGroupScan {

  private ProjectionColumnGroupScan() {
  }

  /** Per-thread predicate mask over one leaf (MAX_ROWS bound — same bound the scans use). */
  private static final ThreadLocal<long[]> MASK =
      ThreadLocal.withInitial(() -> new long[(ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6]);

  /**
   * Sliced twin of {@link ProjectionIndexByteScan#conjunctiveAggregateByGroupNumericFlat}. Leaf
   * range {@code [fromLeaf, toLeaf)} in ABSOLUTE store indices; the caller resolves every column
   * ONCE before the parallel fan-out (twenty workers racing the first fill would multiply the
   * I/O) and hands the shared immutable slice arrays in.
   */
  public static void aggregateByGroupNumericFlat(final ProjectionColumnStore store,
      final ColumnPredicate[] predicates, final ColumnSlice[][] predCols, final ColumnSlice[] groupCol,
      final ColumnSlice[][] aggCols, final int fromLeaf, final int toLeaf, final NumericGroupAggTable out,
      final long[] missingAcc, final int distinctBlock, final Long2ObjectOpenHashMap<LongOpenHashSet> distinctOut,
      final LongOpenHashSet distinctMissing, final long[] budget) {
    if (predicates == null || out == null || missingAcc == null || aggCols == null) {
      throw new IllegalArgumentException("predicates, out, missingAcc and aggCols must not be null");
    }
    final long[] mask = MASK.get();
    final int aggCount = aggCols.length;
    for (int leaf = fromLeaf; leaf < toLeaf; leaf++) {
      if (budget != null && budget[1] != 0) {
        return; // distinct budget exceeded — the caller declines; nothing here is an answer
      }
      final int rowCount = ProjectionColumnScan.evaluateMask(predicates, predCols, leaf, store.rowCount(leaf), mask);
      if (rowCount <= 0) {
        continue;
      }
      final ColumnSlice group = groupCol[leaf];
      final long[] groupValues = group.numericValues();
      final long[] groupPresence = group.presenceWords();
      final long leafOrdinalBase = (long) leaf << 20;
      final int stride = (rowCount + 63) >>> 6;
      for (int w = 0; w < stride; w++) {
        long word = mask[w] & ProjectionIndexByteScan.validRowsMask(w, stride, rowCount);
        final long groupPresWord = groupPresence[w];
        final int rowBase = w << 6;
        while (word != 0L) {
          final int bit = Long.numberOfTrailingZeros(word);
          word &= word - 1L;
          final int rowIdx = rowBase + bit;
          final long[] slotArr;
          final int base;
          LongOpenHashSet dset = null;
          if ((groupPresWord & 1L << bit) == 0L) {
            slotArr = missingAcc;
            base = 0;
            if (slotArr[0] == 0) {
              slotArr[1] = leafOrdinalBase | rowIdx;
            }
            dset = distinctMissing;
          } else {
            final long gv = groupValues[rowIdx];
            if (gv == 0L) {
              slotArr = out.acquireZero(leafOrdinalBase | rowIdx);
              base = 0;
            } else {
              base = out.acquire(gv, leafOrdinalBase | rowIdx);
              slotArr = out.slotsArray();
            }
            if (distinctBlock >= 0) {
              dset = distinctSetFor(distinctOut, gv);
            }
          }
          slotArr[base]++;
          for (int a = 0; a < aggCount; a++) {
            final ColumnSlice agg = aggCols[a][leaf];
            if ((agg.presenceWords()[w] & 1L << bit) == 0L) {
              continue;
            }
            final long v = agg.numericValues()[rowIdx];
            if (a == distinctBlock) {
              if (dset.add(v) && --budget[0] < 0) {
                budget[1] = 1;
              }
              continue;
            }
            final int aggBase = base + 2 + 4 * a;
            slotArr[aggBase]++;
            // Exact sum or DECLINE — the interpreter promotes an overflowing xs:integer sum.
            slotArr[aggBase + 1] = Math.addExact(slotArr[aggBase + 1], v);
            if (v < slotArr[aggBase + 2]) {
              slotArr[aggBase + 2] = v;
            }
            if (v > slotArr[aggBase + 3]) {
              slotArr[aggBase + 3] = v;
            }
          }
        }
      }
    }
  }

  private static LongOpenHashSet distinctSetFor(final Long2ObjectOpenHashMap<LongOpenHashSet> out, final long key) {
    LongOpenHashSet s = out.get(key);
    if (s == null) {
      s = new LongOpenHashSet();
      out.put(key, s);
    }
    return s;
  }

  /**
   * Descriptor-only twin of {@link ProjectionIndexByteScan#requireGroupSumsFitLong}: bound every
   * summed column's whole-column magnitude by {@code Σ_leaf rowCount × max(|min|,|max|)} from the
   * zone descriptors — ZERO segment I/O. Overflow of the bound throws {@link ArithmeticException},
   * the same decline signal the byte pre-flight uses; a column without zone evidence abandons its
   * pre-flight silently (the per-row {@code Math.addExact} in the kernel stays the authority), and
   * {@code min > max} means an all-missing leaf and is skipped. Kind is asserted: a dict column's
   * zone lanes hold DICT IDS, and bounding on those would be reading id-range as value-range.
   */
  public static void requireGroupSumsFitLong(final ProjectionColumnStore store, final int[] summedCols) {
    final long[] range = new long[2];
    for (final int col : summedCols) {
      if (store.columnKind(col) != ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG) {
        throw new IllegalStateException("summed column " + col + " is not NUMERIC_LONG");
      }
      long bound = 0;
      for (int leaf = 0, n = store.rowGroupCount(); leaf < n; leaf++) {
        if (!store.columnZoneRange(leaf, col, range)) {
          bound = -1; // no zone evidence — abandon this column's pre-flight silently
          break;
        }
        if (range[0] > range[1]) {
          continue; // all-missing leaf
        }
        final long mag = Math.max(Math.abs(range[0]), Math.abs(range[1]));
        bound = Math.addExact(bound, Math.multiplyExact((long) store.rowCount(leaf), mag));
      }
    }
  }
}
