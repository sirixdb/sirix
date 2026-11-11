package io.sirix.index.projection;

import static java.util.Objects.requireNonNull;
import io.sirix.index.projection.GroupDistinctAccumulator.Sink;
import io.sirix.index.projection.GroupDistinctAccumulator.Worker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import org.jspecify.annotations.Nullable;

final class GroupArrayReference {
  private static final int WINDOW_ROWS = 64;

  private GroupArrayReference() {}

  static void foldNumericPlainWordWindow(final long record, final long[][] window, final int first,
      final long[][] aggPresence, final int aggCount, final int w, final long rows, final long sumExactMask) {
    GroupKernelLanes.add(record, 0, Long.bitCount(rows));
    for (int a = 0; a < aggCount; a++) {
      long present = aggPresence[a][w] & rows;
      if (present == 0L) {
        continue;
      }
      final int column = 2 + 4 * a;
      final boolean exact = NumericGroupAggTable.sumsExact(sumExactMask, a);
      final long[] values = window[a];
      final int count = Long.bitCount(present);
      GroupKernelLanes.add(record, column, count);
      if (count == 1) {
        // A lone row, which is every row of a sparse selection: folded into the lanes directly,
        // because a loop that is entered for one trip costs more than the row does.
        final long v = values[first + Long.numberOfTrailingZeros(present)];
        if (exact) {
          GroupKernelLanes.set(record, column + 1, Math.addExact(GroupKernelLanes.get(record, column + 1), v));
        }
        GroupKernelLanes.min(record, column + 2, v);
        GroupKernelLanes.max(record, column + 3, v);
        continue;
      }
      long sum = GroupKernelLanes.get(record, column + 1);
      long min = GroupKernelLanes.get(record, column + 2);
      long max = GroupKernelLanes.get(record, column + 3);
      // Counted by the rows rather than run until the word is empty: at most 64 trips, which the
      // compiler knows, so the loop carries no safepoint poll.
      for (int i = 0; i < count; i++) {
        final long v = values[first + Long.numberOfTrailingZeros(present)];
        present &= present - 1L;
        if (exact) {
          sum = Math.addExact(sum, v);
        }
        min = Math.min(min, v);
        max = Math.max(max, v);
      }
      if (exact) {
        GroupKernelLanes.set(record, column + 1, sum);
      }
      GroupKernelLanes.set(record, column + 2, min);
      GroupKernelLanes.set(record, column + 3, max);
    }
  }

  static void foldSumsWindow(final long record, final long[][] window, final long[][] presence, final int columns,
      final int word, final int bit, final long sumExactMask) {
    GroupKernelLanes.increment(record, 0);
    final long presentBit = 1L << bit;
    for (int a = 0; a < columns; a++) {
      if ((presence[a][word] & presentBit) != 0L) {
        final int at = 2 + 2 * a;
        GroupKernelLanes.increment(record, at);
        if (NumericGroupAggTable.sumsExact(sumExactMask, a)) {
          GroupKernelLanes.set(record, at + 1, Math.addExact(GroupKernelLanes.get(record, at + 1), window[a][bit]));
        }
      }
    }
  }

  static void foldSlicedWindow(final long record, final long[][] window, final long[][] aggPresence,
      final int[][] aggIds, final int @Nullable [][] stringLengths, final byte @Nullable [] stringLengthModes,
      final int aggCount, final int w, final int bit, final int rowIdx, final int distinctBlock,
      final @Nullable Sink dset, final long @Nullable [] budget, final byte @Nullable [] cdDictBytes,
      final int @Nullable [] cdDictOffsets, final long @Nullable [] cdHash,
      final @Nullable GroupDistinctBitmaps bitmaps, final long @Nullable [] dwords, final long sumExactMask,
      final int[][] leafLengthTables, final @Nullable Worker directDistinct, final long distinctGroup) {
    GroupKernelLanes.increment(record, 0);
    for (int a = 0; a < aggCount; a++) {
      final boolean stringLengthAgg =
          stringLengthModes != null && stringLengthModes[a] != ProjectionIndexByteScan.STRING_LENGTH_NONE;
      final boolean present = (aggPresence[a][w] & 1L << bit) != 0L;
      if (!present && !stringLengthAgg) {
        continue;
      }
      // fn:string-length(()) is 0, never empty: a row MISSING the operand still contributes 0.
      final long v;
      if (stringLengthAgg) {
        // An id-lane operand's lengths live in the table the leaf setup chose (GLOBAL: the per-query
        // table; SEGMENT: this leaf's segment's), indexed by the row's id lane — a segment cell's low
        // 32 bits are its id; a per-leaf dict operand's in the precomputed per-entry pass, indexed by
        // its dict id.
        v = !present
            ? 0L
            : leafLengthTables[a] != null
                ? leafLengthTables[a][(int) window[a][bit]]
                : requireNonNull(stringLengths,
                    "a dictionary length operand requires its length table")[a][aggIds[a][rowIdx]];
      } else if (cdHash != null && a == distinctBlock) {
        final int cdId = aggIds[a][rowIdx];
        long h = cdHash[cdId];
        // cdDictBytes == null marks a PRECOMPUTED table (the leaf's DICT_HASHES segment): the value
        // read is already the answer, and the array is shared across workers — never written here.
        if (h == 0L && cdDictBytes != null) {
          final int[] offsets = requireNonNull(cdDictOffsets, "dictionary bytes require their offsets");
          final int off = offsets[cdId];
          h = ProjectionIndexByteScan.fnv1a64(cdDictBytes, off, offsets[cdId + 1] - off);
          cdHash[cdId] = h;
        }
        v = h;
      } else {
        v = window[a][bit];
      }
      if (a == distinctBlock) {
        // Dense global ids go to the shared per-group bitmap, which is exact at any cardinality and
        // therefore spends no budget; everything else keeps the per-worker exact set and its ceiling.
        if (dwords != null) {
          if (!requireNonNull(bitmaps, "distinct bitmap words require their accumulator").set(dwords, v)) {
            // An id outside the sized range declines; dropping it would silently lower the count.
            requireNonNull(budget, "a distinct bitmap accumulator requires its budget")[1] = 1;
          }
        } else if (directDistinct != null) {
          directDistinct.addToGroup(distinctGroup, v);
        } else {
          requireNonNull(dset, "a distinct aggregate requires its sink").add(v); // exact and bounded
        }
        continue;
      }
      final int aggBase = 2 + 4 * a;
      GroupKernelLanes.increment(record, aggBase);
      // Exact sum or DECLINE — the interpreter promotes an overflowing xs:integer sum. A lane no
      // sum/avg reads is not folded at all: declining a min-only query over an unread sum would
      // refuse an answer the accumulator's fixed [count, sum, min, max] shape never needed.
      if (NumericGroupAggTable.sumsExact(sumExactMask, a)) {
        GroupKernelLanes.set(record, aggBase + 1, Math.addExact(GroupKernelLanes.get(record, aggBase + 1), v));
      }
      GroupKernelLanes.min(record, aggBase + 2, v);
      GroupKernelLanes.max(record, aggBase + 3, v);
    }
  }

  static boolean gatherPays(final long word) {
    return Long.bitCount(word) > ProjectionVectorKernels.FOLD_WALK_MAX_BITS;
  }

  static long[][] gatherWindow(final MemorySegment[] segs, final long[] laneBases, final int columns, final int w,
      final int rowCount, final long[][] window) {
    return gatherWindowAt(segs, laneBases, columns, w, rowCount, window, 0);
  }

  static long[][] gatherWindowAt(final MemorySegment[] segs, final long[] bases, final int columns, final int w,
      final int rowCount, final long[][] window, final int at) {
    final int rowBase = w << 6;
    final int rows = Math.min(WINDOW_ROWS, rowCount - rowBase);
    final long from = (long) rowBase << 3;
    for (int c = 0; c < columns; c++) {
      final MemorySegment seg = segs[c];
      if (seg != null) {
        MemorySegment.copy(seg, ValueLayout.JAVA_LONG_UNALIGNED, bases[c] + from, window[at + c], 0, rows);
      }
    }
    return window;
  }

  static long[] gatherGroupWindow(final MemorySegment seg, final long laneBase, final int w, final int rowCount,
      final long[] window) {
    final int rowBase = w << 6;
    MemorySegment.copy(seg, ValueLayout.JAVA_LONG_UNALIGNED, laneBase + ((long) rowBase << 3), window, 0,
        Math.min(WINDOW_ROWS, rowCount - rowBase));
    return window;
  }
}
