/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import io.sirix.node.SegmentAccess;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The column-sliced scan kernels read their values IN PLACE off a lane segment, and there is
 * exactly ONE kernel per operation: the same code answers a store-owned off-heap slab and a
 * producer that still decodes to the heap (which reaches it as {@code MemorySegment.ofArray}).
 *
 * <p>
 * Each test therefore asserts two things at once: the off-heap lane and the heap array answer
 * IDENTICALLY (representation equivalence — the property the deleted array twin used to be checked
 * for by hand), and both answer what an independent scalar oracle in this test says (exact
 * answers). A non-zero lane base is used throughout, so a kernel that ignored {@code laneBase}
 * would read the slab's guard bytes and fail rather than pass by luck.
 * </p>
 */
final class LaneScanKernelEquivalenceTest {

  /** Row counts around the word, vector and tail boundaries of the kernels. */
  private static final int[] ROW_COUNTS = {1, 7, 63, 64, 65, 127, 128, 200, 1024};

  /** Byte offset of row 0 in every test slab: never 0, so the base is actually exercised. */
  private static final long LANE_BASE = 64L;

  private static final byte NO_FLAGS = (byte) 0;

  private static final Op[] NUMERIC_OPS = {Op.GT, Op.LT, Op.GE, Op.LE, Op.EQ, Op.NE, Op.BETWEEN_GT_LT, Op.BETWEEN_GT_LE,
      Op.BETWEEN_GE_LT, Op.BETWEEN_GE_LE};

  @Test
  @DisplayName("every numeric compare arm agrees with the oracle, off-heap lane and heap alike")
  void numericCompareArmsAgreeWithTheOracleAndAcrossRepresentations() {
    final Random rnd = new Random(20261106L);
    try (Arena arena = Arena.ofConfined()) {
      for (final int rows : ROW_COUNTS) {
        for (final Op op : NUMERIC_OPS) {
          // Values in a narrow band around the literals, so every arm sees a mixed answer instead of
          // an all-true or all-false word; the presence words leave holes everywhere.
          final long[] values = new long[rows];
          for (int i = 0; i < rows; i++) {
            values[i] = rnd.nextLong(-20L, 20L);
          }
          final long[] presence = randomPresence(rnd, rows);
          final long lit = -5L;
          final long high = 7L;
          final ColumnPredicate p = betweenPredicate(op, lit, high);
          final long[] expected = oracleCompare(values, presence, rows, op, lit, high);
          assertMaskAgrees(arena, p, values, presence, rows, expected, op + " over " + rows + " rows");
        }
      }
    }
  }

  @Test
  @DisplayName("a pre-evaluated global-id verdict is tested per present row, lane and heap alike")
  void globalIdVerdictArmAgrees() {
    final Random rnd = new Random(7L);
    try (Arena arena = Arena.ofConfined()) {
      for (final int rows : ROW_COUNTS) {
        final int idCount = 37;
        final long[] values = new long[rows];
        for (int i = 0; i < rows; i++) {
          // id 0 is the missing-cell sentinel, idCount + 3 is out of the verdict's range: neither
          // may ever match.
          values[i] = rnd.nextInt(idCount + 4);
        }
        final long[] presence = randomPresence(rnd, rows);
        final long[] verdict = new long[(idCount >>> 6) + 1];
        for (int id = 1; id <= idCount; id++) {
          if ((id % 3) == 0) {
            verdict[id >>> 6] |= 1L << (id & 63);
          }
        }
        final ColumnPredicate p = ColumnPredicate.globalStringVerdict(0, Op.STR_CONTAINS,
            "x".getBytes(StandardCharsets.UTF_8), verdict, idCount);
        final long[] expected = new long[stride(rows)];
        for (int row = 0; row < rows; row++) {
          final long id = values[row];
          final boolean match =
              present(presence, row) && id >= 1 && id <= idCount && (verdict[(int) (id >>> 6)] & 1L << (id & 63)) != 0L;
          if (match) {
            expected[row >>> 6] |= 1L << (row & 63);
          }
        }
        assertMaskAgrees(arena, p, values, presence, rows, expected, "global verdict over " + rows + " rows");
      }
    }
  }

  @Test
  @DisplayName("a segment-scoped literal resolves once per leaf, lane and heap alike")
  void segmentScopedEqualityArmAgrees() {
    final Random rnd = new Random(11L);
    try (Arena arena = Arena.ofConfined()) {
      for (final int rows : ROW_COUNTS) {
        final int segment = 2;
        final long[] values = new long[rows];
        for (int i = 0; i < rows; i++) {
          values[i] = ProjectionIndexRowGroupPage.packSegmentCell(segment, rnd.nextInt(1, 9));
        }
        final long[] presence = randomPresence(rnd, rows);
        final long target = ProjectionIndexRowGroupPage.packSegmentCell(segment, 4);
        final long[] cells = new long[segment + 1];
        Arrays.fill(cells, ColumnPredicate.SEGMENT_LITERAL_ABSENT);
        cells[segment] = target;
        for (final Op op : new Op[] {Op.EQ, Op.NE}) {
          final ColumnPredicate p = ColumnPredicate.segmentScopedEquality(0, op, cells);
          final long[] expected = new long[stride(rows)];
          final boolean anyPresent = anyPresent(presence, rows);
          for (int row = 0; row < rows && anyPresent; row++) {
            final boolean match = present(presence, row) && (op == Op.EQ
                ? values[row] == target
                : values[row] != target);
            if (match) {
              expected[row >>> 6] |= 1L << (row & 63);
            }
          }
          assertMaskAgrees(arena, p, values, presence, rows, expected, op + " segment literal over " + rows + " rows");

          // The same leaf against a segment whose dictionary provably lacks the value: EQ matches
          // nothing, NE every PRESENT row — and a leaf with nothing present matches nothing either way.
          final long[] absent = new long[segment + 1];
          Arrays.fill(absent, ColumnPredicate.SEGMENT_LITERAL_ABSENT);
          final ColumnPredicate pAbsent = ColumnPredicate.segmentScopedEquality(0, op, absent);
          final long[] expectedAbsent = new long[stride(rows)];
          if (op == Op.NE && anyPresent) {
            for (int row = 0; row < rows; row++) {
              if (present(presence, row)) {
                expectedAbsent[row >>> 6] |= 1L << (row & 63);
              }
            }
          }
          assertMaskAgrees(arena, pAbsent, values, presence, rows, expectedAbsent,
              op + " absent segment literal over " + rows + " rows");
        }
      }
    }
  }

  @Test
  @DisplayName("a per-cell string verdict memo is read from the lane, heap and slab alike")
  void segmentCellVerdictArmAgrees() {
    final Random rnd = new Random(13L);
    try (Arena arena = Arena.ofConfined()) {
      for (final int rows : ROW_COUNTS) {
        final int segment = 1;
        final long[] values = new long[rows];
        for (int i = 0; i < rows; i++) {
          values[i] = ProjectionIndexRowGroupPage.packSegmentCell(segment, rnd.nextInt(1, 12));
        }
        final long[] presence = randomPresence(rnd, rows);
        // Matches the even ids; a cell naming id 11 resolves to nothing, as a dangling cell would.
        final SegmentCellVerdicts.CellMatcher matcher = cell -> {
          final int id = ProjectionIndexRowGroupPage.idOfCell(cell);
          return id == 11
              ? null
              : (id & 1) == 0;
        };
        final ColumnPredicate p =
            ColumnPredicate.segmentCellVerdict(0, Op.STR_CONTAINS, "a".getBytes(StandardCharsets.UTF_8),
                new SegmentCellVerdicts(matcher, Op.STR_CONTAINS, "a".getBytes(StandardCharsets.UTF_8), segment + 1));
        final long[] expected = new long[stride(rows)];
        final boolean anyPresent = anyPresent(presence, rows);
        for (int row = 0; row < rows && anyPresent; row++) {
          final int id = ProjectionIndexRowGroupPage.idOfCell(values[row]);
          if (present(presence, row) && id != 11 && (id & 1) == 0) {
            expected[row >>> 6] |= 1L << (row & 63);
          }
        }
        // One memo per representation: a shared one would let the first pass settle the second's work.
        final ColumnPredicate pTwin =
            ColumnPredicate.segmentCellVerdict(0, Op.STR_CONTAINS, "a".getBytes(StandardCharsets.UTF_8),
                new SegmentCellVerdicts(matcher, Op.STR_CONTAINS, "a".getBytes(StandardCharsets.UTF_8), segment + 1));
        final String what = "cell verdict over " + rows + " rows";
        assertArrayEquals(expected, evaluate(pTwin, heapSlice(values, presence, rows), rows), "heap: " + what);
        assertArrayEquals(expected, evaluate(p, laneSlice(arena, values, presence, rows), rows), "lane: " + what);
      }
    }
  }

  @Test
  @DisplayName("KEY_IN reads the record keys off the lane, through the mask and through the walk")
  void keyInArmAgreesForBothTheMaskAndTheWalk() {
    try (Arena arena = Arena.ofConfined()) {
      for (final int rows : new int[] {1, 8, 64, 65, 200}) {
        // Mostly ascending with two order exceptions, which is what the merge walk has to survive.
        final long[] keys = new long[rows];
        for (int i = 0; i < rows; i++) {
          keys[i] = 100L + 2L * i;
        }
        if (rows > 4) {
          final long swap = keys[rows - 1];
          keys[rows - 1] = keys[rows / 2];
          keys[rows / 2] = swap;
        }
        final long[] present = allPresent(rows);
        // Strictly ascending and duplicate-free, whatever the row count collapses to.
        final long[] set =
            new LongOpenHashSet(new long[] {98L, keys[0], keys[rows / 2], keys[rows - 1], 1_000_000L}).toLongArray();
        Arrays.sort(set);
        final long[] expected = new long[stride(rows)];
        for (int row = 0; row < rows; row++) {
          if (Arrays.binarySearch(set, keys[row]) >= 0) {
            expected[row >>> 6] |= 1L << (row & 63);
          }
        }
        final String what = "KEY_IN over " + rows + " rows";
        // The walk: no precomputed masks.
        assertMaskAgrees(arena, ColumnPredicate.recordKeysIn(set), keys, present, rows, expected, "walk " + what);
        // The mapped form: the leaf's masks are keyed by its FIRST key, read off the lane.
        assertMaskAgrees(arena, ColumnPredicate.recordKeysIn(set, new long[][] {keys}), keys, present, rows, expected,
            "mask " + what);
        // A leaf no key of the set lives in: the mapped form answers "no rows" from one lane read.
        final long[] elsewhere = {7L, 9L};
        assertMaskAgrees(arena, ColumnPredicate.recordKeysIn(elsewhere, new long[][] {{7L, 9L}}), keys, present, rows,
            new long[stride(rows)], "foreign " + what);
      }
    }
  }

  @Test
  @DisplayName("filterMembership keeps the same rows from a slab lane as from a heap array")
  void filterMembershipAgreesBetweenLaneAndHeap() {
    final Random rnd = new Random(29L);
    try (Arena arena = Arena.ofConfined()) {
      final int rows = 130;
      final long[] values = new long[rows];
      for (int i = 0; i < rows; i++) {
        values[i] = rnd.nextLong(0L, 40L);
      }
      final long[] presence = randomPresence(rnd, rows);
      final long[] keys = new long[rows];
      for (int i = 0; i < rows; i++) {
        keys[i] = 1_000L + i;
      }
      final long[] selected = randomPresence(rnd, rows);
      final LongOpenHashSet wanted = new LongOpenHashSet(new long[] {3L, 11L, 12L, 29L});
      for (final boolean anti : new boolean[] {false, true}) {
        final long[] expected = new long[selected.length];
        boolean any = false;
        for (int row = 0; row < rows; row++) {
          if (!present(selected, row)) {
            continue;
          }
          final boolean member = present(presence, row) && wanted.contains(values[row]);
          if (anti != member) {
            expected[row >>> 6] |= 1L << (row & 63);
            any = true;
          }
        }
        final ProjectionRecordKeySet.Masks heap =
            ProjectionRecordKeySet.filterMembership(new long[][] {selected}, new long[][] {keys},
                new ColumnSlice[] {heapSlice(values, presence, rows)}, leaf -> leaf + 1L, wanted, anti);
        final ProjectionRecordKeySet.Masks lane =
            ProjectionRecordKeySet.filterMembership(new long[][] {selected}, new long[][] {keys},
                new ColumnSlice[] {laneSlice(arena, values, presence, rows)}, leaf -> leaf + 1L, wanted, anti);
        if (any) {
          assertArrayEquals(expected, heap.byFirstKey().get(keys[0]), "heap membership, anti=" + anti);
          assertArrayEquals(expected, lane.byFirstKey().get(keys[0]), "lane membership, anti=" + anti);
          assertEquals(heap.physicalSlots(), lane.physicalSlots(), "slots, anti=" + anti);
        } else {
          assertNull(lane.byFirstKey().get(keys[0]), "an all-empty leaf is not published, anti=" + anti);
          assertNull(heap.byFirstKey().get(keys[0]), "an all-empty leaf is not published, anti=" + anti);
        }
      }
    }
  }

  @Test
  @DisplayName("a selected row with no cell is a non-member: kept by the anti form only")
  void filterMembershipPlacesSelectedRowsWithoutACell() {
    try (Arena arena = Arena.ofConfined()) {
      final int rows = 4;
      // Row 0 holds a member, row 1 a non-member, rows 2 and 3 have no cell at all; every row is
      // selected. The semi form keeps row 0; the anti form keeps rows 1, 2 and 3.
      final long[] values = {7L, 8L, 0L, 0L};
      final long[] presence = {0b0011L};
      final long[] selected = {0b1111L};
      final long[] keys = {10L, 11L, 12L, 13L};
      final LongOpenHashSet wanted = new LongOpenHashSet(new long[] {7L});
      for (final boolean anti : new boolean[] {false, true}) {
        final long expected = anti
            ? 0b1110L
            : 0b0001L;
        for (final ColumnSlice slice : new ColumnSlice[] {heapSlice(values, presence, rows),
            laneSlice(arena, values, presence, rows)}) {
          final ProjectionRecordKeySet.Masks masks =
              ProjectionRecordKeySet.filterMembership(new long[][] {selected.clone()}, new long[][] {keys},
                  new ColumnSlice[] {slice}, leaf -> leaf + 1L, wanted, anti);
          assertArrayEquals(new long[] {expected}, masks.byFirstKey().get(keys[0]), "anti=" + anti);
        }
      }
    }
  }

  @Test
  @DisplayName("a column with no integral lane is refused by filterMembership, not silently skipped")
  void filterMembershipRefusesANonIntegralColumn() {
    final int rows = 4;
    final ColumnSlice booleans =
        new ColumnSlice(rows, NO_FLAGS, 0L, 1L, allPresent(rows), null, new long[] {0b1010L}, null, null, null);
    final IllegalArgumentException refused = assertThrows(IllegalArgumentException.class,
        () -> ProjectionRecordKeySet.filterMembership(new long[][] {allPresent(rows)}, new long[][] {{1L, 2L, 3L, 4L}},
            new ColumnSlice[] {booleans}, leaf -> leaf + 1L, new LongOpenHashSet(new long[] {1L}), false));
    assertTrue(refused.getMessage().contains("integral column"), refused.getMessage());
  }

  @Test
  @DisplayName("the bounded top-k heap selects the same rows from a slab lane as from a heap array")
  void topKHeapSelectsTheSameRowsFromALaneAsFromAHeapArray() {
    final Random rnd = new Random(31L);
    try (Arena arena = Arena.ofConfined()) {
      for (final boolean descending : new boolean[] {false, true}) {
        for (final int keyCount : new int[] {1, 2}) {
          final int rows = 97;
          final int k = 8;
          final long[][] values = new long[keyCount][rows];
          for (int kk = 0; kk < keyCount; kk++) {
            for (int row = 0; row < rows; row++) {
              // A small key domain forces ties, so the heap's document-rank tie-break is exercised.
              values[kk][row] = rnd.nextLong(0L, 12L);
            }
          }
          final long[] present = allPresent(rows);
          final byte[] kinds = new byte[keyCount];
          Arrays.fill(kinds, TopKHeap.KEY_NUMERIC);
          final boolean[] directions = new boolean[keyCount];
          Arrays.fill(directions, descending);
          final ColumnSlice[] heapSlices = new ColumnSlice[keyCount];
          final ColumnSlice[] laneSlices = new ColumnSlice[keyCount];
          for (int kk = 0; kk < keyCount; kk++) {
            heapSlices[kk] = heapSlice(values[kk], present, rows);
            laneSlices[kk] = laneSlice(arena, values[kk], present, rows);
          }
          final long[] fromHeap = topK(k, kinds, directions, heapSlices, rows);
          final long[] fromLane = topK(k, kinds, directions, laneSlices, rows);
          assertEquals(k, fromLane.length, "the heap keeps k rows");
          assertArrayEquals(fromHeap, fromLane, "keys=" + keyCount + " descending=" + descending);
          assertArrayEquals(oracleTopK(values, k, descending, rows), fromLane,
              "keys=" + keyCount + " descending=" + descending);
        }
      }
    }
  }

  @Test
  @DisplayName("the top-k heap refuses an offer that was never bound to a leaf")
  void topKHeapRefusesAnUnboundLeaf() {
    final TopKHeap heap = new TopKHeap(2, new byte[] {TopKHeap.KEY_NUMERIC}, new boolean[] {false},
        new GlobalValueDictionary.ReadView[1]);
    assertThrows(IllegalArgumentException.class, () -> heap.bindLeaf(new ColumnSlice[0]));
  }

  // ----------------------------------------------------------------------------------------------
  // fixtures and oracles
  // ----------------------------------------------------------------------------------------------

  /** The {@code k} best record keys of {@code slices}, offered row by row off the bound leaf. */
  private static long[] topK(final int k, final byte[] kinds, final boolean[] directions, final ColumnSlice[] slices,
      final int rows) {
    final TopKHeap heap = new TopKHeap(k, kinds, directions, new GlobalValueDictionary.ReadView[kinds.length]);
    heap.bindLeaf(slices);
    for (int row = 0; row < rows; row++) {
      heap.offer(row, 1_000L + row, row);
    }
    return heap.sortedRecordKeys();
  }

  /** The same selection by sorting every row: key order per direction, then document rank. */
  private static long[] oracleTopK(final long[][] values, final int k, final boolean descending, final int rows) {
    final Integer[] order = new Integer[rows];
    for (int row = 0; row < rows; row++) {
      order[row] = row;
    }
    Arrays.sort(order, (a, b) -> {
      for (final long[] key : values) {
        final int cmp = Long.compare(key[a], key[b]);
        if (cmp != 0) {
          return descending
              ? -cmp
              : cmp;
        }
      }
      return Integer.compare(a, b);
    });
    final long[] out = new long[Math.min(k, rows)];
    for (int i = 0; i < out.length; i++) {
      out[i] = 1_000L + order[i];
    }
    return out;
  }

  /** The predicate's mask over one leaf, as the scan kernels compute it. */
  private static long[] evaluate(final ColumnPredicate p, final ColumnSlice slice, final int rows) {
    final long[] mask = new long[(ProjectionIndexRowGroupPage.MAX_ROWS + 63) >>> 6];
    final int answered =
        ProjectionColumnScan.evaluateMask(new ColumnPredicate[] {p}, new ColumnSlice[][] {{slice}}, 0, rows, mask);
    return answered <= 0
        ? new long[stride(rows)]
        : Arrays.copyOf(mask, stride(rows));
  }

  /**
   * The heap array and the off-heap slab must answer {@code expected}, word for word, through the one
   * kernel.
   */
  private static void assertMaskAgrees(final Arena arena, final ColumnPredicate p, final long[] values,
      final long[] presence, final int rows, final long[] expected, final String what) {
    final long[] heap = evaluate(p, heapSlice(values, presence, rows), rows);
    final long[] lane = evaluate(p, laneSlice(arena, values, presence, rows), rows);
    assertArrayEquals(expected, heap, "heap: " + what);
    assertArrayEquals(expected, lane, "lane: " + what);
  }

  /** A slice whose values live in a heap array — a producer not (yet) decoding into a slab. */
  private static ColumnSlice heapSlice(final long[] values, final long[] presence, final int rows) {
    final long[] copy = Arrays.copyOf(values, rows);
    return new ColumnSlice(rows, NO_FLAGS, min(values, rows), max(values, rows), Arrays.copyOf(presence, stride(rows)),
        copy, null, null, null, null);
  }

  /** A slice whose values live in an off-heap slab at a non-zero base, with guard bytes around it. */
  private static ColumnSlice laneSlice(final Arena arena, final long[] values, final long[] presence, final int rows) {
    final MemorySegment slab = arena.allocate(LANE_BASE + 8L * rows + 64L, 64);
    slab.fill((byte) 0x5A); // a kernel reading the wrong base reads these, and fails
    for (int row = 0; row < rows; row++) {
      SegmentAccess.putLong(slab, LANE_BASE + ((long) row << 3), values[row]);
    }
    final NumericLane lane = new NumericLane(slab, LANE_BASE, rows);
    assertNotNull(lane.segment());
    final ColumnSlice slice = new ColumnSlice(rows, NO_FLAGS, min(values, rows), max(values, rows),
        Arrays.copyOf(presence, stride(rows)), lane);
    assertNull(slice.numericValues(), "a slab lane exposes no heap value array");
    return slice;
  }

  /** {@code op} as a predicate on column 0: the fused form for the BETWEEN ops, else single-bound. */
  private static ColumnPredicate betweenPredicate(final Op op, final long lit, final long high) {
    return switch (op) {
      case BETWEEN_GT_LT -> ColumnPredicate.numericBetween(0, Op.GT, lit, Op.LT, high);
      case BETWEEN_GT_LE -> ColumnPredicate.numericBetween(0, Op.GT, lit, Op.LE, high);
      case BETWEEN_GE_LT -> ColumnPredicate.numericBetween(0, Op.GE, lit, Op.LT, high);
      case BETWEEN_GE_LE -> ColumnPredicate.numericBetween(0, Op.GE, lit, Op.LE, high);
      default -> ColumnPredicate.numeric(0, op, lit);
    };
  }

  private static long[] oracleCompare(final long[] values, final long[] presence, final int rows, final Op op,
      final long lit, final long high) {
    final long[] out = new long[stride(rows)];
    for (int row = 0; row < rows; row++) {
      if (!present(presence, row)) {
        continue;
      }
      final long v = values[row];
      final boolean match = switch (op) {
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
        default -> throw new IllegalArgumentException("not a numeric op: " + op);
      };
      if (match) {
        out[row >>> 6] |= 1L << (row & 63);
      }
    }
    return out;
  }

  private static long[] randomPresence(final Random rnd, final int rows) {
    final long[] presence = new long[stride(rows)];
    for (int row = 0; row < rows; row++) {
      if (rnd.nextInt(8) != 0) { // ~1/8 of the rows are missing cells
        presence[row >>> 6] |= 1L << (row & 63);
      }
    }
    return presence;
  }

  private static long[] allPresent(final int rows) {
    final long[] presence = new long[stride(rows)];
    for (int row = 0; row < rows; row++) {
      presence[row >>> 6] |= 1L << (row & 63);
    }
    return presence;
  }

  private static boolean present(final long[] presence, final int row) {
    return (presence[row >>> 6] & 1L << (row & 63)) != 0L;
  }

  private static boolean anyPresent(final long[] presence, final int rows) {
    for (int row = 0; row < rows; row++) {
      if (present(presence, row)) {
        return true;
      }
    }
    return false;
  }

  private static int stride(final int rows) {
    return (rows + 63) >>> 6;
  }

  private static long min(final long[] values, final int rows) {
    long min = Long.MAX_VALUE;
    for (int i = 0; i < rows; i++) {
      min = Math.min(min, values[i]);
    }
    return min;
  }

  private static long max(final long[] values, final int rows) {
    long max = Long.MIN_VALUE;
    for (int i = 0; i < rows; i++) {
      max = Math.max(max, values[i]);
    }
    return max;
  }
}
