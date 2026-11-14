/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import static io.sirix.index.projection.NumericLaneTestSupport.copyNumericValues;

import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.projection.NumericLane;
import io.sirix.index.projection.ProjectionColumnStore;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.LaneTarget;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexRowGroupPage;
import io.sirix.node.SegmentAccess;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The executor's own numeric kernels read a column's values from the off-heap lane in place. Each
 * one is run here over the store's lane-backed slices AND over heap-array twins carrying exactly
 * the same values, presence words and zone bounds; the answers must be identical, and the lane run
 * must not copy a single lane to the heap (which the counted {@code numericValues()} seam would do
 * silently). The second half covers {@link SirixVectorizedExecutor.RouteLaneSlab}, the query-local
 * slab a group route decodes its masked lanes into: lazy, exact, and returned on every path.
 */
final class ProjectionLaneKernelParityTest {

  /** Two long columns: 0 holds the wide values, 1 a second operand for the binary fold. */
  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};

  // Fixture arrays are shared with the harness and are not compared by record equality
  @SuppressWarnings("ArrayRecordComponent")
  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher, long[][] values,
      boolean[][] present) {
  }

  private final MemorySegmentAllocator allocator = Allocators.getInstance();
  private long slabBytesBefore;
  private long previousBudget;
  private boolean previousResidency;

  @BeforeEach
  void setUp() {
    slabBytesBefore = allocator.getSlabBytes();
    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE >> 1);
    previousResidency = ProjectionColumnStore.setResidencyHeadroomForTesting(false);
  }

  @AfterEach
  void tearDown() {
    ProjectionColumnStore.setResidencyHeadroomForTesting(previousResidency);
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
  }

  private long slabDelta() {
    return allocator.getSlabBytes() - slabBytesBefore;
  }

  // ---------------------------------------------------------------- kernels

  @Test
  @DisplayName("the bitset count-distinct arm answers the same over a lane and over its array twin")
  void bitsetDistinctMatchesTheArrayVersion() {
    // A narrow span so the bitset arm is the one the executor would pick; repeats across leaves.
    final Fixture f = build(4, 90, 1_000L, 4_000L, 7L);
    final ColumnSlice[] lanes = f.store().column(0, f.fetcher());
    final long min = columnMin(lanes);
    final long span = columnMax(lanes) - min;
    final long fromLanes = SirixVectorizedExecutor.distinctViaBitset(lanes, min, span);
    assertNoHeapValues(lanes);
    assertEquals(expectedDistinct(f), fromLanes, "the lane answer is the model's");
    assertEquals(fromLanes, SirixVectorizedExecutor.distinctViaBitset(heapTwins(lanes), min, span));
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("the hash count-distinct arm answers the same over a lane and over its array twin")
  void hashDistinctMatchesTheArrayVersion() {
    final Fixture f = build(5, 120, Long.MIN_VALUE, Long.MAX_VALUE, 3L);
    final ColumnSlice[] lanes = f.store().column(0, f.fetcher());
    final long fromLanes = SirixVectorizedExecutor.distinctViaHashSet(lanes);
    assertNoHeapValues(lanes);
    assertEquals(expectedDistinct(f), fromLanes);
    assertEquals(fromLanes, SirixVectorizedExecutor.distinctViaHashSet(heapTwins(lanes)));
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("the binary fold answers the same over lanes and over array twins, for every operator")
  void binaryFoldMatchesTheArrayVersion() {
    final Fixture f = build(4, 70, -5_000L, 5_000L, 5L);
    final ColumnSlice[] left = f.store().column(0, f.fetcher());
    final ColumnSlice[] right = f.store().column(1, f.fetcher());
    final ColumnSlice[] leftHeap = heapTwins(left);
    final ColumnSlice[] rightHeap = heapTwins(right);
    for (final String op : new String[] {"+", "-", "*"}) {
      final long fromLanes = SirixVectorizedExecutor.foldBinaryNumeric(left, right, op);
      assertNoHeapValues(left);
      assertNoHeapValues(right);
      assertEquals(expectedFold(f, op), fromLanes, "operator " + op + " over the lanes");
      assertEquals(fromLanes, SirixVectorizedExecutor.foldBinaryNumeric(leftHeap, rightHeap, op),
          "operator " + op + " over the array twins");
    }
    // An unsupported operator declines on both representations, by the same sentinel.
    assertEquals(Long.MIN_VALUE, SirixVectorizedExecutor.foldBinaryNumeric(left, right, "/"));
    assertEquals(Long.MIN_VALUE, SirixVectorizedExecutor.foldBinaryNumeric(leftHeap, rightHeap, "/"));
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("an overflowing fold declines rather than wrapping, on lanes as on arrays")
  void anOverflowingFoldDeclinesOnBothRepresentations() {
    final Fixture f = build(1, 8, 1L << 40, (1L << 40) + 4L, 1L);
    final ColumnSlice[] operand = f.store().column(0, f.fetcher());
    assertEquals(Long.MIN_VALUE, SirixVectorizedExecutor.foldBinaryNumeric(operand, operand, "*"));
    assertEquals(Long.MIN_VALUE,
        SirixVectorizedExecutor.foldBinaryNumeric(heapTwins(operand), heapTwins(operand), "*"));
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  // ------------------------------------------------------------ route slabs

  @Test
  @DisplayName("a route with no keep mask and the empty plan allocate nothing and hand out no target")
  void aRouteWithoutAMaskAllocatesNothing() {
    final Fixture f = build(3, 50, 0L, 1_000L, 2L);
    assertNull(SirixVectorizedExecutor.RouteLaneSlab.NONE.target(0));
    SirixVectorizedExecutor.RouteLaneSlab.NONE.close();
    try (SirixVectorizedExecutor.RouteLaneSlab unmasked =
        SirixVectorizedExecutor.RouteLaneSlab.forColumns(f.store(), null, new int[] {0, 1})) {
      assertNull(unmasked.target(0), "without a keep mask the views are whole-column reads");
      assertEquals(0L, slabDelta());
    }
    try (SirixVectorizedExecutor.RouteLaneSlab empty =
        SirixVectorizedExecutor.RouteLaneSlab.forColumns(f.store(), new long[] {0b111L})) {
      assertNull(empty.target(0));
      assertEquals(0L, slabDelta());
    }
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("the plan allocates on its first target, lays every column out once, and gives the masked view its slab")
  void thePlanAllocatesLazilyAndFillsTheMaskedView() {
    final Fixture f = build(4, 60, 0L, 50_000L, 3L);
    final long[] keep = {0b1011L}; // leaves 0, 1, 3 survive
    try (SirixVectorizedExecutor.RouteLaneSlab plan =
        SirixVectorizedExecutor.RouteLaneSlab.forColumns(f.store(), keep, new int[] {0, 1}, new int[] {0, -1})) {
      assertEquals(0L, slabDelta(), "nothing is allocated before a column is resolved");
      final LaneTarget first = plan.target(0);
      assertNotNull(first);
      final long charged = slabDelta();
      assertTrue(charged > 0L, "the first target allocates the route's slab");
      final LaneTarget second = plan.target(0);
      assertNotNull(second);
      assertEquals(charged, slabDelta(), "a repeated column reuses its own lane region");
      assertNull(plan.target(-1), "a negative column index names no column");

      final ColumnSlice[] view = f.store().columnMaskedView(0, f.fetcher(), keep, first);
      assertTrue(first.filled(), "the masked fill decoded into the route's slab");
      assertFalse(first.resident());
      for (int leaf = 0; leaf < 4; leaf++) {
        if (leaf == 2) {
          assertEquals(0, view[leaf].rowCount(), "a pruned leaf is the rowless sentinel");
          continue;
        }
        final NumericLane lane = view[leaf].lane();
        assertNotNull(lane, "a surviving leaf reads the route's slab in place");
        for (int row = 0; row < f.values()[leaf].length; row++) {
          if (f.present()[leaf][row]) {
            assertEquals(f.values()[leaf][row], SegmentAccess.getLong(lane.segment(), lane.base() + 8L * row),
                "leaf " + leaf + " row " + row);
          }
        }
      }
      // The second column shares the one slab, at bases past the first column's lanes.
      final LaneTarget other = plan.target(1);
      assertNotNull(other);
      f.store().columnMaskedView(1, f.fetcher(), keep, other);
      assertTrue(other.filled());
      assertEquals(charged, slabDelta(), "both columns were laid out in the same allocation");
    }
    assertEquals(0L, slabDelta(), "closing the route returns its slab");
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("a resident column gets no target: it is served pruned from the store's own slab")
  void aResidentColumnIsNotPlanned() {
    final Fixture f = build(3, 40, 0L, 900L, 2L);
    final ColumnSlice[] resident = f.store().column(0, f.fetcher());
    assertTrue(f.store().columnFilled(0));
    final long residentBytes = slabDelta();
    final long[] keep = {0b101L};
    try (SirixVectorizedExecutor.RouteLaneSlab plan =
        SirixVectorizedExecutor.RouteLaneSlab.forColumns(f.store(), keep, new int[] {0})) {
      assertNull(plan.target(0), "the store already holds the column");
      assertEquals(residentBytes, slabDelta(), "a plan with nothing to fill allocates nothing");
      final ColumnSlice[] view = f.store().columnMaskedView(0, f.fetcher(), keep, plan.target(0));
      assertSame(resident[0], view[0], "a resident leaf is handed through, not decoded again");
      assertEquals(0, view[1].rowCount());
    }
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("closing a route twice returns its slab once, and a closed plan hands out no target")
  void closingIsIdempotent() {
    final Fixture f = build(3, 55, 0L, 20_000L, 2L);
    final long[] keep = {0b111L};
    final SirixVectorizedExecutor.RouteLaneSlab plan =
        SirixVectorizedExecutor.RouteLaneSlab.forColumns(f.store(), keep, new int[] {0});
    assertNotNull(plan.target(0));
    assertTrue(slabDelta() > 0L);
    plan.close();
    assertEquals(0L, slabDelta());
    plan.close();
    assertEquals(0L, slabDelta());
    assertNull(plan.target(0), "a closed route no longer hands out memory it has returned");
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("a keep mask too short for the store's leaves plans no slab")
  void aShortKeepMaskPlansNoSlab() {
    // The routes build their mask with predicateKeepMask, which always covers every leaf; the plan
    // refuses a shorter one rather than laying a lane out from a word it cannot read.
    final Fixture f = build(70, 20, 0L, 5_000L, 2L); // 70 leaves need two mask words
    final long[] shortKeep = {-1L}; // one word only: names no leaf past 63
    try (SirixVectorizedExecutor.RouteLaneSlab plan =
        SirixVectorizedExecutor.RouteLaneSlab.forColumns(f.store(), shortKeep, new int[] {0})) {
      assertNull(plan.target(0));
      assertEquals(0L, slabDelta());
    }
    final long[] fullKeep = {-1L, -1L};
    try (SirixVectorizedExecutor.RouteLaneSlab plan =
        SirixVectorizedExecutor.RouteLaneSlab.forColumns(f.store(), fullKeep, new int[] {0})) {
      assertNotNull(plan.target(0), "a mask covering every leaf is laid out");
      assertTrue(slabDelta() > 0L);
    }
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  // ------------------------------------------------------------- the model

  private static long columnMin(final ColumnSlice[] slices) {
    long min = Long.MAX_VALUE;
    for (final ColumnSlice slice : slices) {
      if (slice.rowCount() > 0) {
        min = Math.min(min, slice.min());
      }
    }
    return min;
  }

  private static long columnMax(final ColumnSlice[] slices) {
    long max = Long.MIN_VALUE;
    for (final ColumnSlice slice : slices) {
      if (slice.rowCount() > 0) {
        max = Math.max(max, slice.max());
      }
    }
    return max;
  }

  /** The distinct present values of column 0, straight from the fixture's own rows. */
  private static long expectedDistinct(final Fixture f) {
    final LongOpenHashSet seen = new LongOpenHashSet();
    for (int leaf = 0; leaf < f.values().length; leaf++) {
      for (int row = 0; row < f.values()[leaf].length; row++) {
        if (f.present()[leaf][row]) {
          seen.add(f.values()[leaf][row]);
        }
      }
    }
    return seen.size();
  }

  /** {@code sum(col0 OP col1)} over the rows where both are present, or the decline sentinel. */
  private static long expectedFold(final Fixture f, final String op) {
    long total = 0L;
    try {
      for (int leaf = 0; leaf < f.values().length; leaf++) {
        for (int row = 0; row < f.values()[leaf].length; row++) {
          if (!f.present()[leaf][row]) {
            continue;
          }
          final long l = f.values()[leaf][row];
          final long r = l + 1L;
          total = Math.addExact(total, switch (op) {
            case "+" -> Math.addExact(l, r);
            case "-" -> Math.subtractExact(l, r);
            default -> Math.multiplyExact(l, r);
          });
        }
      }
    } catch (final ArithmeticException _) {
      return Long.MIN_VALUE;
    }
    return total;
  }

  private static void assertNoHeapValues(final ColumnSlice[] slices) {
    for (final ColumnSlice slice : slices) {
      if (slice.rowCount() != 0) {
        assertNull(slice.numericValues(), "a slab lane exposes no heap value array");
        assertNotNull(slice.lane());
        assertTrue(slice.lane().segment().isNative());
      }
    }
  }

  /**
   * Heap-array twins of lane-backed slices: the same values, presence words and zone bounds, carried
   * as {@code long[]} the way every producer did before the lanes existed.
   */
  private static ColumnSlice[] heapTwins(final ColumnSlice[] slices) {
    final ColumnSlice[] twins = new ColumnSlice[slices.length];
    for (int i = 0; i < slices.length; i++) {
      final ColumnSlice slice = slices[i];
      if (slice.rowCount() == 0) {
        twins[i] = slice;
        continue;
      }
      final long[] values = copyNumericValues(slice);
      twins[i] = new ColumnSlice(slice.rowCount(), slice.flags(), slice.min(), slice.max(), slice.presenceWords(),
          values, null, null, null, null);
    }
    return twins;
  }

  // ------------------------------------------------------------ the fixture

  /**
   * {@code leaves} encoded row groups of two long columns: column 0 holds values drawn from
   * {@code [low, high]}, column 1 holds {@code column0 + 1}, and every {@code missingEvery}-th row of
   * both columns is absent, so every presence-gated loop sees holes.
   */
  private static Fixture build(final int leaves, final int rowsPerLeaf, final long low, final long high,
      final long missingEvery) {
    final Random rnd = new Random(20261106L);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(leaves);
    final long[][] values = new long[leaves][];
    final boolean[][] present = new boolean[leaves][];
    long nextOffset = 1_000L;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      final int rows = Math.max(1, rowsPerLeaf - leaf); // uneven leaves: distinct lane bases
      values[leaf] = new long[rows];
      present[leaf] = new boolean[rows];
      final long[] cells = new long[KINDS.length];
      final boolean[] bools = new boolean[KINDS.length];
      final String[] strings = new String[KINDS.length];
      final boolean[] cellPresent = new boolean[KINDS.length];
      long recordKey = leaf * 100_000L + 1L;
      for (int row = 0; row < rows; row++) {
        // The seed pins every row through this draw; nextLong(bound) would draw other values.
        @SuppressWarnings("java:S2140")
        final long value = low == Long.MIN_VALUE && high == Long.MAX_VALUE
            ? rnd.nextLong()
            : low + (long) Math.floor(rnd.nextDouble() * (high - low + 1));
        values[leaf][row] = value;
        present[leaf][row] = missingEvery <= 1 || (row % missingEvery) != 0;
        cells[0] = value;
        cells[1] = value + 1L;
        cellPresent[0] = present[leaf][row];
        cellPresent[1] = present[leaf][row];
        page.appendRow(recordKey++, cells, bools, strings, cellPresent, null);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          Objects.requireNonNull(ProjectionIndexColumnSegmentCodec.encode(page.serialize()));
      final int segments = encoded.columnSegmentIds().length;
      final int[] ids = new int[segments];
      final long[] offsets = new long[segments];
      for (int i = 0; i < segments; i++) {
        ids[i] = encoded.columnSegmentIds()[i];
        offsets[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1L + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), ids, offsets, new byte[ids.length][]));
    }
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        out[i] = segmentsByOffset.get(wanted[i]);
      }
      return out;
    };
    return new Fixture(new ProjectionColumnStore(directories), fetcher, values, present);
  }
}
