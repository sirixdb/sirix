/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.Allocators;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.LaneTarget;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.node.SegmentAccess;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.ref.Reference;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Lifetime of the off-heap column lanes (docs/DERIVED_QUERY_CACHES.md, "Column lanes"): a numeric
 * column decodes into one allocator slab that the store owns, and the slab is returned to the
 * allocator exactly once, on every path — a residency release at a query-scope exit, the handle's
 * retirement, the last pin of a retired column, a decode failure, a lost publish race — and never
 * while a query holds the column. Every assertion is a byte delta against the process allocator.
 */
final class ColumnSlabLifetimeTest {

  /** Two long columns: column 0 wide random values, column 1 a constant (so 0 is the fatter one). */
  private static final byte[] KINDS =
      {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG};

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher, Map<Long, byte[]> segments,
      List<RowGroupDirectory> directories) {
  }

  private final MemorySegmentAllocator allocator = Allocators.getInstance();
  private long slabBytesBefore;
  private long previousHeadroom;
  private long previousBudget;
  private boolean previousResidency;
  private boolean previousEvict;

  @BeforeEach
  void setUp() {
    slabBytesBefore = allocator.getSlabBytes();
    previousHeadroom = HeapHeadroom.setHeadroomForTesting(-1L);
    previousBudget = ProjectionColumnStore.setColumnFillBudgetBytesForTesting(Long.MAX_VALUE >> 1);
    previousResidency = ProjectionColumnStore.setResidencyHeadroomForTesting(false);
    previousEvict = ProjectionColumnStore.setResidencyEvictForTesting(true);
  }

  @AfterEach
  void tearDown() {
    ProjectionColumnStore.setResidencyEvictForTesting(previousEvict);
    ProjectionColumnStore.setResidencyHeadroomForTesting(previousResidency);
    ProjectionColumnStore.setColumnFillBudgetBytesForTesting(previousBudget);
    HeapHeadroom.setHeadroomForTesting(previousHeadroom);
    ProjectionColumnStore.sampleHeadroomShare();
  }

  private long slabDelta() {
    return allocator.getSlabBytes() - slabBytesBefore;
  }

  /** The slab one numeric column of {@code f} fills: every leaf's lane at a 64-byte aligned base. */
  private static long slabBytes(final Fixture f) {
    long total = 0L;
    for (int leaf = 0; leaf < f.store().leafCount(); leaf++) {
      total += (8L * f.store().rowCount(leaf) + 63L) & ~63L;
    }
    return total;
  }

  /** What the allocator charges for one slab of {@code bytes}: whole pages. */
  private long charged(final long bytes) {
    final long live = allocator.getSlabBytes();
    final MemorySegment probe = allocator.allocateSlab(bytes);
    assertNotNull(probe);
    final long charged = allocator.getSlabBytes() - live;
    allocator.releaseSlab(probe);
    return charged;
  }

  private static void shareOf(final long bytes) {
    ProjectionColumnStore.setResidencyHeadroomForTesting(true);
    HeapHeadroom.setHeadroomForTesting(Math.max(0L, bytes) * 4L);
    assertEquals(bytes, ProjectionColumnStore.sampleHeadroomShare(), "the share is a quarter of the headroom");
  }

  @Test
  void allocatorRefusalDeclinesFullAndMaskedNumericFillsWithoutHeapFallback() {
    final Fixture full = build(2, 200);
    final Fixture masked = build(2, 200);
    final MemorySegmentAllocator refusing = mock(MemorySegmentAllocator.class);
    final long declined = ProjectionColumnStore.slabFillsDeclinedCount();
    try (MockedStatic<Allocators> allocators = mockStatic(Allocators.class)) {
      allocators.when(Allocators::getInstance).thenReturn(refusing);
      assertThrows(ProjectionColumnStore.FillBudgetExceededException.class,
          () -> full.store().column(0, full.fetcher()));
      assertThrows(ProjectionColumnStore.FillBudgetExceededException.class,
          () -> masked.store().columnMasked(0, masked.fetcher(), new long[] {1L}));
    }
    assertFalse(full.store().columnFilled(0));
    assertFalse(masked.store().columnFilled(0));
    assertFalse(full.store().columnKnownCorrupt(0));
    assertFalse(masked.store().columnKnownCorrupt(0));
    assertEquals(declined + 2, ProjectionColumnStore.slabFillsDeclinedCount());
    assertEquals(0L, slabDelta());
  }

  @Test
  void directMaskedReadersOwnTheirSlabsAcrossUnrelatedScopeClosure() {
    for (final boolean maskedView : new boolean[] {false, true}) {
      final Fixture unrelated = build(2, 200);
      final Fixture direct = build(3, 200);
      final MemorySegmentAllocator observed = mock(MemorySegmentAllocator.class);
      final List<MemorySegment> allocations = new ArrayList<>();
      try (Arena arena = Arena.ofConfined(); MockedStatic<Allocators> allocators = mockStatic(Allocators.class)) {
        allocators.when(Allocators::getInstance).thenReturn(observed);
        when(observed.allocateSlab(anyLong())).thenAnswer(invocation -> {
          final long bytes = invocation.getArgument(0);
          final MemorySegment memory = arena.allocate(bytes, 64);
          allocations.add(memory);
          return memory;
        });
        final ProjectionResidencyScope other = ProjectionResidencyScope.open();
        try {
          unrelated.store().column(0, unrelated.fetcher());
          final ColumnSlice[] slices = maskedView
              ? direct.store().columnMaskedView(0, direct.fetcher(), new long[] {1L}, new LaneTarget())
              : direct.store().columnMasked(0, direct.fetcher(), new long[] {1L});
          assertEquals(2, allocations.size());
          final MemorySegment directAllocation = allocations.get(1);
          assertFalse(direct.store().columnFilled(0));
          assertNotNull(slices[0].lane());
          assertNull(slices[0].numericValues());
          assertEquals(0, slices[1].rowCount());
          assertEquals(0, slices[2].rowCount());
          other.close();
          unrelated.store().retire();
          verify(observed).releaseSlab(allocations.getFirst());
          verify(observed, never()).releaseSlab(directAllocation);
          final long[] expected = heapDecode(direct, 0, 0).numericValues();
          for (int row = 0; row < slices[0].rowCount(); row++) {
            assertEquals(expected[row],
                SegmentAccess.getLong(slices[0].laneSegment(), slices[0].laneBase() + 8L * row));
          }
          Reference.reachabilityFence(slices);
        } finally {
          other.close();
          unrelated.store().retire();
          direct.store().retire();
        }
      }
    }
  }

  @Test
  @DisplayName("a fill decodes every leaf into one slab the slices read in place")
  void aFillDecodesEveryLeafIntoOneSlabReadInPlace() {
    final Fixture f = build(5, 200);
    final ColumnSlice[] slices = f.store().column(0, f.fetcher());
    assertEquals(charged(slabBytes(f)), slabDelta(), "one slab, every leaf's lane 64-byte aligned");
    long expectedBase = 0L;
    for (int leaf = 0; leaf < slices.length; leaf++) {
      final ColumnSlice slice = slices[leaf];
      final NumericLane lane = slice.lane();
      assertNotNull(lane, "a long-lane column carries no heap value array");
      assertNull(slice.numericValues());
      assertTrue(lane.segment().isNative());
      assertEquals(expectedBase, lane.base(), "leaf " + leaf);
      expectedBase += (8L * slice.rowCount() + 63L) & ~63L;
      final long[] heap = heapDecode(f, leaf, 0).numericValues();
      assertNotNull(heap);
      for (int row = 0; row < slice.rowCount(); row++) {
        assertEquals(heap[row], SegmentAccess.getLong(slice.laneSegment(), slice.laneBase() + 8L * row));
      }
    }
    f.store().retire();
    assertEquals(0L, slabDelta(), "retirement returns the unpinned slab");
  }

  @Test
  @DisplayName("a query-scope exit over the headroom share unmaps the released column's slab")
  void aScopeExitOverTheShareUnmapsTheSlab() {
    final Fixture f = build(6, 200);
    final long a = f.store().projectedColumnFillBytes(0);
    final long b = f.store().projectedColumnFillBytes(1);
    shareOf(a + b);
    try (ProjectionResidencyScope scope = ProjectionResidencyScope.open()) {
      assertNotNull(scope);
      f.store().column(0, f.fetcher());
      f.store().column(1, f.fetcher());
    }
    assertEquals(2 * charged(slabBytes(f)), slabDelta(), "within the share both slabs stay");
    shareOf(b);
    try (ProjectionResidencyScope scope = ProjectionResidencyScope.open()) {
      assertNotNull(scope);
      assertTrue(f.store().columnFilled(1), "a query that touched the store sweeps it at its exit");
    }
    assertFalse(f.store().columnFilled(0), "the fatter column left at the exit");
    assertTrue(f.store().columnFilled(1));
    assertEquals(charged(slabBytes(f)), slabDelta(), "its slab went back to the allocator");
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("a column another open query pins is kept, and released when that query leaves")
  void aColumnAnotherQueryPinsIsKept() {
    final Fixture f = build(4, 100);
    shareOf(f.store().projectedColumnFillBytes(0));
    final ProjectionResidencyScope reader = ProjectionResidencyScope.open();
    try {
      f.store().column(0, f.fetcher());
      assertEquals(1, f.store().residencyPins(0));
      shareOf(0L);
      try (ProjectionResidencyScope other = ProjectionResidencyScope.open()) {
        assertNotNull(other);
        assertTrue(f.store().columnFilled(0), "the other query observes the column resident and leaves");
      }
      assertTrue(f.store().columnFilled(0), "the reading query still holds the column");
      assertEquals(charged(slabBytes(f)), slabDelta(), "a pinned slab is never unmapped");
    } finally {
      reader.close(); // a scope left open would pin every later fill in this JVM
    }
    assertEquals(0, f.store().residencyPins(0));
    assertFalse(f.store().columnFilled(0), "the last query's exit releases it");
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("an entry pin survives every scope exit until it is released")
  void anEntryPinSurvivesScopeExitsUntilReleased() {
    final Fixture f = build(4, 100);
    shareOf(f.store().projectedColumnFillBytes(0));
    try (ProjectionResidencyScope scope = ProjectionResidencyScope.open()) {
      assertNotNull(scope);
      f.store().column(0, f.fetcher());
      f.store().retainColumn(0);
    }
    shareOf(0L);
    try (ProjectionResidencyScope scope = ProjectionResidencyScope.open()) {
      assertNotNull(scope);
      assertTrue(f.store().columnFilled(0), "a query observes the column and leaves");
    }
    assertTrue(f.store().columnFilled(0), "a cache entry's pin keeps the column through scope exits");
    assertEquals(charged(slabBytes(f)), slabDelta());
    f.store().releaseColumn(0);
    assertEquals(0, f.store().residencyPins(0));
    try (ProjectionResidencyScope scope = ProjectionResidencyScope.open()) {
      assertNotNull(scope);
      assertTrue(f.store().columnFilled(0), "the next query still finds it resident…");
    }
    assertFalse(f.store().columnFilled(0), "…and its exit releases it, nothing pinning it any more");
    assertEquals(0L, slabDelta());
    assertThrows(IllegalStateException.class, () -> f.store().releaseColumn(0), "more releases than retains");
  }

  @Test
  @DisplayName("retirement frees unpinned columns now and pinned ones at their last unpin")
  void retireFreesUnpinnedColumnsNowAndPinnedOnesAtTheirLastUnpin() {
    final Fixture f = build(5, 150);
    final long oneSlab = charged(slabBytes(f));
    try (ProjectionResidencyScope scope = ProjectionResidencyScope.open()) {
      assertNotNull(scope);
      f.store().column(0, f.fetcher());
      f.store().column(1, f.fetcher());
      f.store().recordKeys(f.fetcher());
      f.store().retainColumn(1);
    }
    assertEquals(2 * oneSlab, slabDelta(), "without the headroom gate nothing leaves at a scope exit");
    final long retainedBefore = f.store().retainedFillBytes();
    assertTrue(retainedBefore > 0L);
    f.store().retire();
    assertTrue(f.store().retired());
    assertFalse(f.store().columnFilled(0), "the unpinned column left with the handle");
    assertFalse(f.store().recordKeysFilled(), "so did the unpinned KEYS lane");
    assertTrue(f.store().columnFilled(1), "the pinned column stays for its reader");
    assertEquals(oneSlab, slabDelta());
    assertEquals(f.store().projectedColumnFillBytes(1), f.store().retainedFillBytes(),
        "the ledger keeps only what is still resident");
    f.store().retire(); // idempotent
    assertEquals(oneSlab, slabDelta());
    f.store().releaseColumn(1);
    assertFalse(f.store().columnFilled(1), "the last unpin of a retired column releases it");
    assertEquals(0L, slabDelta());
    assertEquals(0L, f.store().retainedFillBytes());
    // A retired store still serves a query that holds its handle; what it fills leaves at the pin drop.
    try (ProjectionResidencyScope scope = ProjectionResidencyScope.open()) {
      assertNotNull(scope);
      final ColumnSlice[] again = f.store().column(0, f.fetcher());
      assertNotNull(again[0].lane());
      assertEquals(oneSlab, slabDelta());
    }
    assertEquals(0L, slabDelta(), "the scope's pin drop released the refill of a retired store");
    assertEquals(0L, f.store().retainedFillBytes());
  }

  @Test
  @DisplayName("a fill that fails releases its slab and poisons nothing it did not decode")
  void aFillThatFailsReleasesItsSlab() {
    final Fixture f = build(4, 100);
    final long bodyOffset = segmentOffset(f, 1, 0);
    final ColumnSegmentFetcher corrupting = offsets -> {
      final byte[][] bytes = f.fetcher().fetchAll(offsets);
      for (int i = 0; i < offsets.length; i++) {
        if (offsets[i] == bodyOffset && bytes[i] != null) {
          bytes[i] = bytes[i].clone();
          bytes[i][bytes[i].length - 1] ^= 0x40;
        }
      }
      return bytes;
    };
    assertThrows(IllegalStateException.class, () -> f.store().column(0, corrupting));
    assertFalse(f.store().columnFilled(0));
    assertEquals(0L, slabDelta(), "a failed fill maps nothing that stays");
    assertEquals(0L, f.store().retainedFillBytes());
    assertNotNull(f.store().column(1, f.fetcher()), "the other column is unaffected");
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("a lost publish race releases the loser's slab and both callers read the winner's")
  void aLostPublishRaceReleasesTheLosersSlab() throws Exception {
    final Fixture f = build(4, 100);
    final CountDownLatch bothFetched = new CountDownLatch(2);
    final ColumnSegmentFetcher gated = offsets -> {
      final byte[][] bytes = f.fetcher().fetchAll(offsets);
      bothFetched.countDown();
      try {
        bothFetched.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
      return bytes;
    };
    final ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      final Future<ColumnSlice[]> first = pool.submit(() -> f.store().column(0, gated));
      final Future<ColumnSlice[]> second = pool.submit(() -> f.store().column(0, gated));
      assertSame(first.get(), second.get(), "first publish wins, the loser serves the winner's lanes");
    } finally {
      pool.shutdownNow();
    }
    assertEquals(charged(slabBytes(f)), slabDelta(), "exactly one slab survives the race");
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("a masked view decodes the surviving leaves into the caller's target, or reports resident")
  void aMaskedViewDecodesIntoTheCallersTarget() {
    final Fixture f = build(5, 120);
    final long[] keep = {0b10101L}; // leaves 0, 2, 4 survive
    final long[] bases = new long[5];
    final long bytes = f.store().maskedLaneLayout(0, keep, bases, 128L);
    assertEquals(128L, bases[0]);
    assertEquals(-1L, bases[1]);
    assertEquals(128L + ((8L * f.store().rowCount(0) + 63L) & ~63L), bases[2]);
    assertEquals(-1L, bases[3]);
    assertEquals(bases[2] + ((8L * f.store().rowCount(2) + 63L) & ~63L), bases[4]);
    assertEquals(bases[4] + ((8L * f.store().rowCount(4) + 63L) & ~63L), bytes);
    final MemorySegment slab = allocator.allocateSlab(bytes);
    assertNotNull(slab);
    try {
      final LaneTarget target = new LaneTarget(slab, bases);
      final ColumnSlice[] view = f.store().columnMaskedView(0, f.fetcher(), keep, target);
      assertTrue(target.filled());
      assertFalse(target.resident());
      assertFalse(f.store().columnFilled(0), "a masked fill retains nothing");
      for (int leaf = 0; leaf < 5; leaf++) {
        if (bases[leaf] < 0) {
          assertEquals(0, view[leaf].rowCount(), "a pruned leaf is the rowless sentinel");
          continue;
        }
        final NumericLane lane = view[leaf].lane();
        assertNotNull(lane);
        assertEquals(bases[leaf], lane.base());
        final long[] heap = heapDecode(f, leaf, 0).numericValues();
        assertNotNull(heap);
        for (int row = 0; row < heap.length; row++) {
          assertEquals(heap[row], SegmentAccess.getLong(slab, bases[leaf] + 8L * row));
        }
      }
      f.store().column(0, f.fetcher());
      final LaneTarget resident = new LaneTarget(slab, bases);
      final ColumnSlice[] pruned = f.store().columnMaskedView(0, f.fetcher(), keep, resident);
      assertTrue(resident.resident());
      assertFalse(resident.filled());
      assertSame(f.store().column(0, f.fetcher())[0], pruned[0], "a resident column is served pruned, in place");
      assertEquals(0, pruned[1].rowCount());
    } finally {
      allocator.releaseSlab(slab);
    }
    f.store().retire();
    assertEquals(0L, slabDelta());
  }

  @Test
  @DisplayName("the cleaner backstop returns what a store never did, exactly once")
  void theBackstopReleasesWhatAStoreNeverReturned() {
    final ColumnSlabs slabs = new ColumnSlabs(2);
    final MemorySegment slab = allocator.allocateSlab(4096);
    assertNotNull(slab);
    slabs.set(1, slab);
    assertThrows(IllegalStateException.class, () -> slabs.set(1, slab));
    assertSame(slab, slabs.get(1));
    assertTrue(slabDelta() > 0L);
    slabs.run();
    assertEquals(0L, slabDelta());
    assertNull(slabs.take(1));
    slabs.run(); // nothing left: idempotent
    assertEquals(0L, slabDelta());
  }

  private static ColumnSlice heapDecode(final Fixture f, final int leaf, final int col) {
    final RowGroupDirectory directory = f.directories().get(leaf);
    return ProjectionIndexColumnSegmentCodec.decodeBodySlice(directory.descriptor(),
        f.segments().get(segmentOffset(f, leaf, col)), col);
  }

  /** The persisted offset of {@code col}'s BODY segment on {@code leaf}. */
  private static long segmentOffset(final Fixture f, final int leaf, final int col) {
    final RowGroupDirectory directory = f.directories().get(leaf);
    final int id = ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col);
    for (int i = 0; i < directory.columnSegmentIds().length; i++) {
      if (directory.columnSegmentIds()[i] == id) {
        return directory.columnSegmentOffsets()[i];
      }
    }
    throw new IllegalStateException("no BODY segment for column " + col + " on leaf " + leaf);
  }

  private static Fixture build(final int leaves, final int rowsPerLeaf) {
    final Random rnd = new Random(20261106L);
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(leaves);
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < leaves; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(KINDS.clone());
      final long[] longs = new long[KINDS.length];
      final boolean[] bools = new boolean[KINDS.length];
      final String[] strings = new String[KINDS.length];
      long recordKey = leaf * 100_000L + 1;
      for (int row = 0; row < rowsPerLeaf - leaf; row++) { // uneven leaves: distinct lane bases
        longs[0] = rnd.nextLong();
        longs[1] = 42L;
        page.appendRow(recordKey++, longs, bools, strings);
      }
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int segments = encoded.columnSegmentIds().length;
      final int[] ids = new int[segments];
      final long[] offsets = new long[segments];
      for (int i = 0; i < segments; i++) {
        ids[i] = encoded.columnSegmentIds()[i];
        offsets[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
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
    return new Fixture(new ProjectionColumnStore(directories), fetcher, segmentsByOffset, directories);
  }
}
