/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.Allocators;
import io.sirix.cache.FrameSlotAllocator;
import io.sirix.cache.ScratchLease;
import io.sirix.cache.ScratchStats;
import io.sirix.cache.ScratchUse;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A grouped scan's working memory is borrowed from the allocator and returned with the scan. Group
 * tables used to keep their chunks as heap arrays in free lists that outlived the scan and held up
 * to a quarter of the heap; a {@link GroupWorkspace} borrows regions for one scan, carves zeroed
 * chunks out of them, takes chunks back for reuse within the scan and returns every region on
 * close.
 *
 * <p>
 * The assertions are deltas of the process allocator's counters for {@link ScratchUse#AGGREGATION}
 * and of its lent-buffer gauge, so the class runs isolated and first lets the self-returning
 * workspaces earlier tests dropped go back; nothing measures time.
 */
@Isolated
final class GroupWorkspaceTest {

  /** Lanes of a full group-table chunk: 128 KiB. */
  private static final int FULL_CHUNK = 1 << 14;

  @BeforeAll
  static void initializeAllocator() {
    assertTrue(Allocators.getInstance() instanceof FrameSlotAllocator);
    Allocators.getInstance().init(8L * 1024 * 1024 * 1024);
  }

  /**
   * A table or a spill a test built without a workspace took a self-returning one, which a cleaner
   * thread returns whenever the collector finds it unreachable. Let that finish before a test reads
   * the lent-buffer gauge: collect until every workspace is back, or until collecting changes nothing
   * any more (a workspace some other test still holds stays out, and stays put).
   */
  @BeforeEach
  void settle() throws InterruptedException {
    letDroppedWorkspacesReturn();
  }

  /** See {@link #settle()}; shared with the other tests that read the process allocator's figures. */
  static void letDroppedWorkspacesReturn() throws InterruptedException {
    int unchangedRounds = 0;
    long closed = GroupWorkspace.closedCount();
    while (GroupWorkspace.openedCount() != closed && unchangedRounds < 5) {
      System.gc();
      Thread.sleep(20);
      final long now = GroupWorkspace.closedCount();
      unchangedRounds = now == closed
          ? unchangedRounds + 1
          : 0;
      closed = now;
    }
  }

  private static ScratchStats stats() {
    return Allocators.getInstance().scratchStats();
  }

  private static long borrows(final ScratchStats stats) {
    return stats.acquisitions(ScratchUse.AGGREGATION);
  }

  private static void assertAllZero(final GroupBuffer chunk, final String what) {
    final int lanes = GroupLanes.count(chunk);
    for (int lane = 0; lane < lanes; lane++) {
      if (GroupLanes.get(chunk, lane) != 0L) {
        throw new AssertionError(what + ": lane " + lane + " of " + lanes + " holds " + GroupLanes.get(chunk, lane));
      }
    }
  }

  /** Leave 0x5A in whatever the allocator has idle of the capacities a workspace borrows. */
  private static void dirtyIdleScratch() {
    for (final long capacity : new long[] {GroupWorkspace.FIRST_REGION_BYTES, 128L << 10, 256L << 10, 512L << 10,
        1L << 20, GroupWorkspace.MAX_REGION_BYTES}) {
      final List<ScratchLease> held = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        final ScratchLease lease = ScratchLease.of(capacity, ScratchUse.AGGREGATION);
        lease.segment().fill((byte) 0x5A);
        held.add(lease);
      }
      for (final ScratchLease lease : held) {
        lease.close();
      }
    }
  }

  @Test
  void anOpenWorkspaceBorrowsNothingUntilItIsAskedAndReturnsEverythingOnClose() {
    final ScratchStats before = stats();
    final long openedBefore = GroupWorkspace.openedCount();
    final long closedBefore = GroupWorkspace.closedCount();
    final GroupWorkspace workspace = new GroupWorkspace();
    assertEquals(0, borrows(stats()) - borrows(before), "an idle workspace holds no memory");
    assertEquals(0, workspace.regions());

    final GroupBuffer small = workspace.newBlock(10);
    assertEquals(10, GroupLanes.count(small));
    assertTrue(small.isNative());
    assertEquals(1, workspace.regions());
    assertEquals(GroupWorkspace.FIRST_REGION_BYTES, workspace.borrowedBytes(), "the first region is the small one");
    assertEquals(before.totalLentBuffers() + 1, stats().totalLentBuffers());

    // 512 KiB of full chunks: the regions double from 64 KiB, each chunk is carved from one region.
    final List<GroupBuffer> chunks = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      chunks.add(workspace.takeChunk(FULL_CHUNK));
    }
    assertEquals(4, workspace.chunksCarved());
    assertTrue(workspace.regions() >= 3, "64 KiB cannot hold a chunk: the workspace borrowed larger regions");
    assertEquals(workspace.regions(), stats().totalLentBuffers() - before.totalLentBuffers());
    assertEquals(workspace.regions(), borrows(stats()) - borrows(before), "one borrow per region");
    for (int i = 0; i < chunks.size(); i++) {
      for (int j = i + 1; j < chunks.size(); j++) {
        assertEquals(0L,
            chunks.get(i).segment.asOverlappingSlice(chunks.get(j).segment).map(MemorySegment::byteSize).orElse(0L),
            "chunks " + i + " and " + j + " overlap");
      }
    }

    workspace.close();
    assertTrue(workspace.isClosed());
    assertEquals(0, workspace.regions());
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(), "closing returned every region");
    workspace.close();
    assertEquals(0, stats().staleReleases() - before.staleReleases(), "a repeated close returns nothing twice");
    assertEquals(openedBefore + 1, GroupWorkspace.openedCount());
    assertEquals(closedBefore + 1, GroupWorkspace.closedCount());
    assertThrows(IllegalStateException.class, () -> workspace.takeChunk(FULL_CHUNK));
    assertThrows(IllegalStateException.class, () -> workspace.newBlock(1));
    workspace.giveChunk(chunks.get(0)); // ignored: the memory is the allocator's again
  }

  @Test
  void everythingHandedOutIsZeroWhateverTheAllocatorsMemoryHeldBefore() {
    dirtyIdleScratch();
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      assertAllZero(workspace.newBlock(7), "a small block");
      // Past several region boundaries, so every region size a workspace borrows is carved from.
      final List<GroupBuffer> chunks = new ArrayList<>();
      for (int i = 0; i < 40; i++) {
        final GroupBuffer chunk = workspace.takeChunk(FULL_CHUNK);
        assertAllZero(chunk, "carved chunk " + i);
        chunks.add(chunk);
      }
      assertAllZero(workspace.takeChunk(48), "a small table's chunk");
      // 64 KiB for the block, then 128 KiB, 256 KiB, 512 KiB, 1 MiB and two regions of 2 MiB for the
      // forty chunks and the small one: the regions double up to their full size and stay there.
      assertEquals(7, workspace.regions());
      assertEquals((64L << 10) + (128L << 10) + (256L << 10) + (512L << 10) + (1L << 20) + 2 * (2L << 20),
          workspace.borrowedBytes());

      // A chunk a table filled and handed back is empty for the next table.
      final GroupBuffer used = chunks.get(3);
      for (int lane = 0; lane < FULL_CHUNK; lane++) {
        GroupLanes.set(used, lane, 0x0123_4567_89AB_CDEFL + lane);
      }
      workspace.giveChunk(used);
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(used, 0));
      final GroupBuffer again = workspace.takeChunk(FULL_CHUNK);
      assertNotSame(used, again, "reuse must create a new ownership generation");
      assertEquals(used.address, again.address, "the backing memory is reused");
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(used, 0));
      assertAllZero(again, "a reused chunk");
    }
  }

  @Test
  void chunksAreReusedByLaneCountAndNeverAcrossLaneCounts() {
    final ScratchStats before = stats();
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupBuffer full = workspace.takeChunk(FULL_CHUNK);
      final GroupBuffer ramp = workspace.takeChunk(96);
      final GroupBuffer index = workspace.takeChunk(4096);
      workspace.giveChunk(full);
      workspace.giveChunk(ramp);
      workspace.giveChunk(index);
      assertEquals(1, workspace.spareChunks(FULL_CHUNK));
      assertEquals(1, workspace.spareChunks(96));
      assertEquals(1, workspace.spareChunks(4096));
      assertEquals(0, workspace.spareChunks(97));

      final GroupBuffer other = workspace.takeChunk(192);
      assertNotSame(ramp, other, "a request of another lane count carves its own chunk");
      assertEquals(192, GroupLanes.count(other));
      assertEquals(index.address, workspace.takeChunk(4096).address);
      assertEquals(ramp.address, workspace.takeChunk(96).address);
      assertEquals(full.address, workspace.takeChunk(FULL_CHUNK).address);
      assertEquals(3, workspace.chunksReused());
      assertEquals(4, workspace.chunksCarved());
      assertEquals(3, workspace.chunksGiven());

      // A pass's worth of tables built twice: the second time from the chunks the first handed back.
      final long borrowedBefore = workspace.borrowedBytes();
      final List<GroupBuffer> pass = new ArrayList<>();
      for (int i = 0; i < 24; i++) {
        pass.add(workspace.takeChunk(FULL_CHUNK));
      }
      final long borrowedByOnePass = workspace.borrowedBytes();
      assertTrue(borrowedByOnePass > borrowedBefore);
      for (final GroupBuffer chunk : pass) {
        workspace.giveChunk(chunk);
      }
      final long carved = workspace.chunksCarved();
      final long regionBorrows = borrows(stats());
      for (int i = 0; i < 24; i++) {
        workspace.takeChunk(FULL_CHUNK);
      }
      assertEquals(carved, workspace.chunksCarved(), "the second pass carved nothing");
      assertEquals(borrowedByOnePass, workspace.borrowedBytes(), "and borrowed nothing");
      assertEquals(regionBorrows, borrows(stats()));
    }
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers());
  }

  @Test
  void argumentsAreChecked() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      assertThrows(IllegalArgumentException.class, () -> workspace.takeChunk(0));
      assertThrows(IllegalArgumentException.class, () -> workspace.takeChunk(-1));
      assertThrows(IllegalArgumentException.class, () -> workspace.newBlock(0));
      assertThrows(NullPointerException.class, () -> workspace.giveChunk(null));
      assertFalse(workspace.isClosed());
    }
    assertThrows(NullPointerException.class, () -> new GroupWorkspace(null));
  }

  @Test
  void workersTakeAndGiveChunksConcurrentlyWithoutSharingOne() throws Exception {
    final ScratchStats before = stats();
    final int workers = 8;
    final int rounds = 300;
    final ExecutorService pool = Executors.newFixedThreadPool(workers);
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final CountDownLatch start = new CountDownLatch(1);
      final List<Future<Long>> results = new ArrayList<>();
      for (int w = 0; w < workers; w++) {
        final long stamp = 0x1000L * (w + 1);
        results.add(pool.submit(() -> {
          start.await();
          long mismatches = 0L;
          for (int round = 0; round < rounds; round++) {
            final GroupBuffer chunk = workspace.takeChunk(FULL_CHUNK);
            // Whoever holds a chunk is its only writer: a chunk handed to two workers shows the
            // other's stamp, a chunk reused without being cleared shows an old one.
            for (int lane = 0; lane < FULL_CHUNK; lane += 257) {
              if (GroupLanes.get(chunk, lane) != 0L) {
                mismatches++;
              }
              GroupLanes.set(chunk, lane, stamp + round);
            }
            for (int lane = 0; lane < FULL_CHUNK; lane += 257) {
              if (GroupLanes.get(chunk, lane) != stamp + round) {
                mismatches++;
              }
            }
            workspace.giveChunk(chunk);
          }
          return mismatches;
        }));
      }
      start.countDown();
      long mismatches = 0L;
      for (final Future<Long> result : results) {
        mismatches += result.get(60, TimeUnit.SECONDS);
      }
      assertEquals(0L, mismatches, "a chunk was shared by two workers or reused uncleared");
      assertEquals((long) workers * rounds, workspace.chunksGiven());
      assertEquals((long) workers * rounds, workspace.chunksCarved() + workspace.chunksReused());
      assertTrue(workspace.chunksCarved() <= workers,
          "at most one chunk per concurrent worker was ever carved, not " + workspace.chunksCarved());
    } finally {
      pool.shutdownNow();
    }
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers());
  }

  @Test
  void aTableWithoutAWorkspaceOpensAPrivateOneAndCloseReturnsIt() {
    final ScratchStats before = stats();
    final long privateBefore = NumericGroupAggTable.privateWorkspaceCount();
    final NumericGroupAggTable table = new NumericGroupAggTable(1, 64);
    assertEquals(privateBefore, NumericGroupAggTable.privateWorkspaceCount(), "an empty table holds no memory");
    final int handle = table.acquire(42L, 7L);
    assertEquals(privateBefore + 1, NumericGroupAggTable.privateWorkspaceCount());
    GroupLanes.increment(table.storageAtAccBase(handle), table.offsetAtAccBase(handle));
    assertEquals(1L, GroupLanes.get(table.storageAtAccBase(handle), table.offsetAtAccBase(handle)));
    assertEquals(42L, table.keyAtAccBase(handle));
    assertTrue(stats().totalLentBuffers() > before.totalLentBuffers());
    table.close();
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(), "closing the table returned its workspace");

    // A table in a scan's workspace opens none, and releasing it keeps the scan's regions.
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable inScan = new NumericGroupAggTable(1, 64).attachWorkspace(workspace);
      inScan.acquire(7L, 1L);
      assertEquals(privateBefore + 1, NumericGroupAggTable.privateWorkspaceCount());
      assertSame(workspace, inScan.workspace());
      final int regions = workspace.regions();
      inScan.close();
      assertEquals(regions, workspace.regions(), "a table does not close the workspace of its scan");
      assertFalse(workspace.isClosed());
      try (GroupWorkspace other = new GroupWorkspace()) {
        assertThrows(IllegalStateException.class,
            () -> new NumericGroupAggTable(1, 64).attachWorkspace(workspace).attachWorkspace(other));
      }
    }
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers());
  }

  @Test
  void aSelfReturningWorkspaceReturnsItsRegionsOnCloseExactlyOnce() {
    final ScratchStats before = stats();
    final long closedBefore = GroupWorkspace.closedCount();
    final GroupWorkspace workspace = GroupWorkspace.selfReturning();
    workspace.takeChunk(FULL_CHUNK);
    workspace.newBlock(3);
    assertTrue(workspace.regions() > 0);
    assertEquals(workspace.regions(), stats().totalLentBuffers() - before.totalLentBuffers());
    workspace.close();
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(), "closing returned every region");
    assertEquals(closedBefore + 1, GroupWorkspace.closedCount());
    workspace.close();
    assertEquals(closedBefore + 1, GroupWorkspace.closedCount(), "a repeated close is not a second return");
    assertEquals(0, stats().staleReleases() - before.staleReleases());
    assertThrows(IllegalStateException.class, () -> workspace.takeChunk(FULL_CHUNK));
  }

  @Test
  void aDroppedSelfReturningWorkspaceGivesItsRegionsBack() throws InterruptedException {
    final ScratchStats before = stats();
    final long closedBefore = GroupWorkspace.closedCount();
    borrowInAWorkspaceNobodyKeeps();
    assertTrue(stats().totalLentBuffers() > before.totalLentBuffers());
    // The collector has to find the workspace and the cleaner's thread has to run: give them rounds,
    // not a deadline. Nothing else can return a workspace meanwhile, see settle().
    for (int round = 0; round < 1_500 && GroupWorkspace.closedCount() == closedBefore; round++) {
      System.gc();
      Thread.sleep(20);
    }
    assertEquals(closedBefore + 1, GroupWorkspace.closedCount(), "the dropped workspace was never returned");
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(), "its regions are the allocator's again");
    assertEquals(0, stats().staleReleases() - before.staleReleases());
  }

  /** In a frame of its own, so that no local of the test keeps the workspace reachable. */
  private static void borrowInAWorkspaceNobodyKeeps() {
    final GroupWorkspace workspace = GroupWorkspace.selfReturning();
    assertEquals(FULL_CHUNK, GroupLanes.count(workspace.takeChunk(FULL_CHUNK)));
  }

  @Test
  void theTablesOfASpillWithoutAWorkspaceHoldTheSpillsOwn() {
    final ScratchStats before = stats();
    final GroupTableSpill spill =
        new GroupTableSpill(1, 64, hint -> new NumericGroupAggTable(1, hint), 50_000L, 0, 1, 100_000L);
    final NumericGroupAggTable table = spill.freshLocal();
    final int handle = table.acquire(42L, 7L);
    GroupLanes.increment(table.storageAtAccBase(handle), table.offsetAtAccBase(handle));
    // The table keeps the workspace its memory is in: dropping the spill alone cannot return it.
    assertSame(spill.workspace(), table.workspace());
    assertFalse(table.workspace().isClosed());
    assertTrue(stats().totalLentBuffers() > before.totalLentBuffers());
    table.release();
    spill.close();
    assertTrue(table.workspace().isClosed());
    assertEquals(before.totalLentBuffers(), stats().totalLentBuffers(), "closing the spill returned its workspace");
  }
}
