/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The groups a pass may hold over a wide merge split, against the memory a pass of that many groups
 * takes.
 *
 * <p>
 * A partition owes the partial chunks of its streams only once a group has landed in it, and never
 * more than what it holds calls for. A plan that reserved the full 768 KiB of every one of a
 * thousand partitions before the first group had no room for a group on any grant below 768 MiB,
 * and started with several passes — each a scan of the whole input — where one pass of a few
 * megabytes would do. {@link GroupWorkspace#usableGroups} is the bound that replaced that
 * reservation. Here a pass is filled to exactly the groups it promises and merged with every
 * partition table held, the largest state a pass reaches; a bound that promised too much would fail
 * an allocation.
 */
@Isolated
final class GroupPassTailBoundTest {
  private static final int PARTITIONS = 1024;
  private static final int SHIFT = 64 - 10;
  private static final int WORKERS = 2;
  /** The full tail a partition of full chunks owes: six chunks of 128 KiB. */
  private static final long FULL_TAIL_BYTES = 6L * (128L << 10);

  private String previousHint;
  private int previousFlush;
  private int previousBatch;
  private int previousStripe;

  @BeforeEach
  void useSmallWorkerTables() {
    previousHint = System.setProperty(GroupTableSpill.WORKER_HINT_PROPERTY, "128");
    previousFlush = GroupTableSpill.setFlushGroupsForTesting(128);
    previousBatch = GroupTableSpill.setSubChunkLeavesForTesting(1);
    previousStripe = GroupTableSpill.setStripeSpillForTesting(1);
  }

  @AfterEach
  void restoreGeometry() {
    GroupTableSpill.setStripeSpillForTesting(previousStripe);
    GroupTableSpill.setSubChunkLeavesForTesting(previousBatch);
    GroupTableSpill.setFlushGroupsForTesting(previousFlush);
    if (previousHint == null) {
      System.clearProperty(GroupTableSpill.WORKER_HINT_PROPERTY);
    } else {
      System.setProperty(GroupTableSpill.WORKER_HINT_PROPERTY, previousHint);
    }
  }

  @Test
  @DisplayName("one pass over a thousand partitions holds the groups it plans, narrow and wide stripes alike")
  void aPassOverAThousandPartitionsHoldsItsPlannedGroups() {
    // 32 MiB of aggregation capacity: a twenty-fourth of the split's full tails.
    for (final int aggColumns : new int[] {0, 1, 3}) {
      final long planned = fillOnePass(256L << 20, aggColumns);
      assertTrue(planned > PARTITIONS, "every partition holds groups at this grant: " + planned);
    }
  }

  @Test
  @DisplayName("a grant that holds fewer groups than partitions still plans one pass of them")
  void aPassOfFewerGroupsThanPartitionsHoldsItsPlannedGroups() {
    // 8 MiB of aggregation capacity: the first chunks of a thousand partitions alone would not fit.
    for (final int aggColumns : new int[] {1, 3}) {
      final long planned = fillOnePass(64L << 20, aggColumns);
      assertTrue(planned > 0L && planned < PARTITIONS, "fewer groups than partitions at this grant: " + planned);
    }
  }

  @Test
  @DisplayName("without the stripe spill a partition's tables start at the worker hint: one full tail per group")
  void theTableSpillOwesAFullTailForEveryGroup() {
    GroupTableSpill.setStripeSpillForTesting(0);
    final long planned = fillOnePass(256L << 20, 1);
    assertTrue(planned > 0L && planned < PARTITIONS, "a full tail per group leaves room for few: " + planned);
  }

  /**
   * Plan one pass over every partition on an allocator of {@code allocatorBytes}, fill it with
   * exactly the groups the plan holds, merge every partition and keep all of their tables until the
   * end.
   *
   * @return the groups the pass was planned to hold, all of which it held
   */
  private static long fillOnePass(final long allocatorBytes, final int aggColumns) {
    final int stride = NumericGroupAggTable.strideFor(aggColumns, false, 0);
    final FrameSlotAllocator allocator = new FrameSlotAllocator(allocatorBytes);
    final long planned;
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      assertTrue(workspace.preparePlan(WORKERS, PARTITIONS, stride) > 0L);
      assertEquals(WORKERS, workspace.plannedWorkers());
      assertTrue(PARTITIONS * FULL_TAIL_BYTES > workspace.grantedBytes(),
          "the split's full tails must exceed the grant, or the old reservation would have planned this too");
      assertEquals(0L, workspace.usableGroupBytes(PARTITIONS), "nothing is left once every full tail is reserved");
      planned = workspace.usableGroups(PARTITIONS);
      assertTrue(planned > 0L, "one pass over the whole split must hold groups");
      // Fewer partitions never hold fewer groups: a restart with more passes always gains room.
      long wider = planned;
      for (int active = PARTITIONS >> 1; active >= 1; active >>= 1) {
        final long narrower = workspace.usableGroups(active);
        assertTrue(narrower >= wider, active + " partitions hold " + narrower + " groups, twice as many " + wider);
        wider = narrower;
      }
      final GroupTableSpill spill =
          new GroupTableSpill(PARTITIONS, SHIFT, hint -> new NumericGroupAggTable(aggColumns, hint, false, -1L), 0L, 0,
              PARTITIONS, planned).inWorkspace(workspace);
      final NumericGroupAggTable[] finals = new NumericGroupAggTable[WORKERS];
      final NumericGroupAggTable[] merged = new NumericGroupAggTable[PARTITIONS];
      try {
        assertEquals(GroupTableSpill.stripeSpillEnabled(), spill.stripeSpill());
        for (int worker = 0; worker < WORKERS; worker++) {
          NumericGroupAggTable local = spill.freshLocal();
          // Every key once: the pass's resident state is its distinct groups, the most a budget admits.
          for (long key = worker + 1L; key <= planned; key += WORKERS) {
            final int handle = local.acquire(key, key);
            GroupLanes.increment(local.storageAtAccBase(handle), local.offsetAtAccBase(handle));
            if (spill.shouldFlush(local)) {
              spill.flush(local);
              local = spill.freshLocal();
            }
          }
          finals[worker] = local;
        }
        assertFalse(spill.aborted(), "a pass holding exactly its budget is not over it");
        final int[][][] indexes = new int[WORKERS][][];
        for (int worker = 0; worker < WORKERS; worker++) {
          indexes[worker] = finals[worker].buildPartitionIndex(PARTITIONS, SHIFT);
        }
        long groups = 0L;
        for (int part = 0; part < PARTITIONS; part++) {
          // An unbounded selection keeps every partition's table until its pass has emitted.
          merged[part] = spill.takeOrCreate(part, () -> new NumericGroupAggTable(aggColumns, 16, false, -1L));
          NumericGroupAggTable.mergePartitionIndexed(finals, indexes, part, merged[part]);
          groups += merged[part].sizeIncludingZero();
        }
        assertEquals(planned, groups, "every planned group is held, once");
        assertTrue(workspace.peakUsedBytes() <= workspace.grantedBytes());
      } finally {
        for (final NumericGroupAggTable table : merged) {
          if (table != null && !table.released()) {
            table.release();
          }
        }
        for (final NumericGroupAggTable table : finals) {
          if (table != null && !table.released()) {
            table.release();
          }
        }
        spill.close();
      }
    } finally {
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      allocator.shutdown();
    }
    return planned;
  }
}
