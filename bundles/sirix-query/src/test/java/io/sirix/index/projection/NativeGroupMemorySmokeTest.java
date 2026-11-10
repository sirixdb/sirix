/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the shipped group-record access form in both production and debug native images. */
@Tag("native-image")
final class NativeGroupMemorySmokeTest {
  private static final int GROUPS = 1024;
  private static final int ROUNDS = 8;

  @Test
  void nativeProbesAndFoldsSurviveGrowthAndReturnTheirGrant() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(64L << 20);
    GroupBuffer last = null;
    try {
      try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
        final long[][] values = {{7L}};
        final long[][] presence = {{1L}};
        for (final boolean dense : new boolean[] {false, true}) {
          final NumericGroupAggTable table = new NumericGroupAggTable(1, 16, false, -1L).attachWorkspace(workspace);
          if (dense) {
            table.useDenseIndex();
          }
          for (int round = 0; round < ROUNDS; round++) {
            for (int key = 1; key <= GROUPS; key++) {
              // The kernels' two resolutions in turn: one step, and through the handle.
              final long record = (round & 1) == 0
                  ? table.acquireAccumulator(key, key, 1, 4)
                  : table.accumulatorAt(table.acquire(key, key), 1, 4);
              ProjectionColumnGroupScan.foldNumericPlainWindow(record, values, presence, 1, 0, 0, 0, -1L);
            }
          }
          assertTrue(table.rehashes() > 0);
          for (int key = 1; key <= GROUPS; key++) {
            final int handle = table.acquire(key, key);
            final GroupBuffer buffer = table.storageAtAccBase(handle);
            final int base = table.offsetAtAccBase(handle);
            assertEquals(ROUNDS, GroupLanes.get(buffer, base));
            assertEquals(ROUNDS, GroupLanes.get(buffer, base + 2));
            assertEquals(7L * ROUNDS, GroupLanes.get(buffer, base + 3));
            assertEquals(7L, GroupLanes.get(buffer, base + 4));
            assertEquals(7L, GroupLanes.get(buffer, base + 5));
            if (GroupKernelLanes.DEBUG) {
              final long record = buffer.accumulator(base, 1, 4);
              assertThrows(IndexOutOfBoundsException.class, () -> GroupKernelLanes.get(record, 6));
            }
            last = buffer;
          }
          table.release();
        }
        assertTrue(workspace.peakUsedBytes() <= workspace.grantedBytes());
      }
      final GroupBuffer returned = last;
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(returned, 0));
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      assertEquals(0L, allocator.getActiveMemoryBytes());
      System.out.println("Native group memory verified; bounded debug=" + GroupKernelLanes.DEBUG);
    } finally {
      allocator.shutdown();
    }
  }
}
