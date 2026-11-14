/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import com.sun.management.ThreadMXBean;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.lang.foreign.MemorySegment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** A steady-state row must not allocate a resolved record or a lane wrapper. */
final class GroupRecordAllocationTest {
  private static final int ROWS = 100_000;
  // Write-only on purpose: each resolved buffer escapes here, so scalar replacement hides none.
  @SuppressWarnings({"java:S1068", "UnusedVariable"})
  private static volatile GroupBuffer retained;

  @Test
  void productionResolutionAndFoldAllocateNoPerRowObjects() {
    assumeFalse(GroupKernelLanes.DEBUG, "debug deliberately keeps a bounded segment for each stripe");
    final var platform = ManagementFactory.getThreadMXBean();
    assumeTrue(platform instanceof ThreadMXBean);
    final ThreadMXBean bean = (ThreadMXBean) platform;
    assumeTrue(bean.isThreadAllocatedMemorySupported());
    bean.setThreadAllocatedMemoryEnabled(true);
    final long thread = Thread.currentThread().threadId();
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = new NumericGroupAggTable(1, 2048, false, -1L).attachWorkspace(workspace);
      table.useDenseIndex();
      final MemorySegment[] values = {MemorySegment.ofArray(new long[] {7L})};
      final long[] bases = {0L};
      final long[][] presence = {{1L}};
      for (int batch = 0; batch < 10; batch++) {
        fold(table, values, bases, presence);
      }
      final long start = bean.getThreadAllocatedBytes(thread);
      fold(table, values, bases, presence);
      final long allocated = bean.getThreadAllocatedBytes(thread) - start;
      // Allow fixed JVM bookkeeping, but not even one small wrapper per row (at least 1.6 MB).
      assertTrue(allocated <= 8192L, "steady group rows allocated " + allocated + " bytes");
      assertEquals(1024, table.size());
      table.release();
    } finally {
      retained = null;
    }
  }

  private static void fold(final NumericGroupAggTable table, final MemorySegment[] values, final long[] bases,
      final long[][] presence) {
    for (int row = 0; row < ROWS; row++) {
      final long key = (row & 1023) + 1L;
      final int handle = table.acquire(key, row);
      final GroupBuffer buffer = table.storageAtAccBase(handle);
      // A caller may retain the resolved chunk: do not let scalar replacement hide wrappers.
      retained = buffer;
      // Each of the three resolutions a kernel can use, in turn.
      final long groupRecord = switch (row % 3) {
        case 0 -> buffer.accumulator(table.offsetAtAccBase(handle), 1, 4);
        case 1 -> table.accumulatorAt(handle, 1, 4);
        default -> table.acquireAccumulator(key, row, 1, 4);
      };
      ProjectionColumnGroupScan.foldNumericPlainWordLane(groupRecord, values, bases, presence, 1, 0, 1L, -1L);
    }
  }
}
