/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The dense layout's index chunks within ONE grouped scan: a released table hands them back to the
 * scan's {@link GroupWorkspace} and the next pass's table is built from them, with every group
 * still exact. The chunks used to be kept in process-wide pools across scans; nothing outlives the
 * scan's workspace now, so what is left to prove is the reuse inside it.
 */
final class DenseIndexChunkReuseTest {
  /** Lanes of a dense index chunk of a table whose capacity spans whole index chunks. */
  private static final int INDEX_CHUNK_LANES = NumericGroupAggTable.MAX_STORAGE_CHUNK_LANES;
  private String previousDenseProperty;

  @BeforeEach
  void forceDenseIndex() {
    previousDenseProperty = System.getProperty(GroupTableSpill.DENSE_INDEX_PROPERTY);
    System.setProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, "true");
  }

  @AfterEach
  void restoreDenseIndex() {
    restoreProperty(GroupTableSpill.DENSE_INDEX_PROPERTY, previousDenseProperty);
  }

  @Test
  void repeatedSpillsReuseOnlyReleasedIndexesAndKeepGroupsExact() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupTableSpill first = spill().inWorkspace(workspace);
      assertSame(workspace, first.workspace());
      final NumericGroupAggTable firstTable = fillAndVerify(first, 40_000, 5L);
      // A seven-lane stripe's full chunk is not an index chunk's length: the two never share a free list.
      assertTrue(firstTable.chunkLanes() != INDEX_CHUNK_LANES);
      assertEquals(0, workspace.spareChunks(INDEX_CHUNK_LANES), "a live table's index is never handed out");
      firstTable.release();
      first.releaseTables();
      final int spareIndexes = workspace.spareChunks(INDEX_CHUNK_LANES);
      assertTrue(spareIndexes > 0, "the released table's index chunks wait in the scan's workspace");

      final long reused = workspace.chunksReused();
      final GroupTableSpill second = spill().inWorkspace(workspace);
      assertSame(workspace, second.workspace(), "the next pass of the scan shares the workspace");
      final NumericGroupAggTable secondTable = fillAndVerify(second, 45_000, 9L);
      assertTrue(workspace.spareChunks(INDEX_CHUNK_LANES) < spareIndexes,
          "the next table's index is built from the chunks the released one handed back");
      assertTrue(workspace.chunksReused() > reused);
      secondTable.release();
      second.releaseTables();
      assertTrue(workspace.spareChunks(INDEX_CHUNK_LANES) >= spareIndexes);
    }
  }

  private static GroupTableSpill spill() {
    return new GroupTableSpill(1, 64, hint -> new NumericGroupAggTable(0, hint, true, 0L, 3), 50_000L, 0, 1, 100_000L);
  }

  /**
   * Fill a fresh worker table of {@code spill} with {@code groups} groups and read every one back;
   * returns the table, still live.
   */
  private static NumericGroupAggTable fillAndVerify(final GroupTableSpill spill, final int groups, final long count) {
    final NumericGroupAggTable table = spill.freshLocal();
    final long[] identity = new long[3];
    for (int group = 0; group < groups; group++) {
      identity[0] = group;
      identity[1] = ~group;
      identity[2] = group % 17;
      final int handle = table.acquireExact(group % 8191, group + 1L, identity, 0);
      final GroupBuffer values = table.storageAtAccBase(handle);
      final int offset = table.offsetAtAccBase(handle);
      assertEquals(0L, GroupLanes.get(values, offset));
      GroupLanes.set(values, offset, count);
      table.setAuxAtAccBase(handle, group * 3L);
    }
    assertEquals(groups, table.size());
    for (int group = groups - 1; group >= 0; group--) {
      identity[0] = group;
      identity[1] = ~group;
      identity[2] = group % 17;
      final int handle = table.acquireExact(group % 8191, Long.MAX_VALUE, identity, 0);
      final GroupBuffer values = table.storageAtAccBase(handle);
      final int offset = table.offsetAtAccBase(handle);
      assertEquals(count, GroupLanes.get(values, offset));
      assertEquals(group + 1L, GroupLanes.get(values, offset + 1));
      assertEquals(group * 3L, table.auxAtAccBase(handle));
    }
    return table;
  }

  private static void restoreProperty(final String name, final String value) {
    if (value == null) {
      System.clearProperty(name);
    } else {
      System.setProperty(name, value);
    }
  }
}
