/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.cache.FrameSlotAllocator;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class MapGroupWorkspaceTest {
  @Test
  void everyMapRepresentationOwnsNativeAccumulatorsUntilWorkspaceClose() {
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {
        ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
        ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
    page.appendRow(0L, new long[] {0L, 0L, 7L, 10L}, new boolean[4], new String[] {"a", "x", null, null});
    page.appendRow(1L, new long[] {0L, 0L, 7L, 5L}, new boolean[4], new String[] {"a", "x", null, null});
    page.appendRow(2L, new long[] {0L, 0L, 0L, 30L}, new boolean[4], new String[4],
        new boolean[] {false, false, false, true}, null);
    final List<byte[]> leaves = List.of(page.serialize());
    final ProjectionIndexScan.ColumnPredicate[] predicates = new ProjectionIndexScan.ColumnPredicate[0];
    final GroupWorkspace workspace = new GroupWorkspace();
    final Object2ObjectOpenHashMap<String, GroupBuffer> strings = new Object2ObjectOpenHashMap<>();
    final Long2ObjectOpenHashMap<GroupBuffer> numeric = new Long2ObjectOpenHashMap<>();
    final Object2ObjectOpenHashMap<ProjectionIndexByteScan.GroupKey, GroupBuffer> composite =
        new Object2ObjectOpenHashMap<>();
    final GroupBuffer stringMissing = ProjectionIndexByteScan.newGroupAggAcc(workspace, 1, Long.MAX_VALUE);
    final GroupBuffer numericMissing = ProjectionIndexByteScan.newGroupAggAcc(workspace, 1, Long.MAX_VALUE);
    try {
      ProjectionIndexByteScan.conjunctiveAggregateByGroup(workspace, leaves, predicates, 0, new int[] {3}, strings,
          stringMissing, 0);
      ProjectionIndexByteScan.conjunctiveAggregateByGroupNumeric(workspace, leaves, predicates, 2, new int[] {3},
          numeric, numericMissing, 0);
      ProjectionIndexByteScan.conjunctiveAggregateByGroupMulti(workspace, leaves, predicates, new int[] {0, 1},
          new int[] {3}, composite, 0);
      final long[] ordinary = {2L, 0L, 2L, 15L, 5L, 10L};
      final long[] missing = {1L, 2L, 1L, 30L, 30L, 30L};
      assertArrayEquals(ordinary, GroupLanes.toArray(strings.get("a"), 0, 6));
      assertArrayEquals(ordinary, GroupLanes.toArray(numeric.get(7L), 0, 6));
      assertArrayEquals(missing, GroupLanes.toArray(stringMissing, 0, 6));
      assertArrayEquals(missing, GroupLanes.toArray(numericMissing, 0, 6));
      assertEquals(2, composite.size());
      for (final var entry : composite.entrySet()) {
        assertTrue(entry.getValue().isNative());
        assertArrayEquals(entry.getKey().part(0) == null
            ? missing
            : ordinary, GroupLanes.toArray(entry.getValue(), 0, 6));
      }
      assertTrue(strings.get("a").isNative());
      assertTrue(numeric.get(7L).isNative());
      workspace.close();
      final GroupBuffer closedStringGroup = strings.get("a");
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(closedStringGroup, 0));
      final GroupBuffer closedNumericGroup = numeric.get(7L);
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(closedNumericGroup, 0));
      for (final GroupBuffer buffer : composite.values()) {
        assertThrows(IllegalStateException.class, () -> GroupLanes.get(buffer, 0));
      }
    } finally {
      workspace.close();
    }
  }

  @Test
  void mapAccumulatorCannotBypassItsScanGrant() {
    final FrameSlotAllocator allocator = new FrameSlotAllocator(1L << 20);
    try (GroupWorkspace workspace = GroupWorkspace.forScan(allocator)) {
      final ProjectionIndexRowGroupPage page =
          new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG});
      page.appendRow(0L, new long[] {7L}, new boolean[1], new String[1]);
      workspace.newBlock(16_382);
      final GroupBuffer missing = ProjectionIndexByteScan.newGroupAggAcc(workspace, 0, Long.MAX_VALUE);
      final List<byte[]> leaves = List.of(page.serialize());
      final Long2ObjectOpenHashMap<GroupBuffer> out = new Long2ObjectOpenHashMap<>();
      assertThrows(OutOfMemoryError.class, () -> ProjectionIndexByteScan.conjunctiveAggregateByGroupNumeric(workspace,
          leaves, new ProjectionIndexScan.ColumnPredicate[0], 0, new int[0], out, missing, 0));
    } finally {
      assertEquals(0L, allocator.aggregationBudget().grantedBytes());
      allocator.shutdown();
    }
  }
}
