/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.lang.ref.Reference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class GroupRecordBoundaryTest {
  @Test
  @EnabledIfSystemProperty(named = "sirix.aggregation.debug", matches = "true")
  // The fence stands after the access in program order, so the owner is live across the access
  // however it ends, and after a throw the address is not used again
  @SuppressWarnings("ReachabilityFenceUsage")
  void debugRecordsPreserveLiveFootprintsAcrossCollection() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupBuffer buffer = workspace.takeChunk(32);
      final long groupRecord = buffer.recordAt(13, 13);
      System.gc();
      assertEquals(groupRecord, buffer.recordAt(13, 13));
      GroupKernelLanes.set(groupRecord, 12, 55L);
      assertEquals(55L, GroupKernelLanes.getOpaque(groupRecord, 12));
      assertEquals(55L, GroupKernelLanes.compareAndExchange(groupRecord, 12, 55L, 56L));
      assertEquals(56L, GroupKernelLanes.getAndAdd(groupRecord, 12, 3L));
      assertEquals(59L, GroupLanes.get(buffer, 25));
      for (final int outside : new int[] {-1, 13}) {
        assertThrows(IndexOutOfBoundsException.class, () -> GroupKernelLanes.get(groupRecord, outside));
        assertThrows(IndexOutOfBoundsException.class, () -> GroupKernelLanes.set(groupRecord, outside, 1L));
        assertThrows(IndexOutOfBoundsException.class, () -> GroupKernelLanes.getOpaque(groupRecord, outside));
        assertThrows(IndexOutOfBoundsException.class,
            () -> GroupKernelLanes.compareAndExchange(groupRecord, outside, 0L, 1L));
        assertThrows(IndexOutOfBoundsException.class, () -> GroupKernelLanes.getAndAdd(groupRecord, outside, 1L));
      }
      assertEquals(0L, GroupLanes.get(buffer, 12));
      assertEquals(0L, GroupLanes.get(buffer, 26));
      workspace.giveChunk(buffer);
      assertThrows(IllegalStateException.class, () -> GroupKernelLanes.getOpaque(groupRecord, 12));
      assertThrows(IllegalStateException.class, () -> GroupKernelLanes.compareAndExchange(groupRecord, 12, 59L, 0L));
      assertThrows(IllegalStateException.class, () -> GroupKernelLanes.getAndAdd(groupRecord, 12, 1L));
      Reference.reachabilityFence(buffer);
    }
  }

  @Test
  void completeFootprintsCoverNarrowWideAndIdentityLayouts() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      for (final int columns : new int[] {0, 1, 4, 16, 63, 64, 65, 4094}) {
        for (final boolean dense : new boolean[] {false, true}) {
          final NumericGroupAggTable table =
              new NumericGroupAggTable(columns, 16, true, -1L, 2).attachWorkspace(workspace);
          if (dense) {
            table.useDenseIndex();
          }
          final int first = table.acquireExact(7L, 1L, new long[] {11L, 12L}, 0);
          final int second = table.acquireExact(7L, 2L, new long[] {21L, 22L}, 0);
          final GroupBuffer chunk = table.storageAtAccBase(first);
          final int base = table.offsetAtAccBase(first);
          GroupKernelLanes.set(chunk.accumulator(base, columns, 4), 0, 17L);
          assertEquals(17L, GroupLanes.get(chunk, base), "the record's lane 0 is the accumulator's count lane");
          // The table's own resolution names the same accumulator.
          assertEquals(17L, GroupKernelLanes.get(table.accumulatorAt(first, columns, 4), 0));
          // The complete stripe: key lane first, identity lanes last.
          final long stripe = chunk.recordAt(base - 1, table.stride());
          assertEquals(7L, GroupKernelLanes.get(stripe, 0));
          assertEquals(12L, GroupKernelLanes.get(stripe, table.stride() - 1));
          assertThrows(IndexOutOfBoundsException.class, () -> chunk.accumulator(base, columns + 1, 4));
          assertThrows(IndexOutOfBoundsException.class, () -> table.accumulatorAt(first, columns + 1, 4));
          assertThrows(IndexOutOfBoundsException.class, () -> chunk.accumulator(base + 1, columns, 4),
              "an accumulator starts where the layout puts one");
          final GroupBuffer neighbour = table.storageAtAccBase(second);
          assertEquals(0L, GroupLanes.get(neighbour, table.offsetAtAccBase(second)));
          table.release();
        }
      }
      assertThrows(IllegalArgumentException.class, () -> new NumericGroupAggTable(Integer.MAX_VALUE, 16, true, -1L, 2));
      assertThrows(IllegalArgumentException.class, () -> NumericGroupAggTable.strideFor(0, true, Integer.MAX_VALUE));
    }
  }

  @Test
  void invalidIdentityCannotPublishAPartialRecord() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      for (final boolean dense : new boolean[] {false, true}) {
        final NumericGroupAggTable table = new NumericGroupAggTable(1, 16, false, -1L, 2).attachWorkspace(workspace);
        if (dense) {
          table.useDenseIndex();
        }
        final long[] identity = {11L, 22L};
        for (final int offset : new int[] {-1, 1, Integer.MAX_VALUE}) {
          assertThrows(IndexOutOfBoundsException.class, () -> table.acquireExact(7L, 1L, identity, offset));
        }
        assertEquals(0, table.size());
        final int handle = table.acquireExact(7L, 1L, identity, 0);
        assertEquals(handle, table.acquireExact(7L, 2L, identity, 0));
        assertEquals(1, table.size());
        table.release();
      }
    }
  }

  @Test
  void invalidHandlesAndBucketsFailBeforeAnyProbeRead() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = new NumericGroupAggTable(2, 16, true).attachWorkspace(workspace);
      final int handle = table.acquire(7L, 1L);
      for (final int bad : new int[] {Integer.MIN_VALUE, -2, -1, table.capacity(), Integer.MAX_VALUE}) {
        assertThrows(IndexOutOfBoundsException.class, () -> table.storageAtAccBase(bad));
        assertThrows(IndexOutOfBoundsException.class, () -> table.accumulatorAt(bad, 2, 4));
        assertThrows(IndexOutOfBoundsException.class, () -> table.keyAtBucket(bad));
        assertThrows(IndexOutOfBoundsException.class, () -> table.accBaseOfBucket(bad));
      }
      assertEquals(7L, table.keyAtAccBase(handle));
      table.release();
      assertThrows(IllegalStateException.class, () -> table.acquire(9L, 2L));
      assertThrows(IllegalStateException.class, () -> table.acquire(7L, 2L), "a released table finds nothing");
      assertThrows(IllegalStateException.class, () -> table.acquireAccumulator(7L, 2L, 2, 4));
      assertThrows(IndexOutOfBoundsException.class, () -> table.accumulatorAt(handle, 2, 4));
    }
  }

  @Test
  void recordAndAccumulatorRangesUseOverflowSafeFootprints() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupBuffer buffer = workspace.newBlock(26);
      for (final int[] range : new int[][] {{-1, 1}, {0, -1}, {0, Integer.MAX_VALUE}, {Integer.MAX_VALUE, 1}, {20, 7},
          {27, 0}}) {
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.recordAt(range[0], range[1]));
      }
      assertThrows(IllegalArgumentException.class, () -> buffer.accumulator(0, Integer.MAX_VALUE, 4));
      final long upperRecord = buffer.recordAt(13, 13);
      GroupKernelLanes.set(upperRecord, 12, 77L);
      assertEquals(77L, GroupLanes.get(buffer, 25), "a record's lanes are relative to its first");
      if (GroupKernelLanes.DEBUG) {
        assertThrows(IndexOutOfBoundsException.class, () -> GroupKernelLanes.set(upperRecord, -1, 1L));
        assertThrows(IndexOutOfBoundsException.class, () -> GroupKernelLanes.set(upperRecord, 13, 1L));
      }
      assertEquals(0L, GroupLanes.get(buffer, 12));
    }
  }

  @Test
  void reuseAndClosureInvalidateEveryOldView() {
    try (GroupWorkspace workspace = new GroupWorkspace(); GroupWorkspace other = new GroupWorkspace()) {
      final GroupBuffer original = workspace.takeChunk(128);
      final long originalRecord = original.recordAt(13, 13);
      assertThrows(IllegalArgumentException.class, () -> other.giveChunk(original));
      workspace.giveChunk(original);
      assertThrows(IllegalStateException.class, () -> workspace.giveChunk(original));
      final GroupBuffer reused = workspace.takeChunk(128);
      assertEquals(original.address, reused.address);
      assertNotEquals(original.generation, reused.generation);
      GroupLanes.set(reused, 13, 19L);
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(original, 13));
      assertThrows(IllegalStateException.class, () -> original.recordAt(13, 13), "nor can it be resolved again");
      if (GroupKernelLanes.DEBUG) {
        // The record names the generation it was resolved from, not the memory the next table took.
        assertThrows(IllegalStateException.class, () -> GroupKernelLanes.set(originalRecord, 0, 99L));
      }
      assertEquals(19L, GroupLanes.get(reused, 13));
      workspace.close();
      assertThrows(IllegalStateException.class, () -> GroupLanes.get(reused, 13));
    }
  }

  @Test
  void anInvalidFoldCannotWriteTheFollowingRecord() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupBuffer buffer = workspace.newBlock(15);
      GroupLanes.set(buffer, 14, 0x1234L);
      // Four columns need eighteen lanes: the resolution refuses before a kernel has a record to write.
      assertThrows(IndexOutOfBoundsException.class, () -> buffer.accumulator(0, 4, 4));
      assertEquals(0L, GroupLanes.get(buffer, 0));
      assertEquals(0x1234L, GroupLanes.get(buffer, 14));
    }
  }
}
