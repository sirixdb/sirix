/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Lane access to group accumulators in native memory: a lane number addresses the long an array
 * index used to, and a lane outside its block fails instead of touching a neighbour.
 */
final class GroupLanesTest {

  @Test
  void lanesAreAddressedLikeArrayElements() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupBuffer block = workspace.newBlock(12);
      assertEquals(12, GroupLanes.count(block));
      for (int lane = 0; lane < 12; lane++) {
        GroupLanes.set(block, lane, 100L + lane);
      }
      GroupLanes.add(block, 3, 40L);
      GroupLanes.add(block, 4, -200L);
      GroupLanes.increment(block, 5);
      GroupLanes.add(block, 6, Long.MAX_VALUE); // wraps, like += on a long
      assertArrayEquals(// lane 6: 106 + Long.MAX_VALUE wrapped
          new long[] {100, 101, 102, 143, -96, 106, Long.MIN_VALUE + 105L, 107, 108, 109, 110, 111},
          GroupLanes.toArray(block, 0, 12));
      assertEquals(143L, GroupLanes.get(block, 3));
      assertArrayEquals(new long[] {108, 109}, GroupLanes.toArray(block, 8, 2));

      GroupLanes.fill(block, 2, 5, -1L);
      assertArrayEquals(new long[] {101, -1, -1, -1, 106}, GroupLanes.toArray(block, 1, 5));
    }
  }

  @Test
  void copiesMoveWholeLanesBetweenBlocksAndFromArrays() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupBuffer source = workspace.newBlock(100);
      final GroupBuffer target = workspace.newBlock(100);
      for (int lane = 0; lane < 100; lane++) {
        GroupLanes.set(source, lane, lane * 3L);
      }
      // A stripe's handful of lanes (the lane loop) and a long run (the bulk copy).
      GroupLanes.copy(source, 10, target, 2, 7);
      GroupLanes.copy(source, 20, target, 40, 60);
      for (int lane = 0; lane < 100; lane++) {
        final long expected = lane >= 2 && lane < 9
            ? (lane + 8) * 3L
            : lane >= 40
                ? (lane - 20) * 3L
                : 0L;
        assertEquals(expected, GroupLanes.get(target, lane), "lane " + lane);
      }
      GroupLanes.copy(new long[] {9, 8, 7, 6}, 1, target, 0, 2);
      // Lanes 0 and 1 from the array; lane 2 keeps what the stripe copy above left there.
      assertArrayEquals(new long[] {8, 7, 30}, GroupLanes.toArray(target, 0, 3));
    }
  }

  @Test
  void aLaneOutsideItsBlockFails() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final GroupBuffer block = workspace.newBlock(4);
      // The block's neighbours in the region are other blocks: a write past the end must not reach them.
      final GroupBuffer neighbour = workspace.newBlock(4);
      assertThrows(IndexOutOfBoundsException.class, () -> GroupLanes.get(block, 4));
      assertThrows(IndexOutOfBoundsException.class, () -> GroupLanes.set(block, 4, 1L));
      assertThrows(IndexOutOfBoundsException.class, () -> GroupLanes.increment(block, -1));
      assertThrows(IndexOutOfBoundsException.class, () -> GroupLanes.add(block, 7, 1L));
      assertThrows(IndexOutOfBoundsException.class, () -> GroupLanes.fill(block, 2, 6, 9L));
      assertArrayEquals(new long[4], GroupLanes.toArray(neighbour, 0, 4));
    }
  }
}
