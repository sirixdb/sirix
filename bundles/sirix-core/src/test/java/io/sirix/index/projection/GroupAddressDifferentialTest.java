/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.lang.foreign.MemorySegment;

import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Independent heap oracle across native chunk boundaries, rehashes, identity collisions and reuse.
 */
final class GroupAddressDifferentialTest {
  private static final int GROUPS = 4096;
  private static final int ROWS = 24000;
  private static final int COLUMNS = 2;
  private static final int WIDTH = 2 + 4 * COLUMNS;

  @Test
  void nativeRecordsMatchHeapOracleAfterGrowthAndReuse() {
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      for (final boolean dense : new boolean[] {false, true}) {
        for (final boolean identities : new boolean[] {false, true}) {
          for (int round = 0; round < 2; round++) {
            for (final Resolution resolution : Resolution.values()) {
              compare(workspace, dense, identities, round, resolution);
            }
          }
        }
      }
      assertTrue(workspace.chunksReused() > 0, "the oracle also covers reused native bytes");
    }
  }

  /** How a row reaches its group's accumulator: the three resolutions a kernel can use. */
  private enum Resolution {
    /** Handle, chunk and offset, then the chunk resolves the accumulator. */
    CHUNK,
    /** Handle, then the table resolves it from its own geometry. */
    HANDLE,
    /** One step where the table offers it (a plain key); else as {@link #HANDLE}. */
    FUSED
  }

  private static void compare(final GroupWorkspace workspace, final boolean dense, final boolean identities,
      final int round, final Resolution resolution) {
    final NumericGroupAggTable table = new NumericGroupAggTable(COLUMNS, 16, false, -1L, identities
        ? 2
        : 0).attachWorkspace(workspace);
    if (dense) {
      table.useDenseIndex();
    }
    final long[][] expected = new long[GROUPS][WIDTH];
    final long[] identity = new long[2];
    foldRows(table, expected, identity, identities, round, resolution);
    assertTrue(table.rehashes() > 0, "the fixture must move live records");
    assertRecordsMatchOracle(table, expected, identity, dense, identities, resolution);
    table.release();
  }

  private static void foldRows(final NumericGroupAggTable table, final long[][] expected, final long[] identity,
      final boolean identities, final int round, final Resolution resolution) {
    final long[][] window = new long[COLUMNS][1];
    final MemorySegment[] lanes = new MemorySegment[COLUMNS];
    final long[] bases = new long[COLUMNS];
    for (int column = 0; column < COLUMNS; column++) {
      lanes[column] = MemorySegment.ofArray(window[column]);
    }
    final long[][] present = new long[COLUMNS][1];
    final Random random = new Random(0x5AFEL + round);
    for (int row = 0; row < ROWS; row++) {
      final int key = row < GROUPS
          ? row
          : random.nextInt(GROUPS);
      final long[] oracle = expected[key];
      if (oracle[0] == 0L) {
        oracle[1] = row;
        for (int column = 0; column < COLUMNS; column++) {
          oracle[4 + column * 4] = Long.MAX_VALUE;
          oracle[5 + column * 4] = Long.MIN_VALUE;
        }
      }
      oracle[0]++;
      for (int column = 0; column < COLUMNS; column++) {
        final long value = random.nextLong(-1L << 32, 1L << 32);
        window[column][0] = value;
        present[column][0] = random.nextBoolean()
            ? 1L
            : 0L;
        if (present[column][0] != 0L) {
          final int base = 2 + column * 4;
          oracle[base]++;
          oracle[base + 1] += value;
          oracle[base + 2] = Math.min(oracle[base + 2], value);
          oracle[base + 3] = Math.max(oracle[base + 3], value);
        }
      }
      final long groupRecord = acquireRecord(table, identity, identities, resolution, key, row);
      ProjectionColumnGroupScan.foldNumericPlainWordLane(groupRecord, lanes, bases, present, COLUMNS, 0, 1L, -1L);
    }
  }

  private static long acquireRecord(final NumericGroupAggTable table, final long[] identity, final boolean identities,
      final Resolution resolution, final int key, final int row) {
    if (!identities && key == 0) {
      return table.acquireZero(row).accumulator(0, COLUMNS, 4);
    } else if (!identities && resolution == Resolution.FUSED) {
      return table.acquireAccumulator(key, row, COLUMNS, 4);
    } else {
      final int handle = identities
          ? acquireIdentity(table, identity, key, row)
          : table.acquire(key, row);
      return resolution == Resolution.CHUNK
          ? table.storageAtAccBase(handle).accumulator(table.offsetAtAccBase(handle), COLUMNS, 4)
          : table.accumulatorAt(handle, COLUMNS, 4);
    }
  }

  private static void assertRecordsMatchOracle(final NumericGroupAggTable table, final long[][] expected,
      final long[] identity, final boolean dense, final boolean identities, final Resolution resolution) {
    for (int key = 0; key < GROUPS; key++) {
      final int handle = identities
          ? acquireIdentity(table, identity, key, ROWS)
          : key == 0
              ? -1
              : table.acquire(key, ROWS);
      final GroupBuffer buffer = !identities && key == 0
          ? table.acquireZero(ROWS)
          : table.storageAtAccBase(handle);
      final int base = !identities && key == 0
          ? 0
          : table.offsetAtAccBase(handle);
      for (int lane = 0; lane < WIDTH; lane++) {
        assertEquals(expected[key][lane], GroupLanes.get(buffer, base + lane),
            "dense=" + dense + ", identities=" + identities + ", " + resolution + ", key=" + key + ", lane=" + lane);
      }
    }
  }

  private static int acquireIdentity(final NumericGroupAggTable table, final long[] identity, final int key,
      final long ordinal) {
    identity[0] = key;
    identity[1] = ~key;
    // Every two distinct identities deliberately share a probe hash, including hash zero.
    return table.acquireExact(key >>> 1, ordinal, identity, 0);
  }
}
