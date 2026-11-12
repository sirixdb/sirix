/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A sums-only composite group-by folds a selection word at once: the word's rows acquire their
 * groups first, and their operands are folded column by column afterwards. The answer must be the
 * one the rows give one by one, whatever happens between the two steps: several rows of a word in
 * one group, operands that are absent, a table that grows while the word's rows wait, a pass that
 * owns only part of the keys, and a sum that overflows.
 */
final class CompositeSumsWordFoldTest {
  private static final byte NUMERIC = ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG;
  private static final byte[] KEY_KINDS = {NUMERIC, NUMERIC};
  private static final ColumnPredicate[] NO_PREDICATES = new ColumnPredicate[0];
  private static final int OPERANDS = 2;
  private static final long BOTH_SUMS_EXACT = 3L;

  /** An operand value that the fixture stores as absent. */
  private static final long ABSENT = Long.MIN_VALUE;

  private record Fixture(ProjectionColumnStore store, ColumnSlice[][] keys, ColumnSlice[][] operands) {
  }

  private record Group(long first, long second) {
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aWordFoldsLikeItsRowsOneByOne(final boolean transformedLoop) {
    // Five full words and a tail; 37 groups, so every word holds several rows of one group, and every
    // fifth row lacks its second operand.
    final long[][] rows = new long[5 * 64 + 7][];
    for (int row = 0; row < rows.length; row++) {
      rows[row] = new long[] {row % 37, 7, 3L * row + 1, row % 5 == 0
          ? ABSENT
          : -row};
    }
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = table(workspace, 64);
      scan(fixture(rows), table, transformedLoop);
      assertEquals(0, table.rehashes(), "the table was hinted at its final size: no word was acquired twice");
      assertGroups(oracle(rows), table);
      table.release();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3, 65, 66, 67})
  void aWordOfOneTwoOrThreeRowsFoldsLikeAFullOne(final int rowCount) {
    // A leaf of one, two or three rows is one such word; 65, 66 and 67 rows leave one, two or three
    // rows
    // in the word behind a full one. They take the lone-row path, the pair path and the shortest
    // column-major pass, each with rows in one group and in groups of their own. The second operand is
    // absent on the even rows in one run, so that a short word's first row lacks it, and on rows no
    // short
    // word holds in the other, so that every row of a short word folds both operands.
    for (final boolean oneGroup : new boolean[] {false, true}) {
      for (final boolean evenRowsLackTheSecondOperand : new boolean[] {false, true}) {
        final long[][] rows = new long[rowCount][];
        for (int row = 0; row < rowCount; row++) {
          final boolean absent = evenRowsLackTheSecondOperand
              ? row % 2 == 0
              : row % 5 == 3;
          rows[row] = new long[] {oneGroup
              ? 5
              : 100 + row, 7, row + 1L,
              absent
                  ? ABSENT
                  : 10L * row};
        }
        final Fixture fixture = fixture(rows);
        for (final boolean transformedLoop : new boolean[] {false, true}) {
          try (GroupWorkspace workspace = new GroupWorkspace()) {
            final NumericGroupAggTable table = table(workspace, 128);
            scan(fixture, table, transformedLoop);
            assertEquals(0, table.rehashes(), "no word was acquired twice");
            assertGroups(oracle(rows), table);
            table.release();
          }
        }
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aTableThatGrowsWhileAWordWaitsLosesNoRow(final boolean transformedLoop) {
    // One word, every row a group of its own, in a table hinted far below that: the table grows at an
    // insertion that cannot be the word's first, so rows acquired before it wait with handles the
    // growth invalidated.
    final long[][] rows = new long[64][];
    for (int row = 0; row < rows.length; row++) {
      rows[row] = new long[] {1_000L + row, 7, row + 1L, 2L * row};
    }
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = table(workspace, 4);
      scan(fixture(rows), table, transformedLoop);
      assertTrue(table.rehashes() > 0, "the table grew inside the word");
      assertGroups(oracle(rows), table);
      table.release();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void growthAndRepeatedGroupsInOneWord(final boolean transformedLoop) {
    // Rows of a group on both sides of a growth: the rows before it must not be folded into the
    // stripe the growth moved away from.
    final long[][] rows = new long[3 * 64][];
    for (int row = 0; row < rows.length; row++) {
      rows[row] = new long[] {row % 48, row % 3, row, row % 7 == 0
          ? ABSENT
          : 5L - row};
    }
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = table(workspace, 4);
      scan(fixture(rows), table, transformedLoop);
      assertTrue(table.rehashes() > 0, "the table grew inside a word");
      assertGroups(oracle(rows), table);
      table.release();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void aPassFoldsOnlyTheRowsItOwns(final boolean transformedLoop) {
    final long[][] rows = new long[4 * 64][];
    for (int row = 0; row < rows.length; row++) {
      rows[row] = new long[] {row % 29, 11, row + 3L, row % 4 == 0
          ? ABSENT
          : row};
    }
    final Fixture fixture = fixture(rows);
    final Map<Group, long[]> expected = oracle(rows);
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final Map<Group, long[]> seen = new HashMap<>();
      for (int partition = 0; partition < 2; partition++) {
        final NumericGroupAggTable part = table(workspace, 8);
        part.setPassRange(63, partition, partition + 1);
        scan(fixture, part, transformedLoop);
        final Map<Group, long[]> owned = snapshot(part);
        assertTrue(owned.size() < expected.size(), "a pass owns part of the groups");
        for (final Map.Entry<Group, long[]> group : owned.entrySet()) {
          assertEquals(null, seen.put(group.getKey(), group.getValue()), "two passes own " + group.getKey());
        }
        part.release();
      }
      assertEquals(expected.keySet(), seen.keySet());
      for (final Group group : expected.keySet()) {
        assertArrayEquals(expected.get(group), seen.get(group), group.toString());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void anOverflowingSumStillFails(final boolean transformedLoop) {
    final long[][] rows = {{1, 7, Long.MAX_VALUE, 1}, {2, 7, 5, 1}, {1, 7, 1, 1}};
    try (GroupWorkspace workspace = new GroupWorkspace()) {
      final NumericGroupAggTable table = table(workspace, 16);
      final Fixture fixture = fixture(rows);
      assertThrows(ArithmeticException.class, () -> scan(fixture, table, transformedLoop));
      table.release();
    }
  }

  private static NumericGroupAggTable table(final GroupWorkspace workspace, final int expectedEntries) {
    return NumericGroupAggTable.sumsOnly(OPERANDS, expectedEntries, true, BOTH_SUMS_EXACT, 3)
                               .attachWorkspace(workspace);
  }

  private static void scan(final Fixture fixture, final NumericGroupAggTable out, final boolean transformedLoop) {
    final long[] decline = new long[1];
    ProjectionColumnGroupScan.aggregateByGroupCompositeFlat(fixture.store(), NO_PREDICATES, new ColumnSlice[0][], null,
        null, fixture.keys(), KEY_KINDS, fixture.operands(), 0, 1, out, -1, null, null, transformedLoop
            ? new long[2]
            : null,
        null, decline, null, null, null, null, null);
    assertEquals(0L, decline[0], "the scan declined");
  }

  /**
   * Row by row, with no kernel: rows, the first row's index twice (stamped at insertion and at the
   * first fold), then count and sum per operand.
   */
  private static Map<Group, long[]> oracle(final long[][] rows) {
    final Map<Group, long[]> expected = new HashMap<>();
    for (int row = 0; row < rows.length; row++) {
      final int first = row;
      final long[] group = expected.computeIfAbsent(new Group(rows[row][0], rows[row][1]), key -> {
        final long[] fresh = new long[3 + 2 * OPERANDS];
        fresh[1] = first;
        fresh[2 + 2 * OPERANDS] = first;
        return fresh;
      });
      group[0]++;
      for (int operand = 0; operand < OPERANDS; operand++) {
        final long value = rows[row][2 + operand];
        if (value != ABSENT) {
          group[2 + 2 * operand]++;
          group[3 + 2 * operand] = Math.addExact(group[3 + 2 * operand], value);
        }
      }
    }
    return expected;
  }

  private static void assertGroups(final Map<Group, long[]> expected, final NumericGroupAggTable table) {
    final Map<Group, long[]> actual = snapshot(table);
    assertEquals(expected.size(), table.size());
    assertEquals(expected.keySet(), actual.keySet());
    for (final Group group : expected.keySet()) {
      assertArrayEquals(expected.get(group), actual.get(group), group.toString());
    }
  }

  /** Every group of {@code table}: its accumulator lanes followed by its first-row reference. */
  private static Map<Group, long[]> snapshot(final NumericGroupAggTable table) {
    final int width = table.slotWidth();
    final Map<Group, long[]> result = new HashMap<>();
    for (int c = 0; c < table.storageChunkCount(); c++) {
      final GroupBuffer chunk = table.storageChunkOrNull(c);
      if (chunk == null) {
        continue;
      }
      for (int offset = 0; offset < GroupLanes.count(chunk); offset += table.stride()) {
        if (GroupLanes.get(chunk, offset) != 0) {
          // key lane, accumulator, first-row reference, then the identity: missing mask and both keys.
          assertEquals(0L, GroupLanes.get(chunk, offset + width + 2), "no key of the fixture is missing");
          final Group group =
              new Group(GroupLanes.get(chunk, offset + width + 3), GroupLanes.get(chunk, offset + width + 4));
          assertEquals(null, result.put(group, GroupLanes.toArray(chunk, offset + 1, width + 1)),
              "one stripe per group");
        }
      }
    }
    return result;
  }

  private static Fixture fixture(final long[][] rows) {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {NUMERIC, NUMERIC, NUMERIC, NUMERIC});
    for (int row = 0; row < rows.length; row++) {
      final long[] values = rows[row].clone();
      final boolean[] present = {true, true, values[2] != ABSENT, values[3] != ABSENT};
      for (int column = 2; column < 4; column++) {
        if (!present[column]) {
          values[column] = 0L;
        }
      }
      page.appendRow(row + 1, values, new boolean[4], new String[4], present, new boolean[4], new boolean[4]);
    }
    final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
    final long[] offsets = new long[encoded.columnSegmentIds().length];
    for (int i = 0; i < offsets.length; i++) {
      offsets[i] = i + 1;
    }
    final ProjectionColumnStore store = new ProjectionColumnStore(List.of(new RowGroupDirectory(1, encoded.descriptor(),
        encoded.columnSegmentIds(), offsets, new byte[offsets.length][])));
    final ColumnSlice[][] columns = new ColumnSlice[4][];
    for (int col = 0; col < columns.length; col++) {
      columns[col] = store.column(col, wanted -> {
        final byte[][] segments = new byte[wanted.length][];
        for (int i = 0; i < wanted.length; i++) {
          segments[i] = encoded.segments()[Math.toIntExact(wanted[i] - 1)];
        }
        return segments;
      });
    }
    return new Fixture(store, new ColumnSlice[][] {columns[0], columns[1]},
        new ColumnSlice[][] {columns[2], columns[3]});
  }
}
