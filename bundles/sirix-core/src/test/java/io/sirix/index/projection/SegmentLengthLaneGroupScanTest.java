/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSegmentFetcher;
import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexHOTStorage.RowGroupDirectory;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.settings.Constants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A string-length aggregate over a {@link ProjectionIndexRowGroupPage#COLUMN_KIND_STRING_SEGMENT}
 * operand in the sliced numeric group kernel: the operand's lane holds packed {@code (segment, id)}
 * cells and no per-leaf dictionary, so the fold indexes one {@code id → length} table per SEGMENT,
 * chosen per leaf from the leaf's zone bounds. No canonical id is ever minted — a length is a
 * property of the value, and every segment's dictionary knows the lengths of its own values.
 *
 * <p>
 * Three segments with the same eight values interned in DIFFERENT orders, so a table applied to the
 * wrong segment yields a wrong answer rather than an exception; rows whose operand is absent (they
 * count and contribute length 0, as {@code fn:string-length(())} does); one leaf whose operand is
 * absent in EVERY row (inverted zone bounds — the sentinel path); the zero group key and rows
 * without a group key (the side slot and the missing accumulator). Every expectation is computed
 * from the generator that built the rows, per length mode.
 * </p>
 */
final class SegmentLengthLaneGroupScanTest {

  private static final int SEGMENTS = 3;

  private static final int LEAVES_PER_SEGMENT = 3;

  private static final int LEAVES = SEGMENTS * LEAVES_PER_SEGMENT;

  private static final int ROWS = 40;

  /** Segment 1's middle leaf: every row's operand is absent, so its zone bounds are the sentinels. */
  private static final int EMPTY_OPERAND_LEAF = 4;

  /** Byte and code-point lengths disagree on half of these — the mode has to reach the fold. */
  private static final String[] PALETTE = {"a", "bb", "ccc", "dddd", "é", "😀", "ñandú", "x😀y"};

  private static final int GROUP_COL = 0;

  private static final int OPERAND_COL = 1;

  /** Result row index for rows WITHOUT a group key; groups 0..3 use their own key. */
  private static final int MISSING = 4;

  /** One dictionary per segment: the same bytes get the same id, a new value the next one. */
  private static final class SegmentEncoder implements GlobalValueDictionaryEncoder {
    private final Map<String, Integer> ids = new HashMap<>();

    @Override
    public int intern(final byte[] source, final int offset, final int length) {
      return intern(new String(source, offset, length, StandardCharsets.UTF_8));
    }

    @Override
    public int intern(final String value) {
      return ids.computeIfAbsent(value, v -> ids.size() + 1);
    }

    /** The {@code id → length} table this segment's dictionary would fill for {@code lengthOf}. */
    int[] lengthTable(final ToIntFunction<String> lengthOf) {
      final int[] table = new int[ids.size() + 1];
      for (final Map.Entry<String, Integer> e : ids.entrySet()) {
        table[e.getValue()] = lengthOf.applyAsInt(e.getKey());
      }
      return table;
    }
  }

  private record Fixture(ProjectionColumnStore store, ColumnSegmentFetcher fetcher, SegmentEncoder[] encoders) {

    int[][][] lengthTables(final ToIntFunction<String> lengthOf) {
      final int[][] bySegment = new int[SEGMENTS][];
      for (int segment = 0; segment < SEGMENTS; segment++) {
        bySegment[segment] = encoders[segment].lengthTable(lengthOf);
      }
      return new int[][][] {bySegment};
    }
  }

  private static int segmentOf(final int leaf) {
    return leaf / LEAVES_PER_SEGMENT;
  }

  /** The operand: a rotation that interns the palette in a different order in every segment. */
  private static String valueOf(final int leaf, final int row) {
    return PALETTE[(row + 3 * leaf + 5 * segmentOf(leaf)) % PALETTE.length];
  }

  private static boolean operandPresent(final int leaf, final int row) {
    return leaf != EMPTY_OPERAND_LEAF && row % 11 != 5;
  }

  /** The group key: {@link #MISSING} for a row without one, else {@code 0..3} — {@code 0} is real. */
  private static int groupOf(final int row) {
    return row % 13 == 7
        ? MISSING
        : row % 4;
  }

  private static int utf8Length(final String value) {
    return value.getBytes(StandardCharsets.UTF_8).length;
  }

  private static int codePointLength(final String value) {
    return value.codePointCount(0, value.length());
  }

  private static ToIntFunction<String> lengthFor(final byte mode) {
    return mode == ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS
        ? SegmentLengthLaneGroupScanTest::codePointLength
        : SegmentLengthLaneGroupScanTest::utf8Length;
  }

  private static Fixture buildFixture() {
    final Map<Long, byte[]> segmentsByOffset = new HashMap<>();
    final List<RowGroupDirectory> directories = new ArrayList<>(LEAVES);
    final SegmentEncoder[] encoders = new SegmentEncoder[SEGMENTS];
    for (int segment = 0; segment < SEGMENTS; segment++) {
      encoders[segment] = new SegmentEncoder();
    }
    long nextOffset = 1_000;
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(new byte[] {
          ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT});
      long recordKey = leaf * 100_000L + 1;
      for (int row = 0; row < ROWS; row++) {
        final int group = groupOf(row);
        final boolean operand = operandPresent(leaf, row);
        page.appendRow(recordKey++, new long[] {group == MISSING
            ? 0L
            : group, 0L}, new boolean[] {false, false}, new String[] {null,
                operand
                    ? valueOf(leaf, row)
                    : null},
            new boolean[] {group != MISSING, operand}, new boolean[] {false, false}, new boolean[] {false, false});
      }
      page.convertStringDictColumnToSegment(OPERAND_COL, encoders[segmentOf(leaf)], segmentOf(leaf));
      final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded =
          ProjectionIndexColumnSegmentCodec.encode(page.serialize());
      final int[] idArr = new int[encoded.columnSegmentIds().length];
      final long[] offArr = new long[idArr.length];
      for (int i = 0; i < idArr.length; i++) {
        idArr[i] = encoded.columnSegmentIds()[i];
        offArr[i] = nextOffset;
        segmentsByOffset.put(nextOffset, encoded.segments()[i]);
        nextOffset += 1 + encoded.segments()[i].length;
      }
      directories.add(new RowGroupDirectory(leaf + 1, encoded.descriptor(), idArr, offArr, new byte[idArr.length][]));
    }
    final ColumnSegmentFetcher fetcher = wanted -> {
      final byte[][] out = new byte[wanted.length][];
      for (int i = 0; i < wanted.length; i++) {
        if (wanted[i] != Constants.NULL_ID_LONG) {
          out[i] = segmentsByOffset.get(wanted[i]);
        }
      }
      return out;
    };
    final ProjectionColumnStore store = new ProjectionColumnStore(directories);
    assertEquals(ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SEGMENT, store.columnKind(OPERAND_COL),
        "the fixture must present a segment-scoped operand, or nothing below tests the segment lane");
    for (int segment = 0; segment < SEGMENTS; segment++) {
      assertEquals(PALETTE.length, encoders[segment].ids.size(), "every segment interns the whole palette");
    }
    assertNotEquals(encoders[0].ids, encoders[1].ids, "segments must mint in different orders, or a swap is invisible");
    assertNotEquals(encoders[1].ids, encoders[2].ids, "segments must mint in different orders, or a swap is invisible");
    return new Fixture(store, fetcher, encoders);
  }

  /**
   * {@code [rows, lengthCount, lengthSum, lengthMin, lengthMax]} per result row, from the generator:
   * an absent operand contributes 0 and still counts, exactly as the kernel's fold does.
   */
  private static long[][] expected(final ToIntFunction<String> lengthOf) {
    final long[][] out = new long[MISSING + 1][];
    for (int g = 0; g <= MISSING; g++) {
      out[g] = new long[] {0L, 0L, 0L, Long.MAX_VALUE, Long.MIN_VALUE};
    }
    for (int leaf = 0; leaf < LEAVES; leaf++) {
      for (int row = 0; row < ROWS; row++) {
        final long[] acc = out[groupOf(row)];
        final long v = operandPresent(leaf, row)
            ? lengthOf.applyAsInt(valueOf(leaf, row))
            : 0L;
        acc[0]++;
        acc[1]++;
        acc[2] += v;
        acc[3] = Math.min(acc[3], v);
        acc[4] = Math.max(acc[4], v);
      }
    }
    return out;
  }

  private static long[][] run(final Fixture f, final byte mode, final int[][] globalTables,
      final int[][][] segmentTables) {
    final ColumnPredicate[] none = new ColumnPredicate[0];
    final ColumnSlice[][] predCols = ProjectionColumnScan.resolvePredicateColumnsShared(f.store(), none, f.fetcher());
    final ColumnSlice[] groupCol = f.store().column(GROUP_COL, f.fetcher());
    final ColumnSlice[][] aggCols = {f.store().column(OPERAND_COL, f.fetcher())};
    final ColumnSlice empty = aggCols[0][EMPTY_OPERAND_LEAF];
    assertTrue(empty.rowCount() > 0 && empty.min() > empty.max(),
        "the all-absent leaf must carry rows under inverted zone bounds, or the sentinel path is untested");
    final NumericGroupAggTable out = new NumericGroupAggTable(1, 64);
    final long[] missing = ProjectionIndexByteScan.newGroupAggAcc(1, Long.MAX_VALUE);
    ProjectionColumnGroupScan.aggregateByGroupNumericFlat(f.store(), none, predCols, null, null, groupCol, aggCols,
        new byte[] {mode}, 0, f.store().rowGroupCount(), out, missing, -1, null, null, null, false, globalTables,
        segmentTables);
    return read(out, missing);
  }

  private static long[][] read(final NumericGroupAggTable out, final long[] missing) {
    final long[][] result = new long[MISSING + 1][];
    for (int bucket = 0, n = out.capacity(); bucket < n; bucket++) {
      final long key = out.keyAtBucket(bucket);
      if (key == 0L) {
        continue;
      }
      assertTrue(key > 0 && key < MISSING, "unexpected group key " + key);
      final int handle = out.accBaseOfBucket(bucket);
      result[(int) key] = lanes(out.storageAtAccBase(handle), out.offsetAtAccBase(handle));
    }
    assertTrue(out.hasZeroKey(), "group 0 is a real key and takes the zero side slot");
    result[0] = lanes(out.zeroSlot(), 0);
    result[MISSING] = lanes(missing, 0);
    for (int g = 0; g <= MISSING; g++) {
      assertTrue(result[g] != null, "group " + g + " went unanswered");
    }
    return result;
  }

  /** {@code [rows, count, sum, min, max]} of the ONE aggregate lane behind {@code base}. */
  private static long[] lanes(final long[] block, final int base) {
    return new long[] {block[base], block[base + 2], block[base + 3], block[base + 4], block[base + 5]};
  }

  private static void assertSameGroups(final long[][] expected, final long[][] actual, final String what) {
    for (int g = 0; g <= MISSING; g++) {
      assertArrayEquals(expected[g], actual[g], what + ": group " + (g == MISSING
          ? "MISSING"
          : String.valueOf(g)) + " [rows, count, sum, min, max]");
    }
  }

  @Test
  @DisplayName("each leaf folds through its own segment's table, in both length modes, absent operands as 0")
  void perSegmentTablesFoldEveryRow() {
    final Fixture f = buildFixture();
    final long[][] bytes = expected(SegmentLengthLaneGroupScanTest::utf8Length);
    final long[][] codePoints = expected(SegmentLengthLaneGroupScanTest::codePointLength);
    for (int g = 0; g <= MISSING; g++) {
      assertNotEquals(bytes[g][2], codePoints[g][2], "the palette must make the modes disagree in every group");
      assertEquals(0L, bytes[g][3], "an absent operand folds a 0 into every group's min");
    }
    assertSameGroups(bytes, run(f, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, null,
        f.lengthTables(SegmentLengthLaneGroupScanTest::utf8Length)), "utf8 bytes");
    assertSameGroups(codePoints, run(f, ProjectionIndexByteScan.STRING_LENGTH_CODE_POINTS, null,
        f.lengthTables(SegmentLengthLaneGroupScanTest::codePointLength)), "code points");
  }

  @Test
  @DisplayName("a table applied to another segment's leaves changes the answer — the leaf chooses the table")
  void swappedTablesChangeTheAnswer() {
    final Fixture f = buildFixture();
    final int[][][] tables = f.lengthTables(SegmentLengthLaneGroupScanTest::utf8Length);
    final int[][] rotated = new int[SEGMENTS][];
    for (int segment = 0; segment < SEGMENTS; segment++) {
      rotated[segment] = tables[0][(segment + 1) % SEGMENTS];
    }
    final long[][] expected = expected(SegmentLengthLaneGroupScanTest::utf8Length);
    final long[][] wrong = run(f, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, null, new int[][][] {rotated});
    for (int g = 0; g <= MISSING; g++) {
      assertEquals(expected[g][0], wrong[g][0], "the swap changes lengths, never which rows a group holds");
      assertNotEquals(expected[g][2], wrong[g][2], "group " + g + " summed the right table under the wrong segment");
    }
  }

  @Test
  @DisplayName("a segment with no table is refused by name, not answered with another segment's lengths")
  void aMissingSegmentTableIsRefused() {
    final Fixture f = buildFixture();
    final int[][][] tables = f.lengthTables(SegmentLengthLaneGroupScanTest::utf8Length);
    tables[0][1] = null;
    final IllegalStateException refused = assertThrows(IllegalStateException.class,
        () -> run(f, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, null, tables));
    assertTrue(refused.getMessage().contains("segment 1"), refused.getMessage());
    // Segment 1 lies past a shorter table array just the same.
    final int[][][] tooShort = {Arrays.copyOf(f.lengthTables(SegmentLengthLaneGroupScanTest::utf8Length)[0], 1)};
    assertTrue(assertThrows(IllegalStateException.class,
        () -> run(f, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, null, tooShort)).getMessage()
                                                                                       .contains("segment 1"));
  }

  @Test
  @DisplayName("a table array that does not cover the aggregate roster, or arrives without modes, is refused")
  void tablesMustCoverTheRoster() {
    final Fixture f = buildFixture();
    assertThrows(IllegalArgumentException.class,
        () -> run(f, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, null, new int[0][][]));
    assertThrows(IllegalArgumentException.class,
        () -> run(f, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, new int[0][], null));
    final ColumnPredicate[] none = new ColumnPredicate[0];
    final ColumnSlice[][] predCols = ProjectionColumnScan.resolvePredicateColumnsShared(f.store(), none, f.fetcher());
    final ColumnSlice[] groupCol = f.store().column(GROUP_COL, f.fetcher());
    final ColumnSlice[][] aggCols = {f.store().column(OPERAND_COL, f.fetcher())};
    final int[][][] tables = f.lengthTables(SegmentLengthLaneGroupScanTest::utf8Length);
    assertThrows(IllegalArgumentException.class,
        () -> ProjectionColumnGroupScan.aggregateByGroupNumericFlat(f.store(), none, predCols, null, null, groupCol,
            aggCols, null, 0, f.store().rowGroupCount(), new NumericGroupAggTable(1, 64),
            ProjectionIndexByteScan.newGroupAggAcc(1, Long.MAX_VALUE), -1, null, null, null, false, null, tables),
        "segment tables without a length mode per aggregate name no operand");
  }

  @Test
  @DisplayName("a segment operand handed no table at all is refused, not folded through a dictionary it lacks")
  void aSegmentLaneWithoutTablesIsRefusedByName() {
    final Fixture f = buildFixture();
    final IllegalStateException refused = assertThrows(IllegalStateException.class,
        () -> run(f, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, null, null));
    assertTrue(refused.getMessage().contains("no length table"), refused.getMessage());
  }

  @Test
  @DisplayName("a global table for the lane outranks the segment tables — the two are never both consulted")
  void aGlobalTableOutranksTheSegmentTables() {
    final Fixture f = buildFixture();
    final int[] hundred = new int[PALETTE.length + 1];
    Arrays.fill(hundred, 100);
    final long[][] expected = expected(value -> 100);
    assertSameGroups(expected, run(f, ProjectionIndexByteScan.STRING_LENGTH_UTF8_BYTES, new int[][] {hundred},
        f.lengthTables(SegmentLengthLaneGroupScanTest::utf8Length)), "global table wins");
  }
}
