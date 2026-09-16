/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionColumnStore.PackedDictionaryIds;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexScan.ColumnPredicate;
import io.sirix.index.projection.ProjectionIndexScan.Op;
import io.sirix.index.projection.ProjectionIndexScan.PredicateTree;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class PackedDictionaryPredicateTest {

  @Test
  void packedWordsMatchTheRowOracleForEveryLaneWidthAndPartialWord() {
    final Random random = new Random(0x18d174);
    for (final int width : new int[] {0, 1, 2, 4}) {
      final int alphabet = 1 << width;
      for (final int rows : new int[] {1, 7, 31, 63, 64, 65, 127, 129, 1023, 1024}) {
        final int[] values = new int[rows];
        for (int row = 0; row < rows; row++) {
          values[row] = random.nextInt(alphabet);
        }
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        ProjectionIndexRowGroupCodec.encodePackedIds(output, values, rows, alphabet - 1);
        final PackedDictionaryIds packed = new PackedDictionaryIds(output.toByteArray(), 1, rows, width);
        for (int trial = 0; trial < 50; trial++) {
          final long accepted = random.nextLong() & ((1L << alphabet) - 1);
          for (int from = 0; from < rows; from += 64) {
            final int count = Math.min(64, rows - from);
            final long candidates = random.nextLong() & (count == 64
                ? -1L
                : (1L << count) - 1);
            long expected = 0L;
            for (int row = 0; row < count; row++) {
              if ((accepted & (1L << values[from + row])) != 0) {
                expected |= 1L << row;
              }
            }
            assertEquals(expected & candidates, packed.matchingWord(from, accepted) & candidates,
                "width=" + width + ", rows=" + rows + ", from=" + from);
          }
        }
        assertFalse(packed.isMaterialized());
      }
    }
    assertThrows(IllegalStateException.class, () -> new PackedDictionaryIds(new byte[] {2}, 1, 64, 2));
    assertThrows(IllegalStateException.class, () -> new PackedDictionaryIds(new byte[16], 1, 16, 3));
    assertThrows(IllegalStateException.class, () -> new PackedDictionaryIds(new byte[16], 1, 16, 2));
  }

  @Test
  void predicatesAndBooleanTreesAgreeWithDenseSlicesAndUnicodeRowValues() {
    final Random random = new Random(0x52a9);
    for (final int alphabet : new int[] {1, 2, 4, 16}) {
      final String[] dictionary = new String[alphabet];
      for (int id = 0; id < alphabet; id++) {
        dictionary[id] = id == 0
            ? ""
            : id == 1
                ? "\uE000"
                : id == 2
                    ? "\uD83D\uDE00"
                    : "value-" + id;
      }
      for (final int rows : new int[] {63, 64, 65, 129, 1024}) {
        final String[] values = new String[rows];
        for (int row = 0; row < rows; row++) {
          values[row] = row < alphabet
              ? dictionary[row]
              : row % 17 == 0
                  ? null
                  : dictionary[random.nextInt(alphabet)];
        }
        final EncodedRowGroup encoded = encode(values);
        for (final Op op : new Op[] {Op.EQ, Op.NE, Op.STR_LT, Op.STR_LE, Op.STR_GT, Op.STR_GE, Op.STR_CONTAINS}) {
          for (final String literal : new String[] {"", "\uE000", "\uD83D\uDE00", "absent", "value-"}) {
            final ColumnSlice slice = decode(encoded);
            assertNotNull(slice.packedStringIds());
            final ColumnPredicate predicate = predicate(op, literal);
            final long[] actual = new long[(rows + 63) >>> 6];
            ProjectionColumnScan.evaluateMask(new ColumnPredicate[] {predicate}, new ColumnSlice[][] {{slice}}, 0, rows,
                actual);
            final long[] expected = new long[actual.length];
            for (int row = 0; row < rows; row++) {
              if (matches(values[row], op, literal)) {
                expected[row >>> 6] |= 1L << (row & 63);
              }
            }
            assertArrayEquals(expected, actual, op + " literal=" + literal);
            if (op == Op.EQ || op == Op.NE) {
              assertFalse(slice.packedStringIds().isMaterialized(), "simple predicates must keep the packed view");
            }
            final int[] dense = slice.stringDictIds();
            assertEquals(rows, dense.length);
            assertSame(dense, slice.stringDictIds(), "dense compatibility is memoized once per immutable slice");
            final long[] afterMaterialization = new long[actual.length];
            ProjectionColumnScan.evaluateMask(new ColumnPredicate[] {predicate}, new ColumnSlice[][] {{slice}}, 0, rows,
                afterMaterialization);
            assertArrayEquals(expected, afterMaterialization);
          }
        }
        final ColumnPredicate[] predicates =
            {predicate(Op.EQ, ""), predicate(Op.NE, "\uE000"), predicate(Op.STR_CONTAINS, "value-")};
        final PredicateTree tree = PredicateTree.of(predicates,
            new byte[] {0, 1, PredicateTree.OP_OR, 2, PredicateTree.OP_NOT, PredicateTree.OP_AND});
        final ColumnSlice slice = decode(encoded);
        final ColumnSlice[] column = {slice};
        final long[] actual = new long[(rows + 63) >>> 6];
        ProjectionColumnScan.evaluateMaskTree(tree, new ColumnSlice[][] {column, column, column}, 0, rows, actual);
        final long[] expected = new long[actual.length];
        for (int row = 0; row < rows; row++) {
          if ((matches(values[row], Op.EQ, "") || matches(values[row], Op.NE, "\uE000"))
              && !matches(values[row], Op.STR_CONTAINS, "value-")) {
            expected[row >>> 6] |= 1L << (row & 63);
          }
        }
        assertArrayEquals(expected, actual, "OR/AND/NOT must preserve missing-value semantics");
      }
    }
  }

  private static EncodedRowGroup encode(final String[] values) {
    final ProjectionIndexRowGroupPage page =
        new ProjectionIndexRowGroupPage(new byte[] {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT});
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = new boolean[1];
    final boolean[] flags = new boolean[1];
    for (int row = 0; row < values.length; row++) {
      strings[0] = values[row] == null
          ? ""
          : values[row];
      present[0] = values[row] != null;
      page.appendRow(row + 1L, longs, bools, strings, present, flags, flags);
    }
    return ProjectionIndexColumnSegmentCodec.encode(page.serialize());
  }

  private static ColumnSlice decode(final EncodedRowGroup encoded) {
    byte[] body = null;
    byte[] dictionary = null;
    for (int at = 0; at < encoded.columnSegmentIds().length; at++) {
      if (encoded.columnSegmentIds()[at] == ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(0)) {
        body = encoded.segments()[at];
      } else if (encoded.columnSegmentIds()[at] == ProjectionIndexColumnSegmentCodec.dictColumnSegmentId(0)) {
        dictionary = encoded.segments()[at];
      }
    }
    return ProjectionIndexColumnSegmentCodec.decodeStringSlice(encoded.descriptor(), body, dictionary, 0);
  }

  private static ColumnPredicate predicate(final Op op, final String literal) {
    final byte[] bytes = literal.getBytes(StandardCharsets.UTF_8);
    return switch (op) {
      case EQ -> ColumnPredicate.stringEq(0, bytes);
      case NE -> ColumnPredicate.stringNe(0, bytes);
      case STR_LT -> ColumnPredicate.stringLt(0, bytes);
      case STR_LE -> ColumnPredicate.stringLe(0, bytes);
      case STR_GT -> ColumnPredicate.stringGt(0, bytes);
      case STR_GE -> ColumnPredicate.stringGe(0, bytes);
      case STR_CONTAINS -> ColumnPredicate.stringContains(0, bytes);
      default -> throw new AssertionError(op);
    };
  }

  private static boolean matches(final String value, final Op op, final String literal) {
    if (value == null) {
      return false;
    }
    // Brackit's Str#cmp uses UTF-16 code-unit order, including the supplementary/BMP inversion.
    final int order = value.compareTo(literal);
    return switch (op) {
      case EQ -> order == 0;
      case NE -> order != 0;
      case STR_LT -> order < 0;
      case STR_LE -> order <= 0;
      case STR_GT -> order > 0;
      case STR_GE -> order >= 0;
      case STR_CONTAINS -> value.contains(literal);
      default -> throw new AssertionError(op);
    };
  }

}
