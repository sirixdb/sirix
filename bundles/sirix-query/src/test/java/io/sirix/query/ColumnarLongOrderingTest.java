package io.sirix.query;

import io.brackit.query.Tuple;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.operator.TupleImpl;
import io.brackit.query.util.sort.Ordering;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

final class ColumnarLongOrderingTest {
  @Test
  void primitivePermutationMatchesBrackitWithMissingTiesAndLongExtremes() {
    final Random random = new Random(20261018L);
    final int count = 600;
    final long[][] values = new long[2][count];
    final boolean[][] present = new boolean[2][count];
    final long[][] sourceKeys = new long[2][count];
    for (int row = 0; row < count; row++) {
      for (int field = 0; field < 2; field++) {
        values[field][row] = switch (row % 7) {
          case 0 -> Long.MIN_VALUE;
          case 1 -> Long.MAX_VALUE;
          default -> random.nextInt(4) - 2;
        };
        present[field][row] = random.nextInt(4) != 0;
        sourceKeys[field][row] = random.nextInt(40);
      }
    }
    for (int modifiers = 0; modifiers < 16; modifiers++) {
      final boolean[] ascending = {(modifiers & 1) != 0, (modifiers & 2) != 0};
      final boolean[] emptyLeast = {(modifiers & 4) != 0, (modifiers & 8) != 0};
      final ColumnarRecordSequence.Builder builder = new ColumnarRecordSequence.Builder(
          new QNm[] {new QNm("a"), new QNm("b")}, new boolean[2], 1);
      final Ordering ordering = new Ordering(new Expr[0], new Ordering.OrderModifier[] {
          new Ordering.OrderModifier(ascending[0], emptyLeast[0], null),
          new Ordering.OrderModifier(ascending[1], emptyLeast[1], null),
          new Ordering.OrderModifier(true, true, null), new Ordering.OrderModifier(true, true, null)});
      for (int row = 0; row < count; row++) {
        builder.addRow();
        final Sequence[] keys = new Sequence[4];
        for (int field = 0; field < 2; field++) {
          if (present[field][row]) {
            builder.setLong(field, row, values[field][row]);
            keys[field] = new Int64(values[field][row]);
          }
          keys[field + 2] = new Int64(sourceKeys[field][row]);
        }
        ordering.add(keys, new TupleImpl(new Int64(row)));
      }
      final int[] expected = new int[count];
      try (final Stream<? extends Tuple> sorted = ordering.sorted()) {
        for (int row = 0; row < count; row++) {
          expected[row] = ((IntNumeric) sorted.next().get(0)).intValue();
        }
      }
      assertArrayEquals(expected, builder.orderByLongs(new int[] {0, 1}, ascending, emptyLeast, sourceKeys),
          "order modifier bits " + modifiers);
    }
  }

  @Test
  void emptyAndUnsupportedOrderInputsAreChecked() {
    final ColumnarRecordSequence.Builder empty = new ColumnarRecordSequence.Builder(
        new QNm[] {new QNm("v")}, new boolean[1], 0);
    assertArrayEquals(new int[0], empty.orderByLongs(new int[] {0}, new boolean[] {true}, new boolean[] {true}));
    assertThrows(IllegalArgumentException.class,
        () -> empty.orderByLongs(new int[] {0}, new boolean[0], new boolean[] {true}));
    final ColumnarRecordSequence.Builder text = new ColumnarRecordSequence.Builder(
        new QNm[] {new QNm("v")}, new boolean[] {true}, 0);
    assertThrows(IllegalArgumentException.class,
        () -> text.orderByLongs(new int[] {0}, new boolean[] {true}, new boolean[] {true}));
    empty.addRow();
    assertThrows(IllegalArgumentException.class,
        () -> empty.orderByLongs(new int[0], new boolean[0], new boolean[0], new long[0]));
  }
}
