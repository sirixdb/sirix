package io.sirix.query.compiler.expression;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.QueryContext;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.array.DArray;
import io.brackit.query.operator.TupleImpl;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.sequence.LazySequence;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.jspecify.annotations.Nullable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class MaterializeExprTest {
  private final QueryContext context = mock(QueryContext.class);
  private final Tuple tuple = new TupleImpl();

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void sourceIsConsumedAndClosedOnceForEachBindingEvaluation(final boolean itemSequenceSubclass) {
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    final Expr source = mock(Expr.class);
    final Sequence lazy = new LazySequence() {
      @Override
      public Iter iterate() {
        opened.incrementAndGet();
        return new BaseIter() {
          private int next = 1;

          @Override
          public @Nullable Item next() {
            return next <= 3
                ? new Int32(next++)
                : null;
          }

          @Override
          public void close() {
            closed.incrementAndGet();
          }
        };
      }
    };
    when(source.evaluate(context, tuple)).thenReturn(itemSequenceSubclass
        ? new ItemSequence() {
          @Override
          public Iter iterate() {
            return lazy.iterate();
          }
        }
        : lazy);
    final MaterializeExpr expression = new MaterializeExpr(source);
    for (int binding = 0; binding < 2; binding++) {
      final Sequence result = expression.evaluate(context, tuple);
      assertEquals(binding + 1, opened.get());
      assertEquals(binding + 1, closed.get(), "source cursor closes before consumers start");
      for (int reference = 0; reference < 3; reference++) {
        int expected = 1;
        try (final Iter iterator = result.iterate()) {
          for (Item item = iterator.next(); item != null; item = iterator.next()) {
            assertEquals(expected++, ((Int32) item).intValue());
          }
        }
        assertEquals(4, expected);
      }
      assertEquals(binding + 1, opened.get(), "references traverse only materialized items");
    }
    verify(source, times(2)).evaluate(context, tuple);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 10000})
  void eagerItemSequencesAreReusedForEachBindingEvaluation(final int count) {
    final Item[] items = new Item[count];
    for (int i = 0; i < count; i++)
      items[i] = new Int32(i + 1);
    final ItemSequence first = new ItemSequence(items);
    final ItemSequence second = new ItemSequence(items);
    final Expr source = mock(Expr.class);
    when(source.evaluate(context, tuple)).thenReturn(first, second);
    final MaterializeExpr expression = new MaterializeExpr(source);
    assertSame(first, expression.evaluate(context, tuple));
    assertSame(second, expression.evaluate(context, tuple));
    assertEquals(count, first.size().intValue());
    try (final Iter iterator = first.iterate()) {
      for (int i = 1; i <= count; i++)
        assertEquals(new Int32(i), iterator.next());
      assertNull(iterator.next());
    }
    verify(source, times(2)).evaluate(context, tuple);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void eagerItemEvaluationKeepsCardinalityChecks(final int count) {
    final Item[] items = new Item[count];
    for (int i = 0; i < count; i++)
      items[i] = Int32.ONE;
    final Expr source = mock(Expr.class);
    when(source.evaluate(context, tuple)).thenReturn(new ItemSequence(items));
    final MaterializeExpr expression = new MaterializeExpr(source);
    if (count == 0)
      assertNull(expression.evaluateToItem(context, tuple));
    else if (count == 1)
      assertSame(Int32.ONE, expression.evaluateToItem(context, tuple));
    else
      assertEquals(ErrorCode.ERR_TYPE_INAPPROPRIATE_TYPE,
          assertThrows(QueryException.class, () -> expression.evaluateToItem(context, tuple)).getCode());
  }

  @Test
  void singletonArrayRemainsOneSequenceItem() {
    final DArray array = new DArray(List.of(Int32.ONE, new Int32(2)));
    final Expr source = mock(Expr.class);
    final ItemSequence buffered = new ItemSequence(array);
    when(source.evaluate(context, tuple)).thenReturn(buffered);
    final Sequence result = new MaterializeExpr(source).evaluate(context, tuple);
    assertSame(buffered, result);
    assertEquals(Int32.ONE, result.size());
    try (final Iter iterator = result.iterate()) {
      assertSame(array, iterator.next());
      assertNull(iterator.next());
    }
  }

  @Test
  void lazySingletonArrayRemainsOneSequenceItem() {
    final DArray array = new DArray(List.of(Int32.ONE, new Int32(2)));
    final Expr source = mock(Expr.class);
    when(source.evaluate(context, tuple)).thenReturn(new LazySequence() {
      @Override
      public Iter iterate() {
        return new ItemSequence(array).iterate();
      }
    });
    final Sequence result = new MaterializeExpr(source).evaluate(context, tuple);
    assertEquals(Int32.ONE, result.size());
    try (final Iter iterator = result.iterate()) {
      assertSame(array, iterator.next());
      assertNull(iterator.next());
    }
    assertSame(array, new MaterializeExpr(source).evaluateToItem(context, tuple));
  }

  @Test
  void anArraySourceKeepsItsExistingRepresentation() {
    final DArray array = new DArray(List.of(Int32.ONE, new Int32(2)));
    final Expr source = mock(Expr.class);
    when(source.evaluate(context, tuple)).thenReturn(array);
    assertSame(array, new MaterializeExpr(source).evaluate(context, tuple));
  }

  @Test
  void failedConsumptionClosesCursorAndDoesNotRetainFailureAcrossEvaluations() {
    final AtomicInteger closed = new AtomicInteger();
    final Expr source = mock(Expr.class);
    when(source.evaluate(context, tuple)).thenReturn(new LazySequence() {
      @Override
      public Iter iterate() {
        return new BaseIter() {
          @Override
          public Item next() {
            throw new QueryException(ErrorCode.ERR_DIVISION_BY_ZERO);
          }

          @Override
          public void close() {
            closed.incrementAndGet();
          }
        };
      }
    }).thenReturn(Int32.ONE);
    final MaterializeExpr expression = new MaterializeExpr(source);
    assertThrows(QueryException.class, () -> expression.evaluate(context, tuple));
    assertEquals(1, closed.get());
    assertEquals(Int32.ONE, expression.evaluateToItem(context, tuple));
  }

  @Test
  // Deliberately pass null to verify the constructor's null-source guard.
  @SuppressWarnings("NullAway")
  void constructorRejectsUpdatingExpressionsAndNull() {
    final Expr updating = mock(Expr.class);
    when(updating.isUpdating()).thenReturn(true);
    assertThrows(IllegalArgumentException.class, () -> new MaterializeExpr(updating));
    assertThrows(NullPointerException.class, () -> new MaterializeExpr(null));
  }
}
