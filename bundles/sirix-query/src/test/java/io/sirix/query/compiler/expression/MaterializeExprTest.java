package io.sirix.query.compiler.expression;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryException;
import io.brackit.query.atomic.Int32;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jsonitem.array.DArray;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.sequence.LazySequence;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

final class MaterializeExprTest {
  @Test
  void sourceIsConsumedAndClosedOnceForEachBindingEvaluation() {
    final AtomicInteger opened = new AtomicInteger();
    final AtomicInteger closed = new AtomicInteger();
    final Expr source = mock(Expr.class);
    when(source.evaluate(null, null)).thenReturn(new LazySequence() {
      @Override
      public Iter iterate() {
        opened.incrementAndGet();
        return new BaseIter() {
          private int next = 1;

          @Override
          public Item next() {
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
    });
    final MaterializeExpr expression = new MaterializeExpr(source);
    for (int binding = 0; binding < 2; binding++) {
      final Sequence result = expression.evaluate(null, null);
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
    verify(source, times(2)).evaluate(null, null);
  }

  @Test
  void singletonArrayRemainsOneSequenceItem() {
    final DArray array = new DArray(List.of(Int32.ONE, new Int32(2)));
    final Expr source = mock(Expr.class);
    when(source.evaluate(null, null)).thenReturn(new ItemSequence(array));
    final Sequence result = new MaterializeExpr(source).evaluate(null, null);
    assertEquals(Int32.ONE, result.size());
    try (final Iter iterator = result.iterate()) {
      assertSame(array, iterator.next());
      assertNull(iterator.next());
    }
  }

  @Test
  void anArraySourceKeepsItsExistingRepresentation() {
    final DArray array = new DArray(List.of(Int32.ONE, new Int32(2)));
    final Expr source = mock(Expr.class);
    when(source.evaluate(null, null)).thenReturn(array);
    assertSame(array, new MaterializeExpr(source).evaluate(null, null));
  }

  @Test
  void failedConsumptionClosesCursorAndDoesNotRetainFailureAcrossEvaluations() {
    final AtomicInteger closed = new AtomicInteger();
    final Expr source = mock(Expr.class);
    when(source.evaluate(null, null)).thenReturn(new LazySequence() {
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
    assertThrows(QueryException.class, () -> expression.evaluate(null, null));
    assertEquals(1, closed.get());
    assertEquals(Int32.ONE, expression.evaluateToItem(null, null));
  }

  @Test
  void constructorRejectsUpdatingExpressionsAndNull() {
    final Expr updating = mock(Expr.class);
    when(updating.isUpdating()).thenReturn(true);
    assertThrows(IllegalArgumentException.class, () -> new MaterializeExpr(updating));
    assertThrows(NullPointerException.class, () -> new MaterializeExpr(null));
  }
}
