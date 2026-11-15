package io.sirix.query.compiler.translator;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.ErrorCode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.operator.TupleImpl;
import io.brackit.query.atomic.Int64;
import io.brackit.query.jdm.Expr;
import io.sirix.query.scan.VectorizedRouteRefusedException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * A fallback may change execution cost, but must not hide errors or reinterpret an empty result.
 */
final class VectorizedFallbackExprTest {
  private final QueryContext context = new BrackitQueryContext();
  private final Tuple tuple = new TupleImpl();
  private final Expr vectorized = mock(Expr.class);
  private final Expr generic = mock(Expr.class);
  private final VectorizedFallbackExpr expression = new VectorizedFallbackExpr(vectorized, generic);

  @Test
  void supportedResultDoesNotEvaluateGenericExpression() {
    final Int64 value = new Int64(19);
    when(vectorized.evaluate(context, tuple)).thenReturn(value);
    assertSame(value, expression.evaluateToItem(context, tuple));
    verifyNoInteractions(generic);
  }

  @Test
  void emptySequenceIsAResult() {
    assertNull(expression.evaluate(context, tuple));
    verifyNoInteractions(generic);
  }

  @Test
  void explicitRefusalEvaluatesGenericExpressionWithSameBindings() {
    final Int64 value = new Int64(23);
    when(vectorized.evaluate(context, tuple)).thenThrow(new VectorizedRouteRefusedException("test refusal"));
    when(generic.evaluate(context, tuple)).thenReturn(value);
    assertSame(value, expression.evaluateToItem(context, tuple));
  }

  @Test
  void unrelatedQueryFailurePropagatesUnchanged() {
    final QueryException failure = new QueryException(ErrorCode.BIT_DYN_INT_ERROR, "storage failure");
    when(vectorized.evaluate(context, tuple)).thenThrow(failure);
    assertSame(failure, assertThrows(QueryException.class, () -> expression.evaluate(context, tuple)));
    verifyNoInteractions(generic);
  }

  @Test
  void genericTypeErrorPropagatesUnchanged() {
    final QueryException failure = new QueryException(ErrorCode.ERR_INVALID_ARGUMENT_TYPE);
    when(vectorized.evaluate(context, tuple)).thenThrow(new VectorizedRouteRefusedException("mixed types"));
    when(generic.evaluate(context, tuple)).thenThrow(failure);
    assertSame(failure, assertThrows(QueryException.class, () -> expression.evaluate(context, tuple)));
  }

  @Test
  void preservesGenericExpressionProperties() {
    assertFalse(expression.isUpdating());
    assertFalse(expression.isVacuous());
    when(generic.isUpdating()).thenReturn(true);
    when(generic.isVacuous()).thenReturn(true);
    assertTrue(expression.isUpdating());
    assertTrue(expression.isVacuous());
  }
}
