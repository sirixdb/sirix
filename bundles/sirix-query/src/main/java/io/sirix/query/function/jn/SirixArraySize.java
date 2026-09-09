package io.sirix.query.function.jn;

import io.brackit.query.QueryContext;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.expr.ArrayAccessExpr;
import io.brackit.query.function.AbstractFunction;
import io.brackit.query.function.json.JSONFun;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Signature;
import io.brackit.query.jdm.type.SequenceType;
import io.brackit.query.module.StaticContext;
import io.brackit.query.operator.TupleImpl;
import io.brackit.query.sequence.ItemSequence;
import io.sirix.query.json.AbstractJsonDBArray;

import java.util.concurrent.atomic.LongAdder;

/**
 * Returns an array's stored cardinality and records successful O(1) Sirix-array answers.
 *
 * <p>
 * The O(1) arm is deliberately restricted to a direct Sirix-backed array. Every other operand is
 * evaluated by Brackit's own {@link ArrayAccessExpr} and counted through its
 * {@link Sequence#size()} contract. That preserves ArrayAccessExpr's exact runtime dispatch, type
 * errors, lazy sequence behavior, and empty-member behavior instead of maintaining a subtly
 * different local unbox implementation. Literal and other in-memory arrays therefore receive the
 * ordinary Brackit answer and never move the serving counter.
 */
public final class SirixArraySize extends AbstractFunction {

  public static final QNm SIRIX_ARRAY_SIZE = new QNm(JSONFun.JSON_NSURI, JSONFun.JSON_PREFIX, "sirix-array-size");

  private static final LongAdder STORED_ARRAY_SIZES_SERVED = new LongAdder();
  private static final Sequence EMPTY_ARRAY_INDEX = new ItemSequence();

  public SirixArraySize() {
    super(SIRIX_ARRAY_SIZE, new Signature(SequenceType.INTEGER, SequenceType.ITEM_SEQUENCE), true);
  }

  @Override
  public Sequence execute(final StaticContext staticContext, final QueryContext queryContext, final Sequence[] args) {
    final Sequence value = args[0];
    if (value == null) {
      return Int32.ZERO;
    }
    if (value instanceof AbstractJsonDBArray<?> array) {
      final IntNumeric length = array.length();
      STORED_ARRAY_SIZES_SERVED.increment();
      return length;
    }
    final Sequence unboxed =
        new ArrayAccessExpr(value, EMPTY_ARRAY_INDEX).evaluate(queryContext, TupleImpl.EMPTY_TUPLE);
    return unboxed == null
        ? Int32.ZERO
        : unboxed.size();
  }

  /** Process-wide, monotonic count of successfully served stored-array cardinalities. */
  public static long storedArraySizesServedCount() {
    return STORED_ARRAY_SIZES_SERVED.sum();
  }

  /** Tests only; there is intentionally no process-wide coordination around a reset. */
  static void resetStoredArraySizesServedForTests() {
    STORED_ARRAY_SIZES_SERVED.reset();
  }
}
