package io.sirix.query.compiler.vectorized;

import io.brackit.query.BrackitQueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Dbl;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Str;
import io.brackit.query.block.Sink;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.operator.TupleImpl;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for {@link SimdSelect} — SIMD-accelerated Select block
 * for Brackit's block execution pipeline.
 */
final class SimdSelectTest {

  @Test
  void filterLongGreaterThan() throws QueryException {
    final Tuple[] buf = longTuples(10, 20, 30, 40, 50);
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.GT, 25L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(3, collector.collected.size());
    assertEquals(30L, ((Int64) collector.collected.get(0).get(0)).longValue());
    assertEquals(40L, ((Int64) collector.collected.get(1).get(0)).longValue());
    assertEquals(50L, ((Int64) collector.collected.get(2).get(0)).longValue());
  }

  @Test
  void filterLongEqual() throws QueryException {
    final Tuple[] buf = longTuples(1, 2, 3, 2, 1);
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.EQ, 2L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(2, collector.collected.size());
    assertEquals(2L, ((Int64) collector.collected.get(0).get(0)).longValue());
    assertEquals(2L, ((Int64) collector.collected.get(1).get(0)).longValue());
  }

  @Test
  void filterLongLessThan() throws QueryException {
    final Tuple[] buf = longTuples(10, 20, 30, 40, 50);
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.LT, 30L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(2, collector.collected.size());
    assertEquals(10L, ((Int64) collector.collected.get(0).get(0)).longValue());
    assertEquals(20L, ((Int64) collector.collected.get(1).get(0)).longValue());
  }

  @Test
  void filterLongLessThanOrEqual() throws QueryException {
    final Tuple[] buf = longTuples(10, 20, 30, 40, 50);
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.LE, 30L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(3, collector.collected.size());
  }

  @Test
  void filterLongGreaterThanOrEqual() throws QueryException {
    final Tuple[] buf = longTuples(10, 20, 30, 40, 50);
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.GE, 30L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(3, collector.collected.size());
  }

  @Test
  void filterLongNotEqual() throws QueryException {
    final Tuple[] buf = longTuples(1, 2, 3, 2, 1);
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.NE, 2L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(3, collector.collected.size());
  }

  @Test
  void filterDoubleGreaterThan() throws QueryException {
    final Tuple[] buf = doubleTuples(1.0, 2.5, 3.7, 4.2, 5.0);
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forDouble(0, ComparisonOperator.GT, 3.0, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(3, collector.collected.size());
    assertEquals(3.7, ((Dbl) collector.collected.get(0).get(0)).doubleValue());
    assertEquals(4.2, ((Dbl) collector.collected.get(1).get(0)).doubleValue());
    assertEquals(5.0, ((Dbl) collector.collected.get(2).get(0)).doubleValue());
  }

  @Test
  void filterDoubleWithIntPromotionFallback() throws QueryException {
    // Int64 values should be promoted to double for comparison
    final Tuple[] buf = longTuples(10, 20, 30, 40, 50);
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forDouble(0, ComparisonOperator.GT, 25.0, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(3, collector.collected.size());
  }

  @Test
  void nonNumericTuplesFallbackToExpression() throws QueryException {
    // String tuples → SIMD can't handle, falls back (null pred → drops all)
    final Tuple[] buf = new Tuple[]{
        new TupleImpl(new Str("hello")),
        new TupleImpl(new Str("world"))

    };
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.GT, 0L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(0, collector.collected.size());
  }

  @Test
  void largeBatchExercisesSimdAndScalarTail() throws QueryException {
    final int size = 1000;
    final Tuple[] buf = new Tuple[size];
    for (int i = 0; i < size; i++) {
      buf[i] = new TupleImpl(new Int64(i));
    }
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.GE, 500L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(500, collector.collected.size());
    assertEquals(500L, ((Int64) collector.collected.get(0).get(0)).longValue());
    assertEquals(999L, ((Int64) collector.collected.get(499).get(0)).longValue());
  }

  @Test
  void noMatchesProducesNoOutput() throws QueryException {
    final Tuple[] buf = longTuples(1, 2, 3);
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.GT, 100L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(0, collector.collected.size());
  }

  @Test
  void outputWidthUnchanged() {
    final var block = SimdSelect.forLong(0, ComparisonOperator.GT, 0L, null);
    assertEquals(5, block.outputWidth(5));
    assertEquals(1, block.outputWidth(1));
  }

  @Test
  void sinkForkCreatesIndependentSink() throws QueryException {
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(0, ComparisonOperator.GT, 25L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    final Sink forked = sink.fork();

    // Both sinks should work independently
    sink.begin();
    sink.output(longTuples(10, 30), 2);
    sink.end();

    forked.begin();
    forked.output(longTuples(40, 50), 2);
    forked.end();

    assertEquals(3, collector.collected.size());
  }

  @Test
  void multiColumnTupleFilterOnSecondColumn() throws QueryException {
    // Tuples with (name, value) — filter on column 1 (value)
    final Tuple[] buf = new Tuple[]{
        new TupleImpl(new Sequence[]{new Str("a"), new Int64(10)}),
        new TupleImpl(new Sequence[]{new Str("b"), new Int64(20)}),
        new TupleImpl(new Sequence[]{new Str("c"), new Int64(30)}),
    };
    final var collector = new CollectingTestSink();
    final var block = SimdSelect.forLong(1, ComparisonOperator.GT, 15L, null);
    final Sink sink = block.create(new BrackitQueryContext(), collector);

    sink.begin();
    sink.output(buf, buf.length);
    sink.end();

    assertEquals(2, collector.collected.size());
    assertEquals("b", ((Str) collector.collected.get(0).get(0)).stringValue());
    assertEquals("c", ((Str) collector.collected.get(1).get(0)).stringValue());
  }

  // --- Validation ---

  @Test
  void rejectsNegativeTuplePosition() {
    assertThrows(IllegalArgumentException.class,
        () -> SimdSelect.forLong(-1, ComparisonOperator.GT, 0L, null));
  }

  @Test
  void rejectsNullOperator() {
    assertThrows(IllegalArgumentException.class,
        () -> SimdSelect.forLong(0, null, 0L, null));
  }

  // --- Helpers ---

  private static Tuple[] longTuples(long... values) {
    final Tuple[] tuples = new Tuple[values.length];
    for (int i = 0; i < values.length; i++) {
      tuples[i] = new TupleImpl(new Int64(values[i]));
    }
    return tuples;
  }

  private static Tuple[] doubleTuples(double... values) {
    final Tuple[] tuples = new Tuple[values.length];
    for (int i = 0; i < values.length; i++) {
      tuples[i] = new TupleImpl(new Dbl(values[i]));
    }
    return tuples;
  }

  /**
   * Test sink that collects all output tuples for assertion.
   */
  private static final class CollectingTestSink implements Sink {
    final List<Tuple> collected = new ArrayList<>();

    @Override
    public void output(Tuple[] buf, int len) {
      for (int i = 0; i < len; i++) {
        collected.add(buf[i]);
      }
    }

    @Override
    public Sink fork() {
      return this; // fan-in: forked sinks write to same collector
    }

    @Override
    public Sink partition(Sink stopAt) {
      return this;
    }

    @Override
    public void begin() {}

    @Override
    public void end() {}

    @Override
    public void fail() {}
  }
}
