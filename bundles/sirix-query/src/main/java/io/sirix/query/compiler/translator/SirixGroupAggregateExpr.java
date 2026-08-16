package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.expr.Cast;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.operator.TupleImpl;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.ExprUtil;
import io.brackit.query.util.sort.Ordering;
import io.sirix.query.scan.SirixVectorizedExecutor;

import java.util.ArrayList;
import java.util.List;

/**
 * Projection-served PER-GROUP AGGREGATE expression (P5b stage 7a): attempts
 * {@link SirixVectorizedExecutor#executeGroupByAggregate} and, on any decline ({@code null} — store
 * not covering, gates failed, transient fill trouble), evaluates the GENERIC pipeline expression
 * compiled alongside. Unlike Brackit's vectorized expressions (which throw on unsupported), this
 * wrapper makes serving strictly best-effort: the answer is always produced, the projection only
 * decides how fast.
 *
 * <p>
 * When the pipeline carried an {@code order by} over the grouped stream, the served groups are
 * sorted here with Brackit's own {@link Ordering} — the same comparator and the same stable
 * {@code TupleSort} the interpreter would have used, rather than a second implementation of
 * XQuery's ordering rules that could drift from it on empty keys, {@code NaN}, or ties. The
 * executor emits groups in document first-appearance order, so a stable sort over that input
 * reproduces the interpreter's tie order exactly.
 */
public final class SirixGroupAggregateExpr implements Expr {

  private final SirixVectorizedExecutor executor;
  private final String[] sourcePath;
  private final PredicateNode predicateOrNull;
  private final String[] groupFields;
  private final String[] keyNames;
  private final String[] funcs;
  private final String[] aggFields;
  private final String[] outNames;
  /** Emitted-entry index per order-by spec, or {@code null} when the pipeline had no order by. */
  private final int[] orderIndexes;
  private final boolean[] orderAsc;
  private final boolean[] orderEmptyLeast;
  private final Ordering.OrderModifier[] orderModifiers;
  /**
   * Sole-consumer {@code fn:subsequence} cap over the ORDERED groups ({@code start+length-1}), or
   * {@code -1}: with a cap the executor may heap-select the first {@code limit} groups of the
   * stable order instead of sorting and materializing every group.
   */
  private final long limit;
  /** Non-null only for a VARIABLE source (external variable): re-verified per evaluation. */
  private final SourceRef runtimeSourceRef;
  private final Expr genericFallback;

  public SirixGroupAggregateExpr(final SirixVectorizedExecutor executor, final String[] sourcePath,
      final PredicateNode predicateOrNull, final String[] groupFields, final String[] keyNames, final String[] funcs,
      final String[] aggFields, final String[] outNames, final int[] orderIndexes, final boolean[] orderAsc,
      final boolean[] orderEmptyLeast, final long limit, final SourceRef runtimeSourceRef, final Expr genericFallback) {
    this.executor = executor;
    this.sourcePath = sourcePath;
    this.predicateOrNull = predicateOrNull;
    this.groupFields = groupFields;
    this.keyNames = keyNames;
    this.funcs = funcs;
    this.aggFields = aggFields;
    this.outNames = outNames;
    this.runtimeSourceRef = runtimeSourceRef;
    this.genericFallback = genericFallback;
    if (orderIndexes == null || orderIndexes.length == 0) {
      this.orderIndexes = null;
      this.orderAsc = null;
      this.orderEmptyLeast = null;
      this.orderModifiers = null;
      this.limit = -1L;
    } else {
      this.orderIndexes = orderIndexes.clone();
      this.orderAsc = orderAsc.clone();
      this.orderEmptyLeast = orderEmptyLeast.clone();
      this.orderModifiers = new Ordering.OrderModifier[orderIndexes.length];
      for (int i = 0; i < orderIndexes.length; i++) {
        // Collation stays null: the detection stage declines every non-default collation, and
        // null is what Brackit's own compiler passes for the codepoint default.
        this.orderModifiers[i] = new Ordering.OrderModifier(orderAsc[i], orderEmptyLeast[i], null);
      }
      this.limit = limit;
    }
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) throws QueryException {
    // Runtime source gate: an external-variable source is verifiable only now, when the
    // context carries the actual binding — a foreign binding falls back to the generic
    // pipeline, which evaluates the same binding and stays correct.
    if (runtimeSourceRef != null && !executor.acceptsSource(runtimeSourceRef, ctx)) {
      return genericFallback.evaluate(ctx, tuple);
    }
    if (executor.canExecute(ctx)) {
      final SirixVectorizedExecutor.ServedGroups served = executor.executeGroupByAggregate(ctx, sourcePath,
          predicateOrNull, groupFields, keyNames, funcs, aggFields, outNames, orderIndexes, orderAsc, orderEmptyLeast,
          limit);
      if (served != null) {
        if (orderIndexes == null || served.ordered()) {
          // Either no order-by, or the kernel already ordered (and under a limit, truncated to
          // the first `limit` groups of the stable order — the downstream fn:subsequence still
          // slices its window out of that prefix, which is all it can ever pull).
          return served.groups();
        }
        final Sequence sorted = sort(served.groups());
        if (sorted != null) {
          return sorted;
        }
        // Unsortable here (a key that is not an atomic, incomparable types across groups). The
        // generic pipeline re-derives the same groups and either orders them or raises the very
        // error the interpreter would raise — both are answers this wrapper must not invent.
      }
    }
    return genericFallback.evaluate(ctx, tuple);
  }

  /**
   * Sort the served groups by the annotated specs, or return {@code null} to decline.
   *
   * @return the ordered sequence, or {@code null} when the groups cannot be ordered here
   */
  private Sequence sort(final Sequence served) {
    try {
      final Ordering ordering = new Ordering(new Expr[0], orderModifiers);
      int count = 0;
      try (final Iter iter = served.iterate()) {
        for (Item item = iter.next(); item != null; item = iter.next()) {
          if (!(item instanceof final Object record)) {
            return null; // not the record shape the annotation described
          }
          final Sequence[] keys = new Sequence[orderIndexes.length];
          for (int i = 0; i < orderIndexes.length; i++) {
            // Mirrors Ordering#sortKeys: atomize, and cast untyped to string, so a key
            // reaching the comparator is exactly what the interpreter would have handed it.
            final Sequence value = record.value(orderIndexes[i]);
            if (value == null) {
              keys[i] = null;
              continue;
            }
            if (!(value instanceof final Item valueItem)) {
              return null;
            }
            Atomic atomic = valueItem.atomize();
            if (atomic != null && atomic.type().instanceOf(Type.UNA)) {
              atomic = Cast.cast(null, atomic, Type.STR);
            }
            keys[i] = atomic;
          }
          ordering.add(keys, new TupleImpl(item));
          count++;
        }
      }
      if (count == 0) {
        return served;
      }
      final List<Item> out = new ArrayList<>(count);
      try (final Stream<? extends Tuple> stream = ordering.sorted()) {
        for (Tuple next = stream.next(); next != null; next = stream.next()) {
          out.add((Item) next.get(0));
        }
      }
      return new ItemSequence(out.toArray(new Item[0]));
    } catch (final QueryException cannotOrderHere) {
      return null;
    } catch (final RuntimeException cannotOrderHere) {
      // Ordering#compare rethrows an inappropriate-type error as ClassCastException, and
      // TimSort raises IllegalArgumentException on a comparator that is not a total order.
      return null;
    }
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) throws QueryException {
    // ExprUtil.asItem unwraps singletons and raises XPTY0004 on >1 — the PipeExpr contract.
    return ExprUtil.asItem(evaluate(ctx, tuple));
  }

  @Override
  public boolean isUpdating() {
    return false;
  }

  @Override
  public boolean isVacuous() {
    return false;
  }
}
