package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.compiler.optimizer.PredicateNode;
import io.brackit.query.compiler.optimizer.SourceRef;
import io.brackit.query.expr.Cast;
import io.brackit.query.jsonitem.object.ArrayObject;
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
import io.sirix.query.scan.SirixExecutorProvider;
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

  private final SirixExecutorProvider executorProvider;
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
   * {@code -1}: with a cap the executor may heap-select the first {@code limit} groups of the stable
   * order instead of sorting and materializing every group.
   */
  private final long limit;
  /** Per-key transform annotations (see the detection stage), or {@code null} for plain keys. */
  private final long[] keyOffsets;
  private final int[] keySubstr;
  /** Conditional key transform (Q39's CASE WHEN shape), or {@code null}s for none. */
  private final String[] keyCondFields;
  private final long[] keyCondLits;
  private final String[] keyCondElse;
  /** Regex key transform (Q28's REGEXP_REPLACE shape), or {@code null}s for none. */
  private final String[] keyRegexPattern;
  private final String[] keyRegexRepl;
  /** Integer date-part key transform ({@code {divisor, modulus}} per key), or {@code null}. */
  private final long[] keyDivMod;
  /** Per key: {@code fn:string(<chain>)} — a missing field groups and prints as {@code ""}. */
  private final boolean[] keyStringify;
  /** HAVING over the group count ({@code long[]{op, literal}}), or {@code null}. */
  private final long[] having;
  /** Concat-emitted key entries: literal prefix/suffix decoration per annotated position. */
  private final int[] decorPos;
  private final String[] decorPrefix;
  private final String[] decorSuffix;
  /** CONSTANT key entries to splice back into each served record, or {@code null}. */
  private final int[] constEntryPos;
  private final String[] constEntryNames;
  private final long[] constEntryValues;
  /**
   * Carries the admitted source into the revision-stable evaluation lease and the runtime gate: a
   * {@link SourceRef.Kind#VARIABLE} ref cannot be judged at compile time, so this expr re-checks the
   * binding at evaluation time and declines to its generic fallback when it is foreign.
   */
  private final SourceRef sourceRef;
  private final Expr genericFallback;

  public SirixGroupAggregateExpr(final SirixExecutorProvider executorProvider, final String[] sourcePath,
      final PredicateNode predicateOrNull, final String[] groupFields, final String[] keyNames, final String[] funcs,
      final String[] aggFields, final String[] outNames, final int[] orderIndexes, final boolean[] orderAsc,
      final boolean[] orderEmptyLeast, final long limit, final long[] keyOffsets, final int[] keySubstr,
      final String[] keyCondFields, final long[] keyCondLits, final String[] keyCondElse,
      final String[] keyRegexPattern, final String[] keyRegexRepl, final long[] keyDivMod, final boolean[] keyStringify,
      final long[] having, final int[] decorPos, final String[] decorPrefix, final String[] decorSuffix,
      final int[] constEntryPos, final String[] constEntryNames, final long[] constEntryValues,
      final SourceRef sourceRef, final Expr genericFallback) {
    this.executorProvider = executorProvider;
    this.sourcePath = sourcePath;
    this.predicateOrNull = predicateOrNull;
    this.groupFields = groupFields;
    this.keyNames = keyNames;
    this.funcs = funcs;
    this.aggFields = aggFields;
    this.outNames = outNames;
    this.sourceRef = sourceRef;
    this.genericFallback = genericFallback;
    this.keyOffsets = keyOffsets;
    this.keySubstr = keySubstr;
    this.keyCondFields = keyCondFields;
    this.keyCondLits = keyCondLits;
    this.keyCondElse = keyCondElse;
    this.keyRegexPattern = keyRegexPattern;
    this.keyRegexRepl = keyRegexRepl;
    this.keyDivMod = keyDivMod;
    this.keyStringify = keyStringify;
    this.having = having;
    this.decorPos = decorPos;
    this.decorPrefix = decorPrefix;
    this.decorSuffix = decorSuffix;
    this.constEntryPos = constEntryPos;
    this.constEntryNames = constEntryNames;
    this.constEntryValues = constEntryValues;
    if (orderIndexes == null || orderIndexes.length == 0) {
      this.orderIndexes = null;
      this.orderAsc = null;
      this.orderEmptyLeast = null;
      this.orderModifiers = null;
      // The cap survives WITHOUT an order-by: LIMIT alone means the first K groups in the
      // pipeline's emission order, which the kernel selects by first-seen ordinal.
      this.limit = limit;
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
    final SirixExecutorProvider.Lease lease = executorProvider.acquire(ctx, sourceRef);
    if (lease != null) {
      try (lease) {
        final SirixVectorizedExecutor executor = lease.executor();
        if ((sourceRef == null || executor.acceptsSource(sourceRef, ctx)) && executor.canExecute(ctx)) {
          final SirixVectorizedExecutor.ServedGroups served = executor.executeGroupByAggregate(ctx, sourcePath,
              predicateOrNull, groupFields, keyNames, funcs, aggFields, outNames, orderIndexes, orderAsc,
              orderEmptyLeast, limit, keyOffsets, keySubstr, keyCondFields, keyCondLits, keyCondElse, keyRegexPattern,
              keyRegexRepl, keyDivMod, keyStringify, having);
          if (served != null) {
            if (orderIndexes == null || served.ordered()) {
              return postProcess(served.groups());
            }
            final Sequence sorted = sort(served.groups());
            if (sorted != null) {
              return postProcess(sorted);
            }
          }
        }
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

  /** Key decoration (concat literals) then constant-entry splicing — K records, emission cost. */
  private Sequence postProcess(final Sequence served) throws QueryException {
    return spliceConstEntries(decorateKeys(served));
  }

  /**
   * Rewrite each concat-emitted key entry's value to {@code prefix + key + suffix} — the record
   * carried the RAW served key (which is also what any order-by compared); the concat is pure
   * emission decoration.
   */
  private Sequence decorateKeys(final Sequence served) throws QueryException {
    if (decorPos == null) {
      return served;
    }
    final List<Item> out = new ArrayList<>();
    try (final Iter iter = served.iterate()) {
      for (Item item = iter.next(); item != null; item = iter.next()) {
        if (!(item instanceof final Object record)) {
          return served; // not the annotated shape — fail-soft
        }
        final int total = record.len();
        final QNm[] names = new QNm[total];
        final Sequence[] vals = new Sequence[total];
        for (int pos = 0; pos < total; pos++) {
          names[pos] = record.name(pos);
          vals[pos] = record.value(pos);
        }
        for (int d = 0; d < decorPos.length; d++) {
          final int pos = decorPos[d];
          final Sequence v = vals[pos];
          if (v == null) {
            // concat over the missing key's empty sequence: fn:concat((), lit) = lit.
            vals[pos] = new Str(decorPrefix[d] + decorSuffix[d]);
          } else if (v instanceof final Atomic atomic) {
            vals[pos] = new Str(decorPrefix[d] + atomic.stringValue() + decorSuffix[d]);
          } else {
            // Key entries are Str/Int64/null by construction — anything else is a defect, and
            // emitting the UNDECORATED record here would be a silent wrong answer.
            throw new IllegalStateException("non-atomic key entry under a concat decoration");
          }
        }
        out.add(new ArrayObject(names, vals));
      }
    }
    return new ItemSequence(out.toArray(new Item[0]));
  }

  /**
   * Weave the CONSTANT key entries (`group by $one, $k` with `$one := 1`) back into each served
   * record at their annotated positions. They partition nothing, so the kernels never see them; K
   * records at most, so this is emission cost, not scan cost.
   */
  private Sequence spliceConstEntries(final Sequence served) throws QueryException {
    if (constEntryPos == null) {
      return served;
    }
    final List<Item> out = new ArrayList<>();
    try (final Iter iter = served.iterate()) {
      for (Item item = iter.next(); item != null; item = iter.next()) {
        if (!(item instanceof final Object record)) {
          return served; // not the annotated shape — leave untouched (fail-soft)
        }
        final int total = record.len() + constEntryPos.length;
        final QNm[] names = new QNm[total];
        final Sequence[] vals = new Sequence[total];
        int src = 0;
        int constIdx = 0;
        for (int pos = 0; pos < total; pos++) {
          if (constIdx < constEntryPos.length && constEntryPos[constIdx] == pos) {
            names[pos] = new QNm(constEntryNames[constIdx]);
            vals[pos] = new Int64(constEntryValues[constIdx]);
            constIdx++;
          } else {
            names[pos] = record.name(src);
            vals[pos] = record.value(src);
            src++;
          }
        }
        out.add(new ArrayObject(names, vals));
      }
    }
    return new ItemSequence(out.toArray(new Item[0]));
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
