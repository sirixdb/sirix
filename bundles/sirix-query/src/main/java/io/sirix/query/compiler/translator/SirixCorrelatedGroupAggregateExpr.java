package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.Int32;
import io.brackit.query.atomic.Int64;
import io.brackit.query.atomic.Numeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.atomic.Str;
import io.brackit.query.expr.Cast;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Stream;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jsonitem.object.ArrayObject;
import io.brackit.query.operator.Cursor;
import io.brackit.query.operator.Operator;
import io.brackit.query.operator.TupleImpl;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.util.ExprUtil;
import io.brackit.query.util.sort.Ordering;
import io.sirix.query.scan.SirixExecutorProvider;
import io.sirix.query.scan.SirixVectorizedExecutor;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Serving of the CORRELATED index-routed grouping (see
 * {@code CorrelatedGroupAggregateDetectionStage}): the outer prefix runs as an ordinary operator
 * chain; per outer tuple the outer keys are evaluated by the interpreter and the inner grouping is
 * served from the projection under that tuple's row mask; the served groups are merged on (outer
 * keys, inner keys) with the mergeable aggregates folded ({@code count} and {@code sum} added
 * exactly, {@code min}/{@code max} compared); the real records are assembled and ordered with
 * Brackit's own {@link Ordering} under the pipeline's order-by, which names every key. Any inner
 * serve that declines declines the whole expression to the generic pipeline compiled alongside.
 */
public final class SirixCorrelatedGroupAggregateExpr implements Expr {

  private final SirixExecutorProvider executorProvider;
  private final Operator outer;
  private final Expr[] outerKeyExprs;
  private final String[] outerKeyNames;
  private final SirixGroupAggregateExpr.RoutedSource routed;
  private final RoutedGroupRequest inner;
  private final int innerKeyCount;
  private final int[] entryKinds;
  private final String[] entryNames;
  private final int[] orderIndexes;
  private final Ordering.OrderModifier[] orderModifiers;
  private final Expr genericFallback;
  private static final boolean DIAG = Boolean.getBoolean("sirix.projDiag");

  private static @Nullable Sequence decline(final String why) {
    if (DIAG) {
      System.err.println("[corr-serve] decline: " + why);
    }
    return null;
  }

  public SirixCorrelatedGroupAggregateExpr(final SirixExecutorProvider executorProvider, final Operator outer,
      final Expr[] outerKeyExprs, final String[] outerKeyNames, final SirixGroupAggregateExpr.RoutedSource routed,
      final RoutedGroupRequest inner, final int[] entryKinds, final String[] entryNames, final int[] orderIndexes,
      final boolean[] orderAsc, final boolean[] orderEmptyLeast, final Expr genericFallback) {
    this.executorProvider = executorProvider;
    this.outer = outer;
    this.outerKeyExprs = outerKeyExprs;
    this.outerKeyNames = outerKeyNames;
    this.routed = routed;
    this.inner = inner;
    this.innerKeyCount = inner.groupFields().length;
    this.entryKinds = entryKinds;
    this.entryNames = entryNames;
    this.orderIndexes = orderIndexes;
    this.orderModifiers = new Ordering.OrderModifier[orderIndexes.length];
    for (int i = 0; i < orderIndexes.length; i++) {
      this.orderModifiers[i] = new Ordering.OrderModifier(orderAsc[i], orderEmptyLeast[i], null);
    }
    this.genericFallback = genericFallback;
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) throws QueryException {
    final Sequence served = serve(ctx, tuple);
    return served != null
        ? served
        : genericFallback.evaluate(ctx, tuple);
  }

  private @Nullable Sequence serve(final QueryContext ctx, final Tuple tuple) throws QueryException {
    // Insertion order is outer-major first appearance; the order-by below totally orders the
    // groups, so this order only decides nothing.
    final Map<GroupKey, Sequence[]> groups = new LinkedHashMap<>();
    final String[] funcs = inner.funcs();
    final Cursor cursor = outer.create(ctx, tuple);
    cursor.open(ctx);
    try {
      for (Tuple outerTuple = cursor.next(ctx); outerTuple != null; outerTuple = cursor.next(ctx)) {
        final Atomic[] outerKeys = new Atomic[outerKeyExprs.length];
        for (int k = 0; k < outerKeys.length; k++) {
          final Item item = outerKeyExprs[k].evaluateToItem(ctx, outerTuple);
          if (item == null) {
            outerKeys[k] = null;
            continue;
          }
          final Atomic normalised = normalise(item);
          if (normalised == null) {
            return decline("outer key kind " + item.getClass().getSimpleName());
          }
          outerKeys[k] = normalised;
        }
        final SirixVectorizedExecutor.ServedGroups served = inner.serve(executorProvider, ctx, outerTuple, routed);
        if (served == null) {
          return decline("inner grouping not served for an outer tuple");
        }
        try (final Iter iter = served.groups().iterate()) {
          for (Item item = iter.next(); item != null; item = iter.next()) {
            if (!(item instanceof final Object record) || record.len() != innerKeyCount + funcs.length) {
              return decline("served record shape");
            }
            final Atomic[] key = Arrays.copyOf(outerKeys, outerKeys.length + innerKeyCount);
            for (int k = 0; k < innerKeyCount; k++) {
              final Sequence component = record.value(k);
              if (component == null) {
                key[outerKeys.length + k] = null;
              } else if (component instanceof Int64 || component instanceof Int32 || component instanceof Str) {
                key[outerKeys.length + k] = (Atomic) component;
              } else {
                return decline("inner key kind " + component.getClass().getSimpleName());
              }
            }
            final GroupKey groupKey = new GroupKey(key);
            final Sequence[] existing = groups.get(groupKey);
            if (existing == null) {
              final Sequence[] fresh = new Sequence[funcs.length];
              for (int a = 0; a < funcs.length; a++) {
                fresh[a] = record.value(innerKeyCount + a);
              }
              groups.put(groupKey, fresh);
            } else {
              for (int a = 0; a < funcs.length; a++) {
                existing[a] = merge(funcs[a], existing[a], record.value(innerKeyCount + a));
                if (existing[a] == DECLINE) {
                  return decline("aggregate merge of " + funcs[a]);
                }
              }
            }
          }
        }
      }
    } finally {
      cursor.close(ctx);
    }
    final List<Item> records = new ArrayList<>(groups.size());
    for (final Map.Entry<GroupKey, Sequence[]> group : groups.entrySet()) {
      final QNm[] names = new QNm[entryKinds.length];
      final Sequence[] values = new Sequence[entryKinds.length];
      final Atomic[] key = group.getKey().components;
      for (int i = 0; i < entryKinds.length; i++) {
        names[i] = new QNm(entryNames[i]);
        final int kind = entryKinds[i];
        if (kind >= 0) {
          values[i] = key[kind];
        } else {
          final int syntheticEntry = -(kind + 1);
          values[i] = syntheticEntry < innerKeyCount
              ? key[outerKeys(key) + syntheticEntry]
              : group.getValue()[syntheticEntry - innerKeyCount];
        }
      }
      records.add(new ArrayObject(names, values));
    }
    return sort(records);
  }

  /**
   * The key representation this merge groups and emits: an integer-typed atomic (a plain
   * {@code Int64}, or the integer JSON item the interpreter hands back for a stored number) as
   * {@code Int64}, a string-typed one as {@code Str}, anything else {@code null} = not replicable
   * here. An integer beyond a long declines too: the interpreter groups it exactly.
   */
  private static @Nullable Atomic normalise(final Item item) {
    if (!(item instanceof Atomic atomic)) {
      return null;
    }
    if (atomic instanceof Str) {
      return atomic;
    }
    final Type type = atomic.type();
    if (type == null) {
      return null;
    }
    if (type.instanceOf(Type.STR)) {
      return new Str(atomic.stringValue());
    }
    if (type.instanceOf(Type.INR) && atomic instanceof Numeric numeric) {
      final Int64 asLong = new Int64(numeric.longValue());
      return asLong.cmp(numeric) == 0
          ? asLong
          : null;
    }
    return null;
  }

  private int outerKeys(final Atomic[] key) {
    return key.length - innerKeyCount;
  }

  /** Sentinel for a merge the expression cannot do exactly (an overflowing sum). */
  private static final Sequence DECLINE = new ItemSequence();

  /**
   * Fold one aggregate entry across two outer tuples. {@code count} and {@code sum} add (an empty sum
   * — an all-missing group — is the identity), {@code min}/{@code max} compare, an empty extremum is
   * skipped. Only the four mergeable functions are admitted upstream.
   */
  private static @Nullable Sequence merge(final String func, final @Nullable Sequence left,
      final @Nullable Sequence right) {
    switch (func) {
      case "count", "sum" -> {
        if (left == null) {
          return right;
        }
        if (right == null) {
          return left;
        }
        if (left instanceof Int64 a && right instanceof Int64 b) {
          try {
            return new Int64(Math.addExact(a.longValue(), b.longValue()));
          } catch (final ArithmeticException overflow) {
            return DECLINE;
          }
        }
        return DECLINE;
      }
      case "min", "max" -> {
        if (left == null) {
          return right;
        }
        if (right == null) {
          return left;
        }
        if (!(left instanceof Atomic a) || !(right instanceof Atomic b)) {
          return DECLINE;
        }
        final int cmp = a.cmp(b);
        return "min".equals(func)
            ? (cmp <= 0
                ? left
                : right)
            : (cmp >= 0
                ? left
                : right);
      }
      default -> {
        return DECLINE;
      }
    }
  }

  private Sequence sort(final List<Item> records) throws QueryException {
    final Ordering ordering = new Ordering(new Expr[0], orderModifiers);
    for (final Item item : records) {
      final Object record = (Object) item;
      final Sequence[] keys = new Sequence[orderIndexes.length];
      for (int i = 0; i < orderIndexes.length; i++) {
        final Sequence value = record.value(orderIndexes[i]);
        if (value == null) {
          keys[i] = null;
          continue;
        }
        Atomic atomic = ((Item) value).atomize();
        if (atomic != null && atomic.type().instanceOf(Type.UNA)) {
          atomic = Cast.cast(null, atomic, Type.STR);
        }
        keys[i] = atomic;
      }
      ordering.add(keys, new TupleImpl(item));
    }
    final List<Item> out = new ArrayList<>(records.size());
    if (!records.isEmpty()) {
      try (final Stream<? extends Tuple> stream = ordering.sorted()) {
        for (Tuple next = stream.next(); next != null; next = stream.next()) {
          out.add((Item) next.get(0));
        }
      }
    }
    return new ItemSequence(out.toArray(new Item[0]));
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) throws QueryException {
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

  /**
   * A group's identity: integer and string atomics compared by value, the empty sequence by absence.
   * Integers hash by their long value so an {@code Int32} and an {@code Int64} of the same number
   * meet in one group, exactly as the interpreter's grouping compares them.
   */
  private static final class GroupKey {
    private final Atomic[] components;
    private final int hash;

    GroupKey(final Atomic[] components) {
      this.components = components;
      int h = 1;
      for (final Atomic component : components) {
        h = 31 * h + (component == null
            ? 0
            : component instanceof Int64 i64
                ? Long.hashCode(i64.longValue())
                : component instanceof Int32 i32
                    ? Long.hashCode(i32.longValue())
                    : component.stringValue().hashCode());
      }
      this.hash = h;
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(final java.lang.Object other) {
      if (!(other instanceof GroupKey that) || that.components.length != components.length) {
        return false;
      }
      for (int i = 0; i < components.length; i++) {
        final Atomic a = components[i];
        final Atomic b = that.components[i];
        if (a == null || b == null) {
          if (a != b) {
            return false;
          }
          continue;
        }
        final boolean aString = a instanceof Str;
        final boolean bString = b instanceof Str;
        if (aString != bString) {
          return false;
        }
        if (aString
            ? !a.stringValue().equals(b.stringValue())
            : ((Int64.class.isInstance(a)
                ? ((Int64) a).longValue()
                : ((Int32) a).longValue()) != (Int64.class.isInstance(b)
                    ? ((Int64) b).longValue()
                    : ((Int32) b).longValue()))) {
          return false;
        }
      }
      return true;
    }
  }
}
