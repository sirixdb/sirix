package io.sirix.query.compiler.expression;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.LonNumeric;
import io.brackit.query.atomic.Null;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.Reference;
import io.brackit.query.expr.DeclVariable;
import io.brackit.query.jdm.AbstractItem;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.jdm.type.AnyItemType;
import io.brackit.query.jdm.type.ItemType;
import io.brackit.query.util.ExprUtil;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.lang.ref.SoftReference;
import java.util.Objects;

/**
 * Creates a fresh, lazy membership lookup for each binding of the enclosing scope.
 *
 * <p>
 * This expression is a direct child of its {@link MembershipProbeExpr} rather than a hoisted
 * {@code let} binding, so the lookup never occupies a pipeline tuple slot. A slot would have to
 * survive tuple serialization, which a spilling {@code group by} or {@code order by} performs on
 * every slot it carries, and the lookup is not a serializable JDM item.
 *
 * <p>
 * One build per enclosing binding is kept instead by memoizing on the inner relation's binding,
 * read at its source rather than through a reference to it. A local binding is read straight out of
 * its tuple slot, because {@link io.brackit.query.expr.BoundVariable} re-wraps any non-item slot
 * value in a fresh {@code TypedSequence} that no identity comparison could ever match; a
 * module-level variable is read from the query context for the same reason. Both are the same
 * object for every row of the outer {@code for} and a different object once an enclosing binding
 * moves on.
 *
 * <p>
 * The memo holds one entry and holds it softly, so a finished evaluation's key set does not stay
 * pinned by the compiled expression and concurrent block-pipeline workers sitting on different
 * enclosing bindings rebuild rather than share. Correctness never depends on a hit: every caller
 * receives a lookup built from its own binding.
 */
public final class MembershipIndexExpr implements Expr, Reference {
  private final Expr source;
  private final Expr declaredScope;
  private final QNm declared;
  private final QNm field;
  private int slot = -1;
  private volatile SoftReference<Lookup> cached;

  public MembershipIndexExpr(final Expr source, final Expr scope, final QNm field) {
    this.source = Objects.requireNonNull(source);
    this.field = field;
    // The translator already resolved whether the scope variable is module-level or a local
    // binding, so ask it rather than re-deciding from the name, which a local may shadow.
    if (Objects.requireNonNull(scope) instanceof DeclVariable variable) {
      this.declaredScope = scope;
      this.declared = variable.getName();
    } else {
      this.declaredScope = null;
      this.declared = null;
    }
  }

  /** Receives the scope variable's tuple slot; the translator registers this expression for it. */
  @Override
  public void setPos(final int pos) {
    this.slot = pos;
  }

  @Override
  public Item evaluate(final QueryContext ctx, final Tuple tuple) {
    // Reading the scope variable is a slot or context access; the source and its keys are not
    // evaluated until an outer row actually probes.
    final Sequence binding = binding(ctx, tuple);
    final Lookup snapshot = memoized();
    return snapshot != null && snapshot.binding == binding
        ? snapshot
        : refresh(tuple, binding);
  }

  private Lookup memoized() {
    final SoftReference<Lookup> reference = cached;
    return reference == null
        ? null
        : reference.get();
  }

  private Sequence binding(final QueryContext ctx, final Tuple tuple) {
    if (declared == null) {
      return tuple.get(slot);
    }
    if (!ctx.isBound(declared)) {
      declaredScope.evaluate(ctx, tuple);
    }
    return ctx.resolve(declared);
  }

  private synchronized Lookup refresh(final Tuple tuple, final Sequence binding) {
    final Lookup snapshot = memoized();
    if (snapshot != null && snapshot.binding == binding) {
      return snapshot;
    }
    final Lookup fresh = new Lookup(binding, source, field, tuple);
    cached = new SoftReference<>(fresh);
    return fresh;
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) {
    return evaluate(ctx, tuple);
  }

  @Override
  public boolean isUpdating() {
    return false;
  }

  @Override
  public boolean isVacuous() {
    return false;
  }

  /** Opaque evaluation-local state; consumed by the enclosing probe and never put in a tuple. */
  static final class Lookup extends AbstractItem {
    private final Sequence binding;
    private final Expr source;
    private final QNm field;
    private Tuple origin;
    private volatile Keys keys;

    private Lookup(final Sequence binding, final Expr source, final QNm field, final Tuple origin) {
      this.binding = binding;
      this.source = source;
      this.field = field;
      this.origin = origin;
    }

    /** Returns 1 for a match, 0 for no match, or -1 when the original predicate must decide. */
    int probe(final QueryContext ctx, final Tuple tuple, final Expr probe) {
      Keys snapshot = keys;
      if (snapshot == null) {
        snapshot = initialize(ctx);
      }
      if (snapshot.fallback) {
        return -1;
      }
      if (!snapshot.hasRows) {
        return 0;
      }
      final Item item = probe.evaluateToItem(ctx, tuple);
      if (item == null) {
        return 0;
      }
      final Atomic key;
      try {
        key = item.atomize();
      } catch (final QueryException exception) {
        return -1;
      }
      if (key instanceof Null) {
        // Value equality on null is total: null eq null holds, null eq any other atomic is false.
        return snapshot.hasNull
            ? 1
            : 0;
      }
      if (snapshot.longs != null && key instanceof LonNumeric number) {
        return snapshot.longs.contains(number.longValue())
            ? 1
            : 0;
      }
      if (snapshot.strings != null && stringKey(key)) {
        return snapshot.strings.contains(key.stringValue())
            ? 1
            : 0;
      }
      // Numeric promotion is pairwise (and not transitive for float/double). Preserve the value
      // comparison instead of coercing all keys to a lossy common representation for hashing.
      return snapshot.longs == null && snapshot.strings == null
          ? 0
          : -1;
    }

    private synchronized Keys initialize(final QueryContext ctx) {
      if (keys == null) {
        // Publish immutable-after-build sets. The same let binding can be shared by parallel
        // pipeline workers; only first use synchronizes, ordinary probes perform one volatile read.
        try {
          keys = build(ctx);
        } catch (final QueryException exception) {
          // Speculative key extraction may encounter an error after an earlier matching row.
          // The original expression decides whether that error is reachable.
          keys = Keys.FALLBACK;
        }
        // The tuple is only an evaluation context for the source; holding it would pin one outer
        // row, and with it a database transaction, for as long as the lookup lives.
        origin = null;
      }
      return keys;
    }

    private Keys build(final QueryContext ctx) {
      final Sequence input = source.evaluate(ctx, origin);
      if (input == null) {
        return Keys.EMPTY;
      }
      LongOpenHashSet longs = null;
      ObjectOpenHashSet<String> strings = null;
      boolean hasRows = false;
      boolean hasNull = false;
      try (final Iter iter = input.iterate()) {
        Item row;
        while ((row = iter.next()) != null) {
          hasRows = true;
          final Item value = key(row);
          if (value == null) {
            continue;
          }
          final Atomic atomic = value.atomize();
          if (atomic instanceof Null) {
            // A null key never compares equal to a typed one, so it stays out of both sets and
            // cannot force a mixed domain.
            hasNull = true;
          } else if (atomic instanceof LonNumeric number && strings == null) {
            if (longs == null) {
              longs = new LongOpenHashSet();
            }
            longs.add(number.longValue());
          } else if (stringKey(atomic) && longs == null) {
            if (strings == null) {
              strings = new ObjectOpenHashSet<>();
            }
            strings.add(atomic.stringValue());
          } else {
            return Keys.FALLBACK;
          }
        }
      }
      return new Keys(longs, strings, hasRows, hasNull, false);
    }

    private Item key(final Item row) {
      if (field == null) {
        return row;
      }
      // Same direct-record dereference semantics as Brackit's DerefExpr, including absent fields
      // and non-record items. asItem enforces value comparison's zero-or-one cardinality.
      return row instanceof Object object
          ? ExprUtil.asItem(object.get(field))
          : null;
    }

    private static boolean stringKey(final Atomic key) {
      final Type type = key.type();
      // Value equality promotes untypedAtomic and anyURI to strings. Collation is codepoint,
      // exactly as in Brackit's value comparison, independent of the default order-by collation.
      return type.instanceOf(Type.STR) || type.instanceOf(Type.UNA) || type.instanceOf(Type.AURI);
    }

    @Override
    public ItemType itemType() {
      return AnyItemType.ANY;
    }

    @Override
    public Atomic atomize() {
      throw new QueryException(ErrorCode.BIT_DYN_RT_ILLEGAL_STATE_ERROR, "Internal membership lookup cannot escape");
    }

    @Override
    public boolean booleanValue() {
      throw new QueryException(ErrorCode.BIT_DYN_RT_ILLEGAL_STATE_ERROR, "Internal membership lookup cannot escape");
    }
  }

  private record Keys(LongOpenHashSet longs, ObjectOpenHashSet<String> strings, boolean hasRows, boolean hasNull,
      boolean fallback) {
    private static final Keys FALLBACK = new Keys(null, null, false, false, true);
    private static final Keys EMPTY = new Keys(null, null, false, false, false);
  }
}
