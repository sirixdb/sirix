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
 *
 * <p>
 * The key set is filled incrementally rather than drained up front. A probe answers from the keys
 * indexed so far and otherwise resumes the inner scan where the previous probe stopped, ending at
 * its own key or at the end of the inner side. A semi-join therefore keeps the early exit the
 * unoptimized plan has — one matching row can answer {@code exists} without reading the rest — and
 * no direction, the anti-join included, reads the inner relation more than once per binding.
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
    private Iter iter;
    private LongOpenHashSet longs;
    private ObjectOpenHashSet<String> strings;
    private boolean hasNull;
    private volatile boolean rows;
    private volatile boolean complete;
    private volatile boolean fallback;

    private Lookup(final Sequence binding, final Expr source, final QNm field, final Tuple origin) {
      this.binding = binding;
      this.source = source;
      this.field = field;
      this.origin = origin;
    }

    /** Returns 1 for a match, 0 for no match, or -1 when the original predicate must decide. */
    int probe(final QueryContext ctx, final Tuple tuple, final Expr probe) {
      if (fallback) {
        return -1;
      }
      if (!rows) {
        start(ctx);
        if (fallback) {
          return -1;
        }
        // An empty inner relation never evaluates the comparison, so the outer key stays unread.
        if (!rows) {
          return 0;
        }
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
      // Once the inner side is exhausted the sets are final, and the volatile read of complete
      // publishes them, so the common case answers without taking the monitor.
      return complete
          ? found(key)
              ? 1
              : absent(key)
          : search(ctx, key);
    }

    private synchronized void start(final QueryContext ctx) {
      while (!rows && !complete && !fallback) {
        advance(ctx);
      }
    }

    /**
     * Answers from the keys indexed so far, then resumes the inner scan where the last probe left it,
     * stopping at this key or at the end of the inner side. Over all probes of one binding the inner
     * relation is therefore read at most once.
     */
    private synchronized int search(final QueryContext ctx, final Atomic key) {
      while (true) {
        if (found(key)) {
          return 1;
        }
        if (fallback) {
          return -1;
        }
        if (complete) {
          return absent(key);
        }
        advance(ctx);
      }
    }

    private boolean found(final Atomic key) {
      if (key instanceof Null) {
        // Value equality on null is total: null eq null holds, null eq any other atomic is false.
        return hasNull;
      }
      if (longs != null && key instanceof LonNumeric number) {
        return longs.contains(number.longValue());
      }
      return strings != null && stringKey(key) && strings.contains(key.stringValue());
    }

    /** The verdict for a key the exhausted inner side does not contain. */
    private int absent(final Atomic key) {
      if (longs == null && strings == null || key instanceof Null) {
        return 0;
      }
      if (longs != null && key instanceof LonNumeric || strings != null && stringKey(key)) {
        return 0;
      }
      // Numeric promotion is pairwise (and not transitive for float/double). Preserve the value
      // comparison instead of coercing all keys to a lossy common representation for hashing.
      return -1;
    }

    private void advance(final QueryContext ctx) {
      try {
        if (iter == null) {
          final Sequence input = source.evaluate(ctx, origin);
          // The tuple is only an evaluation context for the source; holding it would pin one outer
          // row, and with it a database transaction, for as long as the lookup lives.
          origin = null;
          if (input == null) {
            complete = true;
            return;
          }
          iter = input.iterate();
        }
        final Item row = iter.next();
        if (row == null) {
          release();
          complete = true;
          return;
        }
        rows = true;
        index(row);
      } catch (final QueryException exception) {
        // Speculative key extraction may encounter an error that an earlier match would have
        // hidden. The original expression decides whether that error is reachable.
        release();
        fallback = true;
      }
    }

    private void index(final Item row) {
      final Item value = key(row);
      if (value == null) {
        return;
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
        release();
        fallback = true;
      }
    }

    private void release() {
      origin = null;
      if (iter != null) {
        iter.close();
        iter = null;
      }
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
}
