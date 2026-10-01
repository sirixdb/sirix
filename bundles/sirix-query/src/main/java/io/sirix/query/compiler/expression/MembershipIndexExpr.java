package io.sirix.query.compiler.expression;

import io.brackit.query.ErrorCode;
import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.LonNumeric;
import io.brackit.query.atomic.QNm;
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

import java.util.Objects;

/** Creates a fresh, lazy membership lookup for each binding of the enclosing tuple. */
public final class MembershipIndexExpr implements Expr {
  private final Expr source;
  private final QNm field;

  public MembershipIndexExpr(final Expr source, final QNm field) {
    this.source = Objects.requireNonNull(source);
    this.field = field;
  }

  @Override
  public Item evaluate(final QueryContext ctx, final Tuple tuple) {
    // Neither the source nor its keys are evaluated until an outer row actually probes.
    return new Lookup(source, field, tuple);
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

  /** Opaque tuple-local state; never placed in a plan cache or retained by a compiled expression. */
  static final class Lookup extends AbstractItem {
    private final Expr source;
    private final QNm field;
    private final Tuple scope;
    private volatile Keys keys;

    private Lookup(final Expr source, final QNm field, final Tuple scope) {
      this.source = source;
      this.field = field;
      this.scope = scope;
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
      if (item == null || snapshot.longs == null && snapshot.strings == null) {
        return 0;
      }
      final Atomic key;
      try {
        key = item.atomize();
      } catch (final QueryException exception) {
        return -1;
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
      return -1;
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
      }
      return keys;
    }

    private Keys build(final QueryContext ctx) {
      final Sequence input = source.evaluate(ctx, scope);
      if (input == null) {
        return Keys.EMPTY;
      }
      LongOpenHashSet longs = null;
      ObjectOpenHashSet<String> strings = null;
      boolean hasRows = false;
      try (final Iter iter = input.iterate()) {
        Item row;
        while ((row = iter.next()) != null) {
          hasRows = true;
          final Item value = key(row);
          if (value == null) {
            continue;
          }
          final Atomic atomic = value.atomize();
          if (atomic instanceof LonNumeric number && strings == null) {
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
      return new Keys(longs, strings, hasRows, false);
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

  private record Keys(LongOpenHashSet longs, ObjectOpenHashSet<String> strings, boolean hasRows, boolean fallback) {
    private static final Keys FALLBACK = new Keys(null, null, false, true);
    private static final Keys EMPTY = new Keys(null, null, false, false);
  }
}
