package io.sirix.query.compiler.operator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.LonNumeric;
import io.brackit.query.atomic.Null;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.translator.Reference;
import io.brackit.query.expr.DeclVariable;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.Type;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.operator.Check;
import io.brackit.query.operator.Cursor;
import io.brackit.query.operator.Operator;
import io.brackit.query.util.ExprUtil;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;

import java.util.Objects;

/**
 * A physical semi/anti join retaining the outer tuples, their order and node identities.
 *
 * <p>
 * Each cursor owns its hash table and inner iterator. Opening another cursor, including an
 * iteration of an already retained lazy declared variable, always starts afresh. No execution state
 * lives on the compiled operator, in the query context, or in spillable tuples. A change of the
 * independent source binding releases the previous table before starting the next scope.
 * Identity-stable bindings therefore share one scan across nested outer loops in this pipeline.
 *
 * <p>
 * The build is demand-driven: no inner rows are read for an empty outer input, and both join
 * directions stop at a match. Later probes resume that same scan, at most once over the relation.
 * The cursor closes the scan on exhaustion, scope change, failure, or early close. Unsupported
 * comparison domains and speculative extraction failures use the original predicate, preserving
 * pairwise numeric promotions and errors without imposing a lossy hash representation.
 */
public final class HashMembershipJoin extends Check implements Operator {
  private final Operator in;
  private final Expr source;
  private final Binding binding;
  private final Expr key;
  private final Expr fallback;
  private final QNm field;
  private final boolean anti;

  public HashMembershipJoin(final Operator in, final Expr source, final Binding binding, final Expr key,
      final Expr fallback, final QNm field, final boolean anti) {
    this.in = Objects.requireNonNull(in);
    this.source = Objects.requireNonNull(source);
    this.binding = Objects.requireNonNull(binding);
    this.key = Objects.requireNonNull(key);
    this.fallback = Objects.requireNonNull(fallback);
    this.field = field;
    this.anti = anti;
  }

  /** A compiled slot reference, with no cached value or per-execution state. */
  public static final class Binding implements Reference {
    private final DeclVariable declared;
    private int slot = -1;

    public Binding(final Expr resolved) {
      declared = Objects.requireNonNull(resolved) instanceof DeclVariable variable
          ? variable
          : null;
    }

    @Override
    public void setPos(final int pos) {
      slot = pos;
    }

    private Sequence read(final QueryContext ctx, final Tuple tuple) {
      if (declared == null) {
        // BoundVariable.evaluate wraps non-item values on each reference. The tuple slot is
        // the actual binding and remains stable across all outer rows in its scope.
        return tuple.get(slot);
      }
      if (!ctx.isBound(declared.getName())) {
        declared.evaluate(ctx, tuple);
      }
      return ctx.resolve(declared.getName());
    }
  }

  @Override
  public Cursor create(final QueryContext ctx, final Tuple tuple) {
    return new JoinCursor(in.create(ctx, tuple));
  }

  @Override
  public Cursor create(final QueryContext ctx, final Tuple[] buffer, final int len) {
    return new JoinCursor(in.create(ctx, buffer, len));
  }

  @Override
  public int tupleWidth(final int initSize) {
    return in.tupleWidth(initSize);
  }

  private final class JoinCursor implements Cursor {
    private final Cursor input;
    private Tuple previous;
    private Tuple pending;
    private Sequence scope;
    private Lookup lookup;
    private boolean closed;

    private JoinCursor(final Cursor input) {
      this.input = input;
    }

    @Override
    public void open(final QueryContext ctx) {
      input.open(ctx);
    }

    @Override
    public Tuple next(final QueryContext ctx) {
      if (closed) {
        return null;
      }
      try {
        Tuple tuple;
        while ((tuple = pending) != null || (tuple = input.next(ctx)) != null) {
          pending = null;
          if (check && dead(tuple) || accepts(ctx, tuple)) {
            previous = tuple;
            return tuple;
          }
          if (check && (previous == null || separate(previous, tuple))) {
            // Preserve Brackit's lifted left-join iteration groups: the last rejected tuple
            // of an otherwise empty group passes through with its local check cleared.
            pending = input.next(ctx);
            if (pending == null || separate(tuple, pending)) {
              previous = tuple.replace(local(), null);
              return previous;
            }
          }
        }
        close(ctx);
        return null;
      } catch (final RuntimeException | Error failure) {
        try {
          close(ctx);
        } catch (final RuntimeException | Error cleanup) {
          failure.addSuppressed(cleanup);
        }
        throw failure;
      }
    }

    private boolean accepts(final QueryContext ctx, final Tuple tuple) {
      final Sequence current = binding.read(ctx, tuple);
      if (lookup == null || scope != current) {
        if (lookup != null) {
          lookup.close();
        }
        scope = current;
        lookup = new Lookup();
      }
      final int result = lookup.probe(ctx, tuple);
      if (result >= 0) {
        return (result != 0) != anti;
      }
      final Sequence original = fallback.evaluate(ctx, tuple);
      return original != null && original.booleanValue();
    }

    @Override
    public void close(final QueryContext ctx) {
      if (closed) {
        return;
      }
      closed = true;
      final Lookup old = lookup;
      lookup = null;
      scope = null;
      previous = null;
      pending = null;
      try {
        if (old != null) {
          old.close();
        }
      } finally {
        input.close(ctx);
      }
    }
  }

  /** Only reachable from its owning cursor; never shared across evaluations or workers. */
  private final class Lookup {
    private LongOpenHashSet longs;
    private ObjectOpenHashSet<String> strings;
    private boolean hasNull;
    private boolean started;
    private boolean complete;
    private boolean delegate;
    private Iter iter;

    /** Returns match=1, absent=0, or fallback=-1. */
    private int probe(final QueryContext ctx, final Tuple tuple) {
      final Atomic probe;
      try {
        final Item item = key.evaluateToItem(ctx, tuple);
        probe = item == null
            ? null
            : item.atomize();
      } catch (final RuntimeException failure) {
        // An empty inner side suppresses even an invalid outer key. Let the original predicate
        // decide whether evaluation reaches it, instead of eagerly exposing its exception.
        return -1;
      }
      if (probe != null && !(probe instanceof Null) && !(probe instanceof LonNumeric) && !stringKey(probe)) {
        return -1;
      }
      if (found(probe)) {
        return 1;
      }
      if (delegate) {
        return -1;
      }
      if (complete) {
        return absent(probe);
      }
      try {
        if (!started) {
          started = true;
          final Sequence rows = source.evaluate(ctx, tuple);
          if (rows == null) {
            complete = true;
            return 0;
          }
          iter = rows.iterate();
        }
        while (true) {
          final Item row = iter.next();
          if (row == null) {
            complete = true;
            closeIterator();
            return absent(probe);
          }
          index(row);
          if (delegate) {
            closeIterator();
            return -1;
          }
          if (found(probe)) {
            return 1;
          }
          if (absent(probe) < 0) {
            // This particular probe requires pairwise promotion. Retain the cursor/table for
            // later supported probes, but do not drain the source before running its fallback.
            return -1;
          }
        }
      } catch (final QueryException failure) {
        if (iter == null && (complete || delegate)) {
          // A close failure is a resource error, not an unsupported comparison.
          throw failure;
        }
        delegate = true;
        closeIterator();
        return -1;
      }
    }

    private boolean found(final Atomic probe) {
      if (probe == null) {
        return false;
      }
      if (probe instanceof Null) {
        return hasNull;
      }
      if (probe instanceof LonNumeric number) {
        return longs != null && longs.contains(number.longValue());
      }
      return strings != null && stringKey(probe) && strings.contains(probe.stringValue());
    }

    private int absent(final Atomic probe) {
      if (probe == null || probe instanceof Null || longs == null && strings == null) {
        return 0;
      }
      return longs != null && probe instanceof LonNumeric || strings != null && stringKey(probe)
          ? 0
          : -1;
    }

    private void index(final Item row) {
      final Item value = field == null
          ? row
          : row instanceof Object object
              ? ExprUtil.asItem(object.get(field))
              : null;
      if (value == null) {
        return;
      }
      final Atomic atomic = value.atomize();
      if (atomic instanceof Null) {
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
        delegate = true;
      }
    }

    private void closeIterator() {
      final Iter old = iter;
      iter = null;
      if (old != null) {
        old.close();
      }
    }

    private void close() {
      longs = null;
      strings = null;
      closeIterator();
    }
  }

  private static boolean stringKey(final Atomic key) {
    final Type type = key.type();
    return type.instanceOf(Type.STR) || type.instanceOf(Type.UNA) || type.instanceOf(Type.AURI);
  }
}
