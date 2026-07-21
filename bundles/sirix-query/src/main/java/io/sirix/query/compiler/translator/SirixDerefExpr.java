package io.sirix.query.compiler.translator;

import io.brackit.query.QueryContext;
import io.brackit.query.QueryException;
import io.brackit.query.Tuple;
import io.brackit.query.atomic.Atomic;
import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.QNm;
import io.brackit.query.compiler.Bits;
import io.brackit.query.jdm.Expr;
import io.brackit.query.jdm.Item;
import io.brackit.query.jdm.Iter;
import io.brackit.query.jdm.Sequence;
import io.brackit.query.jdm.json.Object;
import io.brackit.query.sequence.BaseIter;
import io.brackit.query.sequence.ItemSequence;
import io.brackit.query.sequence.LazySequence;
import io.brackit.query.util.ExprUtil;

/**
 * Drop-in replacement for Brackit's {@link io.brackit.query.expr.DerefExpr} fixing its
 * sequence dispatch.
 *
 * <p>The original dispatches on CONCRETE sequence classes: it maps the deref over
 * {@link ItemSequence} and {@link LazySequence} bases, derefs a single
 * {@link Object record}, and silently returns the EMPTY sequence for every other
 * {@link Sequence} implementation — notably {@code FlatteningSequence}, which is what a
 * parenthesized expression ({@code SequenceExpr}) evaluates to. As a consequence
 * {@code (for $r in $doc[] return {"a": $r.x}).a} returned empty WITHOUT ever evaluating
 * the pipeline. This port keeps every original branch bit-identical and only adds the
 * missing tail: any other non-item {@link Sequence} is mapped lazily via
 * {@link Sequence#iterate()} (non-record items still deref to empty, exactly like the
 * original's per-item mapping does).
 *
 * <p>One further contract repair over the original: each {@code iterate()} call on the
 * returned lazy sequence opens a FRESH iterator over the base sequence — the original
 * captured a single shared iterator, so a second iteration silently resumed an exhausted
 * one.
 *
 * <p>The same fix has been applied upstream in the Brackit repository; this port can be
 * removed once sirix depends on a Brackit release that contains it.
 */
final class SirixDerefExpr implements Expr {

  private final Expr object;
  private final Expr field;

  SirixDerefExpr(final Expr object, final Expr field) {
    this.object = object;
    this.field = field;
  }

  @Override
  public Sequence evaluate(final QueryContext ctx, final Tuple tuple) {
    final Sequence sequence = object.evaluate(ctx, tuple);
    if (sequence == null) {
      return null;
    }
    if (sequence instanceof ItemSequence itemSequence) {
      return getLazySequence(ctx, tuple, itemSequence);
    }
    if (sequence instanceof LazySequence lazySequence) {
      return getLazySequence(ctx, tuple, lazySequence);
    }
    if (sequence instanceof Object obj) {
      final Item itemField = field.evaluateToItem(ctx, tuple);
      if (itemField == null) {
        return null;
      }
      return getSequenceByRecordField(obj, itemField);
    }
    if (sequence instanceof Item) {
      // Non-record single item (atomic, array): deref is empty — original behavior.
      return null;
    }
    // THE FIX: any other multi-item sequence implementation (FlatteningSequence & friends)
    // is mapped exactly like the ItemSequence/LazySequence branches instead of being
    // swallowed into the empty sequence.
    return getLazySequence(ctx, tuple, sequence);
  }

  private LazySequence getLazySequence(final QueryContext ctx, final Tuple tuple,
      final Sequence base) {
    return new LazySequence() {
      @Override
      public Iter iterate() {
        final Iter iter = base.iterate();
        return new BaseIter() {
          @Override
          public Item next() {
            Item item;
            while ((item = iter.next()) != null) {
              if (!(item instanceof Object obj)) {
                continue;
              }
              final Item itemField = field.evaluateToItem(ctx, tuple);
              if (itemField == null) {
                continue;
              }
              final Sequence fieldValue = getSequenceByRecordField(obj, itemField);
              if (fieldValue == null) {
                continue;
              }
              return fieldValue.evaluateToItem(ctx, tuple);
            }
            return null;
          }

          @Override
          public void close() {
            iter.close();
          }
        };
      }
    };
  }

  private static Sequence getSequenceByRecordField(final Object object, final Item itemField) {
    if (itemField instanceof QNm qNmField) {
      return object.get(qNmField);
    }
    if (itemField instanceof IntNumeric intNumericField) {
      return object.value(intNumericField);
    }
    if (itemField instanceof Atomic atomicField) {
      return object.get(new QNm(atomicField.stringValue()));
    }
    throw new QueryException(Bits.BIT_ILLEGAL_OBJECT_FIELD, "Illegal object itemField reference: %s",
        itemField);
  }

  @Override
  public Item evaluateToItem(final QueryContext ctx, final Tuple tuple) throws QueryException {
    return ExprUtil.asItem(evaluate(ctx, tuple));
  }

  @Override
  public boolean isUpdating() {
    if (object.isUpdating()) {
      return true;
    }
    return field.isUpdating();
  }

  @Override
  public boolean isVacuous() {
    return false;
  }

  @Override
  public String toString() {
    return "." + field;
  }
}
