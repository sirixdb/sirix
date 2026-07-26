package io.sirix.query.json;

import io.brackit.query.atomic.IntNumeric;
import io.brackit.query.atomic.LonNumeric;
import io.sirix.api.json.JsonNodeReadOnlyTrx;

/**
 * A {@link NumericJsonDBItem} wrapping a document-sourced INTEGER (always a brackit {@code Int32} or
 * {@code Int64}, both {@link LonNumeric}).
 *
 * <p>The tag is {@link LonNumeric} (not just {@code IntNumeric}) ON PURPOSE: brackit's
 * {@code Int32}/{@code Int64} arithmetic ({@code mod}/{@code idiv}/{@code div}) dispatch as
 * {@code instanceof IntNumeric} -&gt; {@code instanceof LonNumeric} -&gt; long/int op, and their
 * {@code IntNumeric}-but-NOT-{@code LonNumeric} fall-through branch is a defect that returns
 * {@code other.add(this)} (e.g. {@code 7 mod 2} -&gt; {@code 9}). Since the wrapped delegate is
 * itself always a {@link LonNumeric}, advertising {@link LonNumeric} routes the operation through the
 * correct {@code longValue()} path. {@link LonNumeric} also satisfies {@code RangeExpr}'s
 * {@code IntNumeric} bound requirement and the {@code DecNumeric} comparison branch (mixed
 * integer/decimal {@code min}/{@code max}).
 */
public final class IntNumericJsonDBItem extends NumericJsonDBItem implements LonNumeric {

  /**
   * Constructor.
   *
   * @param rtx        {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param atomic     the integer-valued brackit atomic delegate (an {@code Int32}/{@code Int64},
   *                   i.e. a {@link LonNumeric})
   */
  public IntNumericJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection,
      final LonNumeric atomic) {
    super(rtx, collection, atomic);
  }

  @Override
  public IntNumeric inc() {
    return ((IntNumeric) atomic).inc();
  }
}
