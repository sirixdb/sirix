package io.sirix.query.json;

import io.brackit.query.atomic.DecNumeric;
import io.sirix.api.json.JsonNodeReadOnlyTrx;

/**
 * A {@link NumericJsonDBItem} wrapping a document-sourced DECIMAL (brackit {@code Dec}). Tagging the
 * brackit {@link DecNumeric} interface lets {@code instanceof DecNumeric} dispatch (e.g. in
 * {@code Int32.mod} and the numeric comparison logic) route this value exactly like a brackit
 * decimal literal.
 */
public final class DecNumericJsonDBItem extends NumericJsonDBItem implements DecNumeric {

  /**
   * Constructor.
   *
   * @param rtx        {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param atomic     the decimal-valued brackit atomic delegate (must be a {@link DecNumeric})
   */
  public DecNumericJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection,
      final DecNumeric atomic) {
    super(rtx, collection, atomic);
  }
}
