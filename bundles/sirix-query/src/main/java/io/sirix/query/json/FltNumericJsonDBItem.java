package io.sirix.query.json;

import io.brackit.query.atomic.FltNumeric;
import io.sirix.api.json.JsonNodeReadOnlyTrx;

/**
 * A {@link NumericJsonDBItem} wrapping a document-sourced FLOAT (brackit {@code Flt}). Tagging the
 * brackit {@link FltNumeric} interface lets {@code instanceof FltNumeric} dispatch in the numeric
 * comparison logic route this value exactly like a brackit float literal.
 */
public final class FltNumericJsonDBItem extends NumericJsonDBItem implements FltNumeric {

  /**
   * Constructor.
   *
   * @param rtx        {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param atomic     the float-valued brackit atomic delegate (must be a {@link FltNumeric})
   */
  public FltNumericJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection,
      final FltNumeric atomic) {
    super(rtx, collection, atomic);
  }
}
