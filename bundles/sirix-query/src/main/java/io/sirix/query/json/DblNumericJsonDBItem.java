package io.sirix.query.json;

import io.brackit.query.atomic.DblNumeric;
import io.sirix.api.json.JsonNodeReadOnlyTrx;

/**
 * A {@link NumericJsonDBItem} wrapping a document-sourced DOUBLE (brackit {@code Dbl}). Tagging the
 * brackit {@link DblNumeric} interface lets {@code instanceof DblNumeric} dispatch in the numeric
 * comparison logic route this value exactly like a brackit double literal.
 */
public final class DblNumericJsonDBItem extends NumericJsonDBItem implements DblNumeric {

  /**
   * Constructor.
   *
   * @param rtx        {@link JsonNodeReadOnlyTrx} for providing reading access to the underlying node
   * @param collection {@link JsonDBCollection} reference
   * @param atomic     the double-valued brackit atomic delegate (must be a {@link DblNumeric})
   */
  public DblNumericJsonDBItem(final JsonNodeReadOnlyTrx rtx, final JsonDBCollection collection,
      final DblNumeric atomic) {
    super(rtx, collection, atomic);
  }
}
