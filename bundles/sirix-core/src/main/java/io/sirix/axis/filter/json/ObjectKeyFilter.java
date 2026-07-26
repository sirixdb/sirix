package io.sirix.axis.filter.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.axis.filter.AbstractFilter;

/**
 * Match object-field records — any fused {@code OBJECT_NAMED_*} that plays the field-name
 * role for name-lookup and path-summary scoping.
 *
 * @author Johannes Lichtenberger
 */
public final class ObjectKeyFilter extends AbstractFilter<JsonNodeReadOnlyTrx> {

  /**
   * Default constructor.
   *
   * @param rtx Transaction this filter is bound to.
   */
  public ObjectKeyFilter(final JsonNodeReadOnlyTrx rtx) {
    super(rtx);
  }

  @Override
  public boolean filter() {
    return getTrx().getKind().playsObjectKeyRole();
  }

}
