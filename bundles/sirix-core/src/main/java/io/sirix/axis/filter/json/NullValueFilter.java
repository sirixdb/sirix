package io.sirix.axis.filter.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.NodeKind;
import io.sirix.axis.filter.AbstractFilter;

/**
 * Only match NULL_VALUE nodes.
 *
 * @author Johannes Lichtenberger
 */
public final class NullValueFilter extends AbstractFilter<JsonNodeReadOnlyTrx> {

  /**
   * Default constructor.
   *
   * @param rtx Transaction this filter is bound to.
   */
  public NullValueFilter(final JsonNodeReadOnlyTrx rtx) {
    super(rtx);
  }

  @Override
  public final boolean filter() {
    return getTrx().getKind() == NodeKind.NULL_VALUE;
  }

}
