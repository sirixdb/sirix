package org.sirix.axis.filter.json;

import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.axis.filter.AbstractFilter;
import org.sirix.node.NodeKind;

/**
 * Only match NUMBER_VALUE nodes.
 *
 * @author Johannes Lichtenberger
 */
public final class NumberValueFilter extends AbstractFilter<JsonNodeReadOnlyTrx> {

  /**
   * Default constructor.
   *
   * @param rtx Transaction this filter is bound to.
   */
  public NumberValueFilter(final JsonNodeReadOnlyTrx rtx) {
    super(rtx);
  }

  @Override
  public final boolean filter() {
    return getTrx().getKind() == NodeKind.NUMBER_VALUE;
  }

}
