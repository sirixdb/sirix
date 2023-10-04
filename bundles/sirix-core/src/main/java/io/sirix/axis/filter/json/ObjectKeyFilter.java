package io.sirix.axis.filter.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.NodeKind;
import io.sirix.axis.filter.AbstractFilter;

/**
 * Only match OBJECT_KEY nodes.
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
    return getTrx().getKind() == NodeKind.OBJECT_KEY;
  }

}
