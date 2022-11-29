package org.sirix.axis.filter.json;

import org.sirix.api.json.JsonNodeReadOnlyTrx;
import org.sirix.axis.filter.AbstractFilter;
import org.sirix.node.NodeKind;

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
