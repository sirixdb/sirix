package io.sirix.axis.filter.json;

import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.node.NodeKind;
import io.sirix.axis.filter.AbstractFilter;

/**
 * Only match NUMBER_VALUE nodes.
 *
 * @author Johannes Lichtenberger
 */
public final class NumberValueFilter extends AbstractFilter<JsonNodeReadOnlyTrx> {

	/**
	 * Default constructor.
	 *
	 * @param rtx
	 *            Transaction this filter is bound to.
	 */
	public NumberValueFilter(final JsonNodeReadOnlyTrx rtx) {
		super(rtx);
	}

	@Override
	public final boolean filter() {
		return getTrx().getKind() == NodeKind.NUMBER_VALUE;
	}

}
