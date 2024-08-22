package io.sirix.axis.filter.json;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import io.sirix.api.Filter;
import io.sirix.api.ResourceSession;
import io.sirix.api.json.JsonNodeReadOnlyTrx;
import io.sirix.api.json.JsonNodeTrx;
import io.sirix.axis.AbstractTemporalAxis;

import static java.util.Objects.requireNonNull;

/**
 * Filter for temporal axis.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class TemporalJsonNodeReadFilterAxis<F extends Filter<JsonNodeReadOnlyTrx>>
		extends
			AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> {

	/** Axis to test. */
	private final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis;

	/** Test to apply to axis. */
	private final List<F> axisFilter;

	/**
	 * Constructor initializing internal state.
	 *
	 * @param axis
	 *            axis to iterate over
	 * @param firstAxisTest
	 *            test to perform for each node found with axis
	 * @param axisTest
	 *            tests to perform for each node found with axis
	 */
	@SafeVarargs
	public TemporalJsonNodeReadFilterAxis(final AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> axis,
			final F firstAxisTest, final F... axisTest) {
		requireNonNull(firstAxisTest);
		this.axis = axis;
		axisFilter = new ArrayList<>();
		axisFilter.add(firstAxisTest);

		if (axisTest != null) {
			axisFilter.addAll(Arrays.asList(axisTest));
		}
	}

	@Override
	protected JsonNodeReadOnlyTrx computeNext() {
		while (axis.hasNext()) {
			final JsonNodeReadOnlyTrx rtx = axis.next();
			final boolean filterResult = doFilter(rtx);
			if (filterResult) {
				return rtx;
			}

			rtx.close();
		}
		return endOfData();
	}

	private boolean doFilter(final JsonNodeReadOnlyTrx rtx) {
		boolean filterResult = true;
		for (final F filter : axisFilter) {
			filter.setTrx(rtx);
			filterResult = filter.filter();
			if (!filterResult) {
				break;
			}
		}
		return filterResult;
	}

	/**
	 * Returns the inner axis.
	 *
	 * @return the axis
	 */
	public AbstractTemporalAxis<JsonNodeReadOnlyTrx, JsonNodeTrx> getAxis() {
		return axis;
	}

	@Override
	public ResourceSession<JsonNodeReadOnlyTrx, JsonNodeTrx> getResourceManager() {
		return axis.getResourceManager();
	}
}
