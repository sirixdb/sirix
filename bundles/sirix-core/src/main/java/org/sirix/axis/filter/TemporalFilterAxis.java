package org.sirix.axis.filter;

import org.sirix.api.Axis;
import org.sirix.api.Filter;
import org.sirix.api.NodeReadTrx;
import org.sirix.axis.AbstractTemporalAxis;

public final class TemporalFilterAxis extends AbstractTemporalAxis {
	/** Axis to test. */
	private final AbstractTemporalAxis mAxis;

	/** Test to apply to axis. */
	private final Filter[] mAxisFilter;
	
	/**
	 * Constructor initializing internal state.
	 * 
	 * @param axis
	 *          axis to iterate over
	 * @param firstAxisTest
	 *          test to perform for each node found with axis
	 * @param axisTest
	 *          tests to perform for each node found with axis
	 */
	public TemporalFilterAxis(final AbstractTemporalAxis axis,
			final Filter firstAxisTest, final Filter... axisTest) {
		mAxis = axis;
		final int length = axisTest.length == 0 ? 1 : axisTest.length + 1;
		mAxisFilter = new Filter[length];
		mAxisFilter[0] = firstAxisTest;
		for (int i = 1; i < length; i++) {
			mAxisFilter[i] = axisTest[i - 1];
		}
	}

	@Override
	protected NodeReadTrx computeNext() {
		while (mAxis.hasNext()) {
			final NodeReadTrx rtx = mAxis.next();
			boolean filterResult = true;
			for (final Filter filter : mAxisFilter) {
				filter.setTrx(rtx);
				filterResult = filterResult && filter.filter();
				if (!filterResult) {
					break;
				}
			}
			if (filterResult) {
				return mAxis.getTrx();
			}
		}
		return endOfData();
	}

	/**
	 * Returns the inner axis.
	 * 
	 * @return the axis
	 */
	public AbstractTemporalAxis getAxis() {
		return mAxis;
	}

	@Override
	public NodeReadTrx getTrx() {
		return mAxis.getTrx();
	}
}
