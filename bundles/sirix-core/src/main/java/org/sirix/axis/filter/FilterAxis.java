/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 * * Redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the
 * names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.axis.filter;

import javax.annotation.Nonnull;

import org.sirix.api.Axis;
import org.sirix.api.Filter;
import org.sirix.axis.AbstractAxis;

/**
 * <h1>TestAxis</h1>
 * 
 * <p>
 * Perform a test on a given axis.
 * </p>
 */
public final class FilterAxis extends AbstractAxis {

	/** Axis to test. */
	private final Axis mAxis;

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
	public FilterAxis(final @Nonnull Axis axis,
			final @Nonnull Filter firstAxisTest, final @Nonnull Filter... axisTest) {
		super(axis.getTrx());
		mAxis = axis;
		final int length = axisTest.length == 0 ? 1 : axisTest.length + 1;
		mAxisFilter = new Filter[length];
		mAxisFilter[0] = firstAxisTest;
		if (!mAxis.getTrx().equals(mAxisFilter[0].getTrx())) {
			throw new IllegalArgumentException(
					"The filter must be bound to the same transaction as the axis!");
		}
		for (int i = 1; i < length; i++) {
			mAxisFilter[i] = axisTest[i - 1];
			if (!mAxis.getTrx().equals(mAxisFilter[i].getTrx())) {
				throw new IllegalArgumentException(
						"The filter must be bound to the same transaction as the axis!");
			}
		}
	}

	@Override
	public void reset(final long pNodeKey) {
		super.reset(pNodeKey);
		if (mAxis != null) {
			mAxis.reset(pNodeKey);
		}
	}

	@Override
	protected long nextKey() {
		while (mAxis.hasNext()) {
			final long nodeKey = mAxis.next();
			boolean filterResult = true;
			for (final Filter filter : mAxisFilter) {
				filterResult = filterResult && filter.filter();
			}
			if (filterResult) {
				return nodeKey;
			}
		}
		return done();
	}

	/**
	 * Returns the inner axis.
	 * 
	 * @return the axis
	 */
	public Axis getAxis() {
		return mAxis;
	}
}
