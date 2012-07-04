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

package org.sirix.axis;

import javax.annotation.Nonnull;

import org.sirix.api.IAxis;
import org.sirix.api.IFilter;
import org.sirix.api.INodeReadTrx;

/**
 * <h1>TestAxis</h1>
 * 
 * <p>
 * Perform a test on a given axis.
 * </p>
 */
public final class FilterAxis extends AbsAxis {

  /** Axis to test. */
  private final IAxis mAxis;

  /** Test to apply to axis. */
  private final IFilter[] mAxisFilter;

  /**
   * Constructor initializing internal state.
   * 
   * @param pAxis
   *          axis to iterate over
   * @param pFirstAxisTest
   *          test to perform for each node found with axis
   * @param pAxisTest
   *          tests to perform for each node found with axis
   */
  public FilterAxis(@Nonnull final IAxis pAxis,
    @Nonnull final IFilter pFirstAxisTest, @Nonnull final IFilter... pAxisTest) {
    super(pAxis.getTransaction());
    mAxis = pAxis;
    final int length = pAxisTest.length == 0 ? 1 : pAxisTest.length + 1;
    mAxisFilter = new IFilter[length];
    mAxisFilter[0] = pFirstAxisTest;
    for (int i = 1; i < length; i++) {
      mAxisFilter[i] = pAxisTest[i - 1];
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
  public boolean hasNext() {
    if (isNext()) {
      return true;
    }
    resetToLastKey();
    while (mAxis.hasNext()) {
      mKey = mAxis.next();
      boolean filterResult = true;
      for (final IFilter filter : mAxisFilter) {
        filterResult = filterResult && filter.filter();
      }
      if (filterResult) {
        return true;
      }
    }
    resetToStartKey();
    return false;
  }

  /**
   * Returns the inner axis.
   * 
   * @return the axis
   */
  public IAxis getAxis() {
    return mAxis;
  }
}
