/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.sirix.axis.filter;

import java.util.ArrayList;
import java.util.List;

import io.sirix.api.Axis;
import io.sirix.api.Filter;
import io.sirix.api.NodeCursor;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.axis.AbstractAxis;

/**
 * //todo Does the name need to change
 * <p>
 * Perform a test on a given axis.
 * </p>
 */
public final class FilterAxis<R extends NodeReadOnlyTrx & NodeCursor> extends AbstractAxis {

  /** Axis to test. */
  private final Axis axis;

  /** Test to apply to axis. */
  private final List<Filter<R>> axisFilter;

  /**
   * Constructor initializing internal state.
   *
   * @param axis axis to iterate over
   * @param firstAxisTest test to perform for each node found with axis
   * @param axisTest tests to perform for each node found with axis
   */
  @SuppressWarnings("unlikely-arg-type")
  @SafeVarargs
  public FilterAxis(final Axis axis, final Filter<R> firstAxisTest, final Filter<R>... axisTest) {
    super(axis.getCursor());
    this.axis = axis;
    axisFilter = new ArrayList<>();
    axisFilter.add(firstAxisTest);
    if (!this.axis.getCursor().equals(axisFilter.get(0).getTrx())) {
      throw new IllegalArgumentException("The filter must be bound to the same transaction as the axis!");
    }

    if (axisTest != null) {
      for (final var filter : axisTest) {
        axisFilter.add(filter);
        if (!this.axis.getCursor().equals(axisFilter.get(axisFilter.size() - 1).getTrx())) {
          throw new IllegalArgumentException("The filter must be bound to the same transaction as the axis!");
        }
      }
    }
  }

  @Override
  public void reset(final long nodeKey) {
    super.reset(nodeKey);
    if (axis != null) {
      axis.reset(nodeKey);
    }
  }

  @Override
  protected long nextKey() {
    while (axis.hasNext()) {
      final long nodeKey = axis.nextLong();
      boolean filterResult = true;
      for (final Filter<R> filter : axisFilter) {
        filterResult = filterResult && filter.filter();
        if (!filterResult) {
          break;
        }
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
    return axis;
  }
}
