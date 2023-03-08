/*
 * Copyright (c) 2023, Sirix Contributors
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the <organization> nor the
 *       names of its contributors may be used to endorse or promote products
 *       derived from this software without specific prior written permission.
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

package org.sirix.axis.pathsummary.filter;

import org.sirix.axis.pathsummary.AbstractAxis;
import org.sirix.index.path.summary.PathNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

/**
 * Perform a test on a given axis.
 *
 * @author Johannes Lichtenberger
 */
public final class FilterAxis extends AbstractAxis {

  /**
   * Axis to test.
   */
  private final AbstractAxis axis;

  /**
   * Test to apply to axis.
   */
  private final List<Predicate<PathNode>> axisFilter;

  /**
   * Constructor initializing internal state.
   *
   * @param axis          axis to iterate over
   * @param firstAxisTest test to perform for each node found with axis
   * @param axisTests      tests to perform for each node found with axis
   */
  @SuppressWarnings("unlikely-arg-type")
  @SafeVarargs
  public FilterAxis(final AbstractAxis axis, final Predicate<PathNode> firstAxisTest,
      final Predicate<PathNode>... axisTests) {
    super(axis.getStartPathNode());
    this.axis = axis;
    axisFilter = new ArrayList<>();
    axisFilter.add(firstAxisTest);
    if (axisTests != null) {
      Collections.addAll(axisFilter, axisTests);
    }
  }

  @Override
  protected PathNode nextNode() {
    while (axis.hasNext()) {
      final var node = axis.next();
      boolean filterResult = true;
      for (final Predicate<PathNode> filter : axisFilter) {
        filterResult = filterResult && filter.test(node);
        if (!filterResult) {
          break;
        }
      }
      if (filterResult) {
        return node;
      }
    }
    return done();
  }
}
