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

import io.sirix.api.Filter;
import io.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * <p>
 * Nests two or more IFilters.
 * </p>
 */
public final class NestedFilter extends AbstractFilter<XmlNodeReadOnlyTrx> {

  /** Tests to apply. */
  private final List<Filter<XmlNodeReadOnlyTrx>> mFilter;

  /**
   * Default constructor.
   *
   * @param rtx {@link XmlNodeReadOnlyTrx} this filter is bound to
   * @param axisTest test to perform for each node found with axis
   */
  public NestedFilter(final XmlNodeReadOnlyTrx rtx, final @NonNull List<Filter<XmlNodeReadOnlyTrx>> axisTest) {
    super(rtx);
    mFilter = new ArrayList<>(axisTest);
  }

  @Override
  public final boolean filter() {
    boolean filterResult = true;

    for (final Filter<XmlNodeReadOnlyTrx> filter : mFilter) {
      filterResult = filterResult && filter.filter();
    }

    return filterResult;
  }
}
