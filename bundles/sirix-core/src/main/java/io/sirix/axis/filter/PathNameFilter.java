/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
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

import io.sirix.index.path.summary.PathSummaryReader;
import org.brackit.xquery.atomic.QNm;

/**
 * <p>
 * Match qname of ELEMENT or ATTRIBUTE by key.
 * </p>
 */
public final class PathNameFilter extends AbstractFilter<PathSummaryReader> {

  /**
   * Key of local name to test.
   */
  private final int localNameKey;

  /**
   * Key of prefix to test.
   */
  private final int prefixKey;

  /**
   * Default constructor.
   *
   * @param rtx  the node trx/node cursor this filter is bound to
   * @param name name to check
   */
  public PathNameFilter(final PathSummaryReader rtx, final QNm name) {
    super(rtx);
    prefixKey = (name.getPrefix() == null || name.getPrefix().isEmpty()) ? -1 : rtx.keyForName(name.getPrefix());
    localNameKey = rtx.keyForName(name.getLocalName());
  }

  /**
   * Default constructor.
   *
   * @param rtx  {@link PathSummaryReader} this filter is bound to
   * @param name name to check
   */
  public PathNameFilter(final PathSummaryReader rtx, final String name) {
    super(rtx);
    final int index = name.indexOf(":");
    if (index != -1) {
      prefixKey = rtx.keyForName(name.substring(0, index));
    } else {
      prefixKey = -1;
    }

    localNameKey = rtx.keyForName(name.substring(index + 1));
  }

  @Override
  public boolean filter() {
    boolean returnVal = false;
    if (getTrx().isNameNode()) {
      returnVal = (getTrx().getLocalNameKey() == localNameKey && getTrx().getPrefixKey() == prefixKey);
    }
    return returnVal;
  }
}
