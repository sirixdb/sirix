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

package org.sirix.axis.filter;

import static com.google.common.base.Preconditions.checkNotNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.Filter;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import com.google.common.base.Predicate;

/**
 * <p>
 * Filter node of transaction this filter is bound to.
 * </p>
 */
public abstract class AbstractFilter<R extends NodeReadOnlyTrx & NodeCursor> implements Filter<R>, Predicate<Long> {

  /** Iterate over transaction exclusive to this step. */
  private R rtx;

  /**
   * Bind axis step to transaction.
   *
   * @param rtx transaction to operate with
   */
  protected AbstractFilter(final R rtx) {
    this.rtx = checkNotNull(rtx);
  }

  @Override
  public final R getTrx() {
    return rtx;
  }

  @Override
  public void setTrx(R rtx) {
    this.rtx = checkNotNull(rtx);
  }

  @Override
  public abstract boolean filter();

  @Override
  public boolean apply(final @Nullable Long nodeKey) {
    rtx.moveTo(checkNotNull(nodeKey));
    return filter();
  }
}
