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

import org.sirix.api.IFilter;
import org.sirix.api.INodeReadTrx;

/**
 * <h1>AbstractFilter</h1>
 * 
 * <p>
 * Filter node of transaction this filter is bound to.
 * </p>
 */
public abstract class AbsFilter implements IFilter {

  /** Iterate over transaction exclusive to this step. */
  private INodeReadTrx mRTX;

  /**
   * Bind axis step to transaction.
   * 
   * @param rtx
   *          Transaction to operate with.
   */
  protected AbsFilter(final INodeReadTrx rtx) {
    mRTX = rtx;
  }

  /**
   * Getting the Transaction of this filter
   * 
   * @return the transaction of this filter
   */
  protected final INodeReadTrx getTransaction() {
    return mRTX;
  }

  @Override
  public abstract boolean filter();

  @Override
  public synchronized void setTransaction(final INodeReadTrx rtx) {
    mRTX = rtx;
  }
}
