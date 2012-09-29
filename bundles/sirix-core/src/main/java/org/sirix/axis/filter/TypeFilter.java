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

import org.sirix.api.INodeReadTrx;

/**
 * <h1>TypeFilter</h1>
 * 
 * <p>
 * Only match nodes with the specified value type.
 * </p>
 */
public class TypeFilter extends AbsFilter {

  /** Type information. */
  private final int mType;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param pRtx
   *          transaction this filter is bound to
   * @param pType
   *          type to match
   */
  public TypeFilter(final @Nonnull INodeReadTrx pRtx, final int pType) {
    super(pRtx);
    mType = pType;
  }

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param pRtx
   *          transaction this filter is bound to
   * @param pTypeName
   *          name of the type to match
   */
  public TypeFilter(final INodeReadTrx pRtx, final @Nonnull String pTypeName) {
    this(pRtx, pRtx.keyForName(pTypeName));
  }

  @Override
  public final boolean filter() {
    return getTransaction().getTypeKey() == mType;
  }

}
