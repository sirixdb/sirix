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

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.Nonnull;

import org.sirix.api.IAxis;

/**
 * <h1>NestedAxis</h1>
 * 
 * <p>
 * Chains two axis operations.
 * </p>
 */
public final class NestedAxis extends AbsAxis {

  /** Parent axis. */
  private final IAxis mParentAxis;

  /** Child axis to apply to each node found with parent axis. */
  private final IAxis mChildAxis;

  /** Is it the first run of parent axis? */
  private boolean mIsFirst;

  /**
   * Constructor initializing internal state.
   * 
   * @param parentAxis
   *          Inner nested axis.
   * @param mChildAxis
   *          Outer nested axis.
   */
  public NestedAxis(@Nonnull final IAxis pParentAxis, @Nonnull final IAxis pChildAxis) {
    super(pParentAxis.getTransaction());
    mParentAxis = checkNotNull(pParentAxis);
    mChildAxis = checkNotNull(pChildAxis);
    mIsFirst = true;
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    if (mParentAxis != null) {
      mParentAxis.reset(pNodeKey);
    }
    if (mChildAxis != null) {
      mChildAxis.reset(pNodeKey);
    }
    mIsFirst = true;
  }

  @Override
  public boolean hasNext() {
    if (isNext()) {
      return true;
    } else {
      resetToLastKey();

      // Make sure that parent axis is moved for the first time.
      if (mIsFirst) {
        mIsFirst = false;
        if (mParentAxis.hasNext()) {
          mKey = mParentAxis.next();
          mChildAxis.reset(mKey);
        } else {
          resetToStartKey();
          return false;
        }
      }

      // Execute child axis for each node found with parent axis.
      boolean hasNext = false;
      while (!(hasNext = mChildAxis.hasNext())) {
        if (mParentAxis.hasNext()) {
          mKey = mParentAxis.next();
          mChildAxis.reset(mKey);
        } else {
          break;
        }
      }
      if (hasNext) {
        mKey = mChildAxis.next();
        return true;
      }

      resetToStartKey();
      return false;
    }
  }

}
