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

package org.sirix.diff;

/**
 * Container for transaction cursor depths in both the old and new revision.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
final class DepthCounter {
  /** Depth in new revision. */
  private transient int mNewDepth;

  /** Depth in old revision. */
  private transient int mOldDepth;

  /**
   * Constructor.
   * 
   * @param paramNewDepth
   *          depth of transaction cursor in new revision
   * @param paramOldDepth
   *          depth of transaction cursor in old revision
   */
  DepthCounter(final int paramNewDepth, final int paramOldDepth) {
    mNewDepth = paramNewDepth;
    mOldDepth = paramOldDepth;
  }

  /** Increment depth in new revision. */
  void incrementNewDepth() {
    mNewDepth++;
  }

  /** Decrement depth in new revision. */
  void decrementNewDepth() {
    mNewDepth--;
  }

  /** Increment depth in old revision. */
  void incrementOldDepth() {
    mOldDepth++;
  }

  /** Decrement depth in old revision. */
  void decrementOldDepth() {
    mOldDepth--;
  }

  /**
   * Get depth in new revision.
   * 
   * @return depth in new revision
   */
  int getNewDepth() {
    return mNewDepth;
  }

  /**
   * Get depth in old revision.
   * 
   * @return depth in old revision
   */
  int getOldDepth() {
    return mOldDepth;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return new StringBuilder("newDepth: ").append(mNewDepth).append("oldDepth: ").append(mOldDepth)
      .toString();
  }
}
