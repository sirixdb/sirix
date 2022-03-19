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

package org.sirix.service.xml.xpath.expr;

import java.util.Arrays;
import java.util.List;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.service.xml.xpath.AbstractAxis;

/**
 * <p>
 * Axis that represents a sequence of singleExpressions, normally separated by a ','.
 * </p>
 * <p>
 * Calling hasNext() returns the results of the singleExpressions consecutively.
 * </p>
 * 
 */
public class SequenceAxis extends AbstractAxis {

  private final List<Axis> mSeq;
  private Axis mCurrent;
  private int mNum;

  /**
   * 
   * Constructor. Initializes the internal state.
   * 
   * @param rtx Exclusive (immutable) trx to iterate with.
   * @param axis The singleExpressions contained by the sequence
   */
  public SequenceAxis(final XmlNodeReadOnlyTrx rtx, final Axis... axis) {

    super(rtx);
    mSeq = Arrays.asList(axis);
    mNum = 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void reset(final long mNodeKey) {
    super.reset(mNodeKey);
    if (mSeq != null) {
      for (Axis ax : mSeq) {
        ax.reset(mNodeKey);
      }
    }
    mCurrent = null;
    mNum = 0;

  }

  @Override
  public boolean hasNext() {

    resetToLastKey();

    if (mCurrent != null) {
      if (mCurrent.hasNext()) {
        key = mCurrent.next();
        return true;
      } else {
        // // necessary, because previous hasNext() changes state
        // resetToLastKey();
        // mKey =
      }
    }

    while (mNum < mSeq.size()) {
      mCurrent = mSeq.get(mNum++);

      // mCurrent.getTransaction().moveTo(getTransaction().getKey());
      mCurrent.reset(asXdmNodeReadTrx().getNodeKey());
      // mCurrent.resetToLastKey();
      if (mCurrent.hasNext()) {
        key = mCurrent.next();
        return true;
      }
    }

    resetToStartKey();
    return false;

  }

}
