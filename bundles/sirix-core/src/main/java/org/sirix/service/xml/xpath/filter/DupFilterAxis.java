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

package org.sirix.service.xml.xpath.filter;

import java.util.HashSet;
import java.util.Set;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.NestedAxis;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.service.xml.xpath.AbstractAxis;
import org.sirix.service.xml.xpath.expr.UnionAxis;

/**
 * <p>
 * Duplicate Filter. Assures that the resulting node set contains no duplicates.
 * </p>
 * <p>
 * Encapsulates a given XPath axis and only passes on those items that have not already been passed.
 * This does not break the pipeline since every intermediary result is immediately passed on, as
 * long as it is not already in the set (which indicates that it was already returned).
 * </p>
 */
public class DupFilterAxis extends AbstractAxis {

  /** Sequence that may contain duplicates. */
  private final Axis mAxis;

  /** Set that stores all already returned item keys. */
  private final Set<Long> mDupSet;

  /**
   * Defines whether next() has to be called for the dupAxis after calling hasNext(). In some cases
   * next() has already been called by another axis.
   */
  private final boolean mCallNext;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx Exclusive (immutable) trx to iterate with.
   * @param pDupAxis Sequence that may return duplicates.
   */
  public DupFilterAxis(final XmlNodeReadOnlyTrx rtx, final Axis pDupAxis) {

    super(rtx);
    mAxis = pDupAxis;
    mDupSet = new HashSet<Long>();
    // if the dupAxis is not one of the specified axis, 'next()' has
    // explicitly
    // be called for those axis after calling 'hasNext()'. For all other
    // axis
    // next() has already been called by another axis.
    mCallNext =
        !(mAxis instanceof FilterAxis || mAxis instanceof NestedAxis || mAxis instanceof UnionAxis);

  }

  @Override
  public final void reset(final long mNodeKey) {

    super.reset(mNodeKey);
    if (mAxis != null) {
      mAxis.reset(mNodeKey);
    }
  }

  @Override
  public final boolean hasNext() {
    if (isNext()) {
      return true;
    } else {
      resetToLastKey();

      while (mAxis.hasNext()) {
        // call next(), if it was not already called for that axis.
        // if (((IAxis)mAxis).isNext()) {
        // mKey = mAxis.next();
        // } else {
        mAxis.next();
        mKey = mAxis.asXdmNodeReadTrx().getNodeKey();
        // }

        // add current item key to the set. If true is returned the item is
        // no
        // duplicate and can be returned by the duplicate filter.
        if (mDupSet.add(asXdmNodeReadTrx().getNodeKey())) {
          return true;
        }
      }

      resetToStartKey();
      return false;
    }
  }

}
