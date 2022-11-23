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

import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.service.xml.xpath.AbstractAxis;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>
 * Evaluated the given binding sequence, the variable is bound to and stores in a list that can be
 * accessed by other sequences and notifies its observers, as soon as a new value of the binding
 * sequence has been evaluated.
 * </p>
 */
public class VariableAxis extends AbstractAxis {

  /** Sequence that defines the values, the variable is bound to. */
  private final Axis mBindingSeq;

  private final List<VarRefExpr> mVarRefs;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param pRtx exclusive (immutable) trx to iterate with
   * @param pInSeq sequence, the variable is bound to
   */
  public VariableAxis(final XmlNodeReadOnlyTrx pRtx, @NonNull final Axis pInSeq) {
    super(pRtx);
    mBindingSeq = checkNotNull(pInSeq);
    mVarRefs = new ArrayList<VarRefExpr>();
  }

  @Override
  public void reset(final long pNodeKey) {
    super.reset(pNodeKey);
    if (mBindingSeq != null) {
      mBindingSeq.reset(pNodeKey);
    }
  }

  @Override
  public boolean hasNext() {
    if (isNext()) {
      return true;
    }

    resetToLastKey();

    if (mBindingSeq.hasNext()) {
      key = mBindingSeq.next();
      notifyObs();
      return true;
    }

    resetToStartKey();
    return false;
  }

  /**
   * Tell all observers that a new item of the binding sequence has been evaluated.
   */
  private void notifyObs() {
    for (final VarRefExpr varRef : mVarRefs) {
      varRef.update(asXmlNodeReadTrx().getNodeKey());
    }
  }

  /**
   * Add an observer to the list.
   * 
   * @param pObserver axis that wants to be notified of any change of this axis
   */
  public void addObserver(final VarRefExpr pObserver) {
    mVarRefs.add(checkNotNull(pObserver));
  }

}
