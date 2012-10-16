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

package org.sirix.service.xml.xpath;

import org.sirix.api.Axis;
import org.sirix.axis.AncestorAxis;
import org.sirix.axis.ChildAxis;
import org.sirix.axis.DescendantAxis;
import org.sirix.axis.FollowingAxis;
import org.sirix.axis.FollowingSiblingAxis;
import org.sirix.axis.NestedAxis;
import org.sirix.axis.ParentAxis;
import org.sirix.axis.PrecedingAxis;
import org.sirix.axis.PrecedingSiblingAxis;
import org.sirix.axis.filter.FilterAxis;
import org.sirix.service.xml.xpath.expr.UnionAxis;
import org.sirix.service.xml.xpath.filter.DupFilterAxis;

/**
 * <h1>ExpresseionSingle</h1>
 * <p>
 * This class builds an execution chain to execute a XPath query. All added axis are build together by using
 * NestedAxis.
 */
public class ExpressionSingle {

  /**
   * Counts the number of added axis. This is used to handle the special
   * behavior for the first and second axis that are added.
   */
  private int mNumber;

  /** The first added axis has to be stored till a second one is added. */
  private Axis mFirstAxis;

  /** Contains the execution chain consisting of nested NestedAxis. */
  private Axis mExpr;

  /** Current ordering state. */
  private OrdState mOrd;

  /** Current duplicate state. */
  private DupState mDup;

  /**
   * Constructor. Initializes the internal state.
   */
  public ExpressionSingle() {

    mNumber = 0;

    mOrd = OrdState.MAX1;
    mOrd.init();
    mDup = DupState.MAX1;

  }

  /**
   * Adds a new Axis to the expression chain. The first axis that is added has
   * to be stored till a second axis is added. When the second axis is added,
   * it is nested with the first one and builds the execution chain.
   * 
   * @param pAx
   *          the axis to add
   */
  public void add(final Axis pAx) {
    Axis axis = pAx;
      if (isDupOrd(axis)) {
        axis = new DupFilterAxis(axis.getTrx(), axis);
        DupState.nodup = true;
      }

      switch (mNumber) {
      case 0:
        mFirstAxis = axis;
        mNumber++;
        break;
      case 1:
        mExpr = new NestedAxis(mFirstAxis, axis);
        mNumber++;
        break;
      default:
        final Axis cache = mExpr;
        mExpr = new NestedAxis(cache, axis);
      }
  }

  /**
   * Returns a chain to execute the query. If there is only one axis added,
   * the chain was not build yet, so only this axis is returned.
   * 
   * @return The query execution chain
   */
  public Axis getExpr() {

    return (mNumber == 1) ? mFirstAxis : mExpr;
  }

  /**
   * Returns the number of axis in this expression.
   * 
   * @return size of the expression
   */
  public int getSize() {

    return mNumber;
  }

  /**
   * Determines for a given string representation of an axis, whether this
   * axis leads to duplicates in the result sequence or not. Furthermore it
   * determines the new state for the order state that specifies, if the
   * result sequence is in document order. This method is implemented
   * according to the automata in [Hidders, J., Michiels, P., "Avoiding
   * Unnecessary Ordering Operations in XPath", 2003]
   * 
   * @param ax
   *          name of the current axis
   * @return true, if expression is still duplicate free
   */
  public boolean isDupOrd(final Axis ax) {

    Axis axis = ax;

    while (axis instanceof FilterAxis) {
      axis = ((FilterAxis)axis).getAxis();
    }

    if (axis instanceof UnionAxis) {
      mOrd = mOrd.updateOrdUnion();
      mDup = mDup.updateUnion();

    } else if (axis instanceof ChildAxis) {
      mOrd = mOrd.updateOrdChild();
      mDup = mDup.updateDupChild();

    } else if (axis instanceof ParentAxis) {

      mOrd = mOrd.updateOrdParent();
      mDup = mDup.updateDupParent();

    } else if (axis instanceof DescendantAxis) {

      mOrd = mOrd.updateOrdDesc();
      mDup = mDup.updateDupDesc();

    } else if (axis instanceof AncestorAxis) {

      mOrd = mOrd.updateOrdAncestor();
      mDup = mDup.updateDupAncestor();

    } else if (axis instanceof FollowingAxis || axis instanceof PrecedingAxis) {

      mOrd = mOrd.updateOrdFollPre();
      mDup = mDup.updateDupFollPre();

    } else if (axis instanceof FollowingSiblingAxis
      || axis instanceof PrecedingSiblingAxis) {

      mOrd = mOrd.updateOrdFollPreSib();
      mDup = mDup.updateDupFollPreSib();
    }

    return !DupState.nodup;
  }

  /**
   * @return true, if the result is in document order
   */
  public boolean isOrdered() {

    // the result sequence is unordered, if the order rank is greater than
    // zero
    // or the order state is in state UNORD
    // return (ord != OrdState.UNORD && ord.mOrdRank == 0);
    return (mOrd != OrdState.UNORD && OrdState.mOrdRank == 0);
  }
}
