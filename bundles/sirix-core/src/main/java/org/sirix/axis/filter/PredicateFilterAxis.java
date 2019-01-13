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
import org.sirix.api.Axis;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.axis.AbstractAxis;

/**
 * <h1>PredicateFilterAxis</h1>
 * <p>
 * The PredicateAxis evaluates a predicate (in the form of an axis) and returns true, if the
 * predicates has a value (axis.hasNext() == true) and this value if not the boolean value false.
 * Otherwise false is returned. Since a predicate is a kind of filter, the transaction that has been
 * altered by means of the predicate's evaluation has to be reset to the key that it was set to
 * before the evaluation.
 * </p>
 */
public final class PredicateFilterAxis extends AbstractAxis {

  /** First run. */
  private boolean mIsFirst;

  /** Predicate axis. */
  private final Axis mPredicate;

  /**
   * Constructor. Initializes the internal state.
   * 
   * @param rtx exclusive (immutable) trx to iterate with
   * @param predicate predicate expression
   */
  public PredicateFilterAxis(final XdmNodeReadTrx rtx, final Axis predicate) {
    super(rtx);
    mIsFirst = true;
    mPredicate = checkNotNull(predicate);
  }

  @Override
  public final void reset(final long nodeKey) {
    super.reset(nodeKey);
    if (mPredicate != null) {
      mPredicate.reset(nodeKey);
    }
    mIsFirst = true;
  }

  @Override
  protected long nextKey() {
    // A predicate has to evaluate to true only once.
    if (mIsFirst) {
      mIsFirst = false;

      final long currKey = getTrx().getNodeKey();
      mPredicate.reset(currKey);

      if (mPredicate.hasNext()) {
        mPredicate.next();
        if (isBooleanFalse()) {
          return done();
        }
        return currKey;
      }
    }
    return done();
  }

  /**
   * Tests whether current item is an atomic value with boolean value "false".
   * 
   * @return {@code true}, if item is boolean typed atomic value with type "false".
   */
  private boolean isBooleanFalse() {
    if (getTrx().getNodeKey() >= 0) {
      return false;
    } else { // is AtomicValue
      if (getTrx().getTypeKey() == getTrx().keyForName("xs:boolean")) {
        // atomic value of type boolean
        // return true, if atomic values's value is false
        return !(Boolean.parseBoolean(getTrx().getValue()));
      } else {
        return false;
      }
    }
  }

}
