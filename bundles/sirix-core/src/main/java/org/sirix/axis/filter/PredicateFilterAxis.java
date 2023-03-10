/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
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

import org.sirix.api.Axis;
import org.sirix.api.NodeCursor;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.AbstractAxis;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * The PredicateAxis evaluates a predicate (in the form of an axis) and returns true, if the
 * predicates has a value (axis.hasNext() == true) and this value if not the boolean value false.
 * Otherwise false is returned. Since a predicate is a kind of filter, the transaction that has been
 * altered by means of the predicate's evaluation has to be reset to the key that it was set to
 * before the evaluation.
 * </p>
 */
public final class PredicateFilterAxis extends AbstractAxis {

  /**
   * First run.
   */
  private boolean isFirst;

  /**
   * Predicate axis.
   */
  private final Axis predicate;

  /**
   * Constructor. Initializes the internal state.
   *
   * @param nodeCursor exclusive (immutable) cursor to iterate with
   * @param predicate  predicate expression
   */
  public PredicateFilterAxis(final NodeCursor nodeCursor, final Axis predicate) {
    super(nodeCursor);
    isFirst = true;
    this.predicate = requireNonNull(predicate);
  }

  @Override
  public final void reset(final long nodeKey) {
    super.reset(nodeKey);
    if (predicate != null) {
      predicate.reset(nodeKey);
    }
    isFirst = true;
  }

  @Override
  protected long nextKey() {
    // A predicate has to evaluate to true only once.
    if (isFirst) {
      isFirst = false;

      final long currKey = getCursor().getNodeKey();
      predicate.reset(currKey);

      if (predicate.hasNext()) {
        predicate.next();
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
    if (getTrx() instanceof XmlNodeReadOnlyTrx) {
      final XmlNodeReadOnlyTrx rtx = asXmlNodeReadTrx();
      if (rtx.getNodeKey() >= 0) {
        return false;
      } else { // is AtomicValue
        if (rtx.getTypeKey() == rtx.keyForName("xs:boolean")) {
          // atomic value of type boolean
          // return true, if atomic values's value is false
          return !(Boolean.parseBoolean(rtx.getValue()));
        } else {
          return false;
        }
      }
    }

    return false;
  }

}
