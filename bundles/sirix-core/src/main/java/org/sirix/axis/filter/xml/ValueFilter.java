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

package org.sirix.axis.filter.xml;

import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.axis.filter.AbstractFilter;
import org.sirix.node.NodeKind;
import org.sirix.utils.TypedValue;

import static java.util.Objects.requireNonNull;

/**
 * <p>
 * Only match nodes of kind TEXT or ATTRIBUTE whoe's value matches.
 * </p>
 */
public final class ValueFilter extends AbstractFilter<XmlNodeReadOnlyTrx> {

  /** Value test to do. */
  private final byte[] mValue;

  /**
   * Constructor initializing internal state.
   *
   * @param rtx transaction this filter is bound to
   * @param value value to find
   */
  public ValueFilter(final XmlNodeReadOnlyTrx rtx, final byte[] value) {
    super(rtx);
    mValue = requireNonNull(value);
  }

  /**
   * Constructor initializing internal state.
   *
   * @param rtx Transaction to bind filter to.
   * @param value Value to find.
   */
  public ValueFilter(final XmlNodeReadOnlyTrx rtx, final String value) {
    this(rtx, TypedValue.getBytes(value));
  }

  /**
   * Constructor initializing internal state.
   *
   * @param rtx Transaction to bind filter to.
   * @param value Value to find.
   */
  public ValueFilter(final XmlNodeReadOnlyTrx rtx, final int value) {
    this(rtx, TypedValue.getBytes(value));
  }

  /**
   * Constructor initializing internal state.
   *
   * @param rtx Transaction to bind filter to.
   * @param mValue Value to find.
   */
  public ValueFilter(final XmlNodeReadOnlyTrx rtx, final long mValue) {
    this(rtx, TypedValue.getBytes(mValue));
  }

  @Override
  public final boolean filter() {
    return (getTrx().getKind() == NodeKind.TEXT || getTrx().getKind() == NodeKind.ATTRIBUTE)
        && (TypedValue.equals(getTrx().getValue(), mValue));
  }

}
