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

import org.sirix.api.INodeReadTrx;
import org.sirix.node.ENode;
import org.sirix.utils.TypedValue;

/**
 * <h1>ValueAxisTest</h1>
 * 
 * <p>
 * Only match nodes of kind TEXT whoms value matches.
 * </p>
 */
public class ValueFilter extends AbsFilter {

  /** Value test to do. */
  private final byte[] mValue;

  /**
   * Constructor initializing internal state.
   * 
   * @param rtx
   *          Transaction to bind filter to.
   * @param mValue
   *          Value to find.
   */
  public ValueFilter(final INodeReadTrx rtx, final byte[] mValue) {
    super(rtx);
    this.mValue = mValue;
  }

  /**
   * Constructor initializing internal state.
   * 
   * @param rtx
   *          Transaction to bind filter to.
   * @param mValue
   *          Value to find.
   */
  public ValueFilter(final INodeReadTrx rtx, final String mValue) {
    this(rtx, TypedValue.getBytes(mValue));
  }

  /**
   * Constructor initializing internal state.
   * 
   * @param rtx
   *          Transaction to bind filter to.
   * @param mValue
   *          Value to find.
   */
  public ValueFilter(final INodeReadTrx rtx, final int mValue) {
    this(rtx, TypedValue.getBytes(mValue));
  }

  /**
   * Constructor initializing internal state.
   * 
   * @param rtx
   *          Transaction to bind filter to.
   * @param mValue
   *          Value to find.
   */
  public ValueFilter(final INodeReadTrx rtx, final long mValue) {
    this(rtx, TypedValue.getBytes(mValue));
  }

  @Override
  public final boolean filter() {
    return (getTransaction().getNode().getKind() == ENode.TEXT_KIND || getTransaction().getNode().getKind() == ENode.ATTRIBUTE_KIND)
      && (TypedValue.equals(getTransaction().getValueOfCurrentNode(), mValue));
  }

}
