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

package org.sirix.api;

/**
 * <h1>Filter</h1>
 *
 * <h2>Description</h2>
 *
 * <p>
 * Filter the node currently selected by the provided transaction.
 * </p>
 *
 * <h2>Convention</h2>
 *
 * <p>
 * <ol>
 * <li><strong>Precondition</strong> before each call to <code>Filter.filter()</code>:
 * <code>NodeReadTrx.getNodeKey() == n</code>.</li>
 * <li><strong>Postcondition</strong> after each call to <code>Filter.filter()</code>:
 * <code>NodeReadTrx.getNodeKey() == n</code>.</li>
 * </ol>
 * </p>
 *
 * <h2>User Example</h2>
 *
 * <p>
 *
 * <pre>
 * // hasNext() yields true, iff rtx selects an element with local part &quot;foo&quot;.
 * new FilterAxis(new SelfAxis(rtx), new NameFilter(rtx, &quot;foo&quot;));
 * </pre>
 *
 * </p>
 *
 * <h2>Developer Example</h2>
 *
 * <p>
 *
 * <pre>
 * // Must extend &lt;code&gt;AbstractFilter&lt;/code&gt; and implement &lt;code&gt;Filter&lt;/code&gt;.
 * public final class ExampleFilter extends AbstractFilter {
 *
 *   public ExampleFilter(final INodeReadTrx rtx) {
 *     // Must be called as first.
 *     super(rtx);
 *   }
 *
 *   public final boolean filter() {
 *     // Do not move cursor.
 *     return (getTrx().isStructuralNode());
 *   }
 * }
 * </pre>
 *
 * </p>
 */
public interface Filter<R> {

  /**
   * Apply filter on current node of transaction.
   *
   * @return {@code true} if node passes filter, {@code false} otherwise
   */
  boolean filter();

  /**
   * Getting the transaction of this filter.
   *
   * @return the transaction of this filter
   */
  R getTrx();

  /**
   * Setting the transaction of this filter.
   *
   * @return the transaction of this filter
   */
  void setTrx(R rtx);

}
