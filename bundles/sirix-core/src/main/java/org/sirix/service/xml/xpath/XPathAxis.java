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

package org.sirix.service.xml.xpath;

import static com.google.common.base.Preconditions.checkNotNull;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.parser.XPathParser;
import org.sirix.settings.Fixed;

/**
 * <p>
 * Evaluates a given XPath query.
 * </p>
 * <p>
 * Axis to iterate over the items (more precisely the item keys) of the query's result sequence.
 * <code>XPathAxis</code> extends sirixs <code>IAxis</code> that extends the well-known Java
 * <code>Iterator&lt;Long&gt;</code> and <code>Iterable&lt;Long&gt;</code> interfaces.
 * </p>
 * <h2>User Example</h2>
 * <p>
 * In order to use it, at first a sirix session has to be bound to the XML document in question or
 * an tnk file and a <code>ReadTransaction</code> with an <code>INodeList</code> as argument has to
 * be started on it. (For more information how to do that, see the sirix documentation.) Then the
 * <code>XPathAxis</code> can be used like this:
 * </p>
 * <pre>
 *   ...
 *   IReadTransaction rtx = session.beginReadTransaction(new ItemList());
 *
 *   final String query =
 *   &quot;for $a in /articles/article[@name = \&quot;book\&quot;] return $a/price&quot;;
 *
 *   final IAxis axis = new XPathAxis(rtx, query);
 *   while (axis.hasNext()) {
 *     // Move transaction cursor to do something.
 *     axis.next();
 *     System.out.println(rtx.getValueAsInt()););
 *   }
 *   ...
 * </pre>
 *
 * <pre>
 *   ...
 *   for (final long key : new XPathAxis(rtx, query)) {
 *      ...
 *   }
 *   ...
 * </pre>
 *
 *
 */
public final class XPathAxis extends AbstractAxis {

  /** Declares if the evaluation is compatible to XPath 1.0 or not. */
  public static final boolean XPATH_10_COMP = true;

  /** Axis holding the consecutive query execution plans of the query. */
  private Axis mPipeline;

  /**
   * <p>
   * Constructor initializing internal state.
   * </p>
   * <p>
   * Starts the query scanning and parsing and retrieves the builded query execution plan from the
   * parser.
   * </p>
   * <p>
   * <strong>Deprecated: Use the the brackit-binding.</strong>
   * </P>
   *
   * @param pRtx Transaction to operate with.
   * @param pQuery XPath query to process.
   * @throws SirixXPathException throw a sirix xpath exception.
   */
  @Deprecated
  public XPathAxis(final XmlNodeReadOnlyTrx pRtx, final String pQuery) throws SirixXPathException {
    super(pRtx);

    // /** Initializing executor service with fixed thread pool. */
    // EXECUTOR = Executors.newFixedThreadPool(THREADPOOLSIZE);

    // start parsing and get execution plans
    final XPathParser parser = new XPathParser(pRtx, checkNotNull(pQuery));
    parser.parseQuery();
    mPipeline = parser.getQueryPipeline();
  }

  @Override
  protected long nextKey() {
    if (mPipeline.hasNext()) {
      return mPipeline.next();
    } else {
      return Fixed.NULL_NODE_KEY.getStandardProperty();
    }
  }

  // @Override
  // public boolean hasNext() {
  // resetToLastKey();
  //
  // if (mPipeline.hasNext()) {
  // mKey = mPipeline.next();
  // return true;
  // } else {
  // resetToStartKey();
  // return false;
  // }
  // }
}
