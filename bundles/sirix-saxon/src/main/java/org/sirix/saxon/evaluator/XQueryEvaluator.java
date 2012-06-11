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

package org.sirix.saxon.evaluator;

import java.util.concurrent.Callable;

import net.sf.saxon.Configuration;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.XQueryCompiler;
import net.sf.saxon.s9api.XQueryExecutable;
import net.sf.saxon.s9api.XdmValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sirix.api.ISession;
import org.sirix.saxon.wrapper.DocumentWrapper;
import org.sirix.saxon.wrapper.NodeWrapper;

/**
 * <h1>XQuery evaluator</h1>
 * 
 * <p>
 * Evaluates an XQuery expression against a sirix storage and returns an XdmValue instance, which
 * corresponds to zero or more XdmItems.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class XQueryEvaluator implements Callable<XdmValue> {

  /**
   * Log wrapper for better output.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(XQueryEvaluator.class);

  /** XQuery expression. */
  private final transient String mExpression;

  /** sirix session. */
  private final transient ISession mSession;

  /**
   * Constructor.
   * 
   * @param paramExpression
   *          XQuery expression.
   * @param paramSession
   *          sirix database.
   */
  public XQueryEvaluator(final String paramExpression, final ISession paramSession) {
    mExpression = paramExpression;
    mSession = paramSession;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public XdmValue call() throws Exception {
    XdmValue value = null;

    try {
      final Processor proc = new Processor(false);
      final Configuration config = proc.getUnderlyingConfiguration();
      final NodeInfo doc = new DocumentWrapper(mSession, config);
      final XQueryCompiler comp = proc.newXQueryCompiler();
      final XQueryExecutable exp = comp.compile(mExpression);
      final net.sf.saxon.s9api.XQueryEvaluator exe = exp.load();
      exe.setSource(doc);
      value = exe.evaluate();
    } catch (final SaxonApiException e) {
      LOGGER.error("Saxon Exception: " + e.getMessage(), e);
      throw e;
    }

    return value;
  }
}
