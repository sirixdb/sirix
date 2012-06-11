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

package org.treetank.saxon.evaluator;

import java.io.OutputStream;
import java.util.concurrent.Callable;

import net.sf.saxon.Configuration;
import net.sf.saxon.om.NodeInfo;
import net.sf.saxon.s9api.Processor;
import net.sf.saxon.s9api.SaxonApiException;
import net.sf.saxon.s9api.Serializer;
import net.sf.saxon.s9api.XQueryCompiler;
import net.sf.saxon.s9api.XQueryExecutable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.treetank.api.IDatabase;
import org.treetank.api.ISession;
import org.treetank.saxon.wrapper.DocumentWrapper;
import org.treetank.saxon.wrapper.NodeWrapper;

/**
 * <h1>XQuery evaluator</h1>
 * 
 * <p>
 * Evaluates an XQuery expression against a Treetank storage. Output is available through an output stream.
 * </p>
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public final class XQueryEvaluatorOutputStream implements Callable<Void> {
  /**
   * Log wrapper for better output.
   */
  private static final Logger LOGGER = LoggerFactory.getLogger(XQueryEvaluatorOutputStream.class);

  /** XQuery expression. */
  private final transient String mExpression;

  /** Treetank database. */
  private final transient ISession mSession;

  /** Output Stream. */
  private final transient OutputStream mOut;

  /** Serializer to specify serialization output properties. */
  private transient Serializer mSerializer;

  /**
   * Constructor.
   * 
   * @param paramExpression
   *          XQuery expression.
   * @param paramSession
   *          Treetank database.
   * @param paramOut
   *          Output Stream.
   */
  public XQueryEvaluatorOutputStream(final String paramExpression, final ISession paramSession,
    final OutputStream paramOut) {
    this(paramExpression, paramSession, paramOut, null);
  }

  /**
   * Constructor.
   * 
   * @param paramExpression
   *          XQuery expression.
   * @param paramSession
   *          Treetank database
   * @param paramOut
   *          Output Stream.
   * @param paramSerializer
   *          Serializer, for which one can specify output properties
   */
  public XQueryEvaluatorOutputStream(final String paramExpression, final ISession paramSession,
    final OutputStream paramOut, final Serializer paramSerializer) {
    mExpression = paramExpression;
    mSession = paramSession;
    mOut = paramOut;
    mSerializer = paramSerializer;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Void call() throws Exception {
    try {
      final Processor proc = new Processor(false);
      final Configuration config = proc.getUnderlyingConfiguration();
      final NodeInfo doc = new DocumentWrapper(mSession, config);
      final XQueryCompiler comp = proc.newXQueryCompiler();
      final XQueryExecutable exp = comp.compile(mExpression);

      if (mSerializer == null) {
        final Serializer out = new Serializer();
        out.setOutputProperty(Serializer.Property.METHOD, "xml");
        out.setOutputProperty(Serializer.Property.OMIT_XML_DECLARATION, "yes");
        out.setOutputStream(mOut);
        mSerializer = out;
      }

      final net.sf.saxon.s9api.XQueryEvaluator exe = exp.load();
      exe.setSource(doc);
      exe.run(mSerializer);
      return null;
    } catch (final SaxonApiException e) {
      LOGGER.error("Saxon Exception: " + e.getMessage(), e);
      throw e;
    }
  }
}
