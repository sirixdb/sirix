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

package org.sirix.service.xml.xpath.functions.sequences;

import java.util.List;
import org.sirix.api.Axis;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.exception.SirixXPathException;
import org.sirix.service.xml.xpath.functions.AbstractFunction;
import org.sirix.utils.TypedValue;

/**
 * <p>
 * IAxis that represents the function fn:count specified in
 * <a href="http://www.w3.org/TR/xquery-operators/"> XQuery 1.0 and XPath 2.0 Functions and
 * Operators</a>.
 * </p>
 * <p>
 * The function returns the number of given arguments.
 * </p>
 */
public class FNCount extends AbstractFunction {

  /**
   * Constructor.
   * 
   * Initializes internal state and do a statical analysis concerning the function's arguments.
   * 
   * @param rtx Transaction to operate on
   * @param args List of function arguments
   * @param min min number of allowed function arguments
   * @param max max number of allowed function arguments
   * @param returnType the type that the function's result will have
   * @throws SirixXPathException if function check fails
   */
  public FNCount(final XmlNodeReadOnlyTrx rtx, final List<Axis> args, final int min, final int max,
      final int returnType) throws SirixXPathException {

    super(rtx, args, min, max, returnType);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected byte[] computeResult() {

    final Axis axis = getArgs().get(0);

    Integer count = 0;
    while (axis.hasNext()) {
      axis.next();
      count++;
    }

    return TypedValue.getBytes(count.toString());

  }

}
