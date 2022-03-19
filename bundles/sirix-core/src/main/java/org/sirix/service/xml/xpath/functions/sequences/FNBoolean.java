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
import org.sirix.service.xml.xpath.EXPathError;
import org.sirix.service.xml.xpath.functions.AbstractFunction;
import org.sirix.service.xml.xpath.types.Type;
import org.sirix.utils.TypedValue;

/**
 * <p>
 * IAxis that represents the function fn:boolean specified in
 * <a href="http://www.w3.org/TR/xquery-operators/"> XQuery 1.0 and XPath 2.0 Functions and
 * Operators</a>.
 * </p>
 * <p>
 * The function returns the effective boolean value of given arguments.
 * </p>
 */
public class FNBoolean extends AbstractFunction {

  /**
   * Constructor. Initializes internal state and do a statical analysis concerning the function's
   * arguments.
   * 
   * @param rtx Transaction to operate on
   * @param args List of function arguments
   * @param min min number of allowed function arguments
   * @param max max number of allowed function arguments
   * @param returnType the type that the function's result will have
   * @throws SirixXPathException if function check fails
   */
  public FNBoolean(final XmlNodeReadOnlyTrx rtx, final List<Axis> args, final int min, final int max,
      final int returnType) throws SirixXPathException {

    super(rtx, args, min, max, returnType);
  }

  /**
   * {@inheritDoc}
   * 
   */
  @Override
  protected byte[] computeResult() throws SirixXPathException {

    final Axis axis = getArgs().get(0);
    boolean value = false;

    if (axis.hasNext()) {
      key = axis.next();

      final XmlNodeReadOnlyTrx rtx = axis.asXdmNodeReadTrx();

      if (rtx.getNodeKey() >= 0) { // first item is a real node
                                   // ->
        // true
        value = true;
      } else {

        final Type type = Type.getType(rtx.getTypeKey());

        if (type.derivesFrom(Type.BOOLEAN)) {
          value = Boolean.parseBoolean(rtx.getValue());
          // value = TypedValue.parseBoolean(rtx.getRawValue());
        } else if (type.derivesFrom(Type.STRING) || type.derivesFrom(Type.ANY_URI)
            || type.derivesFrom(Type.UNTYPED_ATOMIC)) {
          // if length = 0 -> false
          value = (rtx.getValue().length() > 0);
        } else if (type.isNumericType()) {
          final double dValue = Double.parseDouble(rtx.getValue());
          value = !(Double.isNaN(dValue) || dValue == 0.0d);
        } else {
          // for all other types throw error FORG0006
          throw EXPathError.FORG0006.getEncapsulatedException();
        }

        // if is not a singleton
        if (axis.hasNext()) {
          throw EXPathError.FORG0006.getEncapsulatedException();
        }
      }

    } else {
      // expression is an empty sequence -> false
      value = false;
    }

    return TypedValue.getBytes(Boolean.toString(value));

  }
}
