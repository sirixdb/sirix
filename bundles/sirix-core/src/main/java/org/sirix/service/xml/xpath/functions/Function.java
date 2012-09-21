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

package org.sirix.service.xml.xpath.functions;

import java.util.ArrayList;
import java.util.List;

import org.sirix.api.IAxis;
import org.sirix.api.INodeReadTrx;
import org.sirix.axis.AbsAxis;
import org.sirix.exception.SirixXPathException;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.IValNode;
import org.sirix.service.xml.xpath.AtomicValue;
import org.sirix.service.xml.xpath.EXPathError;
import org.sirix.service.xml.xpath.functions.sequences.FNBoolean;
import org.sirix.service.xml.xpath.types.Type;

public class Function {

  public static boolean ebv(final IAxis axis) throws SirixXPathException {
    final FuncDef ebv = FuncDef.BOOLEAN;
    final List<IAxis> param = new ArrayList<IAxis>();
    param.add(axis);
    final AbsAxis bAxis =
      new FNBoolean(axis.getTransaction(), param, ebv.getMin(), ebv.getMax(), axis.getTransaction()
        .keyForName(ebv.getReturnType()));
    if (bAxis.hasNext()) {
      bAxis.next();
      final boolean result = Boolean.parseBoolean(bAxis.getTransaction().getValueOfCurrentNode());
      if (!bAxis.hasNext()) {
        bAxis.reset(axis.getTransaction().getNode().getNodeKey());

        return result;
      }
    }
    throw new IllegalStateException("This should not happen!"); // TODO!!
  }

  public static boolean empty(final INodeReadTrx rtx, final AbsAxis axis) {

    final boolean result = !axis.hasNext();

    final int itemKey = rtx.getItemList().addItem(new AtomicValue(result));
    rtx.moveTo(itemKey);
    return true;
  }

  public static boolean exactlyOne(final INodeReadTrx rtx, final AbsAxis axis) throws SirixXPathException {

    if (axis.hasNext()) {
      if (axis.hasNext()) {
        throw EXPathError.FORG0005.getEncapsulatedException();
      } else {
        final int itemKey = rtx.getItemList().addItem(new AtomicValue(true));
        rtx.moveTo(itemKey); // only once

      }

    } else {
      throw EXPathError.FORG0005.getEncapsulatedException();
    }

    return true;
  }

  public static boolean exists(final INodeReadTrx rtx, final AbsAxis axis) {

    final boolean result = axis.hasNext();
    final int itemKey = rtx.getItemList().addItem(new AtomicValue(result));
    rtx.moveTo(itemKey);
    return true;
  }

  /**
   * <p>
   * The effective boolean value of a value is defined as the result of applying the fn:boolean function to
   * the value, as defined in [XQuery 1.0 and XPath 2.0 Functions and Operators].]
   * </p>
   * <p>
   * <li>If its operand is an empty sequence, fn:boolean returns false.</li>
   * <li>If its operand is a sequence whose first item is a node, fn:boolean returns true.</li>
   * <li>If its operand is a singleton value of type xs:boolean or derived from xs:boolean, fn:boolean returns
   * the value of its operand unchanged.</li>
   * <li>If its operand is a singleton value of type xs:string, xs:anyURI, xs:untypedAtomic, or a type derived
   * from one of these, fn:boolean returns false if the operand value has zero length; otherwise it returns
   * true.</li>
   * <li>If its operand is a singleton value of any numeric type or derived from a numeric type, fn:boolean
   * returns false if the operand value is NaN or is numerically equal to zero; otherwise it returns true.</li>
   * <li>In all other cases, fn:boolean raises a type error [err:FORG0006].</li>
   * </p>
   * 
   * @param rtx
   *          the transaction to operate on.
   * @param axis
   *          Expression to get the effective boolean value for
   * @return true if sucessfull, false otherwise
   * @throws SirixXPathException
   */
  public static boolean fnBoolean(final INodeReadTrx rtx, final AbsAxis axis) throws SirixXPathException {

    final boolean ebv = ebv(axis);
    final int itemKey = rtx.getItemList().addItem(new AtomicValue(ebv));
    rtx.moveTo(itemKey);
    return true;
  }

  /**
   * fn:data takes a sequence of items and returns a sequence of atomic
   * values. The result of fn:data is the sequence of atomic values produced
   * by applying the following rules to each item in the input sequence: If
   * the item is an atomic value, it is returned. If the item is a node, its
   * typed value is returned (err:FOTY0012 is raised if the node has no typed
   * value.)
   * 
   * @param rtx
   *          the transaction to operate on
   * @param axis
   *          The sequence to atomize.
   * @return true, if an atomic value can be returned
   */
  public static boolean fnData(final INodeReadTrx rtx, final AbsAxis axis) {

    if (axis.hasNext()) {

      if (rtx.getNode().getNodeKey() >= 0) {
        // set to typed value
        // if has no typed value
        // TODO // throw new XPathError(FOTY0012);

        final int itemKey =
          rtx.getItemList().addItem(
            new AtomicValue(rtx.getValueOfCurrentNode().getBytes(), rtx.getNode().getTypeKey()));
        rtx.moveTo(itemKey);
        return true;
      } else {
        // return current item -> do nothing
        return true;
      }
    } else {
      // no more items.
      return false;
    }

  }

  /**
   * <p>
   * fn:nilled($arg as node()?) as xs:boolean?
   * </p>
   * <p>
   * Returns an xs:boolean indicating whether the argument node is "nilled". If the argument is not an element
   * node, returns the empty sequence. If the argument is the empty sequence, returns the empty sequence.
   * </p>
   * 
   * @param rtx
   *          the transaction to operate on
   * @param axis
   *          The sequence containing the node to test its nilled property
   * @return true, if current item is a node that has the nilled property
   *         (only elements)
   */
  public static boolean fnNilled(final INodeReadTrx rtx, final AbsAxis axis) {

    if (axis.hasNext() && rtx.getNode().getKind() == EKind.ELEMENT) {
      final boolean nilled = false; // TODO how is the nilled property
                                    // defined?
      final int itemKey = rtx.getItemList().addItem(new AtomicValue(nilled));
      rtx.moveTo(itemKey);
      return true;
    }
    return false; // empty sequence
  }

  /**
   * <p>
   * fn:node-name($arg as node()?) as xs:QName?
   * </p>
   * <p>
   * Returns an expanded-QName for node kinds that can have names. For other kinds of nodes it returns the
   * empty sequence. If $arg is the empty sequence, the empty sequence is returned.
   * <p>
   * 
   * @param rtx
   *          the transaction to operate on
   * @param axis
   *          The sequence, containing the node, to return its QName
   * @return true, if node has a name
   */
  public static boolean fnNodeName(final INodeReadTrx rtx, final AbsAxis axis) {

    if (axis.hasNext()) {

      final String name = rtx.getQNameOfCurrentNode().getLocalPart();
      if (!name.equals("-1")) {
        final int itemKey = rtx.getItemList().addItem(new AtomicValue(name, Type.STRING));
        rtx.moveTo(itemKey);
        return true;
      }
    }
    // node has no name or axis is empty sequence
    // TODO: check if -1 really is the null-name-key
    return false;

  }

  public static boolean fnnot(final INodeReadTrx rtx, final AbsAxis axis) {
    if (axis.hasNext()) {
      axis.next();
      final AtomicValue item = new AtomicValue(((IValNode)rtx.getNode()).getRawValue()[0] == 0);
      final int itemKey = rtx.getItemList().addItem(item);
      rtx.moveTo(itemKey);
      return true;
    } else {
      return false;
    }

  }

  /**
   * Returns the value of the context item after atomization converted to an
   * xs:double.
   * 
   * @param rtx
   *          Read Transaction.
   * @return fnnumber boolean.
   */
  public static boolean fnnumber(final INodeReadTrx rtx) {

    // TODO: add error handling
    final AtomicValue item = new AtomicValue(rtx.getValueOfCurrentNode().getBytes(), rtx.keyForName("xs:double"));
    final int itemKey = rtx.getItemList().addItem(item);
    rtx.moveTo(itemKey);

    return true;
  }

  public static AtomicValue not(final AtomicValue mValue) {

    return new AtomicValue(!Boolean.parseBoolean(new String(mValue.getRawValue())));
  }

  public static boolean oneOrMore(final INodeReadTrx rtx, final AbsAxis axis) throws SirixXPathException {

    if (!axis.hasNext()) {
      throw EXPathError.FORG0004.getEncapsulatedException();
    } else {
      final int itemKey = rtx.getItemList().addItem(new AtomicValue(true));
      rtx.moveTo(itemKey);

    }

    return true;
  }

  public static boolean sum(final INodeReadTrx rtx, final AbsAxis axis) {

    Double value = 0.0;
    while (axis.hasNext()) {
      value = value + Double.parseDouble(rtx.getValueOfCurrentNode());
    }

    final int itemKey = rtx.getItemList().addItem(new AtomicValue(value, Type.DOUBLE));
    rtx.moveTo(itemKey);
    return true;
  }

  public static boolean sum(final INodeReadTrx rtx, final AbsAxis axis, final AbsAxis mZero) {

    Double value = 0.0;
    if (!axis.hasNext()) {
      mZero.hasNext(); // if is empty sequence, return values specified
                       // for
      // zero
    } else {
      do {
        value = value + Double.parseDouble(rtx.getValueOfCurrentNode());
      } while (axis.hasNext());
      final int itemKey = rtx.getItemList().addItem(new AtomicValue(value, Type.DOUBLE));
      rtx.moveTo(itemKey);
    }
    return true;
  }

  public static boolean zeroOrOne(final INodeReadTrx rtx, final AbsAxis axis) throws SirixXPathException {

    final boolean result = true;

    if (axis.hasNext() && axis.hasNext()) { // more than one result
      throw EXPathError.FORG0003.getEncapsulatedException();
    }

    final int itemKey = rtx.getItemList().addItem(new AtomicValue(result));
    rtx.moveTo(itemKey);
    return true;
  }

}
