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

import org.sirix.exception.SirixXPathException;

public enum EXPathError {

	/** XPath static error 0001. */
	XPST0001(
			"err:XPST0001:  Some component of the static context has not " + "been assigned a value."),

	/** XPath dynamic error 0004. */
	XPDY0002("err:XPDY0002:   Some part of the dynamic context has not " + "been assigned a value."),

	/** XPath static error 0003. */
	XPST0003(
			"err:XPST0003: Expression is not a valid instance of the grammar" + "defined in A.1 EBNF."),

	/** XPath type error 0004. */
	XPTY0004("err:XPTY0004 The type is not appropriate the expression or the "
			+ "typedoes not match a required type as specified by the matching rules."),

	/** XPath static error 0005. */
	XPST0005("err:XPST0005:  The static type assigned to an expression other than the "
			+ "expression() or data(()) is empty-sequence()."),

	/** XPath static error 0008. */
	XPST0008(
			"err:XPST0008 Expression refers to a name that is not defined in the " + "static context"),

	/** XPath static error 0010. */
	XPST0010("err:XPST0010: " + "Axis is not supported."),

	/** XPath static error 0017. */
	XPST0017("err:XPST0017  The expanded QName and number of arguments in a "
			+ "function call do not match the name and arity of a function " + "signature."),

	/** XPath type error 0018. */
	XPTY0018("err:XPTY0018  Result of the last step "
			+ "in a path expression contains both nodes and atomic values."),

	/** XPath type error 0019. */
	XPTY0019("err:XPTY0019  Result of a step (other "
			+ "than the last step) in a path expression contains an atomic value."),

	/** XPath type error 0020. */
	XPTY0020("err:XPTY0020 Context item in an axis step is not a node."),

	/** XPath dynamic error 0050. */
	XPDY0050("err:XPDY0050 " + "Dynamic type of the operand of a treat expression does not match"
			+ " the sequence type specified by the treat expression."),

	/** XPath static error 0051. */
	XPST0051(
			"err:XPST0051 " + "Type is not defined in the in-scope schema types as an " + "atomic type."),

	/** XPath static error 0080. */
	XPST0080("err:XPST0080 " + "Target type of a cast or castable expression must not be "
			+ "xs:NOTATION or xs:anyAtomicType."),

	/** XPath static error 0081. */
	XPST0081("err:XPST0081 " + "Namespace prefix cannot be expanded into a namespace URI by "
			+ "using the statically known namespaces."),

	FOCA0001("err:FOCA0001, Input value too large for decimal."), FOCA0002(
			"err:FOCA0002, Invalid lexical value."), FOCA0003(
					"err:FOCA0003, Input value too large for integer."), FOCA0005(
							"err:FOCA0005, NaN supplied as float/double value."), FOCA0006(
									"err:FOCA0006, String to be cast to decimal has too many digits of precision."), FOCH0001(
											"err:FOCH0001, Code point not valid."), FOCH0002(
													"err:FOCH0002, Unsupported collation."), FOCH0003(
															"err:FOCH0003, Unsupported normalization form."), FORH0004(
																	"err:FOCH0004, Collation does not support collation units."), FODC0001(
																			"err:FODC0001, No context document."), FODC0002(
																					"err:FODC0002, Error retrieving resource."), FODC0003(
																							"err:FODC0003, Function stability not defined."), FODC0004(
																									"err:FODC0004, Invalid argument to fn:collection."), FODC0005(
																											"err:FODC0005, Invalid argument to fn:doc or fn:doc-available."), FODT0001(
																													"err:FODT0001, Overflow/underflow in date/time operation."), FODT0002(
																															"err:FODT0002, Overflow/underflow in duration operation."), FODT0003(
																																	"err:FODT0003, Invalid timezone value."), FONS0004(
																																			"err:FONS0004, No namespace found for prefix."), FONS0005(
																																					"err:FONS0005, Base-uri not defined in the static context."),
	/**
	 * XQuery and XPath Function and Operators error 0001. This error is raised whenever an attempt is
	 * made to divide by zero.
	 */
	FOAR0001("err:FOAR0001: Division by zero."),
	/**
	 * XQuery and XPath Function and Operators error 0002. This error is raised whenever numeric
	 * operations result in an overflow or underflow.
	 */
	FOAR0002("err:FOAR0002: Numeric operation overflow/underflow."), FOER0000(
			"err:FOER0000: Unidentified error."), FORG0001(
					"err:FORG0001, Invalid value for cast/constructor."), FORG0002(
							"err:FORG0002, Invalid argument to fn:resolve-uri()."), FORG0003(
									"err:FORG0003, fn:zero-or-one called with a sequence containing more than one item."), FORG0004(
											"err:FORG0004, fn:one-or-more called with a sequence containing no items."), FORG0005(
													"err:FORG0005, fn:exactly-one called with a sequence containing zero or more than one item."),
	/** XQuery and XPath Function and Operators error 0006. */
	FORG0006("err:FORG0006 Invalid argument type."), FORG0007(""), FORG0008(
			"err:FORG0008, Both arguments to fn:dateTime have a specified timezone."), FORG0009(
					"err:FORG0009, Error in resolving a relative URI against a base URI in fn:resolve-uri."), FORX0001(
							"err:FORX0001, Invalid regular expression. flags"), FORX0002(
									"err:FORX0002, Invalid regular expression."), FORX0003(
											"err:FORX0003, Regular expression matches zero-length string."), FORX0004(
													"err:FORX0004, Invalid replacement string."), FOTY0012(
															"err:FOTY0012, Argument node does not have a typed value."),;

	/** error message. */
	private final String mMessage;

	/**
	 * Encapsulated Exception for the specified Enum.
	 */
	private final SirixXPathException mException;

	/**
	 * Constructor. Initializes the internal state.
	 * 
	 * @param msg the error message
	 */
	private EXPathError(final String msg) {
		mMessage = msg;
		mException = new SirixXPathException(mMessage);
	}

	/**
	 * Returns the error message of the respective error.
	 * 
	 * @return error message
	 */
	public String getMsg() {
		return mMessage;
	}

	/**
	 * Getting the specific exception for a type.
	 * 
	 * @return {@link SirixXPathException} encapsulated
	 */
	public SirixXPathException getEncapsulatedException() {
		return mException;
	}

}
