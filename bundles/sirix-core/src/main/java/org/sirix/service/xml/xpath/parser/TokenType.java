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

package org.sirix.service.xml.xpath.parser;

public enum TokenType {
	/** Invalid type. */
	INVALID(""),
	/** Text type. */
	TEXT(""),
	/** Name type. */
	NAME(""),
	/** Value type. */
	VALUE(""),
	/** Token type that represents a '/' . */
	SLASH("/"),
	/** Token type that represents a descendant step. */
	DESC_STEP("//"),
	/** Token type that represents a left parenthesis. */
	OPEN_BR("("),
	/** Token type that represents a right parenthesis. */
	CLOSE_BR(")"),
	/** Token type that represents a comparison. */
	COMP("="),
	/** Token type that represents an equality comparison. */
	EQ("="),
	/** Token type that represents a diversity comparison. */
	N_EQ("!="),
	/** Token type that represents an opening squared bracket. */
	OPEN_SQP("["),
	/** Token type that represents a closing squared bracket. */
	CLOSE_SQP("]"),
	/** Token type that represents the @ symbol. */
	AT("@"),
	/** Token type that represents the point. */
	POINT("."),
	/** Token type that represents a colon : . */
	COLON(":"),
	/** Token type that represents a normal quote : " . */
	DBL_QUOTE("\'"),
	/** Token type that represents a single quote : ' . */
	SINGLE_QUOTE("'"),
	/** Token type that represents the dollar sign : $ . */
	DOLLAR("$"),
	/** Token type that represents a plus. */
	PLUS("+"),
	/** Token type that represents a minus. */
	MINUS("-"),
	/** Token type that represents a interrogation mark: ? */
	INTERROGATION("?"),
	/** Token type that represents a star. */
	STAR("*"),
	/** Token type that represents a left shift: << . */
	L_SHIFT("<<"),
	/** Token type that represents a right shift: >> . */
	R_SHIFT(">>"),
	/** Token type that represents a shortcut for the parent: .. . */
	PARENT(".."),
	/** Token type that represents a comma. . */
	COMMA(","),
	/** Token type that represents the or sign: | . */
	OR("|"),
	/** Token type that represents a comment (: ...... :) */
	COMMENT(""),
	/** Token type that represents a whitespace. */
	SPACE(""),
	/**
	 * Token type that represents an 'E' or an 'e' that is part of a double value.
	 */
	E_NUMBER(""),
	/** Token type for the end of the string to parse. */
	END("");

	private final String mContent;

	TokenType(final String paramContent) {
		mContent = paramContent;
	}
}
