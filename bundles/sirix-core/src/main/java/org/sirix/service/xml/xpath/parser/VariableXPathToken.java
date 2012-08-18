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

package org.sirix.service.xml.xpath.parser;

/**
 * <h1>XPathToken</h1>
 * <p>
 * Categorized block of text.
 * </p>
 */
public final class VariableXPathToken implements IXPathToken {

  /**
   * The content of the token, a text sequence that represents a text, a
   * number, a special character etc.
   */
  private final String mContent;

  /** Specifies the type that the content of the token has. */
  private final TokenType mType;

  /**
   * Constructor initializing internal state.
   * 
   * @param mStr
   *          the content of the token
   * @param mStorage
   *          the type of the token
   */
  public VariableXPathToken(final String paramStr, final TokenType paramType) {
    mContent = paramStr;
    mType = paramType;
  }

  /**
   * Gets the content of the token.
   * 
   * @return the content
   */
  @Override
  public String getContent() {
    return mContent;
  }

  /**
   * Gets the type of the token.
   * 
   * @return the type
   */
  @Override
  public TokenType getType() {
    return mType;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return new StringBuilder(mType.toString()).append(":").append(mContent).toString();
  }

}
