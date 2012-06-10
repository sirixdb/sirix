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

package org.treetank.exception;

/**
 * Exception to hold all relevant failures upcoming from Treetank.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public abstract class AbsTTException extends Exception {

  /** General ID. */
  private static final long serialVersionUID = 1L;

  /**
   * Constructor to encapsulate parsing.
   * 
   * @param mExc
   *          to encapsulate
   */
  public AbsTTException(final Exception mExc) {
    super(mExc);
  }

  /**
   * Constructor.
   * 
   * @param message
   *          , convinience for super-constructor
   */
  private AbsTTException(final StringBuilder message) {
    super(message.toString());
  }

  /**
   * Constructor.
   * 
   * @param message
   *          message as string, they are concatenated with spaces in
   *          between
   */
  public AbsTTException(final String... message) {
    this(concat(message));
  }

  /**
   * Util method to provide StringBuilder functionality.
   * 
   * @param message
   *          to be concatenated
   * @return the StringBuilder for the combined string
   */
  public static StringBuilder concat(final String... message) {
    final StringBuilder builder = new StringBuilder();
    for (final String mess : message) {
      builder.append(mess);
      builder.append(" ");
    }
    return builder;
  }

}
