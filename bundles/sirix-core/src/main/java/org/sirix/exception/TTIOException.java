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

package org.sirix.exception;

import java.io.IOException;

import javax.xml.stream.XMLStreamException;

import com.sleepycat.je.DatabaseException;

/**
 * All sirix IO Exception are wrapped in this class. It inherits from
 * IOException since it is a sirix IO Exception.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class TTIOException extends AbsTTException {

  /**
   * serializable id.
   */
  private static final long serialVersionUID = 4099242625448155216L;

  /**
   * Constructor.
   * 
   * @param pExc
   *          exception to be wrapped
   */
  public TTIOException(final XMLStreamException pExc) {
    super(pExc);
  }

  /**
   * Constructor.
   * 
   * @param pExc
   *          exception to be wrapped
   */
  public TTIOException(final IOException pExc) {
    super(pExc);
  }
  
  /**
   * Constructor.
   * 
   * @param pExc
   *          exception to be wrapped
   */
  public TTIOException(final TTByteHandleException pExc) {
    super(pExc);
  }

  /**
   * Constructor.
   * 
   * @param pExc
   *          exception to be wrapped
   */
  public TTIOException(final DatabaseException pExc) {
    super(pExc);
  }

  /**
   * Constructor.
   * 
   * @param message
   *          for the overlaying {@link IOException}
   */
  public TTIOException(final String... message) {
    super(message);
  }

}
