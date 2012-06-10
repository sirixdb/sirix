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
package org.sirix.node.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import org.sirix.io.ITTSink;

/**
 * {@link NodeSink} implementation for writing node output.
 * 
 * @author Patrick Lang, University of Konstanz
 * 
 */
public class NodeOutputSink implements ITTSink {

  /**
   * Output stream for node sink.
   */
  private final OutputStream mNodeOutput;

  /**
   * Constructor.
   */
  public NodeOutputSink() {
    mNodeOutput = new ByteArrayOutputStream();
  }

  /**
   * {@inheritDoc}
   * 
   * @throws IOException
   */
  @Override
  public void writeLong(final long pLongVal) {
    try {
      mNodeOutput.write(longToByteArray(pLongVal));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeInt(final int pIntVal) {
    try {
      mNodeOutput.write(intToByteArray(pIntVal));
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void writeByte(byte pByteVal) {
    try {
      mNodeOutput.write(pByteVal);
    } catch (final IOException e) {
      new RuntimeException(e);
    }
  }

  /**
   * Converting an integer value to byte array.
   * 
   * @param mValue
   *          Integer value to convert.
   * @return Byte array of integer value.
   */
  private byte[] intToByteArray(final int pIntVal) {
    final byte[] buffer = new byte[4];

    buffer[0] = (byte)(0xff & (pIntVal >>> 24));
    buffer[1] = (byte)(0xff & (pIntVal >>> 16));
    buffer[2] = (byte)(0xff & (pIntVal >>> 8));
    buffer[3] = (byte)(0xff & pIntVal);

    return buffer;
  }

  /**
   * Converting a Long value to byte array.
   * 
   * @param pValue
   *          long value to convert
   * @return byte array of long value
   */
  private byte[] longToByteArray(final long pValue) {
    final byte[] buffer = new byte[8];

    buffer[0] = (byte)(0xff & (pValue >> 56));
    buffer[1] = (byte)(0xff & (pValue >> 48));
    buffer[2] = (byte)(0xff & (pValue >> 40));
    buffer[3] = (byte)(0xff & (pValue >> 32));
    buffer[4] = (byte)(0xff & (pValue >> 24));
    buffer[5] = (byte)(0xff & (pValue >> 16));
    buffer[6] = (byte)(0xff & (pValue >> 8));
    buffer[7] = (byte)(0xff & pValue);

    return buffer;
  }

  public OutputStream getOutputStream() {
    return mNodeOutput;
  }
}
