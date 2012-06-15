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

package org.sirix.io.file;

import java.nio.ByteBuffer;

import org.sirix.io.ITTSink;
import org.sirix.io.ITTSource;
import org.sirix.utils.IConstants;

/**
 * This class represents the byte input/output mechanism for File-access. After
 * all, it is just a simple wrapper for the ByteBuffer and exists only for
 * convenience reasons.
 * 
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class ByteBufferSinkAndSource implements ITTSink, ITTSource {

  /** Internal buffer. */
  private ByteBuffer mBuffer;

  /**
   * Constructor.
   */
  public ByteBufferSinkAndSource() {
    mBuffer = ByteBuffer.allocate(IConstants.BUFFER_SIZE);
  }

  @Override
  public void writeByte(final byte pByteVal) {
    checkAndIncrease(1);
    mBuffer.put(pByteVal);
  }

  @Override
  public void writeLong(final long pLongVal) {
    checkAndIncrease(8);
    mBuffer.putLong(pLongVal);
  }

  /**
   * Setting position in buffer.
   * 
   * @param pVal
   *          new position to set
   */
  public void position(final int pVal) {
    mBuffer.position(pVal);
  }

  /**
   * Getting position in buffer.
   * 
   * @return position to get
   */
  public int position() {
    return mBuffer.position();
  }

  /**
   * Getting more bytes and fill it in the buffer.
   * 
   * @param pDst
   *          to fill
   * @param pOffset
   *          offset in buffer
   * @param pLength
   *          length of bytes
   */
  public void get(final byte[] pDst, final int pOffset, final int pLength) {
    mBuffer.get(pDst, pOffset, pLength);
  }

  @Override
  public byte readByte() {
    return mBuffer.get();
  }

  @Override
  public long readLong() {
    return mBuffer.getLong();
  }

  @Override
  public void writeInt(final int pIntVal) {
    checkAndIncrease(4);
    mBuffer.putInt(pIntVal);
  }

  @Override
  public int readInt() {
    return mBuffer.getInt();
  }

  /**
   * Checking of length is sufficient, if not, increase the bytebuffer.
   * 
   * @param pLength
   *          for the bytes which have to be inserted
   */
  private void checkAndIncrease(final int pLength) {
    final int position = mBuffer.position();
    if (mBuffer.position() + pLength >= mBuffer.capacity()) {
      mBuffer.position(0);
      final ByteBuffer newBuffer = ByteBuffer.allocate(mBuffer.capacity() + IConstants.BUFFER_SIZE);
      newBuffer.put(mBuffer);
      mBuffer = newBuffer;
      mBuffer.position(position);
    }
  }
}
