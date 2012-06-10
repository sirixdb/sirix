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
package org.treetank.node.io;

import java.io.ByteArrayInputStream;

import org.treetank.io.ITTSource;

/**
 * {@link NodeSource} implementation for reading node input.
 * 
 * @author Patrick Lang, University of Konstanz
 * 
 */
public class NodeInputSource implements ITTSource {

  /**
   * Input stream for node source.
   */
  private final ByteArrayInputStream mNodeInput;

  /**
   * Offset for byte operations.
   */
  private int mOffset;

  /**
   * Constructor.
   */
  public NodeInputSource(final byte[] mByteStream) {
    mNodeInput = new ByteArrayInputStream(mByteStream);
    mOffset = 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long readLong() {
    final byte[] mLongBytes = new byte[8];

    final int mStatus = mNodeInput.read(mLongBytes, mOffset, 8);

    if (mStatus < 0) {
      throw new IndexOutOfBoundsException();
    }

    return byteArrayToLong(mLongBytes);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public byte readByte() {

    final int mNextByte = mNodeInput.read();

    if (mNextByte < 0) {
      throw new IndexOutOfBoundsException();
    }

    return (byte)mNextByte;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int readInt() {
    final byte[] mIntBytes = new byte[4];

    final int mStatus = mNodeInput.read(mIntBytes, mOffset, 4);

    if (mStatus < 0) {
      throw new IndexOutOfBoundsException();
    }

    return byteArrayToInt(mIntBytes);
  }

  /**
   * Converting a byte array to integer.
   * 
   * @param mByteArray
   *          Byte array to convert.
   * @return converted integer value.
   */
  private int byteArrayToInt(final byte[] mByteArray) {
    final int mConvInt =
      ((mByteArray[0] & 0xff) << 24) | ((mByteArray[1] & 0xff) << 16) | ((mByteArray[2] & 0xff) << 8)
        | (mByteArray[3] & 0xff);

    return mConvInt;
  }

  /**
   * Converting a byte array to long.
   * 
   * @param mByteArray
   *          Byte array to convert.
   * @return converted long value.
   */
  private long byteArrayToLong(final byte[] mByteArray) {
    final long mConvLong =
      ((long)(mByteArray[0] & 0xff) << 56) | ((long)(mByteArray[1] & 0xff) << 48)
        | ((long)(mByteArray[2] & 0xff) << 40) | ((long)(mByteArray[3] & 0xff) << 32)
        | ((long)(mByteArray[4] & 0xff) << 24) | ((long)(mByteArray[5] & 0xff) << 16)
        | ((long)(mByteArray[6] & 0xff) << 8) | (mByteArray[7] & 0xff);

    return mConvLong;
  }

}
