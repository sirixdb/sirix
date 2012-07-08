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

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.sirix.diff.algorithm.fmse.FMSE;
import org.sirix.utils.LogWrapper;
import org.slf4j.LoggerFactory;

public class CryptoJavaImpl {

  /** Logger. */
  private static final LogWrapper LOGWRAPPER = new LogWrapper(LoggerFactory
    .getLogger(FMSE.class));

  /** {@link Deflater} reference. */
  private final Deflater mCompressor;

  /** {@link Inflater} reference. */
  private final Inflater mDecompressor;

  /** Temp data. */
  private final byte[] mTmp;

  /** {@link ByteArrayOutputStream} reference. */
  private final ByteArrayOutputStream mOut;

  /**
   * Initialize compressor.
   */
  public CryptoJavaImpl() {
    mCompressor = new Deflater();
    mDecompressor = new Inflater();
    mTmp = new byte[FileFactory.BUFFER_SIZE];
    mOut = new ByteArrayOutputStream();
  }

  /**
   * Compress data.
   * 
   * @param pLength
   *          of the data to be compressed
   * @param pBuffer
   *          data that should be compressed
   * @return compressed data
   */
  public int crypt(final int pLength, final ByteBufferSinkAndSource pBuffer) {
    pBuffer.position(FileReader.OTHER_BEACON);
    final byte[] tmp = new byte[pLength - FileReader.OTHER_BEACON];
    pBuffer.readBytes(tmp, 0, tmp.length);
    mCompressor.reset();
    mOut.reset();
    mCompressor.setInput(tmp);
    mCompressor.finish();
    int count;
    while (!mCompressor.finished()) {
      count = mCompressor.deflate(mTmp);
      mOut.write(mTmp, 0, count);
    }
    final byte[] result = mOut.toByteArray();
    pBuffer.position(FileReader.OTHER_BEACON);
    pBuffer.writeBytes(result);
    return pBuffer.position();
  }

  /**
   * Decompress data.
   * 
   * @param buffer
   *          data that should be decompressed
   * @param pLength
   *          of the data to be decompressed
   * @return decompressed data
   */
  public int decrypt(final int pLength, final ByteBufferSinkAndSource buffer) {
    buffer.position(FileReader.OTHER_BEACON);
    final byte[] tmp = new byte[pLength - FileReader.OTHER_BEACON];
    buffer.readBytes(tmp, 0, tmp.length);
    mDecompressor.reset();
    mOut.reset();
    mDecompressor.setInput(tmp);
    int count;
    try {
      while (!mDecompressor.finished()) {
        count = mDecompressor.inflate(mTmp);
        mOut.write(mTmp, 0, count);
      }
    } catch (final DataFormatException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }
    final byte[] result = mOut.toByteArray();
    buffer.position(FileReader.OTHER_BEACON);
    buffer.writeBytes(result);
    return buffer.position();
  }

}
