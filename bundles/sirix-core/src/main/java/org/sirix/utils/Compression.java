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
package org.sirix.utils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import javax.annotation.Nonnull;

import org.sirix.io.file.FileStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Compression/Decompression for text values.
 * 
 * @author Johannes Lichtenberger, University of Konstanz
 * 
 */
public class Compression {

  /** Logger. */
  private static final Logger LOGWRAPPER = LoggerFactory.getLogger(Compression.class);

  /** Compressor. */
  private static final Deflater mCompressor = new Deflater();

  /** Decompressor. */
  private static final Inflater mDecompressor = new Inflater();

  /**
   * Compress data based on the {@link Deflater}.
   * 
   * @param pToCompress
   *          input byte-array
   * @param pLevel
   *          compression level (between -1 and 9 whereas 0 is the weakest and -1 is default)
   * @return compressed byte-array
   * @throws NullPointerException
   *           if {@code pToCompress} is {@code null}
   */
  public static byte[] compress(@Nonnull final byte[] pToCompress, final int pLevel) {
    checkNotNull(pToCompress);
    checkArgument(pLevel >= -1 && pLevel <= 9, "pLevel must be between 0 and 9!");

    // Compressed result.
    byte[] compressed = new byte[] {};

    // Reset the compressor.
    mCompressor.reset();
    
    // Set compression level.
    mCompressor.setLevel(pLevel);

    // Give the compressor the data to compress.
    mCompressor.setInput(pToCompress);
    mCompressor.finish();

    /*
     * Create an expandable byte array to hold the compressed data.
     * You cannot use an array that's the same size as the orginal because
     * there is no guarantee that the compressed data will be smaller than
     * the uncompressed data.
     */
    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream(pToCompress.length)) {
      // Compress the data.
      final byte[] buf = new byte[FileStorage.BUFFER_SIZE];
      while (!mCompressor.finished()) {
        final int count = mCompressor.deflate(buf);
        bos.write(buf, 0, count);
      }

      // Get the compressed data.
      compressed = bos.toByteArray();
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    } catch (final Error e) {
    	e.printStackTrace();
    }

    return compressed;
  }

  /**
   * Decompress data based on the {@link Inflater}.
   * 
   * @param pCompressed
   *          input string
   * @return compressed byte-array
   * @throws NullPointerException
   *           if {@code pCompressed} is {@code null}
   */
  public static byte[] decompress(@Nonnull final byte[] pCompressed) {
    checkNotNull(pCompressed);

    // Reset the decompressor and give it the data to compress.
    mDecompressor.reset();
    mDecompressor.setInput(pCompressed);

    byte[] decompressed = new byte[] {};

    // Create an expandable byte array to hold the decompressed data.
    try (final ByteArrayOutputStream bos = new ByteArrayOutputStream(pCompressed.length)) {
      // Decompress the data.
      final byte[] buf = new byte[FileStorage.BUFFER_SIZE];
      while (!mDecompressor.finished()) {
        try {
          final int count = mDecompressor.inflate(buf);
          bos.write(buf, 0, count);
        } catch (final DataFormatException e) {
          LOGWRAPPER.error(e.getMessage(), e);
          throw new RuntimeException(e);
        }
      }
      // Get the decompressed data.
      decompressed = bos.toByteArray();
    } catch (final IOException e) {
      LOGWRAPPER.error(e.getMessage(), e);
    }

    return decompressed;
  }
}
