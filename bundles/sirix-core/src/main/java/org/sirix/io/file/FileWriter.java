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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import javax.annotation.Nonnull;

import org.sirix.exception.TTIOException;
import org.sirix.io.IWriter;
import org.sirix.page.PagePersistenter;
import org.sirix.page.PageReference;
import org.sirix.page.interfaces.IPage;

/**
 * File Writer for providing read/write access for file as a sirix backend.
 * 
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * 
 */
public final class FileWriter implements IWriter {

  /** Random access mFile to work on. */
  private final RandomAccessFile mFile;

  /** Compressor to compress the page. */
  private final CryptoJavaImpl mCompressor;

  /** Reader instance for this writer. */
  private final FileReader reader;

  /**
   * Constructor.
   * 
   * 
   * @param pStorage
   *          the concrete storage
   * @throws TTIOException
   *           if an I/O error occurs
   */
  public FileWriter(@Nonnull final File pStorage) throws TTIOException {
    try {
      mFile = new RandomAccessFile(pStorage, "rw");
    } catch (final FileNotFoundException fileExc) {
      throw new TTIOException(fileExc);
    }

    mCompressor = new CryptoJavaImpl();
    reader = new FileReader(pStorage);
  }

  /**
   * Write page contained in page reference to storage.
   * 
   * @param pageReference
   *          Page reference to write.
   * @throws TTIOException
   *           due to errors during writing.
   */
  @Override
  public long write(@Nonnull final PageReference pPageReference) throws TTIOException {
    // Serialise page.
    final ByteBufferSinkAndSource buffer = new ByteBufferSinkAndSource();
    buffer.position(FileReader.OTHER_BEACON);
    final IPage page = pPageReference.getPage();
    assert page != null;
    PagePersistenter.serializePage(buffer, page);
    final int inputLength = buffer.position();

    // Perform crypto operations.
    final int outputLength = mCompressor.crypt(inputLength, buffer);
    if (outputLength == 0) {
      throw new TTIOException("Page crypt error.");
    }

    // Normally, the first bytes until FileReader.OTHERBEACON are reserved and cut of resulting in
    final byte[] tmp = new byte[outputLength];
    buffer.position(0);
    // Because of the missing offset, we can write the length directly at the front of the buffer to see
    // it afterwards in the byte array as well. Do not forget to reset the position before transition to
    // the array
    buffer.writeInt(outputLength);
    buffer.position(0);
    buffer.get(tmp, 0, tmp.length);

    try {
      // Getting actual offset and appending to the end of the current
      // file
      final long fileSize = mFile.length();
      final long offset = fileSize == 0 ? FileReader.FIRST_BEACON : fileSize;
      mFile.seek(offset);
      mFile.write(tmp);
      // Remember page coordinates.
      pPageReference.setKey(offset);

      return offset;
    } catch (final IOException paramExc) {
      throw new TTIOException(paramExc);
    }
  }

  @Override
  public void close() throws TTIOException {
    try {
      if (mFile != null) {
        reader.close();
        mFile.close();
      }
    } catch (final IOException e) {
      throw new TTIOException(e);
    }
  }

  /**
   * Close file handle in case it is not properly closed by the application.
   * 
   * @throws Throwable
   *           if the finalization of the superclass does not work.
   */
  @Override
  protected void finalize() throws Throwable {
    try {
      close();
    } finally {
      super.finalize();
    }
  }

  @Override
  public void writeFirstReference(@Nonnull final PageReference pPageReference)
    throws TTIOException {
    try {
      write(pPageReference);
      mFile.seek(0);
      mFile.writeLong(pPageReference.getKey());
    } catch (final IOException exc) {
      throw new TTIOException(exc);
    }
  }

  @Override
  public IPage read(final long pKey) throws TTIOException {
    return reader.read(pKey);
  }

  @Override
  public PageReference readFirstReference() throws TTIOException {
    return reader.readFirstReference();
  }

}
