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

import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import javax.annotation.Nonnull;

import org.sirix.exception.TTByteHandleException;
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
  public long write(@Nonnull final PageReference pPageReference)
    throws TTIOException {
    // Serialise page.
    final IPage page = pPageReference.getPage();
    assert page != null;
    final ByteArrayDataOutput output = ByteStreams.newDataOutput();
    PagePersistenter.serializePage(output, page);

    // Perform byte operations.
    try {
      final byte[] decryptedPage =
        reader.mByteHandler.serialize(output.toByteArray());

      final byte[] writtenPage =
        new byte[decryptedPage.length + FileReader.OTHER_BEACON];
      ByteBuffer buffer = ByteBuffer.allocate(writtenPage.length);
      buffer.putInt(decryptedPage.length);
      buffer.put(decryptedPage);
      buffer.position(0);
      buffer.get(writtenPage, 0, writtenPage.length);

      // Getting actual offset and appending to the end of the current
      // file
      final long fileSize = mFile.length();
      final long offset = fileSize == 0 ? FileReader.FIRST_BEACON : fileSize;
      mFile.seek(offset);
      mFile.write(writtenPage);

      // Remember page coordinates.
      pPageReference.setKey(offset);
      return offset;
    } catch (final IOException e) {
      throw new TTIOException(e);
    } catch (final TTByteHandleException e) {
      throw new TTIOException(e);
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
   *           if the finalization of the superclass does not work
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
