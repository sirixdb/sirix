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

package org.treetank.io.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

import org.treetank.exception.TTIOException;
import org.treetank.io.IKey;
import org.treetank.io.IReader;
import org.treetank.page.PagePersistenter;
import org.treetank.page.PageReference;
import org.treetank.page.UberPage;
import org.treetank.page.interfaces.IPage;
import org.treetank.utils.IConstants;

/**
 * File Reader. Used for ReadTransaction to provide read only access on a
 * RandomAccessFile.
 * 
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz.
 * 
 */
public final class FileReader implements IReader {

  /** Random access mFile to work on. */
  private final RandomAccessFile mFile;

  /** Inflater to decompress. */
  private final CryptoJavaImpl mDecompressor;

  /** Temporary data buffer. */
  private transient ByteBufferSinkAndSource mBuffer;

  /**
   * Constructor.
   * 
   * @throws TTIOException
   *           if something bad happens
   */
  public FileReader(final File mConcreteStorage) throws TTIOException {
    try {
      if (!mConcreteStorage.exists()) {
        mConcreteStorage.getParentFile().mkdirs();
        mConcreteStorage.createNewFile();
      }

      mFile = new RandomAccessFile(mConcreteStorage, IConstants.READ_ONLY);
      mDecompressor = new CryptoJavaImpl();
      mBuffer = new ByteBufferSinkAndSource();
    } catch (final IOException exc) {
      throw new TTIOException(exc);
    }
  }

  /**
   * Read page from storage.
   * 
   * @param pageReference
   *          to read.
   * @return Byte array reader to read bytes from.o
   * @throws TTIOException
   *           if there was an error during reading.
   */
  @Override
  public IPage read(final IKey pKey) throws TTIOException {
    if (pKey == null) {
      return null;
    }

    try {
      final FileKey fileKey = (FileKey)pKey;

      // Prepare environment for read.
      final int inputLength = fileKey.getLength() + IConstants.BEACON_LENGTH;
      mBuffer.position(12);

      // Read page from file.
      final byte[] page = new byte[fileKey.getLength()];
      mFile.seek(fileKey.getOffset());
      mFile.read(page);
      for (final byte byteVal : page) {
        mBuffer.writeByte(byteVal);
      }

      // Perform crypto operations.
      final int outputLength = mDecompressor.decrypt(inputLength, mBuffer);
      if (outputLength == 0) {
        throw new TTIOException("Page decrypt error.");
      }

    } catch (final IOException exc) {
      throw new TTIOException(exc);
    }

    // Return reader required to instantiate and deserialize page.
    mBuffer.position(12);
    return PagePersistenter.deserializePage(mBuffer);

  }

  @Override
  public PageReference readFirstReference() throws TTIOException {
    final PageReference uberPageReference = new PageReference();
    try {
      // Read primary beacon.
      mFile.seek(IConstants.BEACON_START);

      final FileKey key = new FileKey(mFile.readLong(), mFile.readInt());

      uberPageReference.setKey(key);

      // Check to writer ensure writing after the Beacon_Start
      if (mFile.getFilePointer() < IConstants.BEACON_START + IConstants.BEACON_LENGTH) {
        mFile.setLength(IConstants.BEACON_START + IConstants.BEACON_LENGTH);
      }

      final UberPage page = (UberPage)read(uberPageReference.getKey());
      uberPageReference.setPage(page);

      return uberPageReference;
    } catch (final IOException exc) {
      throw new TTIOException(exc);
    }
  }

  @Override
  public void close() throws TTIOException {
    try {
      mFile.close();
    } catch (final IOException exc) {
      throw new TTIOException(exc);

    }
  }

}
