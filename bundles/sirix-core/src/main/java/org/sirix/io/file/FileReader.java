/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS;
 * OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT,
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.sirix.io.file;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.sirix.api.PageReadTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersistenter;
import org.sirix.page.PageReference;
import org.sirix.page.SerializationType;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.Page;

/**
 * File Reader. Used for {@link PageReadTrx} to provide read only access on a RandomAccessFile.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 *
 */
public final class FileReader implements Reader {

  /** Beacon of first references. */
  final static int FIRST_BEACON = 12;

  /** Beacon of the other references. */
  final static int OTHER_BEACON = 4;

  /** Random access mFile to work on. */
  private final RandomAccessFile mFile;

  /** Inflater to decompress. */
  final ByteHandler mByteHandler;

  private final SerializationType mType;

  /**
   * Constructor.
   *
   * @param concreteStorage storage file
   * @param handler {@link ByteHandler} instance
   * @throws SirixIOException if something bad happens
   */
  public FileReader(final RandomAccessFile file, final ByteHandler handler,
      final SerializationType type) {
    mFile = checkNotNull(file);
    mByteHandler = checkNotNull(handler);
    mType = checkNotNull(type);
  }

  @Override
  public Page read(final @Nonnull PageReference reference, final @Nullable PageReadTrx pageReadTrx)
      throws SirixIOException {
    try {
      // Read page from file.
      switch (mType) {
        case DATA:
          mFile.seek(reference.getKey());
          break;
        case TRANSACTION_INTENT_LOG:
          mFile.seek(reference.getPersistentLogKey());
          break;
        default:
          // Must not happen.
      }
      final int dataLength = mFile.readInt();
      reference.setLength(dataLength + FileReader.OTHER_BEACON);
      final byte[] page = new byte[dataLength];
      mFile.read(page);

      // Perform byte operations.
      final DataInputStream input =
          new DataInputStream(mByteHandler.deserialize(new ByteArrayInputStream(page)));

      // Return reader required to instantiate and deserialize page.
      return PagePersistenter.deserializePage(input, pageReadTrx, mType);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public PageReference readUberPageReference() throws SirixIOException {
    final PageReference uberPageReference = new PageReference();
    try {
      // Read primary beacon.
      mFile.seek(0);
      uberPageReference.setKey(mFile.readLong());
      final UberPage page = (UberPage) read(uberPageReference, null);
      uberPageReference.setPage(page);
      return uberPageReference;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() throws SirixIOException {
    try {
      mFile.close();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }
}
