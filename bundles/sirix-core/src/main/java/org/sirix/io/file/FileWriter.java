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
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.SerializationType;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.Page;

/**
 * File Writer for providing read/write access for file as a Sirix backend.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class FileWriter extends AbstractForwardingReader implements Writer {

  /** Random access to work on. */
  private final RandomAccessFile mDataFile;

  /** {@link FileReader} reference for this writer. */
  private final FileReader mReader;

  private final SerializationType mType;

  private final RandomAccessFile mRevisionsOffsetFile;

  private final PagePersister mPagePersister;

  /**
   * Constructor.
   *
   * @param dataFile the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param handler the byte handler
   * @param serializationType the serialization type (for the transaction log or the data file)
   * @param pagePersister transforms in-memory pages into byte-arrays and back
   */
  public FileWriter(final RandomAccessFile dataFile, final RandomAccessFile revisionsOffsetFile,
      final ByteHandler handler, final SerializationType serializationType,
      final PagePersister pagePersister) {
    mDataFile = checkNotNull(dataFile);
    mType = checkNotNull(serializationType);
    mRevisionsOffsetFile = mType == SerializationType.DATA
        ? checkNotNull(revisionsOffsetFile)
        : null;
    mPagePersister = checkNotNull(pagePersister);
    mReader =
        new FileReader(dataFile, revisionsOffsetFile, handler, serializationType, pagePersister);
  }

  @Override
  public Writer truncateTo(final int revision) {
    UberPage uberPage = (UberPage) mReader.readUberPageReference().getPage();

    while (uberPage.getRevisionNumber() != revision) {
      uberPage = (UberPage) mReader.read(
          new PageReference().setKey(uberPage.getPreviousUberPageKey()), null);
      if (uberPage.getRevisionNumber() == revision) {
        try {
          mDataFile.setLength(uberPage.getPreviousUberPageKey());
        } catch (final IOException e) {
          throw new UncheckedIOException(e);
        }
        break;
      }
    }

    return this;
  }

  /**
   * Write page contained in page reference to storage.
   *
   * @param pageReference page reference to write
   * @throws SirixIOException if errors during writing occur
   */
  @Override
  public FileWriter write(final PageReference pageReference) throws SirixIOException {
    // Perform byte operations.
    try {
      // Serialize page.
      final Page page = pageReference.getPage();
      assert page != null;

      final byte[] serializedPage;

      try (final ByteArrayOutputStream output = new ByteArrayOutputStream();
          final DataOutputStream dataOutput =
              new DataOutputStream(mReader.mByteHandler.serialize(output))) {
        mPagePersister.serializePage(dataOutput, page, mType);
        dataOutput.flush();
        serializedPage = output.toByteArray();
      }

      final byte[] writtenPage = new byte[serializedPage.length + FileReader.OTHER_BEACON];
      final ByteBuffer buffer = ByteBuffer.allocate(writtenPage.length);
      buffer.putInt(serializedPage.length);
      buffer.put(serializedPage);
      buffer.position(0);
      buffer.get(writtenPage, 0, writtenPage.length);

      // Getting actual offset and appending to the end of the current file.
      final long fileSize = mDataFile.length();
      final long offset = fileSize == 0
          ? FileReader.FIRST_BEACON
          : fileSize;
      mDataFile.seek(offset);
      mDataFile.write(writtenPage);

      // Remember page coordinates.
      switch (mType) {
        case DATA:
          pageReference.setKey(offset);
          break;
        case TRANSACTION_INTENT_LOG:
          pageReference.setPersistentLogKey(offset);
          break;
        default:
          // Must not happen.
      }

      pageReference.setLength(writtenPage.length);
      pageReference.setHash(mReader.mHashFunction.hashBytes(writtenPage).asBytes());

      if (mType == SerializationType.DATA && page instanceof RevisionRootPage) {
        mRevisionsOffsetFile.seek(mRevisionsOffsetFile.length());
        mRevisionsOffsetFile.writeLong(offset);
      }

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() throws SirixIOException {
    try {
      if (mDataFile != null) {
        mDataFile.close();
      }
      if (mRevisionsOffsetFile != null) {
        mRevisionsOffsetFile.close();
      }
      if (mReader != null) {
        mReader.close();
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Writer writeUberPageReference(final PageReference pageReference) throws SirixIOException {
    try {
      write(pageReference);
      mDataFile.seek(0);
      mDataFile.writeLong(pageReference.getKey());

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  protected Reader delegate() {
    return mReader;
  }

  @Override
  public Writer truncate() {
    try {
      mDataFile.setLength(0);
      mRevisionsOffsetFile.setLength(0);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }
}
