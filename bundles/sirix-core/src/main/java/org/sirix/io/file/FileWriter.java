/*
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

import com.github.benmanes.caffeine.cache.AsyncCache;
import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.Writer;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * File Writer for providing read/write access for file as a Sirix backend.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 */
public final class FileWriter extends AbstractForwardingReader implements Writer {

  private static final short REVISION_ROOT_PAGE_BYTE_ALIGN = 256;

  private static final byte PAGE_FRAGMENT_BYTE_ALIGN = 64;

  /**
   * Random access to work on.
   */
  private final RandomAccessFile dataFile;

  /**
   * {@link FileReader} reference for this writer.
   */
  private final FileReader reader;

  private final SerializationType type;

  private final RandomAccessFile revisionsOffsetFile;

  private final PagePersister pagePersister;

  private final AsyncCache<Integer, RevisionFileData> cache;

  /**
   * Constructor.
   *
   * @param dataFile            the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param serializationType   the serialization type (for the transaction log or the data file)
   * @param pagePersister       transforms in-memory pages into byte-arrays and back
   * @param cache               the revision file data cache
   * @param reader              the reader delegate
   */
  public FileWriter(final RandomAccessFile dataFile, final RandomAccessFile revisionsOffsetFile,
      final SerializationType serializationType, final PagePersister pagePersister,
      final AsyncCache<Integer, RevisionFileData> cache, final FileReader reader) {
    this.dataFile = checkNotNull(dataFile);
    type = checkNotNull(serializationType);
    this.revisionsOffsetFile = type == SerializationType.DATA ? checkNotNull(revisionsOffsetFile) : null;
    this.pagePersister = checkNotNull(pagePersister);
    this.cache = cache;
    this.reader = checkNotNull(reader);
  }

  @Override
  public Writer truncateTo(final int revision) {
    UberPage uberPage = (UberPage) reader.readUberPageReference().getPage();

    while (uberPage.getRevisionNumber() != revision) {
      uberPage = (UberPage) reader.read(new PageReference().setKey(uberPage.getPreviousUberPageKey()), null);
      if (uberPage.getRevisionNumber() == revision) {
        try {
          dataFile.setLength(uberPage.getPreviousUberPageKey());
        } catch (final IOException e) {
          throw new SirixIOException(e);
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
  public FileWriter write(final PageReference pageReference) {
    // Perform byte operations.
    try {
      // Serialize page.
      final Page page = pageReference.getPage();
      assert page != null;

      final byte[] serializedPage;

      try (final ByteArrayOutputStream output = new ByteArrayOutputStream();
           final DataOutputStream dataOutput = new DataOutputStream(reader.byteHandler.serialize(output))) {
        pagePersister.serializePage(dataOutput, page, type);
        dataOutput.flush();
        serializedPage = output.toByteArray();
      }

      final byte[] writtenPage = new byte[serializedPage.length + FileReader.OTHER_BEACON];
      final ByteBuffer buffer = ByteBuffer.allocate(writtenPage.length);
      buffer.putInt(serializedPage.length);
      buffer.put(serializedPage);
      buffer.position(0);
      buffer.get(writtenPage);

      // Getting actual offset and appending to the end of the current file.
      final long fileSize = dataFile.length();
      long offset = fileSize == 0 ? FileReader.FIRST_BEACON : fileSize;
      if (type == SerializationType.DATA) {
        if (page instanceof RevisionRootPage) {
          if (offset % REVISION_ROOT_PAGE_BYTE_ALIGN != 0) {
            offset += REVISION_ROOT_PAGE_BYTE_ALIGN - (offset % REVISION_ROOT_PAGE_BYTE_ALIGN);
          }
        } else if (offset % PAGE_FRAGMENT_BYTE_ALIGN != 0) {
          offset += PAGE_FRAGMENT_BYTE_ALIGN - (offset % PAGE_FRAGMENT_BYTE_ALIGN);
        }
      }
      dataFile.seek(offset);
      dataFile.write(writtenPage);

      // Remember page coordinates.
      switch (type) {
        case DATA:
          pageReference.setKey(offset);
          break;
        case TRANSACTION_INTENT_LOG:
          pageReference.setPersistentLogKey(offset);
          break;
        default:
          // Must not happen.
      }

      //      pageReference.setLength(writtenPage.length);
      pageReference.setHash(reader.hashFunction.hashBytes(serializedPage).asBytes());

      if (type == SerializationType.DATA && page instanceof RevisionRootPage revisionRootPage) {
        revisionsOffsetFile.seek(revisionsOffsetFile.length());
        revisionsOffsetFile.writeLong(offset);
        revisionsOffsetFile.writeLong(revisionRootPage.getRevisionTimestamp());
        if (cache != null) {
          final long currOffset = offset;
          cache.put(revisionRootPage.getRevision(),
                    CompletableFuture.supplyAsync(() -> new RevisionFileData(currOffset,
                                                                             Instant.ofEpochMilli(revisionRootPage.getRevisionTimestamp()))));
        }
      }

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() {
    try {
      if (dataFile != null) {
        dataFile.close();
      }
      if (revisionsOffsetFile != null) {
        revisionsOffsetFile.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Writer writeUberPageReference(final PageReference pageReference) {
    try {
      write(pageReference);
      dataFile.seek(0);
      dataFile.writeLong(pageReference.getKey());

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  protected Reader delegate() {
    return reader;
  }

  @Override
  public Writer truncate() {
    try {
      dataFile.setLength(0);

      if (revisionsOffsetFile != null) {
        revisionsOffsetFile.setLength(0);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }
}
