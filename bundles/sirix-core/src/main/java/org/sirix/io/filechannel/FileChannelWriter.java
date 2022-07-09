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

package org.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.AsyncCache;
import net.openhft.chronicle.bytes.Bytes;
import org.jetbrains.annotations.NotNull;
import org.sirix.exception.SirixIOException;
import org.sirix.io.*;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * File Writer for providing read/write access for file as a Sirix backend.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 */
public final class FileChannelWriter extends AbstractForwardingReader implements Writer {

  private static final short REVISION_ROOT_PAGE_BYTE_ALIGN = 256;

  private static final byte PAGE_FRAGMENT_BYTE_ALIGN = 64;

  /**
   * Random access to work on.
   */
  private final FileChannel dataFileChannel;

  /**
   * {@link FileChannelReader} reference for this writer.
   */
  private final FileChannelReader reader;

  private final SerializationType type;

  private final FileChannel revisionsFileChannel;

  private final PagePersister pagePersister;

  private final AsyncCache<Integer, RevisionFileData> cache;

  private boolean isFirstUberPage;

  /**
   * Constructor.
   *
   * @param dataFileChannel            the data file channel
   * @param revisionsOffsetFileChannel the channel to the file, which holds pointers to the revision root pages
   * @param serializationType          the serialization type (for the transaction log or the data file)
   * @param pagePersister              transforms in-memory pages into byte-arrays and back
   * @param cache                      the revision file data cache
   * @param reader                     the reader delegate
   */
  public FileChannelWriter(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final SerializationType serializationType, final PagePersister pagePersister,
      final AsyncCache<Integer, RevisionFileData> cache, final FileChannelReader reader) {
    this.dataFileChannel = dataFileChannel;
    type = checkNotNull(serializationType);
    this.revisionsFileChannel = revisionsOffsetFileChannel;
    this.pagePersister = checkNotNull(pagePersister);
    this.cache = checkNotNull(cache);
    this.reader = checkNotNull(reader);
  }

  @Override
  public Writer truncateTo(final int revision) {
    // FIXME | TODO

    return this;
  }

  /**
   * Write page contained in page reference to storage.
   *
   * @param pageReference page reference to write
   * @throws SirixIOException if errors during writing occur
   */
  @Override
  public FileChannelWriter write(final PageReference pageReference) {
    try {
      final long fileSize = dataFileChannel.size();
      long offset = fileSize == 0 ? IOStorage.FIRST_BEACON : fileSize;
      return writePageReference(pageReference, offset);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @NotNull
  private FileChannelWriter writePageReference(PageReference pageReference, long offset) {
    // Perform byte operations.
    try {
      // Serialize page.
      final Page page = pageReference.getPage();
      assert page != null;

      final byte[] serializedPage;

      final var byteBufferBytes = Bytes.elasticByteBuffer();

      pagePersister.serializePage(byteBufferBytes, page, type);

      //reader.byteHandler.serialize(byteBufferBytes.outputStream());

      //      byteBufferBytes.writePosition(Integer.BYTES);
      //      byteBufferBytes.writeInt(0, byteBufferBytes.length());
      serializedPage = byteBufferBytes.toByteArray();
      final int writtenPageLength = serializedPage.length + IOStorage.OTHER_BEACON;
      ByteBuffer buffer = ByteBuffer.allocate(writtenPageLength);
      buffer.putInt(serializedPage.length);
      buffer.put(serializedPage);
      buffer.flip();

      // Getting actual offset and appending to the end of the current file.
      if (type == SerializationType.DATA) {
        if (page instanceof RevisionRootPage) {
          if (offset % REVISION_ROOT_PAGE_BYTE_ALIGN != 0) {
            offset += REVISION_ROOT_PAGE_BYTE_ALIGN - (offset % REVISION_ROOT_PAGE_BYTE_ALIGN);
          }
        } else if (offset % PAGE_FRAGMENT_BYTE_ALIGN != 0) {
          offset += PAGE_FRAGMENT_BYTE_ALIGN - (offset % PAGE_FRAGMENT_BYTE_ALIGN);
        }
      }

      dataFileChannel.write(buffer, offset);

      // Remember page coordinates.
      switch (type) {
        case DATA -> pageReference.setKey(offset);
        case TRANSACTION_INTENT_LOG -> pageReference.setPersistentLogKey(offset);
        default -> {
          // Must not happen.
        }
      }

      pageReference.setHash(reader.hashFunction.hashBytes(serializedPage).asBytes());

      if (type == SerializationType.DATA) {
        if (page instanceof RevisionRootPage revisionRootPage) {
          buffer = ByteBuffer.allocate(16);
          buffer.putLong(offset);
          buffer.position(8);
          buffer.putLong(revisionRootPage.getRevisionTimestamp());
          buffer.position(0);
          final long revisionsFileOffset;
          if (revisionRootPage.getRevision() == 0) {
            revisionsFileOffset = revisionsFileChannel.size() + IOStorage.FIRST_BEACON;
          } else {
            revisionsFileOffset = revisionsFileChannel.size();
          }
          revisionsFileChannel.write(buffer, revisionsFileOffset);
          final long currOffset = offset;
          cache.put(revisionRootPage.getRevision(),
                    CompletableFuture.supplyAsync(() -> new RevisionFileData(currOffset,
                                                                             Instant.ofEpochMilli(revisionRootPage.getRevisionTimestamp()))));
        } else if (page instanceof UberPage && isFirstUberPage) {
          buffer = ByteBuffer.allocate(IOStorage.FIRST_BEACON >> 1);
          buffer.put(serializedPage);
          buffer.position(0);
          revisionsFileChannel.write(buffer, 0);
          buffer.position(0);
          revisionsFileChannel.write(buffer, IOStorage.FIRST_BEACON >> 1);
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
      if (dataFileChannel != null) {
        dataFileChannel.force(true);
      }
      if (revisionsFileChannel != null) {
        revisionsFileChannel.force(true);
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
    isFirstUberPage = true;
    writePageReference(pageReference, 0);
    isFirstUberPage = false;
    writePageReference(pageReference, 0);
    return this;
  }

  @Override
  protected Reader delegate() {
    return reader;
  }

  @Override
  public Writer truncate() {
    try {
      dataFileChannel.truncate(0);

      if (revisionsFileChannel != null) {
        revisionsFileChannel.truncate(0);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }
}
