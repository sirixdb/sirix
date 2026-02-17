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

package io.sirix.io.file;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.io.*;
import io.sirix.page.*;
import io.sirix.page.interfaces.Page;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * File Writer for providing read/write access for file as a Sirix backend.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 */
public final class FileWriter extends AbstractForwardingReader implements Writer {

  /**
   * Random access to work on.
   */
  private final RandomAccessFile dataFile;

  /**
   * {@link FileReader} reference for this writer.
   */
  private final FileReader reader;

  private final SerializationType type;

  private final RandomAccessFile revisionsFile;

  private final PagePersister pagePersister;

  private final AsyncCache<Integer, RevisionFileData> cache;

  private final RevisionIndexHolder revisionIndexHolder;

  private boolean isFirstUberPage;

  private final BytesOut<?> byteBufferBytes = Bytes.elasticOffHeapByteBuffer(1_000);

  /**
   * Constructor.
   *
   * @param dataFile the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param serializationType the serialization type (for the transaction log or the data file)
   * @param pagePersister transforms in-memory pages into byte-arrays and back
   * @param cache the revision file data cache
   * @param revisionIndexHolder the holder for the optimized revision index
   * @param reader the reader delegate
   */
  public FileWriter(final RandomAccessFile dataFile, final RandomAccessFile revisionsOffsetFile,
      final SerializationType serializationType, final PagePersister pagePersister,
      final AsyncCache<Integer, RevisionFileData> cache, final RevisionIndexHolder revisionIndexHolder,
      final FileReader reader) {
    this.dataFile = requireNonNull(dataFile);
    type = requireNonNull(serializationType);
    this.revisionsFile = type == SerializationType.DATA
        ? requireNonNull(revisionsOffsetFile)
        : null;
    this.pagePersister = requireNonNull(pagePersister);
    this.cache = cache;
    this.revisionIndexHolder = requireNonNull(revisionIndexHolder);
    this.reader = requireNonNull(reader);
  }

  /**
   * Constructor (backward compatibility).
   */
  public FileWriter(final RandomAccessFile dataFile, final RandomAccessFile revisionsOffsetFile,
      final SerializationType serializationType, final PagePersister pagePersister,
      final AsyncCache<Integer, RevisionFileData> cache, final FileReader reader) {
    this(dataFile, revisionsOffsetFile, serializationType, pagePersister, cache, new RevisionIndexHolder(), reader);
  }

  @Override
  public Writer truncateTo(final StorageEngineReader pageReadOnlyTrx, final int revision) {
    try {
      final var dataFileRevisionRootPageOffset =
          cache.get(revision, (unused) -> getRevisionFileData(revision)).get(5, TimeUnit.SECONDS).offset();

      // Read page from file.
      dataFile.seek(dataFileRevisionRootPageOffset);
      final int dataLength = dataFile.readInt();

      dataFile.getChannel().truncate(dataFileRevisionRootPageOffset + IOStorage.OTHER_BEACON + dataLength);
    } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
      throw new IllegalStateException(e);
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
  public FileWriter write(final ResourceConfiguration resourceConfiguration, final PageReference pageReference,
      final Page page, final BytesOut<?> bufferedBytes) {
    try {
      final long fileSize = dataFile.length();
      long offset = fileSize == 0
          ? IOStorage.FIRST_BEACON
          : fileSize;
      return writePageReference(resourceConfiguration, pageReference, page, offset);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @NonNull
  private FileWriter writePageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, long offset) {
    // Perform byte operations.
    try {
      // Serialize page.
      final byte[] serializedPage;

      try (final ByteArrayOutputStream output = new ByteArrayOutputStream(1_000)) {
        try (final DataOutputStream dataOutput = new DataOutputStream(reader.byteHandler.serialize(output))) {
          pagePersister.serializePage(resourceConfiguration, byteBufferBytes, page, type);
          final var byteArray = byteBufferBytes.toByteArray();
          dataOutput.write(byteArray);
          dataOutput.flush();
        }
        serializedPage = output.toByteArray();
      }

      byteBufferBytes.clear();

      final byte[] writtenPage = new byte[serializedPage.length + IOStorage.OTHER_BEACON];
      final ByteBuffer buffer = ByteBuffer.allocate(writtenPage.length);
      buffer.putInt(serializedPage.length);
      buffer.put(serializedPage);
      buffer.flip();
      buffer.get(writtenPage);

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
      dataFile.seek(offset);
      dataFile.write(writtenPage);

      // Remember page coordinates.
      pageReference.setKey(offset);

      // Compute hash on compressed bytes for ALL page types (consistent approach)
      pageReference.setHash(PageHasher.compute(serializedPage));

      if (type == SerializationType.DATA) {
        if (page instanceof RevisionRootPage revisionRootPage) {
          if (revisionRootPage.getRevision() == 0) {
            revisionsFile.seek(revisionsFile.length() + IOStorage.FIRST_BEACON);
          } else {
            revisionsFile.seek(revisionsFile.length());
          }
          revisionsFile.writeLong(offset);
          final long currTimestamp = revisionRootPage.getRevisionTimestamp();
          revisionsFile.writeLong(currTimestamp);
          if (cache != null) {
            final long currOffset = offset;
            cache.put(revisionRootPage.getRevision(), CompletableFuture.supplyAsync(
                () -> new RevisionFileData(currOffset, Instant.ofEpochMilli(currTimestamp))));
            // Update the optimized revision index
            revisionIndexHolder.addRevision(currOffset, currTimestamp);
          }
        } else if (page instanceof UberPage && isFirstUberPage) {
          revisionsFile.seek(0);
          revisionsFile.write(serializedPage);
          revisionsFile.seek(IOStorage.FIRST_BEACON >> 1);
          revisionsFile.write(serializedPage);
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
      if (revisionsFile != null) {
        revisionsFile.close();
      }
      if (reader != null) {
        reader.close();
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Writer writeUberPageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes) {
    isFirstUberPage = true;
    writePageReference(resourceConfiguration, pageReference, page, 0);
    isFirstUberPage = false;
    writePageReference(resourceConfiguration, pageReference, page, 100);
    return this;
  }

  @Override
  protected Reader delegate() {
    return reader;
  }

  @Override
  public Writer truncate() {
    try {
      dataFile.setLength(0);

      if (revisionsFile != null) {
        revisionsFile.setLength(0);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }

  @Override
  public void forceAll() {
    try {
      if (dataFile != null) {
        dataFile.getFD().sync();
      }
      if (revisionsFile != null) {
        revisionsFile.getFD().sync();
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }
}
