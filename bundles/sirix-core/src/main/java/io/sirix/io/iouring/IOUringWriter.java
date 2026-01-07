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

package io.sirix.io.iouring;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.api.StorageEngineWriter;
import io.sirix.exception.SirixIOException;
import io.sirix.io.*;
import io.sirix.page.*;
import io.sirix.page.interfaces.Page;
import io.sirix.node.BytesIn;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import io.sirix.node.MemorySegmentBytesIn;
import one.jasyncfio.AsyncFile;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * File writer for providing read/write access for file as a Sirix backend.
 *
 * @author Johannes Lichtenberger
 */
public final class IOUringWriter extends AbstractForwardingReader implements Writer {

  /**
   * Random access to work on.
   */
  private final AsyncFile dataFile;

  /**
   * {@link IOUringReader} reference for this writer.
   */
  private final IOUringReader reader;

  private final SerializationType serializationType;

  private final AsyncFile revisionsFile;

  private final PagePersister pagePersister;

  private final AsyncCache<Integer, RevisionFileData> cache;
  
  private final RevisionIndexHolder revisionIndexHolder;

  private final Path dataFilePath;

  private final Path revisionsOffsetFilePath;

  private boolean isFirstUberPage;

  private final BytesOut<?> byteBufferBytes = Bytes.elasticOffHeapByteBuffer(1_000);

  /**
   * Constructor.
   *
   * @param dataFile            the data file channel
   * @param revisionsOffsetFile the channel to the file, which holds pointers to the revision root pages
   * @param serializationType   the serialization type (for the transaction log or the data file)
   * @param pagePersister       transforms in-memory pages into byte-arrays and back
   * @param cache               the revision file data cache
   * @param revisionIndexHolder the holder for the optimized revision index
   * @param reader              the reader delegate
   */
  public IOUringWriter(final AsyncFile dataFile, final AsyncFile revisionsOffsetFile, final Path dataFilePath,
      final Path revisionsOffsetFilePath, final SerializationType serializationType, final PagePersister pagePersister,
      final AsyncCache<Integer, RevisionFileData> cache, final RevisionIndexHolder revisionIndexHolder,
      final IOUringReader reader) {
    this.dataFile = dataFile;
    this.revisionsFile = revisionsOffsetFile;
    this.dataFilePath = dataFilePath;
    this.revisionsOffsetFilePath = revisionsOffsetFilePath;
    this.serializationType = requireNonNull(serializationType);
    this.pagePersister = requireNonNull(pagePersister);
    this.cache = requireNonNull(cache);
    this.revisionIndexHolder = requireNonNull(revisionIndexHolder);
    this.reader = requireNonNull(reader);
  }
  
  /**
   * Constructor (backward compatibility).
   */
  public IOUringWriter(final AsyncFile dataFile, final AsyncFile revisionsOffsetFile, final Path dataFilePath,
      final Path revisionsOffsetFilePath, final SerializationType serializationType, final PagePersister pagePersister,
      final AsyncCache<Integer, RevisionFileData> cache, final IOUringReader reader) {
    this(dataFile, revisionsOffsetFile, dataFilePath, revisionsOffsetFilePath, serializationType, 
         pagePersister, cache, new RevisionIndexHolder(), reader);
  }

  @Override
  public Writer truncateTo(final StorageEngineReader pageReadOnlyTrx, final int revision) {
    try {
      final var dataFileRevisionRootPageOffset =
          cache.get(revision, (_) -> getRevisionFileData(revision)).get(5, TimeUnit.SECONDS).offset();

      // Read page from file.
      final var buffer = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.nativeOrder());

      dataFile.read(buffer, dataFileRevisionRootPageOffset).join();

      buffer.position(0);
      final int dataLength = buffer.getInt();

      new RandomAccessFile(dataFilePath.toFile(), "rw").getChannel()
                                                       .truncate(dataFileRevisionRootPageOffset + IOStorage.OTHER_BEACON
                                                                     + dataLength);
    } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
      throw new IllegalStateException(e);
    }

    return this;
  }

  @Override
  public IOUringWriter write(final ResourceConfiguration resourceConfiguration, final PageReference pageReference,
      final Page page, final BytesOut<?> bufferedBytes) {
    try {
      final long offset = getOffset(bufferedBytes);
      return writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, offset);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private long getOffset(BytesOut<?> bufferedBytes) throws IOException {
    final long fileSize = dataFile.size().join();
    long offset;

    if (fileSize == 0) {
      offset = IOStorage.FIRST_BEACON;
      offset += (PAGE_FRAGMENT_BYTE_ALIGN - (offset & (PAGE_FRAGMENT_BYTE_ALIGN - 1)));
      offset += bufferedBytes.writePosition();
    } else {
      offset = fileSize + bufferedBytes.writePosition();
    }

    return offset;
  }

  private byte[] buildSerializedPage(final ResourceConfiguration resourceConfiguration, final Page page) throws IOException {
    final BytesIn<?> uncompressedBytes = byteBufferBytes.bytesForRead();

    if (page instanceof KeyValueLeafPage keyValueLeafPage && keyValueLeafPage.getBytes() != null) {
      // Use cached compressed bytes when available
      return keyValueLeafPage.getBytes().toByteArray();
    }

    final var pipeline = resourceConfiguration.byteHandlePipeline;

    if (pipeline.supportsMemorySegments() && uncompressedBytes instanceof MemorySegmentBytesIn segmentIn) {
      MemorySegment compressedSegment = pipeline.compress(segmentIn.getSource());
      return segmentToByteArray(compressedSegment);
    }

    final byte[] byteArray = uncompressedBytes.toByteArray();

    try (final ByteArrayOutputStream output = new ByteArrayOutputStream(byteArray.length);
         final DataOutputStream dataOutput = new DataOutputStream(reader.getByteHandler().serialize(output))) {
      dataOutput.write(byteArray);
      dataOutput.flush();
      return output.toByteArray();
    }
  }

  private static byte[] segmentToByteArray(MemorySegment segment) {
    return segment.toArray(java.lang.foreign.ValueLayout.JAVA_BYTE);
  }

  @NonNull
  private IOUringWriter writePageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, BytesOut<?> bufferedBytes, long offset) {
    try {
      POOL.submit(() -> writePage(resourceConfiguration, pageReference, page, bufferedBytes, offset)).get();
      return this;
    } catch (InterruptedException | ExecutionException e) {
      throw new SirixIOException(e);
    }
  }

  @NonNull
  private IOUringWriter writePage(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes, long offset) {
    // Perform byte operations.
    try {
      // Serialize page.
      pagePersister.serializePage(resourceConfiguration, byteBufferBytes, page, serializationType);
      final byte[] serializedPage = buildSerializedPage(resourceConfiguration, page);

      byteBufferBytes.clear();

      int offsetToAdd = 0;

      // Getting actual offset and appending to the end of the current file.
      if (serializationType == SerializationType.DATA) {
        if (page instanceof UberPage) {
          offsetToAdd =
              UBER_PAGE_BYTE_ALIGN - ((serializedPage.length + IOStorage.OTHER_BEACON) % UBER_PAGE_BYTE_ALIGN);
        } else if (page instanceof RevisionRootPage && offset % REVISION_ROOT_PAGE_BYTE_ALIGN != 0) {
          offsetToAdd = (int) (REVISION_ROOT_PAGE_BYTE_ALIGN - (offset & (REVISION_ROOT_PAGE_BYTE_ALIGN - 1)));
          offset += offsetToAdd;
        } else if (offset % PAGE_FRAGMENT_BYTE_ALIGN != 0) {
          offsetToAdd = (int) (PAGE_FRAGMENT_BYTE_ALIGN - (offset & (PAGE_FRAGMENT_BYTE_ALIGN
              - 1)));//(offset % PAGE_FRAGMENT_BYTE_ALIGN));
          offset += offsetToAdd;
        }
      }

      final var pageBuffer = ByteBuffer.allocateDirect(serializedPage.length + IOStorage.OTHER_BEACON + offsetToAdd)
                                       .order(ByteOrder.nativeOrder());

      //      if (!(page instanceof UberPage) && offsetToAdd > 0) {
      //        var buffer = new byte[(int) offsetToAdd];
      //        pageBuffer.put(buffer);
      //      }

      pageBuffer.putInt(serializedPage.length);
      pageBuffer.put(serializedPage);

      if (page instanceof UberPage && offsetToAdd > 0) {
        final byte[] bytesToAdd = new byte[(int) offsetToAdd];
        pageBuffer.put(bytesToAdd);
      }

      pageBuffer.flip();

      dataFile.write(pageBuffer, offset).join();

      // Remember page coordinates.
      pageReference.setKey(offset);

      // Compute hash on compressed bytes for ALL page types (consistent approach)
      pageReference.setHash(PageHasher.compute(serializedPage));

      if (serializationType == SerializationType.DATA) {
        if (page instanceof RevisionRootPage revisionRootPage) {
          ByteBuffer buffer = ByteBuffer.allocateDirect(16).order(ByteOrder.nativeOrder());
          buffer.putLong(offset);
          buffer.position(8);
          buffer.putLong(revisionRootPage.getRevisionTimestamp());
          buffer.position(0);
          final long revisionsFileOffset;
          if (revisionRootPage.getRevision() == 0) {
            revisionsFileOffset = revisionsFile.size().join() + IOStorage.FIRST_BEACON;
          } else {
            revisionsFileOffset = revisionsFile.size().join();
          }
          revisionsFile.write(buffer, revisionsFileOffset).join();
          final long currOffset = offset;
          final long currTimestamp = revisionRootPage.getRevisionTimestamp();
          cache.put(revisionRootPage.getRevision(),
                    CompletableFuture.supplyAsync(() -> new RevisionFileData(currOffset,
                                                                             Instant.ofEpochMilli(currTimestamp))));
          // Update the optimized revision index
          revisionIndexHolder.addRevision(currOffset, currTimestamp);
        } else if (page instanceof UberPage && isFirstUberPage) {
          final ByteBuffer firstUberPageBuffer =
              ByteBuffer.allocateDirect(Writer.UBER_PAGE_BYTE_ALIGN).order(ByteOrder.nativeOrder());
          firstUberPageBuffer.put(serializedPage);
          firstUberPageBuffer.position(0);
          revisionsFile.write(firstUberPageBuffer, 0L).join();
          final ByteBuffer secondUberPageBuffer =
              ByteBuffer.allocateDirect(Writer.UBER_PAGE_BYTE_ALIGN).order(ByteOrder.nativeOrder());
          secondUberPageBuffer.put(serializedPage);
          secondUberPageBuffer.position(0);
          revisionsFile.write(secondUberPageBuffer, (long) Writer.UBER_PAGE_BYTE_ALIGN).join();
          revisionsFile.dataSync().join();
        }
      }

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() {
    if (dataFile != null) {
      dataFile.dataSync().join();
    }
    if (revisionsFile != null) {
      revisionsFile.dataSync().join();
    }
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  public Writer writeUberPageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, BytesOut<?> bufferedBytes) {
    isFirstUberPage = true;
    writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, 0);
    isFirstUberPage = false;
    writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, IOStorage.FIRST_BEACON >> 1);

    dataFile.dataSync().join();

    return this;
  }

  private void flushBuffer(final StorageEngineWriter pageTrx, final ByteBuffer buffer) throws IOException {
    final long fileSize = dataFile.size().join();
    long offset;

    if (fileSize == 0) {
      offset = IOStorage.FIRST_BEACON;
      offset += (PAGE_FRAGMENT_BYTE_ALIGN - (offset % PAGE_FRAGMENT_BYTE_ALIGN));
    } else {
      offset = fileSize;
    }

    dataFile.write(buffer, offset).join();
  }

  @Override
  protected Reader delegate() {
    return reader;
  }

  @Override
  public Writer truncate() {
    try {
      new RandomAccessFile(dataFilePath.toFile(), "rw").getChannel().truncate(0);

      if (revisionsFile != null) {
        new RandomAccessFile(revisionsOffsetFilePath.toFile(), "rw").getChannel().truncate(0);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }

  @Override
  public void forceAll() {
    if (dataFile != null) {
      dataFile.dataSync().join();
    }
    if (revisionsFile != null) {
      revisionsFile.dataSync().join();
    }
  }
}
