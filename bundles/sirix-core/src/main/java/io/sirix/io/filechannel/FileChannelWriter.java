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

package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.io.*;
import io.sirix.page.*;
import io.sirix.page.interfaces.Page;
import io.sirix.node.BytesOut;
import io.sirix.node.Bytes;
import io.sirix.node.BytesIn;
import io.sirix.node.MemorySegmentBytesIn;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
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
 * @author Johannes Lichtenberger
 */
public final class FileChannelWriter extends AbstractForwardingReader implements Writer {

  /**
   * Random access to work on.
   */
  private final FileChannel dataFileChannel;

  /**
   * {@link FileChannelReader} reference for this writer.
   */
  private final FileChannelReader reader;

  private final SerializationType serializationType;

  private final FileChannel revisionsFileChannel;

  private final PagePersister pagePersister;

  private final AsyncCache<Integer, RevisionFileData> cache;

  private boolean isFirstUberPage;

  private final BytesOut<?> byteBufferBytes = Bytes.elasticOffHeapByteBuffer(1_000);

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
    this.serializationType = requireNonNull(serializationType);
    this.revisionsFileChannel = revisionsOffsetFileChannel;
    this.pagePersister = requireNonNull(pagePersister);
    this.cache = requireNonNull(cache);
    this.reader = requireNonNull(reader);
  }

  @Override
  public Writer truncateTo(final StorageEngineReader pageReadOnlyTrx, final int revision) {
    try {
      final var dataFileRevisionRootPageOffset =
          cache.get(revision, _ -> getRevisionFileData(revision)).get(5, TimeUnit.SECONDS).offset();

      // Read page from file.
      final var buffer = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.nativeOrder());

      dataFileChannel.read(buffer, dataFileRevisionRootPageOffset);

      buffer.position(0);
      final int dataLength = buffer.getInt();

      dataFileChannel.truncate(dataFileRevisionRootPageOffset + IOStorage.OTHER_BEACON + dataLength);
    } catch (InterruptedException | ExecutionException | TimeoutException | IOException e) {
      throw new IllegalStateException(e);
    }

    return this;
  }

  @Override
  public FileChannelWriter write(final ResourceConfiguration resourceConfiguration, final PageReference pageReference,
      final Page page, final BytesOut<?> bufferedBytes) {
    try {
      final long offset = getOffset(bufferedBytes);
      return writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, offset);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private long getOffset(BytesOut<?> bufferedBytes) throws IOException {
    final long fileSize = dataFileChannel.size();
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
      final var cached = keyValueLeafPage.getBytes();
      if (cached instanceof io.sirix.node.MemorySegmentBytesOut msOut) {
        MemorySegment segment = msOut.getDestination();
        return segmentToByteArray(segment);
      }
      return cached.toByteArray();
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
  private FileChannelWriter writePageReference(final ResourceConfiguration resourceConfiguration,
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

      if (!(page instanceof UberPage) && offsetToAdd > 0) {
        bufferedBytes.writePosition(bufferedBytes.writePosition() + offsetToAdd);
      }

      bufferedBytes.writeInt(serializedPage.length);
      writeToBufferedBytes(bufferedBytes, serializedPage);

      if (page instanceof UberPage && offsetToAdd > 0) {
        final byte[] bytesToAdd = new byte[(int) offsetToAdd];
        bufferedBytes.write(bytesToAdd);
      }

      if (bufferedBytes.writePosition() > FLUSH_SIZE) {
        flushBuffer(bufferedBytes);
      }

      // Remember page coordinates.
      pageReference.setKey(offset);

      if (page instanceof KeyValueLeafPage keyValueLeafPage) {
        pageReference.setHash(keyValueLeafPage.getHashCode());
      } else {
        pageReference.setHash(reader.hashFunction.hashBytes(serializedPage).asBytes());
      }

      if (serializationType == SerializationType.DATA) {
        if (page instanceof RevisionRootPage revisionRootPage) {
          ByteBuffer buffer = ByteBuffer.allocateDirect(16).order(ByteOrder.nativeOrder());
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
          ByteBuffer buffer = ByteBuffer.allocateDirect(Writer.UBER_PAGE_BYTE_ALIGN).order(ByteOrder.nativeOrder());
          buffer.put(serializedPage);
          buffer.position(0);
          revisionsFileChannel.write(buffer, 0);
          buffer.position(0);
          revisionsFileChannel.write(buffer, Writer.UBER_PAGE_BYTE_ALIGN);
        }
      }

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private void writeToBufferedBytes(BytesOut<?> bufferedBytes, byte[] serializedPage) {
    if (serializedPage == null) {
      return;
    }
    bufferedBytes.write(serializedPage);
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
  public Writer writeUberPageReference(final ResourceConfiguration resourceConfiguration,
      final PageReference pageReference, final Page page, final BytesOut<?> bufferedBytes) {
    try {
      if (bufferedBytes.writePosition() > 0) {
        flushBuffer(bufferedBytes);
      }

      isFirstUberPage = true;
      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, 0);
      isFirstUberPage = false;
      writePageReference(resourceConfiguration, pageReference, page, bufferedBytes, IOStorage.FIRST_BEACON >> 1);

      @SuppressWarnings("DataFlowIssue") final var buffer = ((ByteBuffer) bufferedBytes.underlyingObject()).rewind();
      buffer.limit((int) bufferedBytes.readLimit());
      dataFileChannel.write(buffer, 0L);
      dataFileChannel.force(false);
      bufferedBytes.clear();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }

  private void flushBuffer(BytesOut<?> bufferedBytes) throws IOException {
    final long fileSize = dataFileChannel.size();
    long offset;

    if (fileSize == 0) {
      offset = IOStorage.FIRST_BEACON;
      offset += (PAGE_FRAGMENT_BYTE_ALIGN - (offset % PAGE_FRAGMENT_BYTE_ALIGN));
    } else {
      offset = fileSize;
    }

    @SuppressWarnings("DataFlowIssue") final var buffer = ((ByteBuffer) bufferedBytes.underlyingObject()).rewind();
    buffer.limit((int) bufferedBytes.readLimit());
    dataFileChannel.write(buffer, offset);
    bufferedBytes.clear();
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
