/*
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without modification, are permitted
 * provided that the following conditions are met: * Redistributions of source code must retain the
 * above copyright notice, this list of conditions and the following disclaimer. * Redistributions
 * in binary form must reproduce the above copyright notice, this list of conditions and the
 * following disclaimer in the documentation and/or other materials provided with the distribution.
 * * Neither the name of the University of Konstanz nor the names of its contributors may be used to
 * endorse or promote products derived from this software without specific prior written permission.
 * <p>
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

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.hash.HashFunction;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.io.AbstractReader;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.page.*;
import io.sirix.page.interfaces.Page;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySegment;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.time.Instant;

/**
 * File Reader. Used for {@link StorageEngineReader} to provide read only access on a RandomAccessFile.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class FileChannelReader extends AbstractReader {

  /**
   * The hash function used to hash pages/page fragments.
   */
  final HashFunction hashFunction = Reader.hashFunction;

  /**
   * Data file channel.
   */
  private final FileChannel dataFileChannel;

  /**
   * Revisions offset file channel.
   */
  private final FileChannel revisionsOffsetFileChannel;

  private final Cache<Integer, RevisionFileData> cache;
  
  /**
   * ThreadLocal reusable direct ByteBuffer for reading page data.
   * Using direct buffer avoids heap allocation and enables zero-copy path.
   * Initial size: 128KB (typical compressed page size).
   */
  private static final ThreadLocal<ByteBuffer> READ_BUFFER = 
      ThreadLocal.withInitial(() -> ByteBuffer.allocateDirect(128 * 1024).order(ByteOrder.nativeOrder()));

  /**
   * Constructor.
   *
   * @param dataFileChannel            the data file channel
   * @param revisionsOffsetFileChannel the file, which holds pointers to the revision root pages
   * @param handler                    {@link ByteHandler} instance
   */
  public FileChannelReader(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final ByteHandler handler, final SerializationType type, final PagePersister pagePersistenter,
      final Cache<Integer, RevisionFileData> cache) {
    super(handler, pagePersistenter, type);
    this.dataFileChannel = dataFileChannel;
    this.revisionsOffsetFileChannel = revisionsOffsetFileChannel;
    this.cache = cache;
  }

  public Page read(final @NonNull PageReference reference, final @Nullable ResourceConfiguration resourceConfiguration) {
    try {
      final long position = reference.getKey();
      
      // Read page length header (4 bytes)
      ByteBuffer headerBuffer = READ_BUFFER.get();
      headerBuffer.clear().limit(4);
      dataFileChannel.read(headerBuffer, position);
      headerBuffer.flip();
      final int dataLength = headerBuffer.getInt();
      
      // Get or grow the read buffer if needed
      ByteBuffer dataBuffer = READ_BUFFER.get();
      if (dataBuffer.capacity() < dataLength) {
        // Grow buffer with headroom to reduce future reallocations
        int newSize = Math.max(dataLength, dataBuffer.capacity() * 2);
        dataBuffer = ByteBuffer.allocateDirect(newSize).order(ByteOrder.nativeOrder());
        READ_BUFFER.set(dataBuffer);
      }
      
      // Read page data
      dataBuffer.clear().limit(dataLength);
      dataFileChannel.read(dataBuffer, position + 4);
      dataBuffer.flip();
      
      // Use zero-copy MemorySegment path if ByteHandler supports it
      if (byteHandler.supportsMemorySegments()) {
        // Wrap the direct ByteBuffer as a MemorySegment (zero-copy!)
        MemorySegment segment = MemorySegment.ofBuffer(dataBuffer);
        return deserializeFromSegment(resourceConfiguration, segment);
      } else {
        // Fallback: copy to byte array for stream-based decompression
        final byte[] page = new byte[dataLength];
        dataBuffer.get(page);
        return deserialize(resourceConfiguration, page);
      }
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public PageReference readUberPageReference() {
    final PageReference uberPageReference = new PageReference();
    uberPageReference.setKey(0);
    final UberPage page = (UberPage) read(uberPageReference, null);
    uberPageReference.setPage(page);
    return uberPageReference;
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final ResourceConfiguration resourceConfiguration) {
    try {
      final var dataFileOffset = cache.get(revision, (unused) -> getRevisionFileData(revision)).offset();

      // Read page length header using reusable buffer
      ByteBuffer headerBuffer = READ_BUFFER.get();
      headerBuffer.clear().limit(4);
      dataFileChannel.read(headerBuffer, dataFileOffset);
      headerBuffer.flip();
      final int dataLength = headerBuffer.getInt();

      // Get or grow the read buffer if needed
      ByteBuffer dataBuffer = READ_BUFFER.get();
      if (dataBuffer.capacity() < dataLength) {
        int newSize = Math.max(dataLength, dataBuffer.capacity() * 2);
        dataBuffer = ByteBuffer.allocateDirect(newSize).order(ByteOrder.nativeOrder());
        READ_BUFFER.set(dataBuffer);
      }
      
      // Read page data
      dataBuffer.clear().limit(dataLength);
      dataFileChannel.read(dataBuffer, dataFileOffset + 4);
      dataBuffer.flip();

      // Use zero-copy MemorySegment path if ByteHandler supports it
      if (byteHandler.supportsMemorySegments()) {
        MemorySegment segment = MemorySegment.ofBuffer(dataBuffer);
        return (RevisionRootPage) deserializeFromSegment(resourceConfiguration, segment);
      } else {
        // Fallback: copy to byte array
        final byte[] page = new byte[dataLength];
        dataBuffer.get(page);
        return (RevisionRootPage) deserialize(resourceConfiguration, page);
      }
    } catch (IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Instant readRevisionRootPageCommitTimestamp(int revision) {
    return cache.get(revision, (_) -> getRevisionFileData(revision)).timestamp();
  }

  @Override
  public RevisionFileData getRevisionFileData(int revision) {
    try {
      final var fileOffset = revision * 8 * 2 + IOStorage.FIRST_BEACON;
      final ByteBuffer buffer = ByteBuffer.allocateDirect(16).order(ByteOrder.nativeOrder());
      revisionsOffsetFileChannel.read(buffer, fileOffset);
      buffer.position(8);
      revisionsOffsetFileChannel.read(buffer, fileOffset + 8);
      buffer.flip();
      final var offset = buffer.getLong();
      buffer.position(8);
      final var timestamp = buffer.getLong();
      return new RevisionFileData(offset, Instant.ofEpochMilli(timestamp));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    try {
      dataFileChannel.close();
      revisionsOffsetFileChannel.close();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

}
