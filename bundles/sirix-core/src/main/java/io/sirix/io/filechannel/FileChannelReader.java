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
 * File Reader. Used for {@link StorageEngineReader} to provide read only access on a
 * RandomAccessFile.
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
   * Striped buffer pool for reading page data - virtual-thread-friendly.
   * 
   * <p>
   * Unlike ThreadLocal which creates one buffer per thread (problematic with millions of virtual
   * threads), this uses a fixed number of buffers with striping. Memory usage is bounded:
   * O(STRIPE_COUNT) not O(num_threads).
   * 
   * <p>
   * The entire read-deserialize operation is synchronized on the stripe lock, ensuring the buffer
   * isn't reused until we're completely done with it.
   */
  private static final int STRIPE_COUNT = Runtime.getRuntime().availableProcessors() * 2;
  private static final ByteBuffer[] READ_BUFFERS = new ByteBuffer[STRIPE_COUNT];
  private static final Object[] STRIPE_LOCKS = new Object[STRIPE_COUNT];

  static {
    for (int i = 0; i < STRIPE_COUNT; i++) {
      STRIPE_LOCKS[i] = new Object();
      READ_BUFFERS[i] = ByteBuffer.allocateDirect(128 * 1024).order(ByteOrder.nativeOrder());
    }
  }

  /**
   * Get stripe index for current thread. Works correctly with both platform and virtual threads.
   */
  private static int getStripeIndex() {
    return (int) (Thread.currentThread().threadId() % STRIPE_COUNT);
  }

  /**
   * Constructor.
   *
   * @param dataFileChannel the data file channel
   * @param revisionsOffsetFileChannel the file, which holds pointers to the revision root pages
   * @param handler {@link ByteHandler} instance
   */
  public FileChannelReader(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final ByteHandler handler, final SerializationType type, final PagePersister pagePersistenter,
      final Cache<Integer, RevisionFileData> cache) {
    super(handler, pagePersistenter, type);
    this.dataFileChannel = dataFileChannel;
    this.revisionsOffsetFileChannel = revisionsOffsetFileChannel;
    this.cache = cache;
  }

  public Page read(final @NonNull PageReference reference,
      final @Nullable ResourceConfiguration resourceConfiguration) {
    final int stripe = getStripeIndex();

    // Synchronize on stripe lock for the ENTIRE read-deserialize operation.
    // This ensures the buffer isn't reused until deserialization is complete.
    // Safe with virtual threads: memory bounded by STRIPE_COUNT, not thread count.
    synchronized (STRIPE_LOCKS[stripe]) {
      try {
        final long position = reference.getKey();

        // Read page length header (4 bytes)
        ByteBuffer buffer = READ_BUFFERS[stripe];
        buffer.clear().limit(4);
        dataFileChannel.read(buffer, position);
        buffer.flip();
        final int dataLength = buffer.getInt();

        // Grow buffer if needed (rare - only for very large pages)
        if (buffer.capacity() < dataLength) {
          int newSize = Math.max(dataLength, buffer.capacity() * 2);
          buffer = ByteBuffer.allocateDirect(newSize).order(ByteOrder.nativeOrder());
          READ_BUFFERS[stripe] = buffer;
        }

        // Read page data
        buffer.clear().limit(dataLength);
        dataFileChannel.read(buffer, position + 4);
        buffer.flip();

        // Deserialize while holding the lock (buffer safe until we return)
        if (byteHandler.supportsMemorySegments()) {
          // Zero-copy: wrap direct ByteBuffer as MemorySegment
          MemorySegment segment = MemorySegment.ofBuffer(buffer);
          // Verify checksum for non-KVLP pages (KVLP verified after decompression)
          verifyChecksumIfNeeded(segment, reference, resourceConfiguration);
          // Pass reference for KVLP verification after decompression
          return deserializeFromSegment(resourceConfiguration, segment, reference);
        } else {
          // Fallback: copy to byte array for stream-based decompression
          final byte[] page = new byte[dataLength];
          buffer.get(page);
          // Verify checksum for non-KVLP pages (KVLP verified after decompression)
          verifyChecksumIfNeeded(page, reference, resourceConfiguration);
          // Pass reference for KVLP verification after decompression
          return deserialize(resourceConfiguration, page, reference);
        }
      } catch (final IOException e) {
        throw new SirixIOException(e);
      }
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
    final int stripe = getStripeIndex();

    // Synchronize entire read-deserialize operation (Loom-safe)
    synchronized (STRIPE_LOCKS[stripe]) {
      try {
        final var dataFileOffset = cache.get(revision, (unused) -> getRevisionFileData(revision)).offset();

        // Read page length header
        ByteBuffer buffer = READ_BUFFERS[stripe];
        buffer.clear().limit(4);
        dataFileChannel.read(buffer, dataFileOffset);
        buffer.flip();
        final int dataLength = buffer.getInt();

        // Grow buffer if needed
        if (buffer.capacity() < dataLength) {
          int newSize = Math.max(dataLength, buffer.capacity() * 2);
          buffer = ByteBuffer.allocateDirect(newSize).order(ByteOrder.nativeOrder());
          READ_BUFFERS[stripe] = buffer;
        }

        // Read page data
        buffer.clear().limit(dataLength);
        dataFileChannel.read(buffer, dataFileOffset + 4);
        buffer.flip();

        // Deserialize while holding the lock
        if (byteHandler.supportsMemorySegments()) {
          MemorySegment segment = MemorySegment.ofBuffer(buffer);
          return (RevisionRootPage) deserializeFromSegment(resourceConfiguration, segment);
        } else {
          final byte[] page = new byte[dataLength];
          buffer.get(page);
          return (RevisionRootPage) deserialize(resourceConfiguration, page);
        }
      } catch (IOException e) {
        throw new SirixIOException(e);
      }
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
