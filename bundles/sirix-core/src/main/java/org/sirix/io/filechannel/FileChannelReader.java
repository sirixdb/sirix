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

package org.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.time.Instant;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * File Reader. Used for {@link PageReadOnlyTrx} to provide read only access on a RandomAccessFile.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class FileChannelReader implements Reader {

  /**
   * Beacon of first references.
   */
  final static int FIRST_BEACON = 12;

  /**
   * Beacon of the other references.
   */
  final static int OTHER_BEACON = 4;

  /**
   * Inflater to decompress.
   */
  final ByteHandler byteHandler;

  /**
   * The hash function used to hash pages/page fragments.
   */
  final HashFunction hashFunction;

  /**
   * Data file channel.
   */
  private final FileChannel dataFileChannel;

  /**
   * Revisions offset file channel.
   */
  private final FileChannel revisionsOffsetFileChannel;

  /**
   * The type of data to serialize.
   */
  private final SerializationType type;

  /**
   * Used to serialize/deserialze pages.
   */
  private final PagePersister pagePersiter;

  private final Cache<Integer, RevisionFileData> cache;

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
    hashFunction = Hashing.sha256();
    this.dataFileChannel = dataFileChannel;
    this.revisionsOffsetFileChannel = revisionsOffsetFileChannel;
    byteHandler = checkNotNull(handler);
    this.type = checkNotNull(type);
    pagePersiter = checkNotNull(pagePersistenter);
    this.cache = cache;
  }

  @Override
  public Page read(final @NonNull PageReference reference, final @Nullable PageReadOnlyTrx pageReadTrx) {
    try {
      // Read page from file.
      ByteBuffer buffer = ByteBuffer.allocate(4);

      final long position;

      switch (type) {
        case DATA -> {
          position = reference.getKey();
          dataFileChannel.read(buffer, position);
        }
        case TRANSACTION_INTENT_LOG -> {
          position = reference.getPersistentLogKey();
          dataFileChannel.read(buffer, position);
        }
        default ->
          // Must not happen.
            throw new IllegalStateException();
      }

      buffer.position(0);
      final int dataLength = buffer.getInt();

      //      reference.setLength(dataLength + FileChannelReader.OTHER_BEACON);
      final byte[] page = new byte[dataLength];

      buffer = ByteBuffer.allocate(dataLength);

      dataFileChannel.read(buffer, position + 4);
      buffer.position(0);
      buffer.get(page);

      // Perform byte operations.
      final DataInputStream input = new DataInputStream(byteHandler.deserialize(new ByteArrayInputStream(page)));

      // Return reader required to instantiate and deserialize page.
      return pagePersiter.deserializePage(input, pageReadTrx, type);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public PageReference readUberPageReference() {
    final PageReference uberPageReference = new PageReference();
    try {
      // Read primary beacon.
      ByteBuffer buffer = ByteBuffer.allocate(8);
      dataFileChannel.read(buffer, 0);
      buffer.position(0);
      uberPageReference.setKey(buffer.getLong());

      final UberPage page = (UberPage) read(uberPageReference, null);
      uberPageReference.setPage(page);
      return uberPageReference;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final PageReadOnlyTrx pageReadTrx) {
    try {
      final var dataFileOffset = cache.get(revision, (unused) -> getRevisionFileData(revision)).offset();

      ByteBuffer buffer = ByteBuffer.allocate(4);
      dataFileChannel.read(buffer, dataFileOffset);
      buffer.position(0);
      final int dataLength = buffer.getInt();
      final byte[] page = new byte[dataLength];

      buffer = ByteBuffer.allocate(dataLength);
      dataFileChannel.read(buffer, dataFileOffset + 4);
      buffer.position(0);
      buffer.get(page);

      // Perform byte operations.
      final DataInputStream input = new DataInputStream(byteHandler.deserialize(new ByteArrayInputStream(page)));

      // Return reader required to instantiate and deserialize page.
      return (RevisionRootPage) pagePersiter.deserializePage(input, pageReadTrx, type);
    } catch (IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Instant readRevisionRootPageCommitTimestamp(int revision) {
    return cache.get(revision, (unused) -> getRevisionFileData(revision)).timestamp();
  }

  @Override
  public RevisionFileData getRevisionFileData(int revision) {
    try {
      final var fileOffset = revision * 8 * 2;
      final ByteBuffer buffer = ByteBuffer.allocate(16);
      revisionsOffsetFileChannel.read(buffer, fileOffset);
      buffer.position(8);
      revisionsOffsetFileChannel.read(buffer, fileOffset + 8);
      buffer.position(0);
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
    cache.invalidateAll();
  }
}
