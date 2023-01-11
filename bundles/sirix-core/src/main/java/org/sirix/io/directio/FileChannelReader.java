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

package org.sirix.io.directio;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.hash.HashFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractReader;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.time.Instant;

/**
 * File Reader. Used for {@link PageReadOnlyTrx} to provide read only access on a RandomAccessFile.
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
   * Constructor.
   *
   * @param dataFileChannel            the data file channel
   * @param revisionsOffsetFileChannel the file, which holds pointers to the revision root pages
   * @param handler                    {@link ByteHandler} instance
   */
  public FileChannelReader(final FileChannel dataFileChannel, final FileChannel revisionsOffsetFileChannel,
      final ByteHandler handler, final SerializationType type, final PagePersister pagePersistenter,
      final Cache<Integer, RevisionFileData> cache) {
    super (handler, pagePersistenter, type);
    this.dataFileChannel = dataFileChannel;
    this.revisionsOffsetFileChannel = revisionsOffsetFileChannel;
    this.cache = cache;
  }

  public Page read(final @NonNull PageReference reference,
      final @Nullable PageReadOnlyTrx pageReadTrx) {
    try {
      // Read page from file.
      ByteBuffer buffer = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.nativeOrder());

      final long position = reference.getKey();
      dataFileChannel.read(buffer, position);

      buffer.flip();
      final int dataLength = buffer.getInt();

      buffer = ByteBuffer.allocate(dataLength).order(ByteOrder.nativeOrder());

      dataFileChannel.read(buffer, position + 4);
      buffer.flip();
      final byte[] page = buffer.array();

      // Perform byte operations.
      return deserialize(pageReadTrx, page);
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
  public RevisionRootPage readRevisionRootPage(final int revision, final PageReadOnlyTrx pageReadTrx) {
    try {
      final var dataFileOffset = cache.get(revision, (unused) -> getRevisionFileData(revision)).offset();

      ByteBuffer buffer = ByteBuffer.allocateDirect(4).order(ByteOrder.nativeOrder());
      dataFileChannel.read(buffer, dataFileOffset);
      buffer.flip();
      final int dataLength = buffer.getInt();

      buffer = ByteBuffer.allocateDirect(dataLength).order(ByteOrder.nativeOrder());
      dataFileChannel.read(buffer, dataFileOffset + 4);
      buffer.flip();
      final byte[] page = new byte[dataLength];
      buffer.get(page);

      // Perform byte operations.
      return (RevisionRootPage) deserialize(pageReadTrx, page);
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
  }

}
