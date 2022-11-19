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

package org.sirix.io.iouring;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.hash.HashFunction;
import net.openhft.chronicle.bytes.Bytes;
import one.jasyncfio.AsyncFile;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.BytesUtils;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * File Reader. Used for {@link PageReadOnlyTrx} to provide read only access on a RandomAccessFile.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class IOUringReader implements Reader {

  /**
   * Inflater to decompress.
   */
  final ByteHandler byteHandler;

  /**
   * The hash function used to hash pages/page fragments.
   */
  final HashFunction hashFunction = Reader.hashFunction;

  /**
   * Data file.
   */
  private final AsyncFile dataFile;

  /**
   * Revisions offset file.
   */
  private final AsyncFile revisionsOffsetFile;

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
   * @param dataFile            the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param handler             {@link ByteHandler} instance
   */
  public IOUringReader(final AsyncFile dataFile, final AsyncFile revisionsOffsetFile, final ByteHandler handler,
      final SerializationType type, final PagePersister pagePersistenter,
      final Cache<Integer, RevisionFileData> cache) {
    this.dataFile = dataFile;
    this.revisionsOffsetFile = revisionsOffsetFile;
    byteHandler = checkNotNull(handler);
    this.type = checkNotNull(type);
    pagePersiter = checkNotNull(pagePersistenter);
    this.cache = cache;
  }

  public Page read(final @NonNull PageReference reference, final @Nullable PageReadOnlyTrx pageReadTrx) {
    try {
      return POOL.submit(() -> readPageFragment(reference, pageReadTrx)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public CompletableFuture<? extends Page> readAsync(final @NonNull PageReference reference,
      final @Nullable PageReadOnlyTrx pageReadTrx) {
      return CompletableFuture.supplyAsync(() -> readPageFragment(reference, pageReadTrx), POOL);
  }

  @NotNull
  private Page readPageFragment(@NotNull PageReference reference, @Nullable PageReadOnlyTrx pageReadTrx) {
    try {
      // Read page from file.
      ByteBuffer buffer = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.nativeOrder());

      final long position;

      switch (type) {
        case DATA -> {
          position = reference.getKey();
          dataFile.read(buffer, position).join();
        }
        case TRANSACTION_INTENT_LOG -> {
          position = reference.getPersistentLogKey();
          dataFile.read(buffer, position).join();
        }
        default -> throw new IllegalStateException();
      }
      buffer.flip();
      final int dataLength = buffer.getInt();

      buffer = ByteBuffer.allocateDirect(dataLength).order(ByteOrder.nativeOrder());

      dataFile.read(buffer, position + Integer.BYTES).join();
      buffer.flip();
      final byte[] page = new byte[dataLength];
      buffer.get(page);

      // Perform byte operations.
      return getPage(pageReadTrx, page);
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

      ByteBuffer buffer = ByteBuffer.allocateDirect(Integer.BYTES).order(ByteOrder.nativeOrder());
      dataFile.read(buffer, dataFileOffset).join();
      buffer.flip();
      final int dataLength = buffer.getInt();

      buffer = ByteBuffer.allocateDirect(dataLength).order(ByteOrder.nativeOrder());
      dataFile.read(buffer, dataFileOffset + Integer.BYTES).join();
      buffer.flip();
      final byte[] page = new byte[dataLength];
      buffer.get(page);

      // Perform byte operations.
      return (RevisionRootPage) getPage(pageReadTrx, page);
    } catch (IOException e) {
      throw new SirixIOException(e);
    }
  }

  @NotNull
  private Page getPage(PageReadOnlyTrx pageReadTrx, byte[] page) throws IOException {
    final var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page));
    final Bytes<ByteBuffer> input = Bytes.elasticByteBuffer();
    BytesUtils.doWrite(input, inputStream.readAllBytes());
    final var deserializedPage = pagePersiter.deserializePage(pageReadTrx, input, type);
    input.clear();
    return deserializedPage;
  }

  @Override
  public Instant readRevisionRootPageCommitTimestamp(int revision) {
    return cache.get(revision, (unused) -> getRevisionFileData(revision)).timestamp();
  }

  @Override
  public RevisionFileData getRevisionFileData(int revision) {
    final long fileOffset = (long) revision * Long.BYTES * 2 + IOStorage.FIRST_BEACON;
    final ByteBuffer buffer = ByteBuffer.allocateDirect(16).order(ByteOrder.nativeOrder());
    revisionsOffsetFile.read(buffer, fileOffset).join();
    buffer.flip();
    final var offset = buffer.getLong();
    buffer.position(Long.BYTES);
    final var timestamp = buffer.getLong();
    return new RevisionFileData(offset, Instant.ofEpochMilli(timestamp));
  }

  @Override
  public void close() {
  }

}
