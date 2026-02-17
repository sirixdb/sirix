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

package io.sirix.io.iouring;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.hash.HashFunction;
import io.sirix.access.ResourceConfiguration;
import io.sirix.api.StorageEngineReader;
import io.sirix.exception.SirixIOException;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.page.PagePersister;
import io.sirix.page.PageReference;
import io.sirix.page.RevisionRootPage;
import io.sirix.page.SerializationType;
import one.jasyncfio.AsyncFile;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import io.sirix.io.AbstractReader;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.page.interfaces.Page;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * File Reader. Used for {@link StorageEngineReader} to provide read only access on a
 * RandomAccessFile.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class IOUringReader extends AbstractReader {

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

  private final Cache<Integer, RevisionFileData> cache;

  /**
   * Constructor.
   *
   * @param dataFile the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param handler {@link ByteHandler} instance
   */
  public IOUringReader(final AsyncFile dataFile, final AsyncFile revisionsOffsetFile, final ByteHandler handler,
      final SerializationType type, final PagePersister pagePersistenter,
      final Cache<Integer, RevisionFileData> cache) {
    super(handler, pagePersistenter, type);
    this.dataFile = dataFile;
    this.revisionsOffsetFile = revisionsOffsetFile;
    this.cache = cache;
  }

  public Page read(final @NonNull PageReference reference,
      final @Nullable ResourceConfiguration resourceConfiguration) {
    try {
      return POOL.submit(() -> readPageFragment(reference, resourceConfiguration)).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public CompletableFuture<? extends Page> readAsync(final @NonNull PageReference reference,
      final @Nullable ResourceConfiguration resourceConfiguration) {
    return CompletableFuture.supplyAsync(() -> readPageFragment(reference, resourceConfiguration), POOL);
  }

  @NonNull
  private Page readPageFragment(@NonNull PageReference reference,
      @Nullable ResourceConfiguration resourceConfiguration) {
    try {
      // Read page from file.
      ByteBuffer buffer = ByteBuffer.allocateDirect(IOStorage.OTHER_BEACON).order(ByteOrder.nativeOrder());

      final long position = reference.getKey();
      dataFile.read(buffer, position).join();

      buffer.flip();
      final int dataLength = buffer.getInt();

      buffer = ByteBuffer.allocateDirect(dataLength).order(ByteOrder.nativeOrder());

      dataFile.read(buffer, position + Integer.BYTES).join();
      buffer.flip();
      final byte[] page = new byte[dataLength];
      buffer.get(page);

      // Verify checksum for non-KVLP pages (KVLP verified after decompression)
      verifyChecksumIfNeeded(page, reference, resourceConfiguration);

      // Perform byte operations (pass reference for KVLP verification after decompression)
      return deserialize(resourceConfiguration, page, reference);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final ResourceConfiguration resourceConfiguration) {
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
      return (RevisionRootPage) deserialize(resourceConfiguration, page);
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
  public void close() {}

}
