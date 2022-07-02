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

package org.sirix.io.file;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.time.Instant;

import com.github.benmanes.caffeine.cache.Cache;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jetbrains.annotations.NotNull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.SerializationType;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.Page;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * File Reader. Used for {@link PageReadOnlyTrx} to provide read only access on a RandomAccessFile.
 *
 * @author Marc Kramis, Seabix
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger
 */
public final class FileReader implements Reader {

  /**
   * Inflater to decompress.
   */
  final ByteHandler byteHandler;

  /**
   * The hash function used to hash pages/page fragments.
   */
  final HashFunction hashFunction;

  /**
   * Data file.
   */
  private final RandomAccessFile dataFile;

  /**
   * Revisions offset file.
   */
  private final RandomAccessFile revisionsOffsetFile;

  /**
   * The type of data to serialize.
   */
  private final SerializationType serializationType;

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
   * @param byteHandler         {@link ByteHandler} instance
   * @throws SirixIOException if something bad happens
   */
  public FileReader(final RandomAccessFile dataFile, final RandomAccessFile revisionsOffsetFile,
      final ByteHandler byteHandler, final SerializationType serializationType, final PagePersister pagePersister,
      final Cache<Integer, RevisionFileData> cache) {
    hashFunction = Hashing.sha256();
    this.dataFile = checkNotNull(dataFile);

    this.revisionsOffsetFile = serializationType == SerializationType.DATA ? checkNotNull(revisionsOffsetFile) : null;
    this.byteHandler = checkNotNull(byteHandler);
    this.serializationType = checkNotNull(serializationType);
    this.pagePersiter = checkNotNull(pagePersister);
    this.cache = cache;
  }

  @Override
  public Page read(final @NonNull PageReference reference, final @Nullable PageReadOnlyTrx pageReadTrx) {
    try {
      // Read page from file.
      switch (serializationType) {
        case DATA -> dataFile.seek(reference.getKey());
        case TRANSACTION_INTENT_LOG -> dataFile.seek(reference.getPersistentLogKey());
        default -> {
        }
        // Must not happen.
      }

      final int dataLength = dataFile.readInt();
      //      reference.setLength(dataLength + FileReader.OTHER_BEACON);
      final byte[] page = new byte[dataLength];
      dataFile.read(page);

      // Perform byte operations.
      final DataInputStream input = new DataInputStream(byteHandler.deserialize(new ByteArrayInputStream(page)));

      // Return reader required to instantiate and deserialize page.
      return pagePersiter.deserializePage(input, pageReadTrx, serializationType);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public PageReference readUberPageReference() {
    final PageReference uberPageReference = new PageReference();
    try {
      // Read primary beacon.
      dataFile.seek(0);
      uberPageReference.setKey(0);

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
      final long offsetIntoDataFile;

      if (cache != null) {
        offsetIntoDataFile = cache.get(revision, (unused) -> getRevisionFileData(revision)).offset();
      } else {
        offsetIntoDataFile = getRevisionFileData(revision).offset();
      }

      dataFile.seek(offsetIntoDataFile);

      final int dataLength = dataFile.readInt();
      final byte[] page = new byte[dataLength];
      dataFile.read(page);

      // Perform byte operations.
      final DataInputStream input = new DataInputStream(byteHandler.deserialize(new ByteArrayInputStream(page)));

      // Return reader required to instantiate and deserialize page.
      return (RevisionRootPage) pagePersiter.deserializePage(input, pageReadTrx, serializationType);
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
      revisionsOffsetFile.seek(fileOffset);
      final long offset = revisionsOffsetFile.readLong();
      revisionsOffsetFile.seek(fileOffset + 8);
      final var timestamp = Instant.ofEpochMilli(revisionsOffsetFile.readLong());
      return new RevisionFileData(offset, timestamp);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() {
    if (cache != null) {
      cache.invalidateAll();
    }
    try {
      if (revisionsOffsetFile != null) {
        revisionsOffsetFile.close();
      }
      dataFile.close();
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }
}
