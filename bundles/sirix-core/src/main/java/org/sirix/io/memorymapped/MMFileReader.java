/*
 * Copyright (c) 2020, SirixDB All rights reserved.
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

package org.sirix.io.memorymapped;

import com.github.benmanes.caffeine.cache.Cache;
import com.google.common.hash.HashFunction;
import net.openhft.chronicle.bytes.Bytes;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
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
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.time.Instant;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reader, to read from a memory-mapped file.
 *
 * @author Johannes Lichtenberger
 */
public final class MMFileReader implements Reader {

  static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
  static final ValueLayout.OfInt LAYOUT_INT = ValueLayout.JAVA_INT;
  static final ValueLayout.OfLong LAYOUT_LONG = ValueLayout.JAVA_LONG;

  /**
   * Inflater to decompress.
   */
  final ByteHandler byteHandler;

  /**
   * The hash function used to hash pages/page fragments.
   */
  final HashFunction hashFunction = Reader.hashFunction;

  /**
   * The type of data to serialize.
   */
  private final SerializationType type;

  /**
   * Used to serialize/deserialize pages.
   */
  private final PagePersister pagePersitenter;
  private final MemorySegment dataFileSegment;

  private final MemorySegment revisionsOffsetFileSegment;

  private final Cache<Integer, RevisionFileData> cache;

  /**
   * Constructor.
   *
   * @param byteHandler {@link ByteHandler} instance
   */
  public MMFileReader(final MemorySegment dataFileSegment, final MemorySegment revisionFileSegment,
      final ByteHandler byteHandler, final SerializationType type, final PagePersister pagePersistenter,
      final Cache<Integer, RevisionFileData> cache) {
    this.byteHandler = checkNotNull(byteHandler);
    this.type = checkNotNull(type);
    this.pagePersitenter = checkNotNull(pagePersistenter);
    this.dataFileSegment = checkNotNull(dataFileSegment);
    this.revisionsOffsetFileSegment = checkNotNull(revisionFileSegment);
    this.cache = checkNotNull(cache);
  }

  @Override
  public Page read(final @NonNull PageReference reference,
      final @Nullable PageReadOnlyTrx pageReadTrx) {
    try {
      long offset;

      final int dataLength = switch (type) {
        case DATA -> {
          if (reference.getKey() < 0) {
            throw new SirixIOException("Reference key is not valid: " + reference.getKey());
          }
          offset = reference.getKey() + LAYOUT_INT.byteSize();
          yield dataFileSegment.get(LAYOUT_INT, reference.getKey());
        }
        case TRANSACTION_INTENT_LOG -> {
          if (reference.getLogKey() < 0) {
            throw new SirixIOException("Reference log key is not valid: " + reference.getPersistentLogKey());
          }
          offset = reference.getPersistentLogKey() + LAYOUT_INT.byteSize();
          yield dataFileSegment.get(LAYOUT_INT, reference.getPersistentLogKey());
        }
        default -> throw new AssertionError();
      };

      final byte[] page = new byte[dataLength];

      MemorySegment.copy(dataFileSegment, LAYOUT_BYTE, offset, page, 0, dataLength);

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

      final int dataLength = dataFileSegment.get(LAYOUT_INT, dataFileOffset);

      final byte[] page = new byte[dataLength];

      MemorySegment.copy(dataFileSegment, LAYOUT_BYTE, dataFileOffset + LAYOUT_INT.byteSize(), page, 0, dataLength);

      return (RevisionRootPage) deserialize(pageReadTrx, page);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Instant readRevisionRootPageCommitTimestamp(int revision) {
    return cache.get(revision, (unused) -> getRevisionFileData(revision)).timestamp();
  }

  @Override
  public RevisionFileData getRevisionFileData(int revision) {
    final var fileOffset = IOStorage.FIRST_BEACON + (revision * LAYOUT_LONG.byteSize() * 2);
    final var revisionOffset = revisionsOffsetFileSegment.get(LAYOUT_LONG, fileOffset);
    final var timestamp =
        Instant.ofEpochMilli(revisionsOffsetFileSegment.get(LAYOUT_LONG, fileOffset + LAYOUT_LONG.byteSize()));
    return new RevisionFileData(revisionOffset, timestamp);
  }

  private Page deserialize(PageReadOnlyTrx pageReadTrx, byte[] page) throws IOException {
    // perform byte operations
    final var inputStream = byteHandler.deserialize(new ByteArrayInputStream(page));
    final Bytes<ByteBuffer> input = Bytes.elasticByteBuffer();
    BytesUtils.doWrite(input, inputStream.readAllBytes());
    final var deserializedPage = pagePersitenter.deserializePage(pageReadTrx, input, type);
    input.clear();
    return deserializedPage;
  }

  @Override
  public void close() {
    dataFileSegment.session().close();
  }
}
