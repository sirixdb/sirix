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
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractReader;
import org.sirix.io.IOStorage;
import org.sirix.io.RevisionFileData;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.SerializationType;
import org.sirix.page.interfaces.Page;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.time.Instant;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reader, to read from a memory-mapped file.
 *
 * @author Johannes Lichtenberger
 */
public final class MMFileReader extends AbstractReader {

  static final ValueLayout.OfByte LAYOUT_BYTE = ValueLayout.JAVA_BYTE;
  static final ValueLayout.OfInt LAYOUT_INT = ValueLayout.JAVA_INT;
  static final ValueLayout.OfLong LAYOUT_LONG = ValueLayout.JAVA_LONG;

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
    super(byteHandler, pagePersistenter, type);
    this.dataFileSegment = checkNotNull(dataFileSegment);
    this.revisionsOffsetFileSegment = checkNotNull(revisionFileSegment);
    this.cache = checkNotNull(cache);
  }

  @Override
  public Page read(final @NonNull PageReference reference, final @Nullable PageReadOnlyTrx pageReadTrx) {
    try {
      final long offset = reference.getKey() + LAYOUT_INT.byteSize();
      final int dataLength = dataFileSegment.get(LAYOUT_INT, reference.getKey());

      final byte[] page = new byte[dataLength];

      MemorySegment.copy(dataFileSegment, LAYOUT_BYTE, offset, page, 0, dataLength);

      return deserialize(pageReadTrx, page);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final PageReadOnlyTrx pageReadTrx) {
    try {
      //noinspection DataFlowIssue
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
    //noinspection DataFlowIssue
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

  @Override
  public void close() {
    dataFileSegment.session().close();
  }
}
