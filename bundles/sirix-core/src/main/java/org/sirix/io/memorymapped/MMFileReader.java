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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import jdk.incubator.foreign.*;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.exception.SirixIOException;
import org.sirix.io.Reader;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Reader, to read from a memory-mapped file.
 *
 * @author Johannes Lichtenberger
 */
public final class MMFileReader implements Reader {

  /**
   * Beacon of first references.
   */
  final static int FIRST_BEACON = 12;

  /**
   * Inflater to decompress.
   */
  final ByteHandler byteHandler;

  /**
   * The hash function used to hash pages/page fragments.
   */
  final HashFunction hashFunction;

  /**
   * The type of data to serialize.
   */
  private final SerializationType type;

  /**
   * Used to serialize/deserialze pages.
   */
  private final PagePersister pagePersiter;

  private MemorySegment dataFileSegment;

  private MemorySegment revisionFileSegment;

  /**
   * Constructor.
   *
   * @param dataFile            the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param handler             {@link ByteHandler} instance
   */
  public MMFileReader(final Path dataFile, final Path revisionsOffsetFile, final ByteHandler handler,
      final SerializationType type, final PagePersister pagePersistenter) throws IOException {
    hashFunction = Hashing.sha256();
    byteHandler = checkNotNull(handler);
    this.type = checkNotNull(type);
    pagePersiter = checkNotNull(pagePersistenter);
    dataFileSegment =
        MemorySegment.mapFile(checkNotNull(dataFile), 0, dataFile.toFile().length(), FileChannel.MapMode.READ_ONLY);
    revisionFileSegment = MemorySegment.mapFile(revisionsOffsetFile, 0, revisionsOffsetFile.toFile().length(),
        FileChannel.MapMode.READ_ONLY);
  }

  /**
   * Constructor.
   *
   * @param handler {@link ByteHandler} instance
   */
  public MMFileReader(final MemorySegment dataFileSegment, final MemorySegment revisionFileSegment,
      final ByteHandler handler, final SerializationType type, final PagePersister pagePersistenter) {
    hashFunction = Hashing.sha256();
    byteHandler = checkNotNull(handler);
    this.type = checkNotNull(type);
    pagePersiter = checkNotNull(pagePersistenter);
    this.dataFileSegment = dataFileSegment;
    this.revisionFileSegment = revisionFileSegment;
  }

  @Override
  public Page read(final @Nonnull PageReference reference, final @Nullable PageReadOnlyTrx pageReadTrx) {
    try {
      long offset;

      final int dataLength = switch (type) {
        case DATA -> {
          if (reference.getKey() < 0) {
            throw new SirixIOException("Reference key is not valid: " + reference.getKey());
          }
          offset = reference.getKey() + 4;
          yield MemoryAccess.getIntAtOffset(dataFileSegment, reference.getKey());
        }
        case TRANSACTION_INTENT_LOG -> {
          if (reference.getLogKey() < 0) {
            throw new SirixIOException("Reference log key is not valid: " + reference.getPersistentLogKey());
          }
          offset = reference.getPersistentLogKey() + 4;
          yield MemoryAccess.getIntAtOffset(dataFileSegment, reference.getPersistentLogKey());
        }
        default -> throw new AssertionError();
      };

      //      reference.setLength(dataLength + MMFileReader.OTHER_BEACON);
      final byte[] page = new byte[dataLength];

      for (int i = 0; i < dataLength; i++) {
        page[i] = MemoryAccess.getByteAtOffset(dataFileSegment, offset + (long)i);
      }

      return deserialize(pageReadTrx, page);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public PageReference readUberPageReference() {
    final PageReference uberPageReference = new PageReference();

    uberPageReference.setKey(MemoryAccess.getLong(dataFileSegment));

    final UberPage page = (UberPage) read(uberPageReference, null);
    uberPageReference.setPage(page);
    return uberPageReference;
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final PageReadOnlyTrx pageReadTrx) {
    try {
      final long dataFileOffset = MemoryAccess.getLongAtOffset(revisionFileSegment, revision * 8);
      final int dataLength = MemoryAccess.getIntAtOffset(dataFileSegment, dataFileOffset);

      final byte[] page = new byte[dataLength];

      for (int i = 0; i < dataLength; i++) {
        page[i] = MemoryAccess.getByteAtOffset(dataFileSegment, dataFileOffset + 4L + (long)i);
      }

      return (RevisionRootPage) deserialize(pageReadTrx, page);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private Page deserialize(PageReadOnlyTrx pageReadTrx, byte[] page) throws IOException {
    // perform byte operations
    final DataInputStream input = new DataInputStream(byteHandler.deserialize(new ByteArrayInputStream(page)));

    // return deserialized page
    return pagePersiter.deserializePage(input, pageReadTrx, type);
  }

  @Override
  public void close() {
    if (dataFileSegment != null && dataFileSegment.isAlive()) {
      dataFileSegment.close();
    }
    if (revisionFileSegment != null && revisionFileSegment.isAlive()) {
      revisionFileSegment.close();
    }
  }

  public void setDataSegment(MemorySegment dataSegment) {
    this.dataFileSegment = dataSegment;
  }
}
