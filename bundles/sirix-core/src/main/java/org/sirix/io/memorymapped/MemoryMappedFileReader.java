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
 * @author Johannes Lichtenberger
 */
public final class MemoryMappedFileReader implements Reader {

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
   * Data segment.
   */
  private final MemorySegment dataFileSegment;

  /**
   * Revisions offset segment.
   */
  private final MemorySegment revisionsOffsetSegment;

  /**
   * The type of data to serialize.
   */
  private final SerializationType type;

  /**
   * Used to serialize/deserialze pages.
   */
  private final PagePersister pagePersiter;

  /**
   * Constructor.
   *
   * @param dataFile            the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param handler             {@link ByteHandler} instance
   * @throws SirixIOException if something bad happens
   */
  public MemoryMappedFileReader(final Path dataFile, final Path revisionsOffsetFile, final ByteHandler handler,
      final SerializationType type, final PagePersister pagePersistenter) throws IOException {
    hashFunction = Hashing.sha256();
    dataFileSegment =
        MemorySegment.mapFromPath(checkNotNull(dataFile), dataFile.toFile().length(), FileChannel.MapMode.READ_ONLY);
    revisionsOffsetSegment =
        type == SerializationType.DATA ? MemorySegment.mapFromPath(checkNotNull(revisionsOffsetFile),
                                                                   revisionsOffsetFile.toFile().length(),
                                                                   FileChannel.MapMode.READ_ONLY) : null;
    byteHandler = checkNotNull(handler);
    this.type = checkNotNull(type);
    pagePersiter = checkNotNull(pagePersistenter);
  }

  @Override
  public Page read(final @Nonnull PageReference reference, final @Nullable PageReadOnlyTrx pageReadTrx) {
    try {
      final VarHandle intVarHandle = MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());
      final MemoryAddress baseAddress = dataFileSegment.baseAddress();

      // Must not happen.
      final MemoryAddress baseAddressPlusOffsetPlusInt;
      final int dataLength = switch (type) {
        case DATA -> {
          baseAddressPlusOffsetPlusInt = baseAddress.addOffset(reference.getKey() + 4);
          yield (int) intVarHandle.get(baseAddress.addOffset(reference.getKey()));
        }
        case TRANSACTION_INTENT_LOG -> {
          baseAddressPlusOffsetPlusInt = baseAddress.addOffset(reference.getPersistentLogKey() + 4);
          yield (int) intVarHandle.get(baseAddress.addOffset(reference.getPersistentLogKey()));
        }
        default -> throw new AssertionError();
      };

      reference.setLength(dataLength + MemoryMappedFileReader.OTHER_BEACON);
      final byte[] page = new byte[dataLength];

      final VarHandle byteVarHandle = MemoryLayout.ofSequence(MemoryLayouts.JAVA_BYTE).varHandle(byte.class, MemoryLayout.PathElement
          .sequenceElement());

      for (int i = 0; i < dataLength; i++) {
        page[i] = (byte) byteVarHandle.get(baseAddressPlusOffsetPlusInt, (long) i);
      }

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

    // Read primary beacon.
    final VarHandle longVarHandle = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());
    final MemoryAddress baseAddress = dataFileSegment.baseAddress();

    uberPageReference.setKey((long) longVarHandle.get(baseAddress));

    final UberPage page = (UberPage) read(uberPageReference, null);
    uberPageReference.setPage(page);
    return uberPageReference;
  }

  @Override
  public RevisionRootPage readRevisionRootPage(final int revision, final PageReadOnlyTrx pageReadTrx) {
    try {
      final VarHandle longVarHandle = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());
      final MemoryAddress revisionFileSegmentBaseAddress = dataFileSegment.baseAddress();
      final MemoryAddress dataFileSegmentBaseAddress = dataFileSegment.baseAddress();

      final long dataFileOffset = (long) longVarHandle.get(revisionFileSegmentBaseAddress.addOffset(revision * 8));

      final VarHandle intVarHandle = MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

      final int dataLength = (int) intVarHandle.get(dataFileSegmentBaseAddress.addOffset(dataFileOffset));

      final VarHandle byteVarHandle = MemoryHandles.varHandle(byte.class, ByteOrder.nativeOrder());

      final byte[] page = new byte[dataLength];

      for (int i = 0; i < dataLength; i++) {
        page[i] = (byte) byteVarHandle.get(dataFileSegmentBaseAddress.addOffset(dataFileOffset + 5 + i));
      }

      // Perform byte operations.
      final DataInputStream input = new DataInputStream(byteHandler.deserialize(new ByteArrayInputStream(page)));

      // Return reader required to instantiate and deserialize page.
      return (RevisionRootPage) pagePersiter.deserializePage(input, pageReadTrx, type);
    } catch (IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() throws SirixIOException {
    if (revisionsOffsetSegment != null) {
      revisionsOffsetSegment.close();
    }
    dataFileSegment.close();
  }
}
