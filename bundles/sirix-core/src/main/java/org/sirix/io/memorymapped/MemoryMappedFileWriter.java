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

package org.sirix.io.memorymapped;

import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemoryHandles;
import jdk.incubator.foreign.MemorySegment;
import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import java.io.*;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * File Writer for providing read/write access for file as a Sirix backend.
 *
 * @author Johannes Lichtenberger
 */
public final class MemoryMappedFileWriter extends AbstractForwardingReader implements Writer {

  private static final short REVISION_ROOT_PAGE_BYTE_ALIGN = 256;

  private static final byte PAGE_FRAGMENT_BYTE_ALIGN = 8;

  /**
   * Random access to work on.
   */
  private final MemorySegment dataFileSegment;

  /**
   * {@link MemoryMappedFileReader} reference for this writer.
   */
  private final MemoryMappedFileReader reader;

  private final SerializationType type;

  private final MemorySegment revisionsOffsetSegment;

  private final PagePersister pagePersister;

  /**
   * Constructor.
   *
   * @param dataFile            the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param handler             the byte handler
   * @param serializationType   the serialization type (for the transaction log or the data file)
   * @param pagePersister       transforms in-memory pages into byte-arrays and back
   */
  public MemoryMappedFileWriter(final Path dataFile, final Path revisionsOffsetFile, final ByteHandler handler,
      final SerializationType serializationType, final PagePersister pagePersister) throws IOException {
    dataFileSegment =
        MemorySegment.mapFromPath(checkNotNull(dataFile), dataFile.toFile().length(), FileChannel.MapMode.READ_WRITE);
    type = checkNotNull(serializationType);
    revisionsOffsetSegment =
        type == SerializationType.DATA ? MemorySegment.mapFromPath(checkNotNull(revisionsOffsetFile),
                                                                   revisionsOffsetFile.toFile().length(),
                                                                   FileChannel.MapMode.READ_WRITE) : null;
    this.pagePersister = checkNotNull(pagePersister);
    reader = new MemoryMappedFileReader(dataFile,
                                        revisionsOffsetFile,
                                        handler,
                                        serializationType,
                                        pagePersister);
  }

  @Override
  public Writer truncateTo(final int revision) {
    UberPage uberPage = (UberPage) reader.readUberPageReference().getPage();

    while (uberPage.getRevisionNumber() != revision) {
      uberPage = (UberPage) reader.read(new PageReference().setKey(uberPage.getPreviousUberPageKey()), null);
      if (uberPage.getRevisionNumber() == revision) {
        // FIXME
//        try {
//          //dataFileSegment.setLength(uberPage.getPreviousUberPageKey());
//        } catch (final IOException e) {
//          throw new UncheckedIOException(e);
//        }
        break;
      }
    }

    return this;
  }

  /**
   * Write page contained in page reference to storage.
   *
   * @param pageReference page reference to write
   * @throws SirixIOException if errors during writing occur
   */
  @Override
  public MemoryMappedFileWriter write(final PageReference pageReference) throws SirixIOException {
    // Perform byte operations.
    try {
      // Serialize page.
      final Page page = pageReference.getPage();
      assert page != null;

      final byte[] serializedPage;

      try (final ByteArrayOutputStream output = new ByteArrayOutputStream();
           final DataOutputStream dataOutput = new DataOutputStream(reader.byteHandler.serialize(output))) {
        pagePersister.serializePage(dataOutput, page, type);
        dataOutput.flush();
        serializedPage = output.toByteArray();
      }

      final byte[] writtenPage = new byte[serializedPage.length + MemoryMappedFileReader.OTHER_BEACON];
      final ByteBuffer buffer = ByteBuffer.allocate(writtenPage.length);
      buffer.putInt(serializedPage.length);
      buffer.put(serializedPage);
      buffer.position(0);
      buffer.get(writtenPage);

      // Getting actual offset and appending to the end of the current file.
      final long fileSize = dataFileSegment.byteSize();
      long offset = fileSize == 0 ? MemoryMappedFileReader.FIRST_BEACON : fileSize;
      if (type == SerializationType.DATA) {
        if (page instanceof RevisionRootPage) {
          if (offset % REVISION_ROOT_PAGE_BYTE_ALIGN != 0) {
            offset += REVISION_ROOT_PAGE_BYTE_ALIGN - (offset % REVISION_ROOT_PAGE_BYTE_ALIGN);
          }
        } else if (offset % PAGE_FRAGMENT_BYTE_ALIGN != 0) {
          offset += PAGE_FRAGMENT_BYTE_ALIGN - (offset % PAGE_FRAGMENT_BYTE_ALIGN);
        }
      }

      final MemoryAddress dataFileSegmentBaseAddress = dataFileSegment.baseAddress();
      final VarHandle byteVarHandle = MemoryHandles.varHandle(byte.class, ByteOrder.nativeOrder());

      for (int i = 0; i < writtenPage.length; i++) {
        byteVarHandle.set(dataFileSegmentBaseAddress.addOffset(offset + i), writtenPage[i]);
      }

      // Remember page coordinates.
      switch (type) {
        case DATA:
          pageReference.setKey(offset);
          break;
        case TRANSACTION_INTENT_LOG:
          pageReference.setPersistentLogKey(offset);
          break;
        default:
          // Must not happen.
      }

      pageReference.setLength(writtenPage.length);
      pageReference.setHash(reader.hashFunction.hashBytes(writtenPage).asBytes());

      if (type == SerializationType.DATA && page instanceof RevisionRootPage) {
        final MemoryAddress revisionFileSegmentBaseAddress = revisionsOffsetSegment.baseAddress();
        final VarHandle longVarHandle = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());

        longVarHandle.set(revisionFileSegmentBaseAddress.addOffset(revisionsOffsetSegment.byteSize()), offset);
      }

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public void close() {
    if (dataFileSegment != null) {
      dataFileSegment.close();
    }
    if (revisionsOffsetSegment != null) {
      revisionsOffsetSegment.close();
    }
    if (reader != null) {
      reader.close();
    }
  }

  @Override
  public Writer writeUberPageReference(final PageReference pageReference) throws SirixIOException {
    write(pageReference);

    final VarHandle longVarHandle = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());
    final MemoryAddress dataFileSegmentBaseAddress = dataFileSegment.baseAddress();

    longVarHandle.set(dataFileSegmentBaseAddress, pageReference.getKey());

    return this;
  }

  @Override
  protected Reader delegate() {
    return reader;
  }

  @Override
  public Writer truncate() {
    // TODO

    return this;
  }
}
