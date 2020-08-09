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

import jdk.incubator.foreign.*;
import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;
import org.sirix.utils.OS;

import java.io.*;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Writer to read/write to a memory mapped file.
 *
 * @author Johannes Lichtenberger
 */
public final class MMFileWriter extends AbstractForwardingReader implements Writer {

  private static final short REVISION_ROOT_PAGE_BYTE_ALIGN = 256;

  private static final byte PAGE_FRAGMENT_WORD_ALIGN = 64;

  private static int TEST_BLOCK_SIZE = 64 * 1024; // Smallest safe block size for Windows 8+.

  private long currByteSizeToMap = Integer.MAX_VALUE;//OS.is64Bit() ? 64L << 20 : TEST_BLOCK_SIZE;

  private static final VarHandle BYTE_VAR_HANDLE = MemoryLayout.ofSequence(MemoryLayouts.JAVA_BYTE)
                                                               .varHandle(byte.class,
                                                                          MemoryLayout.PathElement.sequenceElement());

  private static final VarHandle INT_VAR_HANDLE = MemoryHandles.varHandle(int.class, ByteOrder.nativeOrder());

  private static final VarHandle LONG_VAR_HANDLE = MemoryHandles.varHandle(long.class, ByteOrder.nativeOrder());

  /**
   * Random access to work on.
   */
  private final Path dataFile;

  /**
   * {@link MMFileReader} reference for this writer.
   */
  private final MMFileReader reader;

  private final SerializationType type;

  private final Path revisionsOffsetFile;

  private final PagePersister pagePersister;

  private final MemorySegment revisionsOffsetSegment;

  private MemorySegment dataSegment;

  private long dataSegmentFileSize;

  private long revisionsOffsetSize;

  /**
   * Constructor.
   *
   * @param dataFile            the data file
   * @param revisionsOffsetFile the file, which holds pointers to the revision root pages
   * @param handler             the byte handler
   * @param serializationType   the serialization type (for the transaction log or the data file)
   * @param pagePersister       transforms in-memory pages into byte-arrays and back
   */
  public MMFileWriter(final Path dataFile, final Path revisionsOffsetFile, final ByteHandler handler,
      final SerializationType serializationType, final PagePersister pagePersister) throws IOException {
    this.dataFile = checkNotNull(dataFile);
    dataSegmentFileSize = Files.size(dataFile);
    type = checkNotNull(serializationType);
    this.revisionsOffsetFile = checkNotNull(revisionsOffsetFile);
    revisionsOffsetSize = Files.size(revisionsOffsetFile);
    this.pagePersister = checkNotNull(pagePersister);

    while (currByteSizeToMap < dataSegmentFileSize) {
      currByteSizeToMap = currByteSizeToMap << 1;
    }

    this.dataSegment =
        MemorySegment.mapFromPath(checkNotNull(dataFile), currByteSizeToMap, FileChannel.MapMode.READ_WRITE);

    this.revisionsOffsetSegment =
        MemorySegment.mapFromPath(revisionsOffsetFile, Integer.MAX_VALUE, FileChannel.MapMode.READ_WRITE);

    reader =
        new MMFileReader(dataFile, revisionsOffsetFile, dataSegment, revisionsOffsetSegment, handler, serializationType, pagePersister);
  }

  @Override
  public Writer truncateTo(final int revision) {
    UberPage uberPage = (UberPage) reader.readUberPageReference().getPage();

    while (uberPage.getRevisionNumber() != revision) {
      uberPage = (UberPage) reader.read(new PageReference().setKey(uberPage.getPreviousUberPageKey()), null);
      if (uberPage.getRevisionNumber() == revision) {
        try (final RandomAccessFile file = new RandomAccessFile(dataFile.toFile(), "rw")) {
          file.setLength(uberPage.getPreviousUberPageKey());
        } catch (final IOException e) {
          throw new SirixIOException(e);
        }
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
  public MMFileWriter write(final PageReference pageReference) {
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

      // Getting actual offset and appending to the end of the current file.
      long offset = dataSegmentFileSize == 0 ? MMFileReader.FIRST_BEACON : dataSegmentFileSize;
      if (type == SerializationType.DATA) {
        if (page instanceof RevisionRootPage) {
          if (offset % REVISION_ROOT_PAGE_BYTE_ALIGN != 0) {
            offset += REVISION_ROOT_PAGE_BYTE_ALIGN - (offset % REVISION_ROOT_PAGE_BYTE_ALIGN);
          }
        } else if (offset % PAGE_FRAGMENT_WORD_ALIGN != 0) {
          offset += PAGE_FRAGMENT_WORD_ALIGN - (offset % PAGE_FRAGMENT_WORD_ALIGN);
        }
      }

      dataSegmentFileSize = offset;
      dataSegmentFileSize += 4;
      dataSegmentFileSize += serializedPage.length;

      reInstantiateSegment();

      MemoryAddress dataFileSegmentBaseAddress = dataSegment.baseAddress().addOffset(offset);

      INT_VAR_HANDLE.set(dataFileSegmentBaseAddress, serializedPage.length);

      dataFileSegmentBaseAddress = dataFileSegmentBaseAddress.addOffset(4);

      for (int i = 0; i < serializedPage.length; i++) {
        BYTE_VAR_HANDLE.set(dataFileSegmentBaseAddress, (long) i, serializedPage[i]);
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

//      pageReference.setLength(serializedPage.length + 4);
      pageReference.setHash(reader.hashFunction.hashBytes(serializedPage).asBytes());

      if (type == SerializationType.DATA && page instanceof RevisionRootPage) {
        final MemoryAddress revisionFileSegmentBaseAddress = revisionsOffsetSegment.baseAddress();

        LONG_VAR_HANDLE.set(revisionFileSegmentBaseAddress.addOffset(revisionsOffsetSize), offset);

        revisionsOffsetSize += 8;
      }

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private void reInstantiateSegment() throws IOException {
    if (dataSegmentFileSize > dataSegment.byteSize()) {
      do {
        currByteSizeToMap = currByteSizeToMap << 1;
      } while (dataSegmentFileSize > currByteSizeToMap);

      ifSegmentIsAliveCloseSegment();

      dataSegment = MemorySegment.mapFromPath(dataFile, currByteSizeToMap, FileChannel.MapMode.READ_WRITE);
      reader.setDataSegment(dataSegment);
    }
  }

  private void ifSegmentIsAliveCloseSegment() {
    if (dataSegment.isAlive()) {
      dataSegment.close();
    }
  }

  @Override
  public void close() {
    if (reader != null) {
      try (final FileChannel outChan = new FileOutputStream(dataFile.toFile(), true).getChannel()) {
        outChan.truncate(dataSegmentFileSize);
      } catch (IOException e) {
        throw new SirixIOException(e);
      }
      try (final FileChannel outChan = new FileOutputStream(revisionsOffsetFile.toFile(), true).getChannel()) {
        outChan.truncate(revisionsOffsetSize);
      } catch (IOException e) {
        throw new SirixIOException(e);
      }
      reader.close();
    }
  }

  @Override
  public Writer writeUberPageReference(final PageReference pageReference) {
    write(pageReference);

    try {
      reInstantiateSegment();

      final MemoryAddress dataFileSegmentBaseAddress = dataSegment.baseAddress();

      LONG_VAR_HANDLE.set(dataFileSegmentBaseAddress, pageReference.getKey());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }

  @Override
  protected Reader delegate() {
    return reader;
  }

  @Override
  public Writer truncate() {
    try (final FileChannel outChan = new FileOutputStream(dataFile.toFile(), true).getChannel()) {
      outChan.truncate(0);
    } catch (IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }

  @Override
  public String toString() {
    return "MemoryMappedFileWriter{" + "dataFile=" + dataFile + ", reader=" + reader + ", type=" + type
        + ", revisionsOffsetFile=" + revisionsOffsetFile + ", pagePersister=" + pagePersister + '}';
  }
}
