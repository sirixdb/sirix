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

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.jetbrains.annotations.NotNull;
import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.*;
import org.sirix.page.interfaces.Page;

import java.io.*;
import java.nio.channels.FileChannel;
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

  private static final int TEST_BLOCK_SIZE = 64 * 1024; // Smallest safe block size for Windows 8+.

  private final Path dataFilePath;

  private final Path revisionsOffsetFilePath;

  private long currByteSizeToMap = Integer.MAX_VALUE;//OS.is64Bit() ? 64L << 20 : TEST_BLOCK_SIZE;

  private MemorySegment dataFileSegment;

  /**
   * {@link MMFileReader} reference for this writer.
   */
  private MMFileReader reader;

  private final SerializationType type;

  private MemorySegment revisionsOffsetFileSegment;

  private final PagePersister pagePersister;

  private long dataSegmentFileSize;

  private long revisionsOffsetSegmentFileSize;

  /**
   * Constructor.
   *
   * @param dataFileSegment            the data file segment
   * @param revisionsOffsetFileSegment the revisions offset file segment
   * @param handler                    the byte handler
   * @param serializationType          the serialization type (for the transaction log or the data file)
   * @param pagePersister              transforms in-memory pages into byte-arrays and back
   */
  public MMFileWriter(final Path dataFile, final Path revisionsOffsetFile, final MemorySegment dataFileSegment,
      final MemorySegment revisionsOffsetFileSegment, final ByteHandler handler,
      final SerializationType serializationType, final PagePersister pagePersister) throws IOException {
    this.dataFilePath = dataFile;
    this.revisionsOffsetFilePath = revisionsOffsetFile;
    this.dataFileSegment = dataFileSegment;
    type = checkNotNull(serializationType);
    this.revisionsOffsetFileSegment = revisionsOffsetFileSegment;
    this.pagePersister = checkNotNull(pagePersister);

    while (currByteSizeToMap < dataSegmentFileSize) {
      currByteSizeToMap = currByteSizeToMap << 1;
    }

    reader = new MMFileReader(dataFileSegment, revisionsOffsetFileSegment, handler, serializationType, pagePersister);
  }

  @Override
  public Writer truncateTo(final int revision) {
    UberPage uberPage = (UberPage) reader.readUberPageReference().getPage();
    final int currentRevision = uberPage.getRevisionNumber();

    while (uberPage.getRevisionNumber() != revision) {
      uberPage = (UberPage) reader.read(new PageReference().setKey(uberPage.getPreviousUberPageKey()), null);
      if (uberPage.getRevisionNumber() == revision) {
        try (final RandomAccessFile file = new RandomAccessFile(dataFilePath.toFile(), "rw")) {
          file.setLength(uberPage.getPreviousUberPageKey());
        } catch (final IOException e) {
          throw new SirixIOException(e);
        }
        try (final RandomAccessFile file = new RandomAccessFile(revisionsOffsetFilePath.toFile(), "rw")) {
          file.setLength(file.length() - MMFileReader.LAYOUT_LONG.byteSize() * (currentRevision - revision));
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
      dataSegmentFileSize += MMFileReader.LAYOUT_INT.byteSize();
      dataSegmentFileSize += serializedPage.length;

      reInstantiateDataFileSegment();

      dataFileSegment.set(MMFileReader.LAYOUT_INT, offset, serializedPage.length);

      long currOffsetWithInt = offset + MMFileReader.LAYOUT_INT.byteSize();

      MemorySegment.copy(serializedPage,
                         0,
                         dataFileSegment,
                         MMFileReader.LAYOUT_BYTE,
                         currOffsetWithInt,
                         serializedPage.length);

      // Remember page coordinates.
      switch (type) {
        case DATA -> pageReference.setKey(offset);
        case TRANSACTION_INTENT_LOG -> pageReference.setPersistentLogKey(offset);
      }

      pageReference.setHash(reader.hashFunction.hashBytes(serializedPage).asBytes());

      if (type == SerializationType.DATA && page instanceof RevisionRootPage) {
        revisionsOffsetFileSegment.set(MMFileReader.LAYOUT_LONG, revisionsOffsetSegmentFileSize, offset);

        revisionsOffsetSegmentFileSize += 8;
      }

      return this;
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private void reInstantiateDataFileSegment() throws IOException {
    if (dataSegmentFileSize > dataFileSegment.byteSize()) {
      do {
        currByteSizeToMap = currByteSizeToMap << 1;
      } while (dataSegmentFileSize > currByteSizeToMap);

      dataFileSegment.scope().close();
      final var scope = ResourceScope.newSharedScope();
      dataFileSegment =
          MemorySegment.mapFile(dataFilePath, 0, currByteSizeToMap, FileChannel.MapMode.READ_WRITE, scope);
      reader.setDataSegment(dataFileSegment);
    }
  }

  @Override
  public void close() {
    if (reader != null) {
      reader.close();
    }
    try (final FileChannel outChan = new FileOutputStream(dataFilePath.toFile(), true).getChannel()) {
      outChan.truncate(dataSegmentFileSize);
    } catch (IOException e) {
      throw new SirixIOException(e);
    }
    try (final FileChannel outChan = new FileOutputStream(revisionsOffsetFilePath.toFile(), true).getChannel()) {
      outChan.truncate(revisionsOffsetSegmentFileSize);
    } catch (IOException e) {
      throw new SirixIOException(e);
    }
  }

  @Override
  public Writer writeUberPageReference(final PageReference pageReference) {
    write(pageReference);

    try {
      reInstantiateDataFileSegment();

      dataFileSegment.set(MMFileReader.LAYOUT_LONG, 0, pageReference.getKey());
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
    try (final FileChannel outChan = new FileOutputStream(dataFilePath.toFile(), true).getChannel()) {
      outChan.truncate(0);
    } catch (IOException e) {
      throw new SirixIOException(e);
    }
    try (final FileChannel outChan = new FileOutputStream(revisionsOffsetFilePath.toFile(), true).getChannel()) {
      outChan.truncate(0);
    } catch (IOException e) {
      throw new SirixIOException(e);
    }

    return this;
  }

  @Override
  public @NotNull String toString() {
    return "MemoryMappedFileWriter{" + "dataFile=" + dataFileSegment + ", reader=" + reader + ", type=" + type
        + ", revisionsOffsetFile=" + revisionsOffsetFileSegment + ", pagePersister=" + pagePersister + '}';
  }
}
