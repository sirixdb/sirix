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

import jdk.incubator.foreign.MemoryAccess;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.sirix.exception.SirixIOException;
import org.sirix.io.AbstractForwardingReader;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersister;
import org.sirix.page.PageReference;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.SerializationType;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.Page;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

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

  private final ResourceScope dataFileScope;

  private final ResourceScope revisionsOffsetFileScope;

  private final Path dataFilePath;

  private final Path revisionsOffsetFilePath;

  private final MMStorage storage;

  private long currByteSizeToMap = Integer.MAX_VALUE;//OS.is64Bit() ? 64L << 20 : TEST_BLOCK_SIZE;

  private MemorySegment dataFileSegment;

  /**
   * {@link MMFileReader} reference for this writer.
   */
  private final MMFileReader reader;

  private final SerializationType type;

  private MemorySegment revisionsOffsetFileSegment;

  private final PagePersister pagePersister;

  private long dataSegmentFileSize;

  private long revisionsOffsetSegmentFileSize;

  /**
   * Constructor.
   *
   * @param dataFileSegment            the data file segment
   * @param revisionsOffsetFileSegment the file segment, which holds pointers to the revision root pages
   * @param handler                    the byte handler
   * @param serializationType          the serialization type (for the transaction log or the data file)
   * @param pagePersister              transforms in-memory pages into byte-arrays and back
   */
  public MMFileWriter(final MMStorage storage, final Path dataFilePath, final ResourceScope dataFileScope,
      final MemorySegment dataFileSegment, final Path revisionsOffsetFilePath,
      final ResourceScope revisionsOffsetFileScope, final MemorySegment revisionsOffsetFileSegment,
      final long revisionsOffsetFileSize, final ByteHandler handler, final SerializationType serializationType,
      final PagePersister pagePersister) throws IOException {
    this.storage = checkNotNull(storage);
    this.dataFilePath = checkNotNull(dataFilePath);
    this.dataFileScope = checkNotNull(dataFileScope);
    this.dataFileSegment = checkNotNull(dataFileSegment);
    dataSegmentFileSize = Files.size(dataFilePath);
    type = checkNotNull(serializationType);
    this.revisionsOffsetFilePath = checkNotNull(revisionsOffsetFilePath);
    this.revisionsOffsetFileScope = checkNotNull(revisionsOffsetFileScope);
    this.revisionsOffsetFileSegment = checkNotNull(revisionsOffsetFileSegment);
    revisionsOffsetSegmentFileSize = revisionsOffsetFileSize;
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
          file.setLength(file.length() - 8 * (currentRevision - revision));
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

      reInstantiateDataFileSegment();

      MemoryAccess.setIntAtOffset(dataFileSegment, offset, serializedPage.length);

      long currOffsetWithInt = offset + 4;

      for (int i = 0; i < serializedPage.length; i++) {
        MemoryAccess.setByteAtOffset(dataFileSegment, currOffsetWithInt + (long) i, serializedPage[i]);
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
        if (revisionsOffsetFileSegment.byteSize() < Integer.MAX_VALUE) {
          try {
            storage.semaphore.tryAcquire(5, TimeUnit.SECONDS);
            revisionsOffsetFileSegment = MemorySegment.mapFile(revisionsOffsetFilePath,
                                                               0,
                                                               Integer.MAX_VALUE,
                                                               FileChannel.MapMode.READ_WRITE,
                                                               revisionsOffsetFileScope);
            storage.revisionsOffsetFileScope = revisionsOffsetFileScope;
            storage.revisionsOffsetFileSegment = revisionsOffsetFileSegment;
          } catch (InterruptedException e) {
            throw new SirixIOException(e);
          } finally {
            storage.semaphore.release();
          }
        }

        MemoryAccess.setLongAtOffset(revisionsOffsetFileSegment, revisionsOffsetSegmentFileSize, offset);

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

      try {
        storage.semaphore.tryAcquire(5, TimeUnit.SECONDS);

        dataFileSegment =
            MemorySegment.mapFile(dataFilePath, 0, currByteSizeToMap, FileChannel.MapMode.READ_WRITE, dataFileScope);
        reader.setDataSegment(dataFileSegment);
        storage.dataFileScope = dataFileScope;
        storage.dataFileSegment = dataFileSegment;
      } catch (InterruptedException e) {
        throw new SirixIOException(e);
      } finally {
        storage.semaphore.release();
      }
    }
  }

  @Override
  public void close() {
    try {
      storage.semaphore.tryAcquire(5, TimeUnit.SECONDS);

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
      storage.revisionsOffsetFileSize = revisionsOffsetSegmentFileSize;
      storage.dataFileSegment = null;
      storage.revisionsOffsetFileSegment = null;
    } catch (InterruptedException e) {
      throw new SirixIOException(e);
    } finally {
      storage.semaphore.release();
    }
  }

  @Override
  public Writer writeUberPageReference(final PageReference pageReference) {
    write(pageReference);

    try {
      reInstantiateDataFileSegment();

      MemoryAccess.setLong(dataFileSegment, pageReference.getKey());
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
  public String toString() {
    return "MemoryMappedFileWriter{" + "dataFile=" + dataFileSegment + ", reader=" + reader + ", type=" + type
        + ", revisionsOffsetFile=" + revisionsOffsetFileSegment + ", pagePersister=" + pagePersister + '}';
  }
}
