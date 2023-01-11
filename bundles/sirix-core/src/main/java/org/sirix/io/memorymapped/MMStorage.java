/*
 * Copyright (c) 2022, University of Konstanz, Distributed Systems Group All rights reserved.
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

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.sun.nio.file.ExtendedOpenOption;
import org.sirix.access.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.bytepipe.ByteHandlerPipeline;
import org.sirix.io.filechannel.FileChannelReader;
import org.sirix.io.filechannel.FileChannelWriter;
import org.sirix.page.PagePersister;
import org.sirix.page.SerializationType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.MemorySession;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Storage, to provide offheap memory mapped access.
 *
 * @author Johannes Lichtenberger
 */
public final class MMStorage implements IOStorage {

  /**
   * Data file name.
   */
  private static final String FILENAME = "sirix.data";

  /**
   * Revisions file name.
   */
  private static final String REVISIONS_FILENAME = "sirix.revisions";

  /**
   * Instance to storage.
   */
  private final Path file;

  /**
   * Byte handler pipeline.
   */
  private final ByteHandlerPipeline byteHandlerPipeline;

  final Semaphore semaphore = new Semaphore(1);

  /**
   * Revision file data cache.
   */
  private final AsyncCache<Integer, RevisionFileData> cache;

  private FileChannel dataFileChannel;

  private FileChannel revisionsOffsetFileChannel;

  /**
   * Constructor.
   *
   * @param resourceConfig the resource configuration
   * @param cache          the revision file data cache
   */
  public MMStorage(final ResourceConfiguration resourceConfig, final AsyncCache<Integer, RevisionFileData> cache) {
    assert resourceConfig != null : "resourceConfig must not be null!";
    file = resourceConfig.resourcePath;
    byteHandlerPipeline = resourceConfig.byteHandlePipeline;
    this.cache = cache;
  }

  @Override
  public Reader createReader() {
    try {
      final var sempahoreAcquired = semaphore.tryAcquire(5, TimeUnit.SECONDS);

      if (!sempahoreAcquired) {
        throw new IllegalStateException("Couldn't acquire semaphore.");
      }

      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfItDoesNotExist(revisionsOffsetFilePath);

      final var dataFileSession = MemorySession.openShared();
      final var dataFileSegmentFileSize = Files.size(dataFilePath);

      final var revisionsOffsetSegmentFileSize = Files.size(revisionsOffsetFilePath);

      try (final var dataFileChannel = FileChannel.open(dataFilePath);
           final var revisionsOffsetFileChannel = FileChannel.open(revisionsOffsetFilePath)) {
        final var dataFileSegment =
            dataFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, dataFileSegmentFileSize, dataFileSession);
        final var revisionsOffsetFileSegment = revisionsOffsetFileChannel.map(FileChannel.MapMode.READ_ONLY,
                                                                              0,
                                                                              revisionsOffsetSegmentFileSize,
                                                                              dataFileSession);
        return new MMFileReader(dataFileSegment,
                                revisionsOffsetFileSegment,
                                new ByteHandlerPipeline(byteHandlerPipeline),
                                SerializationType.DATA,
                                new PagePersister(),
                                cache.synchronous());
      }
    } catch (final IOException | InterruptedException e) {
      throw new SirixIOException(e);
    } finally {
      semaphore.release();
    }
  }

  private void createRevisionsOffsetFileIfItDoesNotExist(Path revisionsOffsetFilePath) throws IOException {
    if (!Files.exists(revisionsOffsetFilePath)) {
      Files.createFile(revisionsOffsetFilePath);
    }
  }

  private Path createDirectoriesAndFile() throws IOException {
    final Path concreteStorage = getDataFilePath();

    if (!Files.exists(concreteStorage)) {
      Files.createDirectories(concreteStorage.getParent());
      Files.createFile(concreteStorage);
    }

    return concreteStorage;
  }

  @Override
  public synchronized Writer createWriter() {
    try {
      final var sempahoreAcquired = semaphore.tryAcquire(5, TimeUnit.SECONDS);

      if (!sempahoreAcquired) {
        throw new IllegalStateException("Couldn't acquire semaphore.");
      }

      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfItDoesNotExist(revisionsOffsetFilePath);
      createDataFileChannelIfNotInitialized(dataFilePath);
      createRevisionsOffsetFileChannelIfNotInitialized(revisionsOffsetFilePath);

      final var byteHandlePipeline = new ByteHandlerPipeline(byteHandlerPipeline);
      final var serializationType = SerializationType.DATA;
      final var pagePersister = new PagePersister();
      final var reader = new FileChannelReader(dataFileChannel,
                                               revisionsOffsetFileChannel,
                                               byteHandlePipeline,
                                               serializationType,
                                               pagePersister,
                                               cache.synchronous());

      return new FileChannelWriter(dataFileChannel,
                                   revisionsOffsetFileChannel,
                                   serializationType,
                                   pagePersister,
                                   cache,
                                   reader);
    } catch (final IOException | InterruptedException e) {
      throw new SirixIOException(e);
    } finally {
      semaphore.release();
    }
  }

  private void createDataFileChannelIfNotInitialized(Path dataFilePath) throws IOException {
    if (dataFileChannel == null) {
      dataFileChannel = FileChannel.open(dataFilePath,
                                         StandardOpenOption.READ,
                                         StandardOpenOption.WRITE,
                                         StandardOpenOption.SPARSE);
    }
  }

  private void createRevisionsOffsetFileChannelIfNotInitialized(Path revisionsOffsetFilePath) throws IOException {
    if (revisionsOffsetFileChannel == null) {
      revisionsOffsetFileChannel =
          FileChannel.open(revisionsOffsetFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
    }
  }

  @Override
  public void close() {
    try {
      if (revisionsOffsetFileChannel != null) {
        revisionsOffsetFileChannel.close();
      }
      if (dataFileChannel != null) {
        dataFileChannel.close();
      }
    } catch (final IOException e) {
      throw new SirixIOException(e.getMessage(), e);
    }
  }

  /**
   * Getting path for data file.
   *
   * @return the path for this data file
   */
  private Path getDataFilePath() {
    return file.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve(FILENAME);
  }

  /**
   * Getting concrete storage for this file.
   *
   * @return the concrete storage for this database
   */
  private Path getRevisionFilePath() {
    return file.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath()).resolve(REVISIONS_FILENAME);
  }

  @Override
  public boolean exists() {
    final Path storage = getDataFilePath();
    try {
      return Files.exists(storage) && Files.size(storage) > 0;
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public ByteHandler getByteHandler() {
    return byteHandlerPipeline;
  }
}
