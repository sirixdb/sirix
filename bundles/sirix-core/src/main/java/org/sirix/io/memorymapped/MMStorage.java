/*
 * Copyright (c) 2021, All rights reserved.
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

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.sirix.access.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersister;
import org.sirix.page.SerializationType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
  private final ByteHandlePipeline byteHandlerPipeline;

  long revisionsOffsetFileSize;

  ResourceScope dataFileScope;

  MemorySegment dataFileSegment;

  ResourceScope revisionsOffsetFileScope;

  MemorySegment revisionsOffsetFileSegment;

  final Semaphore semaphore = new Semaphore(1);

  /**
   * Constructor.
   *
   * @param resourceConfig the resource configuration
   */
  public MMStorage(final ResourceConfiguration resourceConfig) {
    assert resourceConfig != null : "resourceConfig must not be null!";
    file = resourceConfig.resourcePath;
    byteHandlerPipeline = resourceConfig.byteHandlePipeline;
  }

  @Override
  public Reader createReader() {
    try {
      semaphore.tryAcquire(5, TimeUnit.SECONDS);
      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfItDoesNotExist(revisionsOffsetFilePath);

      if (revisionsOffsetFileSize == 0)
        revisionsOffsetFileSize = Files.size(revisionsOffsetFilePath);

      createMemoryMappingsIfNeeded(dataFilePath, revisionsOffsetFilePath);

      return new MMFileReader(dataFileSegment,
                              revisionsOffsetFileSegment,
                              new ByteHandlePipeline(byteHandlerPipeline),
                              SerializationType.DATA,
                              new PagePersister());
    } catch (final IOException | InterruptedException e) {
      throw new SirixIOException(e);
    } finally {
      semaphore.release();
    }
  }

  private void createMemoryMappingsIfNeeded(Path dataFilePath, Path revisionsOffsetFilePath) throws IOException {
    if (dataFileScope == null)
      dataFileScope = ResourceScope.newSharedScope();

    if (dataFileSegment == null)
      dataFileSegment = MemorySegment.mapFile(dataFilePath,
                                              0,
                                              Files.size(dataFilePath),
                                              FileChannel.MapMode.READ_WRITE,
                                              dataFileScope);

    if (revisionsOffsetFileScope == null)
      revisionsOffsetFileScope = ResourceScope.newSharedScope();

    if (revisionsOffsetFileSegment == null)
      revisionsOffsetFileSegment = MemorySegment.mapFile(revisionsOffsetFilePath,
                                                         0,
                                                         Files.size(revisionsOffsetFilePath),
                                                         FileChannel.MapMode.READ_WRITE,
                                                         revisionsOffsetFileScope);
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
  public Writer createWriter() {
    try {
      semaphore.tryAcquire(5, TimeUnit.SECONDS);
      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfItDoesNotExist(revisionsOffsetFilePath);

      if (revisionsOffsetFileSize == 0)
        revisionsOffsetFileSize = Files.size(revisionsOffsetFilePath);

      createMemoryMappingsIfNeeded(dataFilePath, revisionsOffsetFilePath);

      return new MMFileWriter(this,
                              dataFilePath,
                              dataFileScope,
                              dataFileSegment,
                              revisionsOffsetFilePath,
                              revisionsOffsetFileScope,
                              revisionsOffsetFileSegment,
                              revisionsOffsetFileSize,
                              new ByteHandlePipeline(byteHandlerPipeline),
                              SerializationType.DATA,
                              new PagePersister());
    } catch (final IOException | InterruptedException e) {
      throw new SirixIOException(e);
    } finally {
      semaphore.release();
    }
  }

  @Override
  public void close() {
    if (dataFileScope != null && dataFileScope.isAlive()) {
      dataFileScope.close();
    }

    if (revisionsOffsetFileScope != null && revisionsOffsetFileScope.isAlive()) {
      revisionsOffsetFileScope.close();
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
