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

import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.ResourceScope;
import org.sirix.access.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.io.filechannel.FileChannelWriter;
import org.sirix.page.PagePersister;
import org.sirix.page.SerializationType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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

  private FileChannel dataFileChannel;

  private FileChannel revisionsOffsetFileChannel;

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
      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfItDoesNotExist(revisionsOffsetFilePath);

      final var dataFileScope = ResourceScope.newSharedScope();
      final var dataFileSegmentFileSize = Files.size(dataFilePath);
      final var dataFileSegment =
          MemorySegment.mapFile(dataFilePath, 0, dataFileSegmentFileSize, FileChannel.MapMode.READ_ONLY, dataFileScope);

      final var revisionsOffsetFileScope = ResourceScope.newSharedScope();
      final var revisionsOffsetSegmentFileSize = Files.size(revisionsOffsetFilePath);
      final var revisionsOffsetFileSegment = MemorySegment.mapFile(revisionsOffsetFilePath,
                                                                   0,
                                                                   revisionsOffsetSegmentFileSize,
                                                                   FileChannel.MapMode.READ_ONLY,
                                                                   revisionsOffsetFileScope);

      return new MMFileReader(dataFileSegment,
                              revisionsOffsetFileSegment,
                              new ByteHandlePipeline(byteHandlerPipeline),
                              SerializationType.DATA,
                              new PagePersister());
    } catch (final IOException e) {
      throw new SirixIOException(e);
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
      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfItDoesNotExist(revisionsOffsetFilePath);
      createDataFileChannelIfNotInitialized(dataFilePath);
      createRevisionsOffsetFileChannelIfNotInitialized(revisionsOffsetFilePath);

      return new FileChannelWriter(dataFileChannel,
                                   revisionsOffsetFileChannel,
                                   new ByteHandlePipeline(byteHandlerPipeline),
                                   SerializationType.DATA,
                                   new PagePersister());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private void createDataFileChannelIfNotInitialized(Path dataFilePath) throws IOException {
    if (dataFileChannel == null) {
      dataFileChannel = FileChannel.open(dataFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
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
