/**
 * Copyright (c) 2011, University of Konstanz, Distributed Systems Group All rights reserved.
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

package io.sirix.io.file;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.Writer;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Factory to provide File access as a backend.
 *
 * @author Sebastian Graf, University of Konstanz.
 */
public final class FileStorage implements IOStorage {

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

  /**
   * The revision file cache.
   */
  private final AsyncCache<Integer, RevisionFileData> cache;

  /**
   * Constructor.
   *
   * @param resourceConfig the resource configuration
   */
  public FileStorage(final ResourceConfiguration resourceConfig, final AsyncCache<Integer, RevisionFileData> cache) {
    assert resourceConfig != null : "resourceConfig must not be null!";
    file = resourceConfig.resourcePath;
    byteHandlerPipeline = resourceConfig.byteHandlePipeline;
    this.cache = cache;
  }

  @Override
  public Reader createReader() {
    try {
      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      if (!Files.exists(revisionsOffsetFilePath)) {
        Files.createFile(revisionsOffsetFilePath);
      }

      return new FileReader(new RandomAccessFile(dataFilePath.toFile(), "r"),
                            new RandomAccessFile(revisionsOffsetFilePath.toFile(), "r"),
                            new ByteHandlerPipeline(byteHandlerPipeline),
                            SerializationType.DATA,
                            new PagePersister(),
                            cache.synchronous());
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
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
      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      if (!Files.exists(revisionsOffsetFilePath)) {
        Files.createFile(revisionsOffsetFilePath);
      }

      final var randomAccessDataFile = new RandomAccessFile(dataFilePath.toFile(), "rw");
      final var randomAccessRevisionDataFile = new RandomAccessFile(revisionsOffsetFilePath.toFile(), "rw");
      final var byteHandlerPipe = new ByteHandlerPipeline(byteHandlerPipeline);
      final var serializationType = SerializationType.DATA;
      final var pagePersister = new PagePersister();
      final var reader = new FileReader(randomAccessDataFile,
                                        randomAccessRevisionDataFile,
                                        byteHandlerPipe,
                                        serializationType,
                                        pagePersister,
                                        cache.synchronous());

      return new FileWriter(randomAccessDataFile,
                            randomAccessRevisionDataFile,
                            serializationType,
                            pagePersister,
                            cache,
                            reader);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() {
    // not used over here
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
