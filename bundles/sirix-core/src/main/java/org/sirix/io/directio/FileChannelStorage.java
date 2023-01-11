package org.sirix.io.directio;

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
import org.sirix.page.PagePersister;
import org.sirix.page.SerializationType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Factory to provide file channel access as a backend.
 *
 * @author Johannes Lichtenberger
 */
public final class FileChannelStorage implements IOStorage {

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

  private FileChannel revisionsOffsetFileChannel;

  private FileChannel dataFileChannel;

  final Semaphore semaphore = new Semaphore(1);

  /**
   * Revision file data cache.
   */
  private final AsyncCache<Integer, RevisionFileData> cache;

  /**
   * Constructor.
   *
   * @param resourceConfig the resource configuration
   */
  public FileChannelStorage(final ResourceConfiguration resourceConfig,
      final AsyncCache<Integer, RevisionFileData> cache) {
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

      createRevisionsOffsetFileIfNotExists(revisionsOffsetFilePath);
      createRevisionsOffsetFileChannelIfNotInitialized(revisionsOffsetFilePath);
      createDataFileChannelIfNotInitialized(dataFilePath);

      return new FileChannelReader(dataFileChannel,
                                   revisionsOffsetFileChannel,
                                   new ByteHandlerPipeline(byteHandlerPipeline),
                                   SerializationType.DATA,
                                   new PagePersister(),
                                   cache.synchronous());
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
                                         StandardOpenOption.SPARSE,
                                         StandardOpenOption.CREATE,
                                         ExtendedOpenOption.DIRECT);
    }
  }

  private void createRevisionsOffsetFileChannelIfNotInitialized(Path revisionsOffsetFilePath) throws IOException {
    if (revisionsOffsetFileChannel == null) {
      revisionsOffsetFileChannel = FileChannel.open(revisionsOffsetFilePath,
                                                    StandardOpenOption.READ,
                                                    StandardOpenOption.WRITE,
                                                    StandardOpenOption.CREATE);
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
      final var sempahoreAcquired = semaphore.tryAcquire(5, TimeUnit.SECONDS);

      if (!sempahoreAcquired) {
        throw new IllegalStateException("Couldn't acquire semaphore.");
      }

      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfNotExists(revisionsOffsetFilePath);
      createRevisionsOffsetFileChannelIfNotInitialized(revisionsOffsetFilePath);
      createDataFileChannelIfNotInitialized(dataFilePath);

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

  private synchronized void createRevisionsOffsetFileIfNotExists(Path revisionsOffsetFilePath) throws IOException {
    if (!Files.exists(revisionsOffsetFilePath)) {
      Files.createFile(revisionsOffsetFilePath);
    }
  }

  @Override
  public void close() {
    try {
      if (revisionsOffsetFileChannel != null) {
        revisionsOffsetFileChannel.close();
      }
      dataFileChannel.close();
    } catch (final IOException e) {
      throw new SirixIOException(e);
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
