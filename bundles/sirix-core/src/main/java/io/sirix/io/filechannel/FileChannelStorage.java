package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.exception.SirixIOException;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;

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
      final FileChannel revisionsOffsetFileChannel = createRevisionsOffsetFileChannel(revisionsOffsetFilePath);
      final FileChannel dataFileChannel = createDataFileChannel(dataFilePath);

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

  private FileChannel createDataFileChannel(Path dataFilePath) throws IOException {
    return FileChannel.open(dataFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SPARSE);
  }

  private FileChannel createRevisionsOffsetFileChannel(Path revisionsOffsetFilePath) throws IOException {
    return FileChannel.open(revisionsOffsetFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE);
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
      final FileChannel revisionsOffsetFileChannel = createRevisionsOffsetFileChannel(revisionsOffsetFilePath);
      final FileChannel dataFileChannel = createDataFileChannel(dataFilePath);

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
    // Do nothing.
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
