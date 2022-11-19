package org.sirix.io.iouring;

import com.github.benmanes.caffeine.cache.AsyncCache;
import one.jasyncfio.AsyncFile;
import one.jasyncfio.EventExecutor;
import one.jasyncfio.OpenOption;
import org.sirix.access.ResourceConfiguration;
import org.sirix.exception.SirixIOException;
import org.sirix.io.IOStorage;
import org.sirix.io.Reader;
import org.sirix.io.RevisionFileData;
import org.sirix.io.Writer;
import org.sirix.io.bytepipe.ByteHandlePipeline;
import org.sirix.io.bytepipe.ByteHandler;
import org.sirix.page.PagePersister;
import org.sirix.page.SerializationType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Factory to provide file channel access as a backend.
 *
 * @author Johannes Lichtenberger
 */
public final class IOUringStorage implements IOStorage {

  /**
   * Instance to storage.
   */
  private final Path file;

  /**
   * Byte handler pipeline.
   */
  private final ByteHandlePipeline byteHandlerPipeline;

  final Semaphore semaphore = new Semaphore(1);

  /**
   * Revision file data cache.
   */
  private final AsyncCache<Integer, RevisionFileData> cache;

  private static final EventExecutor eventExecutor = EventExecutor.builder().entries(1024).build();

  private AsyncFile dataFile;

  private AsyncFile revisionsOffsetFile;

  /**
   * Constructor.
   *
   * @param resourceConfig the resource configuration
   */
  public IOUringStorage(final ResourceConfiguration resourceConfig, final AsyncCache<Integer, RevisionFileData> cache) {
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
      createRevisionsOffsetFileIfNotInitialized(revisionsOffsetFilePath);
      createDataFileIfNotInitialized(dataFilePath);

      return new IOUringReader(dataFile,
                               revisionsOffsetFile,
                               new ByteHandlePipeline(byteHandlerPipeline),
                               SerializationType.DATA,
                               new PagePersister(),
                               cache.synchronous());
    } catch (final IOException | InterruptedException e) {
      throw new SirixIOException(e);
    } finally {
      semaphore.release();
    }
  }

  private void createDataFileIfNotInitialized(Path dataFilePath) {
    CompletableFuture<AsyncFile> asyncFileCompletableFuture =
        AsyncFile.open(dataFilePath, eventExecutor, OpenOption.READ_WRITE, OpenOption.CREATE);
    dataFile = asyncFileCompletableFuture.join();
  }

  private void createRevisionsOffsetFileIfNotInitialized(Path revisionsOffsetFilePath) {
    CompletableFuture<AsyncFile> asyncFileCompletableFuture =
        AsyncFile.open(revisionsOffsetFilePath, eventExecutor, OpenOption.READ_WRITE, OpenOption.CREATE);
    revisionsOffsetFile = asyncFileCompletableFuture.join();
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
      createRevisionsOffsetFileIfNotInitialized(revisionsOffsetFilePath);
      createDataFileIfNotInitialized(dataFilePath);

      final var byteHandlePipeline = new ByteHandlePipeline(byteHandlerPipeline);
      final var serializationType = SerializationType.DATA;
      final var pagePersister = new PagePersister();
      final var reader = new IOUringReader(dataFile,
                                           revisionsOffsetFile,
                                           byteHandlePipeline,
                                           serializationType,
                                           pagePersister,
                                           cache.synchronous());

      return new IOUringWriter(dataFile,
                               revisionsOffsetFile,
                               dataFilePath,
                               revisionsOffsetFilePath,
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
      if (revisionsOffsetFile != null) {
        revisionsOffsetFile.close().join();
      }
      dataFile.close().join();
    } catch (Exception e) {
      throw new RuntimeException(e);
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
