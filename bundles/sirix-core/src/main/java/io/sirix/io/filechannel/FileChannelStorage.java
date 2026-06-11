package io.sirix.io.filechannel;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.exception.SirixIOException;
import io.sirix.io.IOStorage;
import io.sirix.io.Reader;
import io.sirix.io.RevisionFileData;
import io.sirix.io.RevisionIndexHolder;
import io.sirix.io.Writer;
import io.sirix.io.bytepipe.ByteHandler;
import io.sirix.io.bytepipe.ByteHandlerPipeline;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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

  /**
   * Revision file data cache.
   */
  private final AsyncCache<Integer, RevisionFileData> cache;

  /**
   * Revision index holder for fast timestamp lookups.
   */
  private final RevisionIndexHolder revisionIndexHolder;

  /**
   * Constructor.
   *
   * @param resourceConfig the resource configuration
   * @param cache the revision file data cache
   * @param revisionIndexHolder the revision index holder
   */
  public FileChannelStorage(final ResourceConfiguration resourceConfig,
      final AsyncCache<Integer, RevisionFileData> cache, final RevisionIndexHolder revisionIndexHolder) {
    assert resourceConfig != null : "resourceConfig must not be null!";
    file = resourceConfig.resourcePath;
    byteHandlerPipeline = resourceConfig.byteHandlePipeline;
    this.cache = cache;
    this.revisionIndexHolder = revisionIndexHolder;
  }

  /**
   * Constructor (backward compatibility).
   *
   * @param resourceConfig the resource configuration
   * @param cache the revision file data cache
   */
  public FileChannelStorage(final ResourceConfiguration resourceConfig,
      final AsyncCache<Integer, RevisionFileData> cache) {
    this(resourceConfig, cache, new RevisionIndexHolder());
  }

  /**
   * Superblock checks are open-time, not per-reader — and a NEW storage instance is created per
   * request-scoped open, so the once-per-JVM-per-path registry (not a per-instance flag) is what
   * actually avoids the two extra file opens + header reads per request.
   */
  private void validateSuperblocksOnce() {
    io.sirix.io.SuperblockValidator.validateOnce(getDataFilePath(), io.sirix.io.Superblock.ROLE_DATA);
    io.sirix.io.SuperblockValidator.validateOnce(getRevisionFilePath(), io.sirix.io.Superblock.ROLE_REVISIONS);
  }

  @Override
  public Reader createReader() {
    try {
      validateSuperblocksOnce();
      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfNotExists(revisionsOffsetFilePath);
      final FileChannel revisionsOffsetFileChannel = createRevisionsOffsetFileChannel(revisionsOffsetFilePath);
      final FileChannel dataFileChannel = createDataFileChannel(dataFilePath);

      return new FileChannelReader(dataFileChannel, revisionsOffsetFileChannel,
          new ByteHandlerPipeline(byteHandlerPipeline), SerializationType.DATA, new PagePersister(),
          cache.synchronous());
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  private FileChannel createDataFileChannel(Path dataFilePath) throws IOException {
    final FileChannel channel =
        FileChannel.open(dataFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.SPARSE);
    // Optional per-channel {@code posix_fadvise} hint. Default is
    // {@code none} (no hint → Linux autotunes readahead based on observed
    // access pattern), which is the right choice for mixed workloads:
    // point queries only need the 8–32 KB page they request, whereas
    // {@code POSIX_FADV_SEQUENTIAL} triggers 128–512 KB readahead on
    // every read and wastes I/O bandwidth + page cache on the surrounding
    // pages the point query never touches.
    //
    // Bulk-scan workloads (cold projection hydration, PathSummary /
    // PathStatistics load on open) can opt in via
    // {@code -Dsirix.fadvise=sequential} — we measured ~10 % cold-wall
    // win under that hint on the 100 M brackit-scale bench.
    //
    // Override with {@code -Dsirix.fadvise=random} — suppress readahead
    // entirely (for seek-heavy workloads where the kernel's autotune
    // still over-reads).
    final String mode = System.getProperty("sirix.fadvise", "none").toLowerCase();
    switch (mode) {
      case "sequential" -> PosixFadvise.adviseSequential(channel);
      case "random" -> PosixFadvise.adviseRandom(channel);
      default -> {
        // no hint — kernel autotunes
      }
    }
    return channel;
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
      validateSuperblocksOnce();
      final Path dataFilePath = createDirectoriesAndFile();
      final Path revisionsOffsetFilePath = getRevisionFilePath();

      createRevisionsOffsetFileIfNotExists(revisionsOffsetFilePath);
      // WRITER channels: the revisions channel is write-through (SYNC — content AND all
      // metadata per write, since the 32-byte record EXTENDS the file and its size must be
      // durable at write-return even on stacks with weak fdatasync size semantics); its only
      // writes are the per-commit record and the one-time superblock, so the commit protocol
      // needs no separate revisions fsync. The beacon channel is a second DSYNC handle to the
      // data file for the two uber-page slot writes — in-place overwrites, so data-integrity
      // write-through (FUA on NVMe) suffices for ordering + acknowledge. The bulk data channel
      // stays buffered.
      final FileChannel revisionsOffsetFileChannel =
          FileChannel.open(revisionsOffsetFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE,
                           StandardOpenOption.SYNC);
      final FileChannel dataFileChannel = createDataFileChannel(dataFilePath);
      final FileChannel beaconDurableChannel =
          FileChannel.open(dataFilePath, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);

      final var byteHandlePipeline = new ByteHandlerPipeline(byteHandlerPipeline);
      final var serializationType = SerializationType.DATA;
      final var pagePersister = new PagePersister();
      final var reader = new FileChannelReader(dataFileChannel, revisionsOffsetFileChannel, byteHandlePipeline,
          serializationType, pagePersister, cache.synchronous());

      return new FileChannelWriter(dataFileChannel, revisionsOffsetFileChannel, beaconDurableChannel,
          serializationType, pagePersister, cache, revisionIndexHolder, reader);
    } catch (final IOException e) {
      throw new SirixIOException(e);
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

  @Override
  public RevisionIndexHolder getRevisionIndexHolder() {
    return revisionIndexHolder;
  }
}
