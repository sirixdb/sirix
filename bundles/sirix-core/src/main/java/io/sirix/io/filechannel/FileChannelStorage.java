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
import java.util.concurrent.atomic.AtomicInteger;

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
   * Number of shared reader channel stripes per storage. {@link FileChannelReader} uses positional
   * reads exclusively, which are lock-free in the JDK on POSIX (straight {@code pread(2)}), but on
   * Windows {@code FileDispatcherImpl.needsPositionLock()} is {@code true} and every positional
   * read on a channel serializes on that channel's position lock. A single shared channel would
   * therefore serialize concurrent readers of the same resource on Windows; striping restores
   * uncontended reads for up to {@code stripes} concurrent readers.
   *
   * <p>The pool is REFERENCE-COUNTED and closes when the last borrowing reader closes: workloads
   * holding many concurrent read transactions (query evaluation against a long-lived session)
   * share O(stripes) descriptors instead of the per-reader channels that exhausted the process FD
   * limit, while workloads with many short-lived sessions/resources drop back to zero descriptors
   * as soon as their transactions close — holding stripes open for a session's whole lifetime
   * exhausted the FD limit from the other direction (many idle sessions × stripes).
   */
  private static final int READER_CHANNEL_STRIPES =
      Math.max(1, Math.min(8, Runtime.getRuntime().availableProcessors()));

  /**
   * Lock guarding the borrow count and lazy open/close of the shared reader channel stripes.
   */
  private final Object readerChannelLock = new Object();

  /**
   * Round-robin stripe assignment for readers; each reader keeps its stripe for life.
   */
  private final AtomicInteger readerStripeCounter = new AtomicInteger();

  /**
   * Number of live readers borrowing the stripes. Guarded by {@link #readerChannelLock}; the pool
   * closes when this drops to zero.
   */
  private int borrowingReaders;

  /**
   * Shared data file channels handed to readers, one stripe picked per reader at creation.
   * Guarded by {@link #readerChannelLock}.
   */
  private FileChannel[] sharedDataFileChannels;

  /**
   * Shared revisions-offset file channels (same striping as {@link #sharedDataFileChannels}).
   * Guarded by {@link #readerChannelLock}.
   */
  private FileChannel[] sharedRevisionsOffsetFileChannels;

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
    resourceUuidMsb = resourceConfig.resourceUuid != null ? resourceConfig.resourceUuid.getMostSignificantBits() : 0L;
    resourceUuidLsb = resourceConfig.resourceUuid != null ? resourceConfig.resourceUuid.getLeastSignificantBits() : 0L;
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

  /** Resource identity UUID halves from the configuration (0/0 = legacy, no cross-check). */
  private final long resourceUuidMsb;
  private final long resourceUuidLsb;

  /**
   * Superblock checks are open-time, not per-reader — and a NEW storage instance is created per
   * request-scoped open, so the once-per-JVM-per-path registry (not a per-instance flag) is what
   * actually avoids the two extra file opens + header reads per request.
   */
  private void validateSuperblocksOnce() {
    io.sirix.io.SuperblockValidator.validateOnce(getDataFilePath(), io.sirix.io.Superblock.ROLE_DATA,
        resourceUuidMsb, resourceUuidLsb);
    io.sirix.io.SuperblockValidator.validateOnce(getRevisionFilePath(), io.sirix.io.Superblock.ROLE_REVISIONS,
        resourceUuidMsb, resourceUuidLsb);
  }

  @Override
  public Reader createReader() {
    try {
      validateSuperblocksOnce();
      final int stripe = Math.floorMod(readerStripeCounter.getAndIncrement(), READER_CHANNEL_STRIPES);
      final FileChannel dataFileChannel;
      final FileChannel revisionsOffsetFileChannel;
      synchronized (readerChannelLock) {
        // Lazily open only the borrowed stripe: a storage whose readers never overlap holds at
        // most one channel pair, matching the old per-reader footprint.
        if (sharedDataFileChannels == null) {
          sharedDataFileChannels = new FileChannel[READER_CHANNEL_STRIPES];
          sharedRevisionsOffsetFileChannels = new FileChannel[READER_CHANNEL_STRIPES];
        }
        if (sharedDataFileChannels[stripe] == null) {
          final Path dataFilePath = createDirectoriesAndFile();
          final Path revisionsOffsetFilePath = getRevisionFilePath();
          createRevisionsOffsetFileIfNotExists(revisionsOffsetFilePath);
          sharedRevisionsOffsetFileChannels[stripe] = createRevisionsOffsetFileChannel(revisionsOffsetFilePath);
          sharedDataFileChannels[stripe] = createDataFileChannel(dataFilePath);
        }
        dataFileChannel = sharedDataFileChannels[stripe];
        revisionsOffsetFileChannel = sharedRevisionsOffsetFileChannels[stripe];
        borrowingReaders++;
      }

      return new FileChannelReader(dataFileChannel, revisionsOffsetFileChannel,
          new ByteHandlerPipeline(byteHandlerPipeline), SerializationType.DATA, new PagePersister(),
          cache.synchronous(), this::releaseReaderChannels);
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Hand back one reader's borrow of the shared stripes; the pool closes when the last borrower is
   * gone, so descriptors are held only while at least one read transaction is actually open.
   */
  private void releaseReaderChannels() {
    synchronized (readerChannelLock) {
      if (borrowingReaders > 0 && --borrowingReaders == 0) {
        closeSharedReaderChannels();
      }
    }
  }

  /** Close and clear the stripe arrays. Must be called under {@link #readerChannelLock}. */
  private void closeSharedReaderChannels() {
    if (sharedDataFileChannels != null) {
      closeAll(sharedDataFileChannels);
      sharedDataFileChannels = null;
    }
    if (sharedRevisionsOffsetFileChannels != null) {
      closeAll(sharedRevisionsOffsetFileChannels);
      sharedRevisionsOffsetFileChannels = null;
    }
  }

  /**
   * Best-effort close of every non-null channel. Used both for cleanup after a partially failed
   * stripe open (the original failure is rethrown by the caller) and on storage close, where a
   * close failure on a read-only channel must not mask or abort the remaining closes.
   */
  private static void closeAll(final FileChannel[] channels) {
    for (final FileChannel channel : channels) {
      if (channel != null) {
        try {
          channel.close();
        } catch (final IOException ignored) {
          // Intentionally swallowed — see javadoc.
        }
      }
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
    synchronized (readerChannelLock) {
      // Defensive: sessions close all their transactions (and thus every borrowing reader) before
      // closing the storage, but force-release anything still outstanding so descriptors never
      // outlive the storage.
      borrowingReaders = 0;
      closeSharedReaderChannels();
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

  @Override
  public RevisionIndexHolder getRevisionIndexHolder() {
    return revisionIndexHolder;
  }
}
