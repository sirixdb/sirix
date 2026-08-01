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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Factory to provide file channel access as a backend.
 *
 * @author Johannes Lichtenberger
 */
public final class FileChannelStorage implements IOStorage {

  private static final Logger LOGGER = LoggerFactory.getLogger(FileChannelStorage.class);

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
   * Grace period the shared stripes stay open after the LAST reader closes, in milliseconds
   * ({@code 0} restores immediate close). Reference counting alone made a workload of back-to-back
   * short read transactions — the common shape: open transaction, read a handful of nodes, close —
   * pay two {@code open(2)}s and two {@code close(2)}s per transaction, because the count returned
   * to zero between every pair. Lingering keeps the descriptors for a moment so the next
   * transaction finds them warm, while a session that genuinely goes idle still drops to zero
   * descriptors a moment later.
   */
  private static final long READER_CHANNEL_LINGER_MILLIS =
      Math.max(0L, Long.getLong("sirix.io.readerChannelLingerMillis", 2_000L));

  /**
   * One daemon timer for the whole JVM that closes stripe pools whose grace period expired. A
   * single thread suffices: the task is a lock acquisition plus at most {@code 2 × stripes}
   * channel closes, and it only runs for storages that actually went idle.
   */
  private static final ScheduledExecutorService READER_CHANNEL_REAPER =
      Executors.newSingleThreadScheduledExecutor(runnable -> {
        final Thread thread = new Thread(runnable, "sirix-reader-channel-reaper");
        thread.setDaemon(true);
        return thread;
      });

  /**
   * Round-robin stripe assignment, used only as the tie-breaker when every stripe is already
   * borrowed; the primary rule is {@link #pickStripe()}'s "least loaded, already open".
   */
  private final AtomicInteger readerStripeCounter = new AtomicInteger();

  /**
   * Number of live readers borrowing the stripes. Guarded by {@link #readerChannelLock}; the pool
   * closes when this drops to zero (after the linger period).
   */
  private int borrowingReaders;

  /**
   * Per-stripe borrow counts, so a released stripe can be recognised as idle and reused instead of
   * opening another one. Guarded by {@link #readerChannelLock}.
   */
  private final int[] stripeBorrowers = new int[READER_CHANNEL_STRIPES];

  /**
   * Pending linger close, or {@code null}. Guarded by {@link #readerChannelLock}.
   */
  private ScheduledFuture<?> lingerTask;

  /**
   * Set once {@link #close()} ran: no further lingering, and a late release closes immediately
   * rather than arming a timer against a dead storage. Guarded by {@link #readerChannelLock}.
   */
  private boolean readerPoolClosed;

  // ===== Writer channel pool =====

  /**
   * Lock guarding the writer channel triple and its borrow count.
   *
   * <p>A writer needs THREE descriptors (buffered data, revisions, and a second DSYNC handle to the
   * data file for the beacon slots), and writers are per-transaction — under {@code KEEP_OPEN} a
   * fresh one is created per COMMIT — so every commit opened and closed three files purely to
   * re-reach state the previous writer had. All writer I/O is positional, so one triple serves any
   * number of writers; the pool is reference-counted and lingers exactly like the reader pool.
   */
  private final Object writerChannelLock = new Object();

  private int borrowingWriters;

  private FileChannel sharedWriterDataChannel;

  private FileChannel sharedWriterRevisionsChannel;

  private FileChannel sharedWriterBeaconChannel;

  /** Pending linger close of the writer pool. Guarded by {@link #writerChannelLock}. */
  private ScheduledFuture<?> writerLingerTask;

  /** Set once {@link #close()} ran. Guarded by {@link #writerChannelLock}. */
  private boolean writerPoolClosed;

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
      final int borrowedStripe;
      final FileChannel dataFileChannel;
      final FileChannel revisionsOffsetFileChannel;
      synchronized (readerChannelLock) {
        // A borrow cancels any pending linger close. If the task already started it will block on
        // this monitor and then find borrowingReaders > 0, so it cannot close under this reader.
        cancelLingerClose();
        // Lazily open only the borrowed stripe: a storage whose readers never overlap holds at
        // most one channel pair, matching the old per-reader footprint.
        if (sharedDataFileChannels == null) {
          sharedDataFileChannels = new FileChannel[READER_CHANNEL_STRIPES];
          sharedRevisionsOffsetFileChannels = new FileChannel[READER_CHANNEL_STRIPES];
        }
        borrowedStripe = pickStripe();
        if (sharedDataFileChannels[borrowedStripe] == null) {
          final Path dataFilePath = createDirectoriesAndFile();
          final Path revisionsOffsetFilePath = getRevisionFilePath();
          createRevisionsOffsetFileIfNotExists(revisionsOffsetFilePath);
          sharedRevisionsOffsetFileChannels[borrowedStripe] =
              createRevisionsOffsetFileChannel(revisionsOffsetFilePath);
          sharedDataFileChannels[borrowedStripe] = createDataFileChannel(dataFilePath);
        }
        dataFileChannel = sharedDataFileChannels[borrowedStripe];
        revisionsOffsetFileChannel = sharedRevisionsOffsetFileChannels[borrowedStripe];
        stripeBorrowers[borrowedStripe]++;
        borrowingReaders++;
      }

      return new FileChannelReader(dataFileChannel, revisionsOffsetFileChannel,
          new ByteHandlerPipeline(byteHandlerPipeline), SerializationType.DATA, new PagePersister(),
          cache.synchronous(), () -> releaseReaderChannels(borrowedStripe));
    } catch (final IOException e) {
      throw new SirixIOException(e);
    }
  }

  /**
   * Choose the stripe for a new reader. Must be called under {@link #readerChannelLock}.
   *
   * <p>Preference order: an idle stripe that is ALREADY OPEN (free, and costs no {@code open(2)}),
   * then any idle stripe, then round-robin. Round-robin alone assigned overlapping readers to the
   * same stripe while others sat idle — on Windows, where positional reads take the channel's
   * position lock, that is exactly the serialization striping exists to avoid.
   *
   * @return the stripe index to borrow
   */
  private int pickStripe() {
    for (int idle = 0; idle < READER_CHANNEL_STRIPES; idle++) {
      if (stripeBorrowers[idle] == 0 && sharedDataFileChannels[idle] != null) {
        return idle;
      }
    }
    for (int idle = 0; idle < READER_CHANNEL_STRIPES; idle++) {
      if (stripeBorrowers[idle] == 0) {
        return idle;
      }
    }
    return Math.floorMod(readerStripeCounter.getAndIncrement(), READER_CHANNEL_STRIPES);
  }

  /**
   * Hand back one reader's borrow of the shared stripes; the pool closes once the last borrower is
   * gone AND the linger period expires, so descriptors are held only while read transactions are
   * actually being opened.
   *
   * @param stripe the stripe index this reader borrowed
   */
  private void releaseReaderChannels(final int stripe) {
    synchronized (readerChannelLock) {
      if (stripeBorrowers[stripe] > 0) {
        stripeBorrowers[stripe]--;
      }
      if (borrowingReaders > 0 && --borrowingReaders == 0) {
        if (readerPoolClosed || READER_CHANNEL_LINGER_MILLIS == 0L) {
          closeSharedReaderChannels();
        } else {
          cancelLingerClose();
          lingerTask = READER_CHANNEL_REAPER.schedule(this::closeStripesIfStillIdle,
                                                      READER_CHANNEL_LINGER_MILLIS, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  /** Linger expiry: close the stripes unless a reader borrowed them again meanwhile. */
  private void closeStripesIfStillIdle() {
    synchronized (readerChannelLock) {
      lingerTask = null;
      if (borrowingReaders == 0) {
        closeSharedReaderChannels();
      }
    }
  }

  /** Drop any pending linger close. Must be called under {@link #readerChannelLock}. */
  private void cancelLingerClose() {
    if (lingerTask != null) {
      lingerTask.cancel(false);
      lingerTask = null;
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
      // WRITER channels. Lazy-revision-record profile (default, with preallocated commits): the
      // revisions channel is BUFFERED — the per-commit 32-byte record's durability rides the
      // checksummed tail-log in the uber-beacon slots (covered by the data channel's write-ahead
      // fdatasync), removing the synchronous revisions write's device round-trip from every
      // commit. Legacy profile: the revisions channel is write-through (SYNC — content AND all
      // metadata per write, since the 32-byte record EXTENDS the file and its size must be
      // durable at write-return even on stacks with weak fdatasync size semantics); its only
      // writes are the per-commit record and the one-time superblock, so the commit protocol
      // needs no separate revisions fsync. Either way the beacon channel is a second DSYNC handle
      // to the data file for the two uber-page slot writes — in-place overwrites, so
      // data-integrity write-through (FUA on NVMe) suffices for ordering + acknowledge. The bulk
      // data channel stays buffered.
      //
      // ONE derivation point for both flags: the channel's open mode and the writer's durability
      // protocol MUST agree, so they are computed here and passed down rather than re-read.
      final boolean preallocatedCommit = IOStorage.preallocatedCommitsEnabled();
      final boolean lazyRevisionRecords = IOStorage.lazyRevisionRecordsEnabled();

      final FileChannel revisionsOffsetFileChannel;
      final FileChannel dataFileChannel;
      final FileChannel beaconDurableChannel;
      synchronized (writerChannelLock) {
        cancelWriterLingerClose();
        if (sharedWriterDataChannel == null) {
          // The profile flags decide the revisions channel's open mode, so a pool opened under one
          // profile must never be handed to a writer running the other. They are derived from
          // system properties that do not change within a JVM, so opening once is sound — but the
          // pool is dropped wholesale on close(), which is the only place the mode could differ.
          sharedWriterRevisionsChannel = lazyRevisionRecords
              ? FileChannel.open(revisionsOffsetFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE)
              : FileChannel.open(revisionsOffsetFilePath, StandardOpenOption.READ, StandardOpenOption.WRITE,
                                 StandardOpenOption.SYNC);
          sharedWriterBeaconChannel =
              FileChannel.open(dataFilePath, StandardOpenOption.WRITE, StandardOpenOption.DSYNC);
          // Opened LAST: if either open above threw, nothing has been published and the next
          // borrow retries from scratch rather than inheriting a half-built triple.
          sharedWriterDataChannel = createDataFileChannel(dataFilePath);
        }
        revisionsOffsetFileChannel = sharedWriterRevisionsChannel;
        dataFileChannel = sharedWriterDataChannel;
        beaconDurableChannel = sharedWriterBeaconChannel;
        borrowingWriters++;
      }

      final var byteHandlePipeline = new ByteHandlerPipeline(byteHandlerPipeline);
      final var serializationType = SerializationType.DATA;
      final var pagePersister = new PagePersister();
      // The reader delegate shares the pooled channels, so it gets a no-op release: closing it must
      // free its own state without closing channels other writers are still using.
      final var reader = new FileChannelReader(dataFileChannel, revisionsOffsetFileChannel, byteHandlePipeline,
          serializationType, pagePersister, cache.synchronous(), () -> { });

      return new FileChannelWriter(dataFileChannel, revisionsOffsetFileChannel, beaconDurableChannel,
          serializationType, pagePersister, cache, revisionIndexHolder, reader, preallocatedCommit,
          lazyRevisionRecords, revisionsOffsetFilePath, resourceUuidMsb, resourceUuidLsb,
          this::releaseWriterChannels);
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
      readerPoolClosed = true;
      cancelLingerClose();
      borrowingReaders = 0;
      Arrays.fill(stripeBorrowers, 0);
      closeSharedReaderChannels();
    }
    synchronized (writerChannelLock) {
      writerPoolClosed = true;
      cancelWriterLingerClose();
      borrowingWriters = 0;
      closeSharedWriterChannels();
    }
  }

  /**
   * Hand back one writer's borrow of the shared triple. Mirrors
   * {@link #releaseReaderChannels(int)}: the pool closes once the last borrower is gone AND the
   * linger period expires, so a stream of per-commit writers reuses one set of descriptors.
   */
  private void releaseWriterChannels() {
    synchronized (writerChannelLock) {
      if (borrowingWriters > 0 && --borrowingWriters == 0) {
        if (writerPoolClosed || READER_CHANNEL_LINGER_MILLIS == 0L) {
          closeSharedWriterChannels();
        } else {
          cancelWriterLingerClose();
          writerLingerTask = READER_CHANNEL_REAPER.schedule(this::closeWriterChannelsIfStillIdle,
                                                            READER_CHANNEL_LINGER_MILLIS, TimeUnit.MILLISECONDS);
        }
      }
    }
  }

  /** Linger expiry: close the writer triple unless a writer borrowed it again meanwhile. */
  private void closeWriterChannelsIfStillIdle() {
    synchronized (writerChannelLock) {
      writerLingerTask = null;
      if (borrowingWriters == 0) {
        closeSharedWriterChannels();
      }
    }
  }

  /** Drop any pending writer linger close. Must be called under {@link #writerChannelLock}. */
  private void cancelWriterLingerClose() {
    if (writerLingerTask != null) {
      writerLingerTask.cancel(false);
      writerLingerTask = null;
    }
  }

  /**
   * Close the writer triple. Must be called under {@link #writerChannelLock}.
   *
   * <p>The data channel is FORCED first: it is buffered, and the last writer's {@code close()}
   * already forced it, but a pool that outlived that writer may have absorbed writes from a
   * subsequent one that failed before its own force. Closing a buffered channel does not flush, so
   * skipping this could drop bytes a commit believed it had handed to the OS.
   */
  private void closeSharedWriterChannels() {
    final FileChannel[] channels =
        {sharedWriterDataChannel, sharedWriterRevisionsChannel, sharedWriterBeaconChannel};
    sharedWriterDataChannel = null;
    sharedWriterRevisionsChannel = null;
    sharedWriterBeaconChannel = null;
    for (final FileChannel channel : channels) {
      if (channel == null || !channel.isOpen()) {
        continue;
      }
      try {
        channel.force(false);
      } catch (final IOException e) {
        LOGGER.warn("Could not force a pooled writer channel before closing it", e);
      }
    }
    closeAll(channels);
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
