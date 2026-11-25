package io.sirix.io;

import org.jspecify.annotations.Nullable;
import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.sirix.access.InMemoryResources;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.DatabaseConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.UnsupportedStoreFeatureException;
import io.sirix.io.bytepipe.ByteHandlerPipeline;
import io.sirix.io.filechannel.FileChannelReader;
import io.sirix.page.PagePersister;
import io.sirix.page.SerializationType;
import io.sirix.page.UberPage;
import io.sirix.page.StoreFeatures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Open-time bootstrap-vs-load decision for a resource's storage, including the conservative
 * auto-heal for a crash during the resource's FIRST commit (layout-F5 gap).
 *
 * <p>
 * <b>The gap, variant one (interrupted header write):</b> the superblock and both uber-page beacon
 * slots of {@code sirix.data} are only written at the END of the first commit (by
 * {@code Writer.writeUberPageReference}); until then the header region
 * {@code [0, DATA_REGION_START)} is a sparse hole while the buffered page writer may already have
 * flushed &ge; 64 KiB of data pages past {@link IOStorage#DATA_REGION_START}. A process crash in
 * that window leaves a NON-EMPTY data file with an all-zero header (or, dying between the
 * superblock write and the beacon writes, a valid superblock with all-zero beacon slots).
 * {@link IOStorage#exists()} is size-based, so the next open takes the "load" path and fails on
 * superblock/beacon validation — permanently, although nothing was ever committed and a fresh
 * bootstrap is provably safe.
 *
 * <p>
 * <b>The gap, variant two (lost revision record of the bootstrap commit):</b> resource creation
 * itself runs the EMPTY bootstrap commit (revision 0). Its 32-byte revision record is the ONLY path
 * from the (durably written) uber beacons to the revision's root page, and the record lives in a
 * DIFFERENT, just-created file — an environment-level loss (e.g. the new {@code sirix.revisions}
 * directory entry dropped by a power cut, or the file truncated by filesystem repair) leaves
 * checksum-valid beacons advertising revision 0 with no record to dereference. The next open then
 * fails with "Truncated revisions record for revision 0" — permanently, although the only revision
 * ever acknowledged is the empty bootstrap one and nothing on disk is reachable.
 *
 * <p>
 * <b>The heal:</b> when the open fails, the resource is re-initialized empty if and only if the
 * on-disk state PROVES that at most the empty bootstrap revision was ever committed AND no
 * checksum-valid revision record survives and no beacon carries active feature words — i.e. nothing
 * recoverable exists (see
 * {@link #provesAtMostEmptyBootstrapCommitted(ResourceConfiguration, Path, Path)}). Both files are
 * truncated to zero and the open is retried — which then runs the EXACT fresh-resource bootstrap
 * path ({@code exists() == false} → {@code new UberPage()}), not a duplicate of it. When the state
 * is not provable — any beacon advertising a revision past the bootstrap one, any torn/garbage
 * beacon bytes, any checksum-valid revision record, any foreign superblock bytes — data may exist
 * and the original exception (with its existing actionable message) is rethrown unchanged.
 *
 * @author Johannes Lichtenberger
 */
public final class InterruptedFirstCommitRecovery {

  private static final Logger LOGGER = LoggerFactory.getLogger(InterruptedFirstCommitRecovery.class);

  private InterruptedFirstCommitRecovery() {
    throw new AssertionError("May not be instantiated!");
  }

  /** The opened storage plus the uber page the resource session starts from. */
  public record StorageAndUberPage(IOStorage storage, UberPage uberPage) {
  }

  /**
   * Creates the {@link IOStorage} for the resource and loads its {@link UberPage} (or bootstraps a
   * fresh one if the storage holds no data yet) — the single open path shared by the JSON and XML
   * resource-session factories.
   *
   * <p>
   * Any step of the open can surface the interrupted-first-commit state: storage creation pre-loads
   * revision metadata. On failure the on-disk bytes are probed; a resource provably holding nothing
   * but (at most) the unreachable empty bootstrap revision is re-initialized empty (with a WARN log)
   * and the open retried through the fresh-bootstrap path, everything else rethrows the original
   * failure.
   *
   * @param resourceConfig the resource configuration
   * @return the storage and the uber page to start from
   */
  public static StorageAndUberPage openStorageAndLoadUberPage(final ResourceConfiguration resourceConfig) {
    return openStorageAndLoadUberPage(resourceConfig, null);
  }

  public static StorageAndUberPage openStorageAndLoadUberPage(final ResourceConfiguration resourceConfig,
      final @Nullable UberPage openingUberPage) {
    try {
      final IOStorage storage = StorageType.getStorage(resourceConfig, openingUberPage);
      try {
        return new StorageAndUberPage(storage, storage.getOpeningUberPage());
      } catch (final RuntimeException failure) {
        storage.close();
        throw failure;
      }
    } catch (final UnsupportedStoreFeatureException unsupported) {
      throw unsupported;
    } catch (final RuntimeException openFailure) {
      final Path dataFile = dataFilePath(resourceConfig);
      final Path revisionsFile = revisionsFilePath(resourceConfig);
      if (!provesAtMostEmptyBootstrapCommitted(resourceConfig, dataFile, revisionsFile)) {
        // Not provable — keep the existing actionable failure untouched.
        throw openFailure;
      }
      LOGGER.warn("Interrupted FIRST commit detected for {}: the data file is non-empty but its uber-page "
          + "beacons either were never written or advertise only the EMPTY bootstrap revision, and the "
          + "revisions file holds no checksum-valid revision record — no commit carrying data ever became "
          + "durable and nothing on disk is reachable. Re-initializing the resource empty.", dataFile, openFailure);
      reinitializeEmpty(resourceConfig, dataFile, revisionsFile);
      // Retry: with both files truncated to zero, exists() is false and the SAME code path that
      // initializes a brand-new resource runs (bootstrap UberPage, no on-disk reads).
      final IOStorage storage = StorageType.getStorage(resourceConfig);
      return new StorageAndUberPage(storage, storage.getOpeningUberPage());
    }
  }

  /**
   * Open for verification, compaction or erasure with strict feature compatibility before storage
   * initialization, without empty-bootstrap repair. Compatible storage initialization may still heal
   * revision records. The caller owns and must close the returned storage.
   */
  public static StorageAndUberPage openStorageForTool(final ResourceConfiguration resourceConfig) {
    final UberPage openingUberPage = probeUberPage(resourceConfig);
    openingUberPage.getFeatures().requireToolCompatible();
    final IOStorage storage = StorageType.getStorage(resourceConfig, openingUberPage);
    try {
      final UberPage page = storage.getOpeningUberPage();
      page.getFeatures().requireToolCompatible();
      return new StorageAndUberPage(storage, page);
    } catch (final RuntimeException failure) {
      storage.close();
      throw failure;
    }
  }

  public record ReaderAndUberPage(Reader reader, UberPage uberPage) {
  }

  /**
   * Opens existing storage read-only with private revision metadata and fresh disk validation. Unlike
   * session opens, this cannot repair or rebuild the store being verified. The caller must close the
   * returned reader.
   */
  // Ownership transfers to the returned reader: the nested finally blocks close each acquired
  // channel until reader construction, then close the reader unless initialization completes.
  // ToolReaderOwnershipTest checks real OS handles on every acquisition/initialization failure.
  @SuppressWarnings("java:S2095")
  public static ReaderAndUberPage openReaderForTool(final ResourceConfiguration config) {
    // Keep disk-engine beacon, feature, superblock/UUID and revision-index checks with private
    // metadata.
    final UberPage uberPage = probeUberPage(config);
    uberPage.getFeatures().requireToolCompatible();
    IOStorage.requireCommittedBeacon(config, uberPage);
    final Path dataFile = dataFilePath(config);
    final Path revisionsFile = revisionsFilePath(config);
    final long uuidHigh = config.resourceUuid == null
        ? 0L
        : config.resourceUuid.getMostSignificantBits();
    final long uuidLow = config.resourceUuid == null
        ? 0L
        : config.resourceUuid.getLeastSignificantBits();
    SuperblockValidator.validate(dataFile, Superblock.ROLE_DATA, uuidHigh, uuidLow);
    SuperblockValidator.validate(revisionsFile, Superblock.ROLE_REVISIONS, uuidHigh, uuidLow);
    boolean channelsTransferred = false;
    boolean transferred = false;
    try {
      final FileChannel data = FileChannel.open(dataFile, StandardOpenOption.READ);
      try {
        final FileChannel revisions = FileChannel.open(revisionsFile, StandardOpenOption.READ);
        try {
          final FileChannelReader reader =
              new FileChannelReader(data, revisions, new ByteHandlerPipeline(config.byteHandlePipeline),
                  SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build());
          channelsTransferred = true;
          try {
            IOStorage.readRevisionIndex(reader, uberPage.getRevisionNumber());
            final ReaderAndUberPage opened = new ReaderAndUberPage(reader, uberPage);
            transferred = true;
            return opened;
          } finally {
            if (!transferred) {
              reader.close();
            }
          }
        } finally {
          if (!channelsTransferred) {
            revisions.close();
          }
        }
      } finally {
        if (!channelsTransferred) {
          data.close();
        }
      }
    } catch (final IOException failure) {
      throw new SirixIOException(failure);
    }
  }

  public static StoreFeatures probeFeatures(final ResourceConfiguration resourceConfig) {
    return probeUberPage(resourceConfig).getFeatures();
  }

  public static UberPage probeUberPage(final ResourceConfiguration resourceConfig) {
    final Path dataFile = dataFilePath(resourceConfig);
    try {
      if (!Files.exists(dataFile) || Files.size(dataFile) == 0) {
        return new UberPage();
      }
      try (final FileChannel channel = FileChannel.open(dataFile, StandardOpenOption.READ);
          final FileChannelReader reader =
              new FileChannelReader(channel, channel, new ByteHandlerPipeline(resourceConfig.byteHandlePipeline),
                  SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build())) {
        try {
          return reader.readOpeningUberPage();
        } catch (final SirixIOException corrupt) {
          final ByteBuffer slots =
              ByteBuffer.allocate((int) (IOStorage.DATA_REGION_START - IOStorage.PRIMARY_BEACON_OFFSET));
          while (slots.hasRemaining()) {
            if (channel.read(slots, IOStorage.PRIMARY_BEACON_OFFSET + slots.position()) <= 0) {
              break;
            }
          }
          if (isAllZero(slots.array(), 0, slots.capacity())) {
            return new UberPage();
          }
          throw corrupt;
        }
      }
    } catch (final IOException failure) {
      throw new SirixIOException(failure);
    }
  }

  public static void requireDatabaseToolCompatible(final Path database) {
    final Path resources = database.resolve(DatabaseConfiguration.DatabasePaths.DATA.getFile());
    if (!Files.isDirectory(resources)) {
      return;
    }
    try (final DirectoryStream<Path> paths = Files.newDirectoryStream(resources)) {
      for (final Path path : paths) {
        if (!Files.isDirectory(path) || InMemoryResources.isRetiredResourceName(path.getFileName().toString())) {
          continue;
        }
        probeFeatures(ResourceConfiguration.deserialize(path)).requireToolCompatible();
      }
    } catch (final IOException failure) {
      throw new SirixIOException(failure);
    }
  }

  /**
   * Probes the raw on-disk bytes for PROOF that at most the EMPTY bootstrap revision was ever
   * committed and that nothing on disk is reachable. All of the following must hold (anything else —
   * including any probe I/O error — is "not provable" and the open failure must be rethrown):
   *
   * <ol>
   * <li>{@code sirix.data} is non-empty (otherwise the open would have bootstrapped anyway — the
   * failure has another cause) and its superblock block {@code [0, PRIMARY_BEACON_OFFSET)} is either
   * ALL zero (crash before {@code writeUberPageReference} wrote anything) or a checksum-valid
   * {@link Superblock#ROLE_DATA} superblock followed by zeros (crash between the superblock write and
   * the beacon writes);</li>
   * <li>EACH beacon slot in {@code [PRIMARY_BEACON_OFFSET, DATA_REGION_START)} is either entirely
   * zero (never written — no commit was ever acknowledged) or a checksum-valid uber beacon
   * advertising the BOOTSTRAP revision with no active feature words (revision number 0 — only the
   * empty commit that resource creation itself runs was ever acknowledged). Torn/garbage slot bytes,
   * active feature words or any slot advertising a later revision prevent repair;</li>
   * <li>{@code sirix.revisions} holds no checksum-valid revision record in any slot (the bootstrap
   * commit writes revision 0's record at slot 0 — and the first user commit revision 1's at slot 1 —
   * BEFORE the beacons go out; a valid record means a commit got far enough that data may exist, and
   * it also means whatever failed the open was not the missing-record gap this recovery is for).</li>
   * </ol>
   *
   * <p>
   * Under these conditions the only acknowledged state is (at most) the empty bootstrap revision
   * whose record is gone — every byte past the header is unreachable, so re-running the fresh
   * bootstrap loses nothing.
   */
  static boolean provesAtMostEmptyBootstrapCommitted(final ResourceConfiguration resourceConfig, final Path dataFile,
      final Path revisionsFile) {
    try {
      if (!Files.isRegularFile(dataFile) || Files.size(dataFile) == 0) {
        return false;
      }
      final byte[] header = readDataFileHeaderRegion(dataFile);
      if (!superblockRegionProvesNoCommit(header, dataFile)) {
        return false;
      }
      if (!beaconSlotsProveAtMostBootstrapRevision(resourceConfig, header, dataFile)) {
        return false;
      }
      return hasNoChecksumValidRevisionRecord(revisionsFile);
    } catch (final IOException | RuntimeException probeFailure) {
      LOGGER.debug("Interrupted-first-commit probe could not prove anything — keeping the original open failure",
          probeFailure);
      return false;
    }
  }

  /**
   * Each beacon slot must be entirely zero (a file ending before {@code DATA_REGION_START} reads as
   * zeros — never written) or a checksum-valid beacon advertising revision number 0 with no active
   * feature words, the empty bootstrap revision committed by resource creation itself. The slot is
   * parsed by the canonical verifier ({@code AbstractReader#isEmptyBootstrapBeacon}); the beacon-slot
   * format is identical across the file-based storage backends, so the {@link FileChannelReader}
   * parser is authoritative for all of them. The reader's revisions-channel parameter is only used
   * for revision-record reads, which the beacon parse never performs — the data channel is passed in
   * both positions so the probe cannot require {@code sirix.revisions} to exist (its loss may be
   * exactly what is being probed).
   */
  private static boolean beaconSlotsProveAtMostBootstrapRevision(final ResourceConfiguration resourceConfig,
      final byte[] header, final Path dataFile) throws IOException {
    final boolean primaryAllZero =
        isAllZero(header, (int) IOStorage.PRIMARY_BEACON_OFFSET, (int) IOStorage.SECONDARY_BEACON_OFFSET);
    final boolean secondaryAllZero =
        isAllZero(header, (int) IOStorage.SECONDARY_BEACON_OFFSET, (int) IOStorage.DATA_REGION_START);
    if (primaryAllZero && secondaryAllZero) {
      return true;
    }
    try (final FileChannel dataChannel = FileChannel.open(dataFile, StandardOpenOption.READ)) {
      final FileChannelReader reader =
          new FileChannelReader(dataChannel, dataChannel, new ByteHandlerPipeline(resourceConfig.byteHandlePipeline),
              SerializationType.DATA, new PagePersister(), Caffeine.newBuilder().build());
      if (!primaryAllZero && !reader.isEmptyBootstrapBeacon(IOStorage.PRIMARY_BEACON_OFFSET)) {
        return false;
      }
      return secondaryAllZero || reader.isEmptyBootstrapBeacon(IOStorage.SECONDARY_BEACON_OFFSET);
    }
  }

  private static boolean isAllZero(final byte[] bytes, final int from, final int to) {
    for (int i = from; i < to; i++) {
      if (bytes[i] != 0) {
        return false;
      }
    }
    return true;
  }

  /** Reads {@code [0, DATA_REGION_START)} of the data file; bytes past EOF stay zero. */
  private static byte[] readDataFileHeaderRegion(final Path dataFile) throws IOException {
    final byte[] header = new byte[(int) IOStorage.DATA_REGION_START];
    try (final FileChannel channel = FileChannel.open(dataFile, StandardOpenOption.READ)) {
      final ByteBuffer buffer = ByteBuffer.wrap(header);
      int position = 0;
      while (buffer.hasRemaining()) {
        final int read = channel.read(buffer, position);
        if (read < 0) {
          break;
        }
        position += read;
      }
    }
    return header;
  }

  /**
   * The superblock block {@code [0, PRIMARY_BEACON_OFFSET)} proves an interrupted first commit when
   * it is all zeros, or when it is a checksum-valid {@code ROLE_DATA} superblock followed by nothing
   * but zeros. Any other content is unexplained corruption — not provable.
   */
  private static boolean superblockRegionProvesNoCommit(final byte[] header, final Path dataFile) {
    boolean allZero = true;
    for (int i = 0; i < (int) IOStorage.PRIMARY_BEACON_OFFSET; i++) {
      if (header[i] != 0) {
        allZero = false;
        break;
      }
    }
    if (allZero) {
      return true;
    }
    try {
      // Reuses the canonical validation (magic, layout version, endianness, role, checksum).
      Superblock.validate(ByteBuffer.wrap(header, 0, Superblock.BYTES), Superblock.ROLE_DATA,
          dataFile.getFileName().toString());
    } catch (final RuntimeException notAValidSuperblock) {
      return false;
    }
    for (int i = Superblock.BYTES; i < (int) IOStorage.PRIMARY_BEACON_OFFSET; i++) {
      if (header[i] != 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Scans every revisions-file slot (deterministic layout, see
   * {@link IOStorage#revisionsFileOffset(int)}) for a checksum-valid record, reusing the
   * writer/reader checksum contract
   * {@link IOStorage#expectedRevisionRecordChecksum(long, long, long)} for both live records and
   * tombstones. An all-zero slot never matches: the checksum of the three zero identity fields is
   * nonzero, unlike the slot's stored checksum.
   */
  private static boolean hasNoChecksumValidRevisionRecord(final Path revisionsFile) throws IOException {
    if (!Files.isRegularFile(revisionsFile)) {
      return true;
    }
    final long size = Files.size(revisionsFile);
    try (final FileChannel channel = FileChannel.open(revisionsFile, StandardOpenOption.READ)) {
      final ByteBuffer record =
          ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      for (long offset = IOStorage.REVISIONS_RECORDS_START; offset < size; offset +=
          IOStorage.REVISIONS_FILE_RECORD_SIZE) {
        record.clear();
        int position = 0;
        while (record.hasRemaining()) {
          final int read = channel.read(record, offset + position);
          if (read < 0) {
            break;
          }
          position += read;
        }
        if (position < 4 * Long.BYTES) {
          continue; // a torn partial slot cannot hold a verifiable record
        }
        record.flip();
        final long revisionRootOffset = record.getLong();
        final long timestampMicros = record.getLong();
        final long storedChecksum = record.getLong();
        // Both live records and retention tombstones prove that this is not an interrupted
        // first commit. Never reinitialize a resource with a checksum-valid tombstone.
        final long pageHash = record.getLong();
        if (storedChecksum == IOStorage.expectedRevisionRecordChecksum(revisionRootOffset, timestampMicros, pageHash)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Truncates both files back to the pre-first-commit state (size 0) and drops every piece of
   * per-resource metadata a same-process predecessor may have registered for the old bytes, so the
   * retried open and the subsequent first commit behave exactly like a brand-new resource.
   */
  private static void reinitializeEmpty(final ResourceConfiguration resourceConfig, final Path dataFile,
      final Path revisionsFile) {
    try {
      truncateToZero(dataFile);
      truncateToZero(revisionsFile);
    } catch (final IOException e) {
      throw new SirixIOException("Failed to re-initialize the interrupted-first-commit resource at " + dataFile, e);
    }
    // The valid-superblock variant may already have registered the data file as validated; the
    // header the first commit re-creates must be validated again (same contract as removal).
    SuperblockValidator.invalidateUnder(dataFile.getParent());
    // Per-resource revision metadata is populated at WRITE time (before durability) — drop any
    // entries a crashed same-process first commit left behind; harmless cross-process.
    final AsyncCache<Integer, RevisionFileData> revisionFileDataCache = StorageType.CACHE_REPOSITORY.get(dataFile);
    if (revisionFileDataCache != null) {
      revisionFileDataCache.synchronous().invalidateAll();
    }
    final RevisionIndexHolder revisionIndexHolder = StorageType.REVISION_INDEX_REPOSITORY.get(dataFile);
    if (revisionIndexHolder != null) {
      revisionIndexHolder.update(RevisionIndex.EMPTY);
    }
    // The re-initialized resource reuses the file offsets of the truncated bytes; pages of the
    // crashed first commit may sit in the warm global caches under those offsets (same pattern
    // as the .commit-marker truncation recovery).
    io.sirix.access.Databases.clearCachesForDatabase(resourceConfig.getDatabaseId());
  }

  private static void truncateToZero(final Path file) throws IOException {
    if (!Files.exists(file)) {
      return;
    }
    try (final FileChannel channel = FileChannel.open(file, StandardOpenOption.WRITE)) {
      channel.truncate(0);
      channel.force(true);
    }
  }

  private static Path dataFilePath(final ResourceConfiguration resourceConfig) {
    return resourceConfig.resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                      .resolve(IOStorage.FILENAME);
  }

  private static Path revisionsFilePath(final ResourceConfiguration resourceConfig) {
    return resourceConfig.resourcePath.resolve(ResourceConfiguration.ResourcePaths.DATA.getPath())
                                      .resolve(IOStorage.REVISIONS_FILENAME);
  }
}
