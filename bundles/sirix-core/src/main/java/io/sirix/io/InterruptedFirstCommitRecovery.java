package io.sirix.io;

import com.github.benmanes.caffeine.cache.AsyncCache;
import io.sirix.access.ResourceConfiguration;
import io.sirix.exception.SirixIOException;
import io.sirix.page.PageReference;
import io.sirix.page.UberPage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Open-time bootstrap-vs-load decision for a resource's storage, including the conservative
 * auto-heal for a crash during the resource's FIRST commit (layout-F5 gap).
 *
 * <p><b>The gap:</b> the superblock and both uber-page beacon slots of {@code sirix.data} are
 * only written at the END of the first commit (by {@code Writer.writeUberPageReference}); until
 * then the header region {@code [0, DATA_REGION_START)} is a sparse hole while the buffered page
 * writer may already have flushed &ge; 64 KiB of data pages past {@link IOStorage#DATA_REGION_START}.
 * A process crash in that window leaves a NON-EMPTY data file with an all-zero header (or, dying
 * between the superblock write and the beacon writes, a valid superblock with all-zero beacon
 * slots). {@link IOStorage#exists()} is size-based, so the next open takes the "load" path and
 * fails on superblock/beacon validation — permanently, although nothing was ever committed and a
 * fresh bootstrap is provably safe.
 *
 * <p><b>The heal:</b> when the open fails, the resource is re-initialized empty if and only if
 * the on-disk state PROVES no commit ever completed (see
 * {@link #provesNoCommitEverCompleted(Path, Path)}). Both files are truncated to zero and the
 * open is retried — which then runs the EXACT fresh-resource bootstrap path
 * ({@code exists() == false} → {@code new UberPage()}), not a duplicate of it. When the state is
 * not provable, the original exception (with its existing actionable message) is rethrown
 * unchanged.
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
   * Creates the {@link IOStorage} for the resource and loads its {@link UberPage} (or bootstraps
   * a fresh one if the storage holds no data yet) — the single open path shared by the JSON and
   * XML resource-session factories.
   *
   * <p>Any step of the open can surface the interrupted-first-commit state: storage creation
   * pre-loads revision metadata (reading the uber page reference), and the uber-page load
   * re-reads it. On failure the on-disk bytes are probed; a provably-uncommitted resource is
   * re-initialized empty (with a WARN log) and the open retried through the fresh-bootstrap
   * path, everything else rethrows the original failure.
   *
   * @param resourceConfig the resource configuration
   * @return the storage and the uber page to start from
   */
  public static StorageAndUberPage openStorageAndLoadUberPage(final ResourceConfiguration resourceConfig) {
    try {
      final IOStorage storage = StorageType.getStorage(resourceConfig);
      return new StorageAndUberPage(storage, loadUberPage(storage));
    } catch (final RuntimeException openFailure) {
      final Path dataFile = dataFilePath(resourceConfig);
      final Path revisionsFile = revisionsFilePath(resourceConfig);
      if (!provesNoCommitEverCompleted(dataFile, revisionsFile)) {
        // Not provable — keep the existing actionable failure untouched.
        throw openFailure;
      }
      LOGGER.warn("Interrupted FIRST commit detected for {}: the data file is non-empty (uncommitted flushed "
              + "pages) but carries no superblock/uber-page beacon and the revisions file holds no checksum-valid "
              + "revision record — no commit ever completed. Re-initializing the resource empty.",
          dataFile, openFailure);
      reinitializeEmpty(resourceConfig, dataFile, revisionsFile);
      // Retry: with both files truncated to zero, exists() is false and the SAME code path that
      // initializes a brand-new resource runs (bootstrap UberPage, no on-disk reads).
      final IOStorage storage = StorageType.getStorage(resourceConfig);
      return new StorageAndUberPage(storage, loadUberPage(storage));
    }
  }

  /**
   * Loads the {@link UberPage} from storage, or bootstraps a new one if the storage does not
   * hold data yet. This replaces {@code ResourceSessionModule.rootPage()} and was previously
   * duplicated in the JSON and XML database factories.
   */
  private static UberPage loadUberPage(final IOStorage storage) {
    if (storage.exists()) {
      try (final Reader reader = storage.createReader()) {
        final PageReference firstRef = reader.readUberPageReference();
        if (firstRef.getPage() == null) {
          return (UberPage) reader.read(firstRef, null);
        } else {
          return (UberPage) firstRef.getPage();
        }
      }
    } else {
      return new UberPage();
    }
  }

  /**
   * Probes the raw on-disk bytes for PROOF that no commit ever completed on this resource. All
   * of the following must hold (anything else — including any probe I/O error — is "not
   * provable" and the open failure must be rethrown):
   *
   * <ol>
   *   <li>{@code sirix.data} is non-empty (otherwise the open would have bootstrapped anyway —
   *       the failure has another cause) and its superblock block
   *       {@code [0, PRIMARY_BEACON_OFFSET)} is either ALL zero (crash before
   *       {@code writeUberPageReference} wrote anything) or a checksum-valid
   *       {@link Superblock#ROLE_DATA} superblock followed by zeros (crash between the
   *       superblock write and the beacon writes);</li>
   *   <li>BOTH beacon slots {@code [PRIMARY_BEACON_OFFSET, DATA_REGION_START)} are entirely
   *       zero — any nonzero byte (a length prefix, a torn payload, a checksum-valid slot)
   *       means a commit may have completed;</li>
   *   <li>{@code sirix.revisions} holds no checksum-valid revision record in any slot (the
   *       bootstrap commit writes revision 0's record at slot 0 — and the first user commit
   *       revision 1's at slot 1 — BEFORE the beacons go out; a valid record means a commit got
   *       far enough that data may exist).</li>
   * </ol>
   */
  static boolean provesNoCommitEverCompleted(final Path dataFile, final Path revisionsFile) {
    try {
      if (!Files.isRegularFile(dataFile) || Files.size(dataFile) == 0) {
        return false;
      }
      final byte[] header = readDataFileHeaderRegion(dataFile);
      if (!superblockRegionProvesNoCommit(header, dataFile)) {
        return false;
      }
      // Both beacon slots must be entirely zero (a file ending before DATA_REGION_START reads
      // as zeros — no beacon was ever written there either).
      for (int i = (int) IOStorage.PRIMARY_BEACON_OFFSET; i < header.length; i++) {
        if (header[i] != 0) {
          return false;
        }
      }
      return hasNoChecksumValidRevisionRecord(revisionsFile);
    } catch (final IOException | RuntimeException probeFailure) {
      LOGGER.debug("Interrupted-first-commit probe could not prove anything — keeping the original open failure",
          probeFailure);
      return false;
    }
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
   * The superblock block {@code [0, PRIMARY_BEACON_OFFSET)} proves an interrupted first commit
   * when it is all zeros, or when it is a checksum-valid {@code ROLE_DATA} superblock followed
   * by nothing but zeros. Any other content is unexplained corruption — not provable.
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
   * writer/reader checksum contract {@link IOStorage#revisionRecordChecksum(long, long)}. An
   * all-zero slot never matches — the checksum of {@code (0, 0)} is a nonzero constant.
   */
  private static boolean hasNoChecksumValidRevisionRecord(final Path revisionsFile) throws IOException {
    if (!Files.isRegularFile(revisionsFile)) {
      return true;
    }
    final long size = Files.size(revisionsFile);
    try (final FileChannel channel = FileChannel.open(revisionsFile, StandardOpenOption.READ)) {
      final ByteBuffer record =
          ByteBuffer.allocate(IOStorage.REVISIONS_FILE_RECORD_SIZE).order(ByteOrder.LITTLE_ENDIAN);
      for (long offset = IOStorage.REVISIONS_RECORDS_START; offset < size;
          offset += IOStorage.REVISIONS_FILE_RECORD_SIZE) {
        record.clear();
        int position = 0;
        while (record.hasRemaining()) {
          final int read = channel.read(record, offset + position);
          if (read < 0) {
            break;
          }
          position += read;
        }
        if (position < 3 * Long.BYTES) {
          continue; // a torn partial slot cannot hold a verifiable record
        }
        record.flip();
        final long revisionRootOffset = record.getLong();
        final long timestampMillis = record.getLong();
        final long storedChecksum = record.getLong();
        if (storedChecksum == IOStorage.revisionRecordChecksum(revisionRootOffset, timestampMillis)) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Truncates both files back to the pre-first-commit state (size 0) and drops every piece of
   * per-resource metadata a same-process predecessor may have registered for the old bytes, so
   * the retried open and the subsequent first commit behave exactly like a brand-new resource.
   */
  private static void reinitializeEmpty(final ResourceConfiguration resourceConfig, final Path dataFile,
      final Path revisionsFile) {
    try {
      truncateToZero(dataFile);
      truncateToZero(revisionsFile);
    } catch (final IOException e) {
      throw new SirixIOException(
          "Failed to re-initialize the interrupted-first-commit resource at " + dataFile, e);
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
