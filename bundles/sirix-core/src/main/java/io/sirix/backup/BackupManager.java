package io.sirix.backup;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.InMemoryResources;
import io.sirix.access.Databases;
import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.AbstractResourceSession;
import io.sirix.access.trx.page.NodeStorageEngineReader;
import io.sirix.io.Reader;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.api.StorageEngineReader;
import io.sirix.cache.BufferManagerImpl;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.index.aggregate.DerivedIndexMaintainer;
import io.sirix.io.InterruptedFirstCommitRecovery;
import io.sirix.utils.SirixFiles;

import org.jspecify.annotations.Nullable;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Online backup and restore for SirixDB databases.
 *
 * <h2>Consistency mechanism (why the copy is crash-consistent)</h2>
 *
 * SirixDB resources are append-only, and every mutation of a resource's on-disk state — the
 * {@code data/sirix.data} page store, the {@code data/sirix.revisions} revision index, index
 * definitions and the {@code update-operations} diff files — is made by a node read-write
 * transaction. Not all of them are made inside {@code commit()}: an
 * {@link io.sirix.access.trx.node.AfterCommitState#KEEP_OPEN_ASYNC_FLUSH} or
 * {@link io.sirix.access.trx.node.AfterCommitState#KEEP_OPEN_ASYNC_COMMIT} transaction flushes
 * pages to {@code sirix.data} from a background thread before its commit. What bounds all of them
 * is the writer lock: a read-write transaction can only be opened while holding the resource's
 * writer {@link Semaphore}, which is <em>JVM-global per resource path</em> (handed out by
 * {@code WriteLocksRegistry}, shared across every {@code Database}/{@code ResourceSession} handle
 * for the same resource — see {@code AbstractResourceSession#beginNodeTrx}), and closing it awaits
 * every pending asynchronous flush. Holding that semaphore therefore means no thread is writing the
 * resource's files.
 *
 * <p>
 * There is no public "lock only" hook on {@link ResourceSession}, so this class acquires the
 * semaphore the supported way: it opens a read-write transaction via
 * {@link ResourceSession#beginNodeTrx()}, performs <em>no</em> modifications, holds the transaction
 * open while that resource's files are copied, and afterwards closes the owning session, which
 * aborts and closes its transaction. Rolling back an empty transaction is purely in-memory (it
 * clears the transaction-intent log; verified against {@code NodeStorageEngineWriter#rollback}) —
 * it never writes to the resource files.
 *
 * <p>
 * While the writer semaphore is held by our empty transaction:
 * <ul>
 * <li>no other read-write transaction exists for that resource, so no bytes of {@code sirix.data}
 * are appended, the dual uber-page beacon slots (offsets 4096/8192) are not rewritten, and no
 * revision slot is added to {@code sirix.revisions} — the (data, revisions) pair is
 * <b>byte-stable</b> and mutually consistent at the last committed revision;</li>
 * <li>{@code beginNodeTrx} has already executed the leftover-{@code .commit}-marker crash recovery
 * (truncation to the last durable revision) <em>before</em> we start copying, so the image we copy
 * is a recovered, consistent state;</li>
 * <li>because the files are append-only with checksummed beacons, the copied image is exactly what
 * a crash-free shutdown at that revision would have left behind: opening the copy reads the beacon,
 * finds the last committed revision and never looks at bytes beyond it.</li>
 * </ul>
 *
 * <p>
 * Operational prerequisites and caveats are documented in {@code docs/BACKUP.md}.
 *
 * <h2>What is copied</h2>
 *
 * The whole database directory ({@code dbsetting.obj}, {@code keyselector/}, and per resource:
 * {@code data/}, {@code ressetting.obj}, {@code indexes/}, {@code update-operations/}), preserving
 * the directory structure. Runtime files are skipped: the database {@code .lock} file, everything
 * inside a resource's transaction-intent-log directory ({@code log/}, which only ever holds the
 * in-flight {@code .commit} marker), and atomic-write temporaries ({@code *.tmp*} — both
 * {@code dbsetting*.tmp} and diff-file {@code *.json.tmp*} spill files).
 *
 * <p>
 * Each backed-up resource's hidden {@code <name>$derived} sibling is copied with it (TXN-613). It
 * is a resource of its own that holds the temporal-aggregate data the public resource listing
 * hides, so omitting it would silently drop that data from the backup. A derived store is written
 * by the database's own {@code DerivedIndexMaintainer} rather than by a caller's transaction, so
 * the copy holds its history lock — the lock every derived publication, fold and rewind takes —
 * instead of its writer semaphore. A backup names the copied derived resources in
 * {@link BackupSummary#derivedResources()}; one whose maintenance does not become quiescent within
 * {@value #DERIVED_QUIESCENCE_SECONDS} seconds is refused rather than copied mid-build.
 *
 * <p>
 * IN_MEMORY resources are never copied (TXN-615): their committed state lives only in the open
 * database. A backup names them in {@link BackupSummary#notBackedUp()}, and a restore skips any of
 * their leftovers — including a derived sibling — in a cold copy.
 */
public final class BackupManager {

  /** Per-resource result: name, the revision the copy represents, and bytes copied. */
  public record ResourceSummary(String resourceName, int mostRecentRevision, long bytesCopied) {
  }

  /**
   * Result of a backup or restore run.
   *
   * @param resources the visible resources, in the order they were copied
   * @param derivedResources the hidden {@code $derived} resources copied with their source (TXN-613).
   *        They hold temporal-aggregate data, are not part of {@code resources} and are never listed
   *        by {@code Database#listResources()}
   * @param notBackedUp names of the IN_MEMORY resources a backup skipped (TXN-615): their state lives
   *        only in the open database, so a backup never holds them; empty for a restore
   */
  public record BackupSummary(Path sourceDir, Path targetDir, List<ResourceSummary> resources,
      List<ResourceSummary> derivedResources, long totalBytesCopied, List<String> notBackedUp) {

    public BackupSummary {
      resources = List.copyOf(resources);
      derivedResources = List.copyOf(derivedResources);
      notBackedUp = List.copyOf(notBackedUp);
    }

    /** A summary with no hidden derived resource that skipped nothing. */
    public BackupSummary(final Path sourceDir, final Path targetDir, final List<ResourceSummary> resources,
        final long totalBytesCopied) {
      this(sourceDir, targetDir, resources, List.of(), totalBytesCopied, List.of());
    }

    /** Number of visible resources copied; the hidden derived resources are counted separately. */
    public int resourcesCopied() {
      return resources.size();
    }

    /** Number of hidden {@code $derived} resources copied. */
    public int derivedResourcesCopied() {
      return derivedResources.size();
    }
  }

  /** Name of the directory below the database directory that contains the resources. */
  private static final Path RESOURCES_DIR = DatabaseConfiguration.DatabasePaths.DATA.getFile();

  /** Name of the database lock file ({@code .lock}). */
  private static final String LOCK_FILE_NAME =
      DatabaseConfiguration.DatabasePaths.LOCK.getFile().getFileName().toString();

  /**
   * Name of a resource's transaction-intent-log directory ({@code log}) — holds only the transient
   * {@code .commit} marker of an in-flight commit, never backup-worthy data.
   */
  private static final String INTENT_LOG_DIR_NAME =
      ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath().getFileName().toString();

  /**
   * Seconds a backup waits for a hidden derived resource's maintenance to become quiescent before it
   * refuses the backup. Matches the writer-lock acquisition timeout a visible resource fails fast on,
   * so both halves of a resource have the same "another writer is busy, retry later" behaviour.
   */
  static final int DERIVED_QUIESCENCE_SECONDS = 5;

  private static final Duration DERIVED_QUIESCENCE_TIMEOUT = Duration.ofSeconds(DERIVED_QUIESCENCE_SECONDS);

  private BackupManager() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Creates a consistent online backup of the SirixDB database at {@code databaseDir} in
   * {@code targetDir}.
   *
   * <p>
   * The target must either not exist or be an empty directory. Failure cleanup follows the
   * best-effort policy in {@code docs/BACKUP.md}.
   *
   * @param databaseDir the database directory to back up
   * @param targetDir the directory to copy the database into
   * @return a summary (resources copied, bytes, most recent revision per resource)
   * @throws SirixUsageException if {@code databaseDir} is not a SirixDB database, or the target
   *         exists and is not an empty directory, or the target lies inside the source
   * @throws SirixIOException if copying fails
   */
  public static BackupSummary backupDatabase(final Path databaseDir, final Path targetDir) {
    requireNonNull(databaseDir);
    requireNonNull(targetDir);

    final Path source = databaseDir.toAbsolutePath().normalize();
    final Path target = targetDir.toAbsolutePath().normalize();

    requireSirixDatabase(source);
    requireEmptyOrAbsentTarget(target);
    if (target.startsWith(source)) {
      throw new SirixUsageException(
          "Backup target " + target + " must not lie inside the database directory " + source + ".");
    }

    // Resolve the type BEFORE creating the target, so a non-deserializable config fails cleanly.
    final DatabaseType databaseType = Databases.getDatabaseType(source);

    InterruptedFirstCommitRecovery.requireDatabaseToolCompatible(source);

    final boolean targetExisted = Files.exists(target);
    final List<ResourceSummary> resourceSummaries = new ArrayList<>();
    final List<ResourceSummary> derivedSummaries = new ArrayList<>();
    final List<String> notBackedUp = new ArrayList<>();
    long totalBytes = 0;
    boolean success = false;
    final boolean[] interrupted = {false};
    try {
      try {
        Files.createDirectories(target);
      } catch (final IOException e) {
        throw new SirixIOException("Could not create backup target directory " + target + ".", e);
      }

      // Open the database FIRST: opening may mint or re-key the persisted database id (and
      // rewrite dbsetting.obj), so copying the config afterwards captures the final state.
      try (final Database<?> database = openDatabase(source, databaseType)) {
        try {
          // Phase A — database-level files (dbsetting.obj, keyselector/, …); the resource
          // subtrees are copied per-resource under the writer lock in phase B. dbsetting.obj is
          // only ever replaced via an atomic temp-file move, so reading it concurrently is safe.
          totalBytes += copyTree(source, target, rel -> rel.getName(0).toString().equals(RESOURCES_DIR.toString()),
              BackupManager::isTransientDatabaseFile);

          final Path targetResources = target.resolve(RESOURCES_DIR);
          try {
            Files.createDirectories(targetResources);
          } catch (final IOException e) {
            throw new SirixIOException("Could not create " + targetResources + ".", e);
          }

          // Phase B — each resource, copied while holding ITS writer semaphore.
          for (final Path resourcePath : database.listResources()) {
            if (!Files.isDirectory(resourcePath)) {
              continue; // stray non-directory entry below resources/ — not a resource
            }
            final String resourceName = resourcePath.getFileName().toString();
            if (InMemoryResources.isInMemoryOwned(resourcePath)) {
              // TXN-615 clause 10: an IN_MEMORY resource lives only in the open database.
              notBackedUp.add(resourceName);
              continue;
            }
            final CopiedResource copied = copyResource(database, resourcePath, targetResources, interrupted);
            resourceSummaries.add(copied.resource());
            final ResourceSummary derivedSummary = copied.derived();
            if (derivedSummary != null) {
              derivedSummaries.add(derivedSummary);
            }
            totalBytes += copied.totalBytesCopied();
          }
          totalBytes += reconcileResourceIds(target);
        } finally {
          interrupted[0] |= Thread.interrupted();
        }
      }

      success = true;
      return new BackupSummary(source, target, resourceSummaries, derivedSummaries, totalBytes, notBackedUp);
    } finally {
      finishBackup(target, targetExisted, success, interrupted[0]);
    }
  }

  private static void finishBackup(final Path target, final boolean targetExisted, final boolean success,
      final boolean deferredInterruption) {
    final boolean interrupted = Thread.interrupted() || deferredInterruption;
    try {
      if (!success) {
        cleanupTarget(target, targetExisted);
      }
    } finally {
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private static long reconcileResourceIds(final Path target) {
    final DatabaseConfiguration config = DatabaseConfiguration.deserialize(target);
    long nextId = config.getMaxResourceID();
    try (final DirectoryStream<Path> resources = Files.newDirectoryStream(target.resolve(RESOURCES_DIR))) {
      for (final Path resource : resources) {
        if (Files.isDirectory(resource)) {
          nextId = Math.max(nextId, Math.addExact(ResourceConfiguration.deserialize(resource).getID(), 1L));
        }
      }
      final Path settings = target.resolve(DatabaseConfiguration.DatabasePaths.CONFIG_BINARY.getFile());
      final long previousSize = Files.size(settings);
      config.setMaximumResourceID(nextId);
      DatabaseConfiguration.serialize(config);
      return Files.size(settings) - previousSize;
    } catch (final IOException failure) {
      throw new SirixIOException("Could not reserve copied resource IDs in " + target + ".", failure);
    }
  }

  /**
   * What copying one resource produced: its own summary, plus its hidden derived sibling's if it has
   * one.
   */
  private record CopiedResource(ResourceSummary resource, @Nullable ResourceSummary derived) {

    long totalBytesCopied() {
      return resource.bytesCopied() + (derived == null
          ? 0L
          : derived.bytesCopied());
    }
  }

  /**
   * Copies one persistent resource and its hidden derived sibling while holding the resource's writer
   * semaphore and, for the sibling, its maintainer's history lock.
   */
  private static CopiedResource copyResource(final Database<?> database, final Path resourcePath,
      final Path targetResources, final boolean[] interrupted) {
    final String resourceName = resourcePath.getFileName().toString();
    try (final ResourceSession<?, ?> session = database.beginResourceSession(resourceName)) {
      try {
        // Refuse before writer creation can run marker recovery; recheck under the lock below.
        try (final NodeReadOnlyTrx reader = session.beginNodeReadOnlyTrx()) {
          reader.getStorageEngineReader().getUberPage().getFeatures().requireToolCompatible();
        }
        // Acquire the resource's JVM-global writer semaphore (see class javadoc). This also runs
        // leftover-.commit crash recovery before we copy. Fails fast (~5s) if another transaction is
        // writing.
        // This session owns the empty writer and aborts/closes it on every exit.
        final NodeTrx lockHolder = session.beginNodeTrx();
        lockHolder.getStorageEngineWriter().getUberPage().getFeatures().requireToolCompatible();
        final int mostRecentRevision = session.getMostRecentRevisionNumber();
        // The hidden derived sibling is quiesced before the source copy starts, so both halves of
        // this resource are copied from one state the database never left.
        final DerivedQuiescence derived = quiesceDerivedResource(session, resourcePath, interrupted);
        try {
          final long bytes = copyTree(resourcePath, targetResources.resolve(resourceName), rel -> false,
              BackupManager::isTransientResourceFile);
          final Path derivedPath = derived.derivedPath();
          return new CopiedResource(new ResourceSummary(resourceName, mostRecentRevision, bytes), derivedPath == null
              ? null
              : copyDerivedResource(derivedPath, targetResources, mostRecentRevision));
        } finally {
          derived.release();
        }
      } finally {
        interrupted[0] |= Thread.interrupted();
      }
    }
  }

  /**
   * A source resource's hidden derived sibling, held at rest while the resource's files are copied.
   *
   * @param derivedPath the derived resource directory, or {@code null} if the source has none
   * @param held the maintainer resource whose history lock this quiescence holds, or {@code null} if
   *        the source has no derived sibling
   */
  private record DerivedQuiescence(@Nullable Path derivedPath, DerivedIndexMaintainer.@Nullable Resource held) {

    void release() {
      if (held != null) {
        held.unlockHistory();
      }
    }
  }

  /**
   * Brings the hidden derived sibling of {@code resourcePath} to rest so its files can be copied.
   *
   * <p>
   * A derived store is written by the database's own maintainer, not by a caller's transaction, so
   * its writer semaphore stays taken for the maintainer's whole lifetime and is useless as a copy
   * barrier. Its history lock is the barrier: every base publication, aggregate fold, summary fold
   * and rewind holds it.
   *
   * @throws SirixIOException if maintenance does not become quiescent in time — the alternative would
   *         be a partial copy of a store that is being written
   */
  // TXN-613: finishBackup restores interruption after writer/session/database/target cleanup,
  // including
  // cleanup failure. Re-interrupting here would prevent those interruptible lifetimes from closing.
  @SuppressWarnings("java:S2142")
  private static DerivedQuiescence quiesceDerivedResource(final ResourceSession<?, ?> session, final Path resourcePath,
      final boolean[] interrupted) {
    final Path derivedPath = resourcePath.resolveSibling(resourcePath.getFileName() + DerivedIndexMaintainer.SUFFIX);
    if (!Files.isDirectory(derivedPath)) {
      return new DerivedQuiescence(null, null);
    }
    // Opening a visible source with an existing sibling installs that sibling's maintainer.
    final DerivedIndexMaintainer.Resource maintained =
        requireNonNull(((AbstractResourceSession<?, ?>) session).getDerivedResource());
    final boolean quiescent;
    try {
      quiescent = maintained.tryLockHistory(DERIVED_QUIESCENCE_TIMEOUT);
    } catch (final InterruptedException interruption) {
      interrupted[0] = true;
      throw new SirixIOException(
          "Interrupted while waiting for the derived resource " + derivedPath.getFileName() + " to become quiescent.",
          interruption);
    }
    if (!quiescent) {
      throw new SirixIOException("The derived resource " + derivedPath.getFileName()
          + " is still being built: its maintenance did not become quiescent within " + DERIVED_QUIESCENCE_SECONDS
          + "s, and copying it now would make the backup partial. "
          + "Retry the backup once its temporal-aggregate maintenance has caught up.");
    }
    if (maintained.hasPendingTruncate()) {
      maintained.unlockHistory();
      throw new SirixIOException("The derived resource " + derivedPath.getFileName()
          + " has a pending history rewind. Retry the backup once its temporal-aggregate maintenance has caught up.");
    }
    return new DerivedQuiescence(derivedPath, maintained);
  }

  /** Copies a quiesced derived resource and reports the verified copy's state. */
  private static ResourceSummary copyDerivedResource(final Path derivedPath, final Path targetResources,
      final int sourceHead) {
    final Path target = targetResources.resolve(derivedPath.getFileName());
    copyTree(derivedPath, target, rel -> false, BackupManager::isTransientResourceFile);
    return verifyDerivedResource(target, sourceHead);
  }

  /**
   * Restores the backup at {@code backupDir} to {@code targetDir} and verifies the result by opening
   * every restored resource read-only.
   *
   * <p>
   * Before verification, each restored resource receives a fresh incarnation id under FMT-209,
   * including copied hidden derived resources; copied history and last-revert metadata are retained.
   *
   * <p>
   * The target must either not exist or be an empty directory. Failure cleanup follows the
   * best-effort policy in {@code docs/BACKUP.md}.
   *
   * <p>
   * Note: the verification pass opens the restored database; if the database it was copied from is
   * open in the same JVM, the restored copy is transparently re-keyed to a fresh database id (its
   * {@code dbsetting.obj} is rewritten) so both can run side by side.
   *
   * @param backupDir the backup directory (as produced by {@link #backupDatabase(Path, Path)}, or any
   *        cold copy of a database directory)
   * @param targetDir the directory to restore the database into
   * @return a summary (resources restored, bytes, most recent revision per resource)
   * @throws SirixUsageException if {@code backupDir} is not a valid database/backup directory, or the
   *         target exists and is not an empty directory, or the target lies inside the backup
   * @throws SirixIOException if copying, incarnation-id publication or verification fails
   */
  public static BackupSummary restoreDatabase(final Path backupDir, final Path targetDir) {
    requireNonNull(backupDir);
    requireNonNull(targetDir);

    final Path source = backupDir.toAbsolutePath().normalize();
    final Path target = targetDir.toAbsolutePath().normalize();

    // A backup IS a database directory — validate the same structure.
    requireSirixDatabase(source);
    requireEmptyOrAbsentTarget(target);
    if (target.startsWith(source)) {
      throw new SirixUsageException(
          "Restore target " + target + " must not lie inside the backup directory " + source + ".");
    }

    final DatabaseType databaseType = Databases.getDatabaseType(source);

    try {
      InterruptedFirstCommitRecovery.requireDatabaseToolCompatible(source);
    } catch (final RuntimeException failure) {
      throw new SirixIOException("Verification of backup resource features failed: " + failure.getMessage(), failure);
    }

    final boolean targetExisted = Files.exists(target);
    boolean success = false;
    try {
      try {
        Files.createDirectories(target);
      } catch (final IOException e) {
        throw new SirixIOException("Could not create restore target directory " + target + ".", e);
      }

      // A backup is cold — no locking needed; copy everything (minus transients, defensively). A cold
      // copy of a database may hold the leftovers of IN_MEMORY resources: a restore never creates
      // those (TXN-615 clause 10).
      final long totalBytes = copyTree(source, target, rel -> isInMemoryResourceDirectory(source, rel),
          rel -> isTransientDatabaseFile(rel) || isTransientResourceFile(relativeToResource(rel)));

      // FMT-209 / TXN-613/614: every restored resource is a new incarnation. Draw its id before the
      // verification pass below first exposes it; copied history and derived state stay as copied.
      drawRestoredIncarnationIds(target);

      // Verify derived stores without installing their owners first: opening a source session
      // could rebuild a damaged sibling and hide the corruption this pass must report.
      final VerifiedResources verified = verifyRestoredDatabase(target, databaseType);

      success = true;
      return new BackupSummary(source, target, verified.resources(), verified.derived(), totalBytes, List.of());
    } finally {
      if (!success) {
        cleanupTarget(target, targetExisted);
      }
    }
  }

  /**
   * Publishes a fresh incarnation id (FMT-209) for every restored resource directory, hidden
   * {@code $derived} resources included. Publication guarantees belong to
   * {@link ResourceConfiguration#publishFreshIncarnationId(Path)}.
   */
  private static void drawRestoredIncarnationIds(final Path target) {
    final Path resources = target.resolve(RESOURCES_DIR);
    if (!Files.isDirectory(resources)) {
      return;
    }
    try (final DirectoryStream<Path> entries = Files.newDirectoryStream(resources)) {
      for (final Path resource : entries) {
        // A settings file already proves this entry is a resource directory; avoid a second stat.
        if (Files.exists(resource.resolve(ResourceConfiguration.ResourcePaths.CONFIG_BINARY.getPath()))) {
          ResourceConfiguration.publishFreshIncarnationId(resource);
        }
      }
    } catch (final IOException e) {
      throw new SirixIOException("Could not list the restored resources of " + target + ".", e);
    }
  }

  /**
   * Verifies hidden derived stores through isolated tool readers, then opens read-only transactions
   * on the visible resources.
   *
   * <p>
   * The hidden derived resources are verified <em>first</em> and reported separately (TXN-613).
   * Opening a source resource installs its derived maintainer, which deletes and rebuilds a derived
   * store it cannot read — so verifying the sources first would repair the very damage this pass
   * exists to report.
   */
  private static VerifiedResources verifyRestoredDatabase(final Path target, final DatabaseType databaseType) {
    final List<ResourceSummary> resourceSummaries = new ArrayList<>();
    final List<ResourceSummary> derivedSummaries = new ArrayList<>();
    try {
      InterruptedFirstCommitRecovery.requireDatabaseToolCompatible(target);
    } catch (final RuntimeException failure) {
      throw new SirixIOException("Verification of restored resource features failed — "
          + "the restore has been rolled back: " + failure.getMessage(), failure);
    }
    try (final Database<?> database = openDatabase(target, databaseType)) {
      final List<Path> resourcePaths = database.listResources();
      for (final Path resourcePath : resourcePaths) {
        if (!Files.isDirectory(resourcePath)) {
          continue; // stray non-directory entry below resources/ — not a resource
        }
        final Path derivedPath =
            resourcePath.resolveSibling(resourcePath.getFileName() + DerivedIndexMaintainer.SUFFIX);
        if (Files.isDirectory(derivedPath)) {
          final int sourceHead =
              InterruptedFirstCommitRecovery.probeUberPage(ResourceConfiguration.deserialize(resourcePath))
                                            .getRevisionNumber();
          derivedSummaries.add(verifyDerivedResource(derivedPath, sourceHead));
        }
      }
      for (final Path resourcePath : resourcePaths) {
        if (!Files.isDirectory(resourcePath)) {
          continue;
        }
        resourceSummaries.add(verifyRestoredResource(database, resourcePath));
      }
    }
    return new VerifiedResources(resourceSummaries, derivedSummaries);
  }

  /** What a restore's verification pass found: the visible resources and the hidden derived ones. */
  private record VerifiedResources(List<ResourceSummary> resources, List<ResourceSummary> derived) {
  }

  /**
   * Verifies one restored hidden derived resource and reports what it holds.
   *
   * <p>
   * No source session may be opened here: it could rebuild the sibling before its directory,
   * committed root and durable frontier/mappings have been checked. The isolated tool reader also
   * prevents a previous copy at this path from supplying cached revision metadata.
   */
  private static ResourceSummary verifyDerivedResource(final Path derivedPath, final int sourceHead) {
    final String derivedName = derivedPath.getFileName().toString();
    try {
      if (ResourceConfiguration.ResourcePaths.compareStructure(derivedPath) != 0) {
        throw new SirixIOException("Incomplete derived resource: " + derivedName);
      }
      // A verification open observes only this path's on-disk bytes and leaves no process-wide state
      // behind.
      final ResourceConfiguration config = ResourceConfiguration.deserializeForTool(derivedPath);
      final InterruptedFirstCommitRecovery.ReaderAndUberPage opened =
          InterruptedFirstCommitRecovery.openReaderForTool(config);
      try (final Reader disk = opened.reader();
          final BufferManagerImpl buffers = BufferManagerImpl.forVerification();
          final StorageEngineReader reader =
              NodeStorageEngineReader.forTool(config, opened.uberPage(), disk, buffers)) {
        final int mostRecentRevision = opened.uberPage().getRevisionNumber();
        if (reader.getActualRevisionRootPage().getRevision() != mostRecentRevision) {
          throw new SirixIOException("Derived resource " + derivedName + " holds an unexpected revision.");
        }
        DerivedIndexMaintainer.validateBackupSnapshot(reader, sourceHead);
        return new ResourceSummary(derivedName, mostRecentRevision, directorySize(derivedPath));
      }
    } catch (final RuntimeException e) {
      throw new SirixIOException("Verification of derived resource '" + derivedName + "' failed: " + e.getMessage(), e);
    }
  }

  /** Opens one restored resource read-only and reports what it holds. */
  private static ResourceSummary verifyRestoredResource(final Database<?> database, final Path resourcePath) {
    final String resourceName = resourcePath.getFileName().toString();
    try (final ResourceSession<?, ?> session = database.beginResourceSession(resourceName)) {
      final int mostRecentRevision = session.getMostRecentRevisionNumber();
      try (final NodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
        if (rtx.getRevisionNumber() != mostRecentRevision) {
          throw new SirixIOException("Restored resource " + resourceName + " opened at revision "
              + rtx.getRevisionNumber() + " instead of the most recent revision " + mostRecentRevision + ".");
        }
      }
      return new ResourceSummary(resourceName, mostRecentRevision, directorySize(resourcePath));
    } catch (final RuntimeException e) {
      throw new SirixIOException("Verification of restored resource '" + resourceName + "' failed — "
          + "the restore has been rolled back: " + e.getMessage(), e);
    }
  }

  private static Database<?> openDatabase(final Path databaseDir, final DatabaseType databaseType) {
    return switch (databaseType) {
      case JSON -> Databases.openJsonDatabase(databaseDir);
      case XML -> Databases.openXmlDatabase(databaseDir);
    };
  }

  private static void requireSirixDatabase(final Path databaseDir) {
    if (!Files.isDirectory(databaseDir) || DatabaseConfiguration.DatabasePaths.compareStructure(databaseDir) != 0) {
      throw new SirixUsageException(databaseDir + " is not a SirixDB database directory.");
    }
  }

  private static void requireEmptyOrAbsentTarget(final Path target) {
    if (Files.exists(target) && !SirixFiles.isDirectoryEmpty(target)) {
      throw new SirixUsageException("Target " + target + " already exists and is not an empty directory.");
    }
  }

  /**
   * Attempts to remove a partially written target and recreate the bare directory if it pre-existed.
   */
  private static void cleanupTarget(final Path target, final boolean targetExisted) {
    try {
      SirixFiles.recursiveRemove(target);
      if (targetExisted) {
        Files.createDirectories(target);
      }
    } catch (final RuntimeException | IOException ignored) {
      // Best effort — the original failure is what propagates.
    }
  }

  /** Whether {@code relativePath} is a resource directory that belongs to an IN_MEMORY resource. */
  private static boolean isInMemoryResourceDirectory(final Path databaseDir, final Path relativePath) {
    return relativePath.getNameCount() == 2 && relativePath.getName(0).toString().equals(RESOURCES_DIR.toString())
        && InMemoryResources.isInMemoryOwned(databaseDir.resolve(relativePath));
  }

  /** Database-level runtime files excluded from the backup. */
  private static boolean isTransientDatabaseFile(final Path relativePath) {
    final String fileName = relativePath.getFileName().toString();
    return (relativePath.getNameCount() == 1 && fileName.equals(LOCK_FILE_NAME)) || fileName.contains(".tmp");
  }

  /**
   * Transient resource-level entries ({@code relativePath} relative to the resource directory):
   * everything inside the transaction-intent-log directory (the {@code .commit} marker), and
   * atomic-write temporaries (diff-file {@code *.tmp*} spills).
   */
  private static boolean isTransientResourceFile(final @Nullable Path relativePath) {
    if (relativePath == null) {
      return false;
    }
    final String fileName = relativePath.getFileName().toString();
    return relativePath.getName(0).toString().equals(INTENT_LOG_DIR_NAME) || fileName.contains(".tmp");
  }

  /**
   * Maps a path relative to the DATABASE directory to one relative to its resource directory
   * ({@code resources/<name>/rest…} → {@code rest…}), or {@code null} if it is not inside a resource.
   */
  private static @Nullable Path relativeToResource(final Path databaseRelativePath) {
    if (databaseRelativePath.getNameCount() <= 2
        || !databaseRelativePath.getName(0).toString().equals(RESOURCES_DIR.toString())) {
      return null;
    }
    return databaseRelativePath.subpath(2, databaseRelativePath.getNameCount());
  }

  /**
   * Recursively copies {@code sourceRoot} into {@code targetRoot}, preserving the directory structure
   * (including empty directories). {@code skipSubtree} prunes whole subtrees, {@code skipFile} skips
   * individual regular files; both receive paths relative to {@code sourceRoot}. Copies never replace
   * existing files ({@code REPLACE_EXISTING} is deliberately off — the target is validated empty).
   *
   * @return total bytes copied
   */
  private static long copyTree(final Path sourceRoot, final Path targetRoot, final Predicate<Path> skipSubtree,
      final Predicate<Path> skipFile) {
    final long[] bytesCopied = {0L};
    try {
      Files.walkFileTree(sourceRoot, new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs) throws IOException {
          final Path relativePath = sourceRoot.relativize(dir);
          if (relativePath.toString().isEmpty()) {
            Files.createDirectories(targetRoot); // the root itself
          } else {
            if (skipSubtree.test(relativePath)) {
              return FileVisitResult.SKIP_SUBTREE;
            }
            Files.createDirectories(targetRoot.resolve(relativePath));
          }
          return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
          final Path relativePath = sourceRoot.relativize(file);
          if (skipSubtree.test(relativePath) || skipFile.test(relativePath)) {
            return FileVisitResult.CONTINUE;
          }
          Files.copy(file, targetRoot.resolve(relativePath), StandardCopyOption.COPY_ATTRIBUTES);
          bytesCopied[0] += attrs.size();
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (final IOException e) {
      throw new SirixIOException("Copy from " + sourceRoot + " to " + targetRoot + " failed.", e);
    }
    return bytesCopied[0];
  }

  /** Size in bytes of all regular files below {@code dir}. */
  private static long directorySize(final Path dir) {
    final long[] size = {0L};
    try {
      Files.walkFileTree(dir, new SimpleFileVisitor<>() {
        @Override
        public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) {
          size[0] += attrs.size();
          return FileVisitResult.CONTINUE;
        }
      });
    } catch (final IOException e) {
      throw new SirixIOException("Could not determine size of " + dir + ".", e);
    }
    return size[0];
  }
}
