package io.sirix.backup;

import io.sirix.access.DatabaseConfiguration;
import io.sirix.access.DatabaseType;
import io.sirix.access.Databases;
import io.sirix.api.Database;
import io.sirix.api.NodeReadOnlyTrx;
import io.sirix.api.NodeTrx;
import io.sirix.api.ResourceSession;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.utils.SirixFiles;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static java.util.Objects.requireNonNull;

/**
 * Online backup and restore for SirixDB databases.
 *
 * <h2>Consistency mechanism (why the copy is crash-consistent)</h2>
 *
 * SirixDB resources are append-only: all mutations to a resource's on-disk state — the
 * {@code data/sirix.data} page store, the {@code data/sirix.revisions} revision index, index
 * definitions and the {@code update-operations} diff files — happen exclusively inside
 * {@code commit()} of a node read-write transaction. A read-write transaction can only be opened
 * while holding the resource's writer {@link java.util.concurrent.Semaphore}, which is
 * <em>JVM-global per resource path</em> (handed out by {@code WriteLocksRegistry}, shared across
 * every {@code Database}/{@code ResourceSession} handle for the same resource — see
 * {@code AbstractResourceSession#beginNodeTrx}).
 *
 * <p>
 * There is no public "lock only" hook on {@link ResourceSession}, so this class acquires the
 * semaphore the supported way: it opens a read-write transaction via
 * {@link ResourceSession#beginNodeTrx()}, performs <em>no</em> modifications, holds the
 * transaction open while that resource's files are copied, and afterwards calls
 * {@link NodeTrx#rollback()} + {@link NodeTrx#close()}. Rolling back an empty transaction is
 * purely in-memory (it clears the transaction-intent log; verified against
 * {@code NodeStorageEngineWriter#rollback}) — it never writes to the resource files.
 *
 * <p>
 * While the writer semaphore is held by our empty transaction:
 * <ul>
 * <li>no other transaction can commit, so no bytes of {@code sirix.data} are appended, the
 * dual uber-page beacon slots (offsets 4096/8192) are not rewritten, and no revision slot is
 * added to {@code sirix.revisions} — the (data, revisions) pair is <b>byte-stable</b> and
 * mutually consistent at the last committed revision;</li>
 * <li>{@code beginNodeTrx} has already executed the leftover-{@code .commit}-marker crash
 * recovery (truncation to the last durable revision) <em>before</em> we start copying, so the
 * image we copy is a recovered, consistent state;</li>
 * <li>because the files are append-only with checksummed beacons, the copied image is exactly
 * what a crash-free shutdown at that revision would have left behind: opening the copy reads
 * the beacon, finds the last committed revision and never looks at bytes beyond it.</li>
 * </ul>
 *
 * <p>
 * <b>Scope of the guarantee:</b> the writer semaphore is JVM-local. Run backups either embedded
 * in the process that owns the database or against a database no other process has open (SirixDB
 * assumes single-process access to a database directory anyway). Resources created concurrently
 * <em>while</em> the backup is running are not included (the resource list is snapshotted once);
 * if another transaction holds a resource's writer lock, the backup fails fast (after the
 * 5-second acquisition timeout of {@code beginNodeTrx}) instead of copying a moving target.
 *
 * <h2>What is copied</h2>
 *
 * The whole database directory ({@code dbsetting.obj}, {@code keyselector/}, and per resource:
 * {@code data/}, {@code ressetting.obj}, {@code indexes/}, {@code update-operations/}),
 * preserving the directory structure. Transient files are skipped: the database {@code .lock}
 * file, everything inside a resource's transaction-intent-log directory ({@code log/}, which
 * only ever holds the in-flight {@code .commit} marker), and atomic-write temporaries
 * ({@code *.tmp*} — both {@code dbsetting*.tmp} and diff-file {@code *.json.tmp*} spill files).
 */
public final class BackupManager {

  /** Per-resource result: name, the revision the copy represents, and bytes copied. */
  public record ResourceSummary(String resourceName, int mostRecentRevision, long bytesCopied) {
  }

  /** Result of a backup or restore run. */
  public record BackupSummary(Path sourceDir, Path targetDir, List<ResourceSummary> resources, long totalBytesCopied) {

    public BackupSummary {
      resources = List.copyOf(resources);
    }

    /** Number of resources copied. */
    public int resourcesCopied() {
      return resources.size();
    }
  }

  /** Name of the directory below the database directory that contains the resources. */
  private static final Path RESOURCES_DIR = DatabaseConfiguration.DatabasePaths.DATA.getFile();

  /** Name of the database lock file ({@code .lock}). */
  private static final String LOCK_FILE_NAME =
      DatabaseConfiguration.DatabasePaths.LOCK.getFile().getFileName().toString();

  /**
   * Name of a resource's transaction-intent-log directory ({@code log}) — holds only the
   * transient {@code .commit} marker of an in-flight commit, never backup-worthy data.
   */
  private static final String INTENT_LOG_DIR_NAME =
      io.sirix.access.ResourceConfiguration.ResourcePaths.TRANSACTION_INTENT_LOG.getPath().getFileName().toString();

  private BackupManager() {
    throw new AssertionError("May not be instantiated!");
  }

  /**
   * Creates a consistent online backup of the SirixDB database at {@code databaseDir} in
   * {@code targetDir}.
   *
   * <p>
   * The target must either not exist or be an empty directory. On failure, everything written to
   * the target is removed again.
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
      throw new SirixUsageException("Backup target " + target + " must not lie inside the database directory "
          + source + ".");
    }

    // Resolve the type BEFORE creating the target, so a non-deserializable config fails cleanly.
    final DatabaseType databaseType = Databases.getDatabaseType(source);

    final boolean targetExisted = Files.exists(target);
    final List<ResourceSummary> resourceSummaries = new ArrayList<>();
    long totalBytes = 0;
    boolean success = false;
    try {
      try {
        Files.createDirectories(target);
      } catch (final IOException e) {
        throw new SirixIOException("Could not create backup target directory " + target + ".", e);
      }

      // Open the database FIRST: opening may mint or re-key the persisted database id (and
      // rewrite dbsetting.obj), so copying the config afterwards captures the final state.
      try (final Database<?> database = openDatabase(source, databaseType)) {
        // Phase A — database-level files (dbsetting.obj, keyselector/, …); the resource
        // subtrees are copied per-resource under the writer lock in phase B. dbsetting.obj is
        // only ever replaced via an atomic temp-file move, so reading it concurrently is safe.
        totalBytes += copyTree(source, target,
                               rel -> rel.getName(0).toString().equals(RESOURCES_DIR.toString()),
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
          final long bytes;
          final int mostRecentRevision;
          try (final ResourceSession<?, ?> session = database.beginResourceSession(resourceName)) {
            // Acquire the resource's JVM-global writer semaphore (see class javadoc). This also
            // runs leftover-.commit crash recovery before we copy. Fails fast (~5s) if another
            // transaction is writing.
            final NodeTrx lockHolder = session.beginNodeTrx();
            try {
              mostRecentRevision = session.getMostRecentRevisionNumber();
              bytes = copyTree(resourcePath, targetResources.resolve(resourceName),
                               rel -> false,
                               BackupManager::isTransientResourceFile);
            } finally {
              // Rollback of an UNMODIFIED trx is in-memory only; close releases the semaphore.
              lockHolder.rollback();
              lockHolder.close();
            }
          }
          resourceSummaries.add(new ResourceSummary(resourceName, mostRecentRevision, bytes));
          totalBytes += bytes;
        }
      }

      success = true;
      return new BackupSummary(source, target, resourceSummaries, totalBytes);
    } finally {
      if (!success) {
        cleanupTarget(target, targetExisted);
      }
    }
  }

  /**
   * Restores the backup at {@code backupDir} to {@code targetDir} and verifies the result by
   * opening every restored resource read-only.
   *
   * <p>
   * The target must either not exist or be an empty directory. If the copy or the verification
   * pass fails, the partial target is deleted.
   *
   * <p>
   * Note: the verification pass opens the restored database; if the database it was copied from
   * is open in the same JVM, the restored copy is transparently re-keyed to a fresh database id
   * (its {@code dbsetting.obj} is rewritten) so both can run side by side.
   *
   * @param backupDir the backup directory (as produced by {@link #backupDatabase(Path, Path)}, or
   *        any cold copy of a database directory)
   * @param targetDir the directory to restore the database into
   * @return a summary (resources restored, bytes, most recent revision per resource)
   * @throws SirixUsageException if {@code backupDir} is not a valid database/backup directory, or
   *         the target exists and is not an empty directory, or the target lies inside the backup
   * @throws SirixIOException if copying or verification fails
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
      throw new SirixUsageException("Restore target " + target + " must not lie inside the backup directory "
          + source + ".");
    }

    final DatabaseType databaseType = Databases.getDatabaseType(source);

    final boolean targetExisted = Files.exists(target);
    boolean success = false;
    try {
      try {
        Files.createDirectories(target);
      } catch (final IOException e) {
        throw new SirixIOException("Could not create restore target directory " + target + ".", e);
      }

      // A backup is cold — no locking needed; copy everything (minus transients, defensively).
      final long totalBytes = copyTree(source, target,
                                       rel -> false,
                                       rel -> isTransientDatabaseFile(rel) || isTransientResourceFile(
                                           relativeToResource(rel)));

      // Verification pass: open the restored database and every resource read-only; any
      // corruption surfaces here (superblock validation, beacon checksums, revision-slot
      // checksums and the page-checksum chain on the root page read).
      final List<ResourceSummary> resourceSummaries = verifyRestoredDatabase(target, databaseType);

      success = true;
      return new BackupSummary(source, target, resourceSummaries, totalBytes);
    } finally {
      if (!success) {
        cleanupTarget(target, targetExisted);
      }
    }
  }

  /**
   * Opens the restored database and every resource read-only; opening a read transaction on the
   * most recent revision exercises superblock validation, beacon recovery, the checksummed
   * revision slot and the root-page checksum chain.
   */
  private static List<ResourceSummary> verifyRestoredDatabase(final Path target, final DatabaseType databaseType) {
    final List<ResourceSummary> resourceSummaries = new ArrayList<>();
    try (final Database<?> database = openDatabase(target, databaseType)) {
      for (final Path resourcePath : database.listResources()) {
        if (!Files.isDirectory(resourcePath)) {
          continue;
        }
        final String resourceName = resourcePath.getFileName().toString();
        try (final ResourceSession<?, ?> session = database.beginResourceSession(resourceName)) {
          final int mostRecentRevision = session.getMostRecentRevisionNumber();
          try (final NodeReadOnlyTrx rtx = session.beginNodeReadOnlyTrx()) {
            if (rtx.getRevisionNumber() != mostRecentRevision) {
              throw new SirixIOException("Restored resource " + resourceName + " opened at revision "
                  + rtx.getRevisionNumber() + " instead of the most recent revision " + mostRecentRevision + ".");
            }
          }
          resourceSummaries.add(new ResourceSummary(resourceName, mostRecentRevision, directorySize(resourcePath)));
        } catch (final RuntimeException e) {
          throw new SirixIOException("Verification of restored resource '" + resourceName + "' failed — "
              + "the restore has been rolled back.", e);
        }
      }
    }
    return resourceSummaries;
  }

  private static Database<?> openDatabase(final Path databaseDir, final DatabaseType databaseType) {
    return switch (databaseType) {
      case JSON -> Databases.openJsonDatabase(databaseDir);
      case XML -> Databases.openXmlDatabase(databaseDir);
    };
  }

  private static void requireSirixDatabase(final Path databaseDir) {
    if (!Files.isDirectory(databaseDir)
        || DatabaseConfiguration.DatabasePaths.compareStructure(databaseDir) != 0) {
      throw new SirixUsageException(databaseDir + " is not a SirixDB database directory.");
    }
  }

  private static void requireEmptyOrAbsentTarget(final Path target) {
    if (Files.exists(target) && !SirixFiles.isDirectoryEmpty(target)) {
      throw new SirixUsageException("Target " + target + " already exists and is not an empty directory.");
    }
  }

  /** Removes a partially written target; recreates the bare directory if it pre-existed. */
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

  /** Transient database-level entries: the .lock file and atomic-write temporaries. */
  private static boolean isTransientDatabaseFile(final Path relativePath) {
    final String fileName = relativePath.getFileName().toString();
    return (relativePath.getNameCount() == 1 && fileName.equals(LOCK_FILE_NAME)) || fileName.contains(".tmp");
  }

  /**
   * Transient resource-level entries ({@code relativePath} relative to the resource directory):
   * everything inside the transaction-intent-log directory (the {@code .commit} marker),
   * and atomic-write temporaries (diff-file {@code *.tmp*} spills).
   */
  private static boolean isTransientResourceFile(final Path relativePath) {
    if (relativePath == null) {
      return false;
    }
    final String fileName = relativePath.getFileName().toString();
    return relativePath.getName(0).toString().equals(INTENT_LOG_DIR_NAME) || fileName.contains(".tmp");
  }

  /**
   * Maps a path relative to the DATABASE directory to one relative to its resource directory
   * ({@code resources/<name>/rest…} → {@code rest…}), or {@code null} if it is not inside a
   * resource.
   */
  private static Path relativeToResource(final Path databaseRelativePath) {
    if (databaseRelativePath.getNameCount() <= 2
        || !databaseRelativePath.getName(0).toString().equals(RESOURCES_DIR.toString())) {
      return null;
    }
    return databaseRelativePath.subpath(2, databaseRelativePath.getNameCount());
  }

  /**
   * Recursively copies {@code sourceRoot} into {@code targetRoot}, preserving the directory
   * structure (including empty directories). {@code skipSubtree} prunes whole subtrees,
   * {@code skipFile} skips individual regular files; both receive paths relative to
   * {@code sourceRoot}. Copies never replace existing files ({@code REPLACE_EXISTING} is
   * deliberately off — the target is validated empty).
   *
   * @return total bytes copied
   */
  private static long copyTree(final Path sourceRoot, final Path targetRoot, final Predicate<Path> skipSubtree,
      final Predicate<Path> skipFile) {
    final long[] bytesCopied = { 0L };
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
    final long[] size = { 0L };
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
