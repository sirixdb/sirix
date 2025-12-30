package io.sirix.access;

import io.sirix.access.trx.RevisionEpochTracker;
import io.sirix.api.*;
import io.sirix.api.json.JsonResourceSession;
import io.sirix.api.xml.XmlResourceSession;
import io.sirix.cache.*;
import io.sirix.exception.SirixIOException;
import io.sirix.exception.SirixUsageException;
import io.sirix.utils.LogWrapper;
import io.sirix.utils.OS;
import io.sirix.utils.SirixFiles;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Objects.requireNonNull;

/**
 * Utility methods for {@link Database} handling.
 *
 * @author Johannes Lichtenberger
 * @author Sebastian Graf, University of Konstanz
 */
public final class Databases {

  /**
   * Private constructor to prevent instantiation.
   */
  private Databases() {
    throw new AssertionError("May not be instantiated!");
  }

  private static final LogWrapper logger = new LogWrapper(LoggerFactory.getLogger(Databases.class));

  /**
   * Single global BufferManager shared across all databases and resources.
   * This follows the PostgreSQL/MySQL/SQL Server architecture pattern where
   * a single buffer pool serves the entire database server instance.
   * <p>
   * Cache keys include (databaseId, resourceId) to prevent collisions.
   * Initialized lazily when first database is opened.
   */
  private static volatile BufferManager GLOBAL_BUFFER_MANAGER = null;
  
  /**
   * Global RevisionEpochTracker for MVCC-aware eviction across all databases/resources.
   * Tracks minimum active revision globally. ClockSweepers use this to determine
   * which pages can be safely evicted.
   */
  private static volatile RevisionEpochTracker GLOBAL_EPOCH_TRACKER = null;

  /**
   * Database ID counter for assigning unique IDs to databases.
   */
  private static final AtomicLong DATABASE_ID_COUNTER = new AtomicLong(0);

  /**
   * DI component that manages the database.
   */
  static final DatabaseManager MANAGER = DaggerDatabaseManager.create();

  /**
   * Get the database type
   *
   * @param file the database file
   * @return the type of the database
   */
  public static DatabaseType getDatabaseType(final Path file) {
    return DatabaseConfiguration.deserialize(file).getDatabaseType();
  }

  /**
   * Creates an XML-database. This includes loading the database configuration, building up the
   * structure and preparing everything for login.
   *
   * @param dbConfig config which is used for the database, including storage location
   * @return true if creation is valid, false otherwise
   * @throws SirixIOException if something odd happens within the creation process.
   */
  public static synchronized boolean createXmlDatabase(final DatabaseConfiguration dbConfig) {
    return createTheDatabase(dbConfig.setDatabaseType(DatabaseType.XML));
  }

  /**
   * Creates a JSON-database. This includes loading the database configuration, building up the
   * structure and preparing everything for login.
   *
   * @param dbConfig config which is used for the database, including storage location
   * @return true if creation is valid, false otherwise
   * @throws SirixIOException if something odd happens within the creation process.
   */
  public static synchronized boolean createJsonDatabase(final DatabaseConfiguration dbConfig) {
    return createTheDatabase(dbConfig.setDatabaseType(DatabaseType.JSON));
  }

  private static boolean createTheDatabase(final DatabaseConfiguration dbConfig) {
    requireNonNull(dbConfig);

    initAllocator(dbConfig.getMaxSegmentAllocationSize());

    boolean returnVal = true;
    // if file is existing, skipping
    final var databaseFile = dbConfig.getDatabaseFile();
    if (Files.exists(databaseFile) && !SirixFiles.isDirectoryEmpty(databaseFile)) {
      returnVal = false;
    } else {
      try {
        Files.createDirectories(databaseFile);
      } catch (UnsupportedOperationException | IOException | SecurityException e) {
        returnVal = false;
      }
      if (returnVal) {
        // creation of folder structure
        for (final DatabaseConfiguration.DatabasePaths paths : DatabaseConfiguration.DatabasePaths.values()) {
          final Path toCreate = databaseFile.resolve(paths.getFile());
          if (paths.isFolder()) {
            try {
              Files.createDirectory(toCreate);
            } catch (UnsupportedOperationException | IOException | SecurityException e) {
              returnVal = false;
            }
          } else {
            try {
              if (!toCreate.getFileName().equals(DatabaseConfiguration.DatabasePaths.LOCK.getFile().getFileName())) {
                Files.createFile(toCreate);
              }
            } catch (final IOException e) {
              SirixFiles.recursiveRemove(databaseFile);
              throw new SirixIOException(e);
            }
          }
          if (!returnVal) {
            break;
          }
        }
      }
      
      // Assign unique database ID if not already set
      if (dbConfig.getDatabaseId() == 0) {
        dbConfig.setDatabaseId(DATABASE_ID_COUNTER.getAndIncrement());
      }
      
      // serialization of the config
      DatabaseConfiguration.serialize(dbConfig);

      // if something was not correct, delete the partly created
      // substructure
      if (!returnVal) {
        SirixFiles.recursiveRemove(databaseFile);
      }
    }

    return returnVal;
  }

  /**
   * Delete a database. This deletes all relevant data. All running sessions must be closed
   * beforehand.
   *
   * @param dbFile the database at this path should be deleted
   * @throws SirixIOException if Sirix fails to delete the database
   */
  public static synchronized void removeDatabase(final Path dbFile) {
    // check that database must be closed beforehand and if file is existing and folder is a sirix-database, delete it
    if (!MANAGER.sessions().containsAnyEntry(dbFile) && Files.exists(dbFile)) {
      if (DatabaseConfiguration.DatabasePaths.compareStructure(dbFile) == 0) {
        final var databaseConfiguration = DatabaseConfiguration.deserialize(dbFile);
        final var databaseType = databaseConfiguration.getDatabaseType();

        switch (databaseType) {
          case XML -> removeXmlResources(dbFile);
          case JSON -> removeJsonResources(dbFile);
          default -> throw new IllegalStateException("Database type unknown!");
        }
      }

      // CRITICAL FIX: Clear caches for this database to prevent cache pollution
      // Without this, pages from removed databases can pollute caches and cause test failures
      if (GLOBAL_BUFFER_MANAGER != null && DatabaseConfiguration.DatabasePaths.compareStructure(dbFile) == 0) {
        final var dbConfig = DatabaseConfiguration.deserialize(dbFile);
        long databaseId = dbConfig.getDatabaseId();
        GLOBAL_BUFFER_MANAGER.clearCachesForDatabase(databaseId);
      }
      
      SirixFiles.recursiveRemove(dbFile);

      freeAllocatedMemory();
    } else {
      logger.warn("Database at {} could not be removed, because it is either not existing or still in use.", dbFile);
    }
  }

  public static void freeAllocatedMemory() {
    if (MANAGER.sessions().isEmpty()) {
      // NOTE: BufferManager and ClockSweepers are NOT shut down here!
      // They follow PostgreSQL bgwriter pattern - run continuously until JVM shutdown.
      // This prevents race conditions when tests rapidly open/close sessions.
      
      // Only clear caches for test hygiene (keep BufferManager infrastructure alive)
      if (GLOBAL_BUFFER_MANAGER != null) {
        logger.debug("Clearing global caches (BufferManager stays active)");
        GLOBAL_BUFFER_MANAGER.clearAllCaches();
      }
      
      // NOTE: Don't clear epoch tracker - it's global state
      // NOTE: Don't free allocator - it's reused across tests
      
      // ClockSweepers continue running in background (daemon threads)
    }
  }

  private static void removeJsonResources(Path dbFile) {
    try (final Database<?> database = openJsonDatabase(dbFile)) {
      removeResources(database);
    }
  }

  private static void removeXmlResources(Path dbFile) {
    try (final Database<?> database = openXmlDatabase(dbFile)) {
      removeResources(database);
    }
  }

  private static void removeResources(Database<?> database) {
    final var resourcePaths = database.listResources();
    for (final var resourcePath : resourcePaths) {
      database.removeResource(resourcePath.getFileName().toString());
    }
  }

  /**
   * Open database. A database can be opened only once (even across JVMs). Afterwards a singleton
   * instance bound to the {@link File} is returned.
   *
   * @param file determines where the database is located
   * @param user user used to open the database
   * @return {@link Database} instance.
   * @throws SirixIOException     if an I/O exception occurs
   * @throws SirixUsageException  if Sirix is not used properly
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public static synchronized Database<XmlResourceSession> openXmlDatabase(final Path file, final User user) {
    return openDatabase(file, user, DatabaseType.XML);
  }

  /**
   * Open database. A database can be opened only once (even across JVMs). Afterwards a singleton
   * instance bound to the {@link File} is returned.
   *
   * @param file determines where the database is located
   * @param user the user who interacts with the db
   * @return {@link Database} instance.
   * @throws SirixIOException     if an I/O exception occurs
   * @throws SirixUsageException  if Sirix is not used properly
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public static synchronized Database<JsonResourceSession> openJsonDatabase(final Path file, final User user) {
    return openDatabase(file, user, DatabaseType.JSON);
  }

  /**
   * Open database. A database can be opened only once (even across JVMs). Afterwards a singleton
   * instance bound to the {@link File} is returned.
   *
   * @param file determines where the database is located
   * @return {@link Database} instance.
   * @throws SirixIOException     if an I/O exception occurs
   * @throws SirixUsageException  if Sirix is not used properly
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public static synchronized Database<JsonResourceSession> openJsonDatabase(final Path file) {
    return openDatabase(file, createAdminUser(), DatabaseType.JSON);
  }

  /**
   * Creates a reference to an admin user. Each call to this method will return a new user.
   *
   * @return A new admin user.
   */
  private static User createAdminUser() {
    return new User("admin", UUID.randomUUID());
  }

  /**
   * Open database. A database can be opened only once (even across JVMs). Afterwards a singleton
   * instance bound to the {@link File} is returned.
   *
   * @param file determines where the database is located
   * @return {@link Database} instance.
   * @throws SirixIOException     if an I/O exception occurs
   * @throws SirixUsageException  if Sirix is not used properly
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public static synchronized Database<XmlResourceSession> openXmlDatabase(final Path file) {
    return openDatabase(file, createAdminUser(), DatabaseType.XML);
  }

  private static <M extends ResourceSession<R, W>, R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> Database<M> openDatabase(
      final Path file, final User user, final DatabaseType databaseType) {
    requireNonNull(file);
    if (!Files.exists(file)) {
      throw new SirixUsageException("DB could not be opened (since it was not created?) at location", file.toString());
    }
    final DatabaseConfiguration dbConfig = DatabaseConfiguration.deserialize(file);
    if (dbConfig == null) {
      throw new IllegalStateException("Configuration may not be null!");
    }

    // Assign database ID if not already set (backward compatibility)
    if (dbConfig.getDatabaseId() == 0) {
      dbConfig.setDatabaseId(DATABASE_ID_COUNTER.getAndIncrement());
      DatabaseConfiguration.serialize(dbConfig);
    }

    initAllocator(dbConfig.getMaxSegmentAllocationSize());
    return databaseType.createDatabase(dbConfig, user);
  }

  private static void initAllocator(long maxSegmentAllocationSize) {
    if (MANAGER.sessions().isEmpty()) {
      // Initialize the allocator (no pool needed anymore)
      MemorySegmentAllocator segmentAllocator =
          OS.isWindows() ? WindowsMemorySegmentAllocator.getInstance() : LinuxMemorySegmentAllocator.getInstance();
      segmentAllocator.init(maxSegmentAllocationSize);
      
      // Initialize global BufferManager with sizes proportional to memory budget
      initializeGlobalBufferManager(maxSegmentAllocationSize);
    }
  }

  /**
   * Determines if a database already exists.
   *
   * @param dbPath database path
   * @return {@code true}, if database exists, {@code false} otherwise
   */
  public static synchronized boolean existsDatabase(final Path dbPath) {
    return Files.exists(dbPath) && DatabaseConfiguration.DatabasePaths.compareStructure(dbPath) == 0;
  }

  /**
   * Initialize the global BufferManager with sizes based on memory budget.
   * Called when the first database is opened.
   *
   * @param maxSegmentAllocationSize the maximum memory budget for the allocator
   */
  private static synchronized void initializeGlobalBufferManager(long maxSegmentAllocationSize) {
    if (GLOBAL_BUFFER_MANAGER == null) {
      // Only scale record page caches with memory budget
      // Other caches use fixed baseline sizes
      long budgetGB = maxSegmentAllocationSize / (1L << 30);
      int scaleFactor = (int) Math.max(1, budgetGB);
      
      // Scale with budget (these are the main memory consumers)
      int maxRecordPageCacheWeight = 65_536 * 100 * scaleFactor;
      int maxRecordPageFragmentCacheWeight = (65_536 * 100 * scaleFactor) / 2;
      
      // Fixed sizes (don't scale with budget)
      int maxPageCacheWeight = 500_000;
      int maxRevisionRootPageCache = 5_000;
      int maxRBTreeNodeCache = 50_000;
      int maxNamesCacheSize = 500;
      int maxPathSummaryCacheSize = 20;
      
      logger.info("Initializing global BufferManager with memory budget: {} GB", budgetGB);
      logger.info("  - RecordPageCache weight: {} (scaled {}x)", maxRecordPageCacheWeight, scaleFactor);
      logger.info("  - RecordPageFragmentCache weight: {} (scaled {}x)", maxRecordPageFragmentCacheWeight, scaleFactor);
      logger.info("  - PageCache weight: {} (fixed)", maxPageCacheWeight);
      logger.info("  - RevisionRootPageCache size: {} (fixed)", maxRevisionRootPageCache);
      logger.info("  - RBTreeNodeCache size: {} (fixed)", maxRBTreeNodeCache);
      logger.info("  - NamesCache size: {} (fixed)", maxNamesCacheSize);
      logger.info("  - PathSummaryCache size: {} (fixed)", maxPathSummaryCacheSize);
      
      GLOBAL_BUFFER_MANAGER = new BufferManagerImpl(
          maxPageCacheWeight,
          maxRecordPageCacheWeight,
          maxRecordPageFragmentCacheWeight,
          maxRevisionRootPageCache,
          maxRBTreeNodeCache,
          maxNamesCacheSize,
          maxPathSummaryCacheSize);
      
      // Initialize global epoch tracker (large slot count for all databases/resources)
      GLOBAL_EPOCH_TRACKER = new RevisionEpochTracker(4096);
      GLOBAL_EPOCH_TRACKER.setLastCommittedRevision(0);
      
      // Start GLOBAL ClockSweeper threads (PostgreSQL bgwriter pattern)
      // These run continuously until DBMS shutdown
      if (GLOBAL_BUFFER_MANAGER instanceof BufferManagerImpl bufferMgrImpl) {
        bufferMgrImpl.startClockSweepers(GLOBAL_EPOCH_TRACKER);
      }
      
      logger.info("GLOBAL ClockSweeper threads started (PostgreSQL bgwriter pattern)");
    }
  }

  /**
   * Get the global BufferManager instance directly.
   * 
   * @return the single global BufferManager instance
   */
  public static BufferManager getGlobalBufferManager() {
    if (GLOBAL_BUFFER_MANAGER == null) {
      // Initialize with default if called before any database is opened
      initializeGlobalBufferManager(2L * (1L << 30)); // 2GB default
    }
    return GLOBAL_BUFFER_MANAGER;
  }
  
  /**
   * Get the global RevisionEpochTracker instance.
   * All resource sessions register their active revisions with this global tracker.
   * This follows PostgreSQL pattern for MVCC snapshot tracking.
   * 
   * @return the single global RevisionEpochTracker instance
   */
  public static RevisionEpochTracker getGlobalEpochTracker() {
    if (GLOBAL_EPOCH_TRACKER == null) {
      // Initialize with default if called before BufferManager is initialized
      GLOBAL_EPOCH_TRACKER = new RevisionEpochTracker(4096);
      GLOBAL_EPOCH_TRACKER.setLastCommittedRevision(0);
    }
    return GLOBAL_EPOCH_TRACKER;
  }
}
