package org.sirix.access;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.sirix.access.conf.DatabaseConfiguration;
import org.sirix.access.conf.ResourceManagerConfiguration;
import org.sirix.api.Database;
import org.sirix.api.ResourceManager;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;

/**
 * Utility methods for {@link Database} handling.
 *
 * @author Johannes Lichtenberger
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class Databases {

  /** Central repository of all running databases. */
  private static final ConcurrentMap<Path, Set<Database>> DATABASE_SESSIONS =
      new ConcurrentHashMap<>();

  /** Central repository of all running resource managers. */
  private static final ConcurrentMap<Path, Set<ResourceManager>> RESOURCE_MANAGERS =
      new ConcurrentHashMap<>();

  /**
   * Creating a database. This includes loading the database configuration, building up the
   * structure and preparing everything for login.
   *
   * @param dbConfig config which is used for the database, including storage location
   * @return true if creation is valid, false otherwise
   * @throws SirixIOException if something odd happens within the creation process.
   */
  public static synchronized boolean createDatabase(final DatabaseConfiguration dbConfig)
      throws SirixIOException {
    boolean returnVal = true;
    // if file is existing, skipping
    if (Files.exists(dbConfig.getFile())) {
      returnVal = false;
    } else {
      try {
        Files.createDirectories(dbConfig.getFile());
      } catch (UnsupportedOperationException | IOException | SecurityException e) {
        returnVal = false;
      }
      if (returnVal) {
        // creation of folder structure
        for (final DatabaseConfiguration.DatabasePaths paths : DatabaseConfiguration.DatabasePaths.values()) {
          final Path toCreate = dbConfig.getFile().resolve(paths.getFile());
          if (paths.isFolder()) {
            try {
              Files.createDirectory(toCreate);
            } catch (UnsupportedOperationException | IOException | SecurityException e) {
              returnVal = false;
            }
          } else {
            try {
              returnVal = toCreate.getFileName().equals(
                  DatabaseConfiguration.DatabasePaths.LOCK.getFile().getFileName()) ? true
                      : Files.createFile(toCreate) != null;
            } catch (final IOException e) {
              org.sirix.utils.Files.recursiveRemove(dbConfig.getFile());
              throw new SirixIOException(e);
            }
          }
          if (!returnVal) {
            break;
          }
        }
      }
      // serialization of the config
      DatabaseConfiguration.serialize(dbConfig);

      // if something was not correct, delete the partly created
      // substructure
      if (!returnVal) {
        org.sirix.utils.Files.recursiveRemove(dbConfig.getFile());
      }
    }

    return returnVal;
  }

  /**
   * Truncate a database. This deletes all relevant data. All running sessions must be closed
   * beforehand.
   *
   * @param dbConfig the database at this path should be deleted
   * @throws SirixIOException if Sirix fails to delete the database
   */
  public static synchronized void truncateDatabase(final DatabaseConfiguration dbConfig)
      throws SirixIOException {
    // check that database must be closed beforehand
    if (!DATABASE_SESSIONS.containsKey(dbConfig.getFile())) {
      // if file is existing and folder is a tt-dataplace, delete it
      if (Files.exists(dbConfig.getFile())) {
        // && DatabaseConfiguration.Paths.compareStructure(pConf.getFile()) ==
        // 0) {
        // instantiate the database for deletion
        org.sirix.utils.Files.recursiveRemove(dbConfig.getFile());
      }
    }
  }

  /**
   * Open database. A database can be opened only once (even across JVMs). Afterwards a singleton
   * instance bound to the {@link File} is returned.
   *
   * @param file determines where the database is located sessionConf a
   *        {@link ResourceManagerConfiguration} object to set up the session
   * @return {@link Database} instance.
   * @throws SirixIOException if an I/O exception occurs
   * @throws SirixUsageException if Sirix is not used properly
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public static synchronized Database openDatabase(final Path file)
      throws SirixUsageException, SirixIOException {
    checkNotNull(file);
    if (!Files.exists(file)) {
      throw new SirixUsageException(
          "DB could not be opened (since it was not created?) at location", file.toString());
    }
    final DatabaseConfiguration config = DatabaseConfiguration.deserialize(file);
    if (config == null) {
      throw new IllegalStateException("Configuration may not be null!");
    }
    final Database database = new DatabaseImpl(config);
    putDatabase(file, database);
    return database;
  }

  /**
   * Determines if a database already exists.
   *
   * @param dbConfig database configuration
   * @return {@code true}, if database exists, {@code false} otherwise
   */
  public static synchronized boolean existsDatabase(final DatabaseConfiguration dbConfig) {
    return Files.exists(dbConfig.getFile())
        && DatabaseConfiguration.DatabasePaths.compareStructure(dbConfig.getFile()) == 0 ? true
            : false;
  }

  /**
   * Package private method to put a file/database into the internal map.
   *
   * @param file database file to put into the map
   * @param database database handle to put into the map
   */
  static synchronized void putDatabase(final Path file, final Database database) {
    final Set<Database> databases = DATABASE_SESSIONS.getOrDefault(file, new HashSet<>());
    databases.add(database);
    DATABASE_SESSIONS.put(file, databases);
  }

  /**
   * Package private method to remove a database.
   *
   * @param file database file to remove
   */
  static synchronized void removeDatabase(final Path file) {
    DATABASE_SESSIONS.remove(file);
  }

  /**
   * Put a resource manager into the internal map.
   *
   * @param file resource file to put into the map
   * @param resourceManager resourceManager handle to put into the map
   */
  public static synchronized void putResourceManager(final Path file,
      final ResourceManager resourceManager) {
    final Set<ResourceManager> resourceManagers =
        RESOURCE_MANAGERS.getOrDefault(file, new HashSet<>());
    resourceManagers.add(resourceManager);
    RESOURCE_MANAGERS.put(file, resourceManagers);
  }

  /**
   * Remove a resource manager.
   *
   * @param resource manager to remove
   */
  public static synchronized void removeResourceManager(final Path file,
      final ResourceManager resourceManager) {
    final Set<ResourceManager> resourceManagers = RESOURCE_MANAGERS.get(file);

    if (resourceManagers == null) {
      return;
    }

    resourceManagers.remove(resourceManager);

    if (resourceManagers.isEmpty())
      RESOURCE_MANAGERS.remove(file);
  }

  /**
   * Determines if there are any open resource managers.
   *
   * @param file the resource file
   * @return {@code true}, if there are any open resource managers, {@code false} otherwise.
   */
  public static synchronized boolean hasOpenResourceManagers(Path file) {
    final Set<ResourceManager> resourceManagers = RESOURCE_MANAGERS.get(file);

    if (resourceManagers == null || resourceManagers.isEmpty()) {
      return true;
    }

    return false;
  }
}
