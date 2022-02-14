package org.sirix.access;

import org.sirix.api.Database;
import org.sirix.api.NodeCursor;
import org.sirix.api.NodeReadOnlyTrx;
import org.sirix.api.NodeTrx;
import org.sirix.api.ResourceManager;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.api.xml.XmlResourceManager;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.utils.SirixFiles;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Utility methods for {@link Database} handling.
 *
 * @author Johannes Lichtenberger
 * @author Sebastian Graf, University of Konstanz
 *
 */
public final class Databases {

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
    boolean returnVal = true;
    // if file is existing, skipping
    if (Files.exists(dbConfig.getDatabaseFile())) {
      returnVal = false;
    } else {
      try {
        Files.createDirectories(dbConfig.getDatabaseFile());
      } catch (UnsupportedOperationException | IOException | SecurityException e) {
        returnVal = false;
      }
      if (returnVal) {
        // creation of folder structure
        for (final DatabaseConfiguration.DatabasePaths paths : DatabaseConfiguration.DatabasePaths.values()) {
          final Path toCreate = dbConfig.getDatabaseFile().resolve(paths.getFile());
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
              SirixFiles.recursiveRemove(dbConfig.getDatabaseFile());
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
        SirixFiles.recursiveRemove(dbConfig.getDatabaseFile());
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
  public static synchronized void removeDatabase(final Path dbFile) throws SirixIOException {
    // check that database must be closed beforehand
    if (!MANAGER.sessions().containsAnyEntry(dbFile)) {
      // if file is existing and folder is a sirix-database, delete it
      if (Files.exists(dbFile)) {
        // && DatabaseConfiguration.Paths.compareStructure(pConf.getFile()) ==
        // 0) {
        // instantiate the database for deletion
        SirixFiles.recursiveRemove(dbFile);
      }
    }
  }

  /**
   * Open database. A database can be opened only once (even across JVMs). Afterwards a singleton
   * instance bound to the {@link File} is returned.
   *
   * @param file determines where the database is located
   * @param user user used to open the database
   * @return {@link Database} instance.
   * @throws SirixIOException if an I/O exception occurs
   * @throws SirixUsageException if Sirix is not used properly
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public static synchronized Database<XmlResourceManager> openXmlDatabase(final Path file, final User user) {
    return openDatabase(file, user, DatabaseType.XML);
  }

  /**
   * Open database. A database can be opened only once (even across JVMs). Afterwards a singleton
   * instance bound to the {@link File} is returned.
   *
   * @param file determines where the database is located
   * @param user the user who interacts with the db
   * @return {@link Database} instance.
   * @throws SirixIOException if an I/O exception occurs
   * @throws SirixUsageException if Sirix is not used properly
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public static synchronized Database<JsonResourceManager> openJsonDatabase(final Path file, final User user) {
    return openDatabase(file, user, DatabaseType.JSON);
  }

  /**
   * Open database. A database can be opened only once (even across JVMs). Afterwards a singleton
   * instance bound to the {@link File} is returned.
   *
   * @param file determines where the database is located
   * @return {@link Database} instance.
   * @throws SirixIOException if an I/O exception occurs
   * @throws SirixUsageException if Sirix is not used properly
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public static synchronized Database<JsonResourceManager> openJsonDatabase(final Path file) {
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
   * @throws SirixIOException if an I/O exception occurs
   * @throws SirixUsageException if Sirix is not used properly
   * @throws NullPointerException if {@code file} is {@code null}
   */
  public static synchronized Database<XmlResourceManager> openXmlDatabase(final Path file) {
    return openDatabase(file, createAdminUser(), DatabaseType.XML);
  }

  private static <M extends ResourceManager<R, W>,
          R extends NodeReadOnlyTrx & NodeCursor, W extends NodeTrx & NodeCursor> Database<M> openDatabase(
          final Path file,
          final User user,
          final DatabaseType databaseType) {
    checkNotNull(file);
    if (!Files.exists(file)) {
      throw new SirixUsageException("DB could not be opened (since it was not created?) at location", file.toString());
    }
    final DatabaseConfiguration dbConfig = DatabaseConfiguration.deserialize(file);
    if (dbConfig == null) {
      throw new IllegalStateException("Configuration may not be null!");
    }
    return databaseType.createDatabase(dbConfig, user);
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
}
