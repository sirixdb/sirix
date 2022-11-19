package org.sirix.access;

import com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider;
import org.sirix.api.*;
import org.sirix.api.json.JsonResourceSession;
import org.sirix.api.xml.XmlResourceSession;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixUsageException;
import org.sirix.utils.LogWrapper;
import org.sirix.utils.SirixFiles;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

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

  static {
    com.amazon.corretto.crypto.provider.AmazonCorrettoCryptoProvider.install();
  }

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
    try {
      if (Cipher.getInstance("AES/GCM/NoPadding")
                .getProvider()
                .getName()
                .equals(AmazonCorrettoCryptoProvider.PROVIDER_NAME)) {
        // Successfully installed
        logger.debug("Successfully installed Amazon Corretto Crypto Provider.");
      }
    } catch (NoSuchAlgorithmException | NoSuchPaddingException e) {
      throw new IllegalStateException(e);
    }
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

      SirixFiles.recursiveRemove(dbFile);
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
