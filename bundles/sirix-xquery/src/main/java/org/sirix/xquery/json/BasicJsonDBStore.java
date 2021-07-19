package org.sirix.xquery.json;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.brackit.xquery.atomic.Str;
import org.brackit.xquery.xdm.DocumentException;
import org.brackit.xquery.xdm.Stream;
import org.sirix.access.DatabaseConfiguration;
import org.sirix.access.Databases;
import org.sirix.access.ResourceConfiguration;
import org.sirix.api.Database;
import org.sirix.api.json.JsonNodeTrx;
import org.sirix.api.json.JsonResourceManager;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixRuntimeException;
import org.sirix.io.StorageType;
import org.sirix.service.json.shredder.JsonShredder;
import com.google.gson.stream.JsonReader;

/**
 * Database storage.
 *
 * @author Johannes Lichtenberger
 */
public final class BasicJsonDBStore implements JsonDBStore {

  /**
   * User home directory.
   */
  private static final String USER_HOME = System.getProperty("user.home");

  /**
   * Storage for databases: Sirix data in home directory.
   */
  private static final Path LOCATION = Paths.get(USER_HOME, "sirix-data");

  /**
   * {@link Set} of databases.
   */
  private final Set<Database<JsonResourceManager>> databases;

  /**
   * Mapping sirix databases to collections.
   */
  private final ConcurrentMap<Database<JsonResourceManager>, JsonDBCollection> collections;

  /**
   * {@link StorageType} instance.
   */
  private final StorageType storageType;

  /**
   * The location to store created collections/databases.
   */
  private final Path location;

  /**
   * Determines if a path summary should be built.
   */
  private final boolean buildPathSummary;

  /**
   * Get a new builder instance.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Builder setting up the store.
   */
  public static class Builder {
    /**
     * Storage type.
     */
    private StorageType storageType = StorageType.FILE;

    /**
     * The location to store created collections/databases.
     */
    private Path location = LOCATION;

    /**
     * Determines if for resources a path summary should be build.
     */
    private boolean buildPathSummary = true;

    /**
     * Set the storage type (default: file backend).
     *
     * @param storageType storage type
     * @return this builder instance
     */
    public Builder storageType(final StorageType storageType) {
      this.storageType = checkNotNull(storageType);
      return this;
    }

    /**
     * Set if path summaries should be build for resources.
     *
     * @param buildPathSummary {@code true} if path summaries should be build, {@code false} otherwise
     * @return this builder instance
     */
    public Builder buildPathSummary(final boolean buildPathSummary) {
      this.buildPathSummary = buildPathSummary;
      return this;
    }

    /**
     * Set the location where to store the created databases/collections.
     *
     * @param location the location
     * @return this builder instance
     */
    public Builder location(final Path location) {
      this.location = checkNotNull(location);
      return this;
    }

    /**
     * Create a new {@link BasicJsonDBStore} instance
     *
     * @return new {@link BasicJsonDBStore} instance
     */
    public BasicJsonDBStore build() {
      return new BasicJsonDBStore(this);
    }
  }

  /**
   * Private constructor.
   *
   * @param builder builder instance
   */
  private BasicJsonDBStore(final Builder builder) {
    databases = Collections.synchronizedSet(new HashSet<>());
    collections = new ConcurrentHashMap<>();
    storageType = builder.storageType;
    location = builder.location;
    buildPathSummary = builder.buildPathSummary;
  }

  /**
   * Get the location of the generated collections/databases.
   */
  public Path getLocation() {
    return location;
  }

  @Override
  public JsonDBCollection lookup(final String name) {
    final Path dbPath = location.resolve(name);
    if (Databases.existsDatabase(dbPath)) {
      try {
        final var database = Databases.openJsonDatabase(dbPath);
        final Optional<Database<JsonResourceManager>> storedCollection =
            databases.stream().findFirst().filter(db -> db.equals(database));
        if (storedCollection.isPresent()) {
          final var db = storedCollection.get();

          if (db.isOpen()) {
            return collections.get(db);
          } else {
            databases.remove(db);
          }
        }
        databases.add(database);
        final JsonDBCollection collection = new JsonDBCollection(name, database, this);
        collections.put(database, collection);
        return collection;
      } catch (final SirixRuntimeException e) {
        throw new DocumentException(e.getCause());
      }
    }
    return null;
  }

  @Override
  public JsonDBCollection create(final String name) {
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(location.resolve(name));
    try {
      if (Databases.createJsonDatabase(dbConf)) {
        throw new DocumentException("Document with name %s exists!", name);
      }

      final var database = Databases.openJsonDatabase(dbConf.getDatabaseFile());
      databases.add(database);

      final JsonDBCollection collection = new JsonDBCollection(name, database, this);
      collections.put(database, collection);
      return collection;
    } catch (final SirixRuntimeException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public JsonDBCollection create(final String collName, final Path path) {
    return create(collName, null, path);
  }

  @Override
  public JsonDBCollection create(String collName, String json) {
    return create(collName, null, json);
  }

  @Override
  public JsonDBCollection create(String collName, String optResName, String json) {
    if (json == null) {
      return createCollection(collName, optResName, null);
    }
    return createCollection(collName, optResName, JsonShredder.createStringReader(json));
  }

  @Override
  public JsonDBCollection create(final String collName, final String optResName, final Path path) {
    return createCollection(collName, optResName, JsonShredder.createFileReader(path));
  }

  private JsonDBCollection createCollection(final String collName, final String optionalResourceName,
      final JsonReader reader) {
    final Path dbPath = location.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createJsonDatabase(dbConf);
      final var database = Databases.openJsonDatabase(dbPath);
      databases.add(database);

      final String resourceName;
      if (optionalResourceName != null) {
        final var resources = database.listResources();

        final Optional<Path> hasResourceWithName = resources.stream()
                                                            .filter(resource -> resource.getFileName()
                                                                                        .toString()
                                                                                        .equals(optionalResourceName))
                                                            .findFirst();

        if (hasResourceWithName.isPresent()) {
          int i = database.listResources().size() + 1;
          resourceName = "resource" + i;
        } else {
          resourceName = optionalResourceName;
        }
      } else {
        resourceName = "resource" + (database.listResources().size() + 1);
      }

      database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                   .useDeweyIDs(true)
                                                   .useTextCompression(true)
                                                   .buildPathSummary(buildPathSummary)
                                                   .storageType(storageType)
                                                   .build());
      final JsonDBCollection collection = new JsonDBCollection(collName, database, this);
      collections.put(database, collection);

      if (reader == null) {
        return collection;
      }

      try (final JsonResourceManager manager = database.openResourceManager(resourceName);
           final JsonNodeTrx wtx = manager.beginNodeTrx()) {
        wtx.insertSubtreeAsFirstChild(reader);
      }
      return collection;
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public JsonDBCollection create(String collName, Set<JsonReader> jsonReaders) {
    final Path dbPath = location.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createJsonDatabase(dbConf);
      final var database = Databases.openJsonDatabase(dbConf.getDatabaseFile());
      databases.add(database);
      final ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
      int numberOfResources = database.listResources().size();
      for (final var jsonReader : jsonReaders) {
        numberOfResources++;
        final String resourceName = "resource" + numberOfResources;
        pool.submit(() -> createResource(collName, database, jsonReader, resourceName));
      }
      pool.shutdown();
      pool.awaitTermination(15, TimeUnit.SECONDS);
      return new JsonDBCollection(collName, database, this);
    } catch (final SirixRuntimeException | InterruptedException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public JsonDBCollection create(String collectionName, String resourceName, JsonReader jsonReader) {
    return createCollection(collectionName, resourceName, jsonReader);
  }

  @Override
  public JsonDBStore addDatabase(JsonDBCollection jsonDBCollection, Database<JsonResourceManager> database) {
    databases.add(database);
    collections.put(database, jsonDBCollection);
    return this;
  }

  @Override
  public JsonDBStore removeDatabase(final @Nonnull Database<JsonResourceManager> database) {
    databases.remove(database);
    collections.remove(database);
    return this;
  }

  @Override
  public JsonDBCollection createFromJsonStrings(String collName, final @Nullable Stream<Str> jsonStrings) {
    if (jsonStrings == null) {
      return null;
    }

    final Path dbPath = location.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createJsonDatabase(dbConf);
      final var database = Databases.openJsonDatabase(dbConf.getDatabaseFile());
      databases.add(database);
      final ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
      int i = database.listResources().size() + 1;
      try (jsonStrings) {
        Str string;
        while ((string = jsonStrings.next()) != null) {
          final String currentString = string.stringValue();
          final String resourceName = "resource" + i;
          pool.submit(() -> createResource(collName,
                                           database,
                                           JsonShredder.createStringReader(currentString),
                                           resourceName));
          i++;
        }
      }
      pool.shutdown();
      pool.awaitTermination(15, TimeUnit.SECONDS);
      return new JsonDBCollection(collName, database, this);
    } catch (final SirixRuntimeException | InterruptedException e) {
      throw new DocumentException(e.getCause());
    }
  }

  private Void createResource(String collName, final Database<JsonResourceManager> database, final JsonReader reader,
      final String resourceName) {
    database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                 .storageType(storageType)
                                                 .useDeweyIDs(true)
                                                 .useTextCompression(true)
                                                 .buildPathSummary(true)
                                                 .build());
    try (final JsonResourceManager manager = database.openResourceManager(resourceName);
         final JsonNodeTrx wtx = manager.beginNodeTrx()) {
      final JsonDBCollection collection = new JsonDBCollection(collName, database, this);
      collections.put(database, collection);
      wtx.insertSubtreeAsFirstChild(reader);
    }
    return null;
  }

  @Override
  public JsonDBCollection createFromPaths(final String collName, final @Nullable Stream<Path> paths) {
    if (paths == null) {
      return null;
    }

    final Path dbPath = location.resolve(collName);
    final DatabaseConfiguration dbConf = new DatabaseConfiguration(dbPath);
    try {
      removeIfExisting(dbConf);
      Databases.createJsonDatabase(dbConf);
      final var database = Databases.openJsonDatabase(dbConf.getDatabaseFile());
      databases.add(database);
      final ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
      int i = database.listResources().size() + 1;
      try (paths) {
        Path path;
        while ((path = paths.next()) != null) {
          final Path currentPath = path;
          final String resourceName = "resource" + i;
          pool.submit(() -> {
            database.createResource(ResourceConfiguration.newBuilder(resourceName)
                                                         .storageType(storageType)
                                                         .useDeweyIDs(true)
                                                         .useTextCompression(true)
                                                         .buildPathSummary(true)
                                                         .build());
            try (final JsonResourceManager manager = database.openResourceManager(resourceName);
                 final JsonNodeTrx wtx = manager.beginNodeTrx()) {
              final JsonDBCollection collection = new JsonDBCollection(collName, database, this);
              collections.put(database, collection);
              wtx.insertSubtreeAsFirstChild(JsonShredder.createFileReader(currentPath));
            }
            return null;
          });
          i++;
        }
      }
      pool.shutdown();
      pool.awaitTermination(15, TimeUnit.SECONDS);
      return new JsonDBCollection(collName, database, this);
    } catch (final SirixRuntimeException | InterruptedException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void drop(final String name) {
    final Path dbPath = location.resolve(name);
    final DatabaseConfiguration dbConfig = new DatabaseConfiguration(dbPath);
    if (!removeIfExisting(dbConfig)) {
      throw new DocumentException("No collection with the specified name found!");
    }
  }

  private boolean removeIfExisting(final DatabaseConfiguration dbConfig) {
    if (Databases.existsDatabase(dbConfig.getDatabaseFile())) {
      try {
        final Predicate<Database<JsonResourceManager>> databasePredicate =
            currDatabase -> currDatabase.getDatabaseConfig().getDatabaseFile().equals(dbConfig.getDatabaseFile());

        databases.removeIf(databasePredicate);
        collections.keySet().removeIf(databasePredicate);
        Databases.removeDatabase(dbConfig.getDatabaseFile());
      } catch (final SirixRuntimeException e) {
        throw new DocumentException(e);
      }
      return true;
    }

    return false;
  }

  @Override
  public void makeDir(final String path) {
    try {
      Files.createDirectory(java.nio.file.Paths.get(path));
    } catch (final IOException e) {
      throw new DocumentException(e.getCause());
    }
  }

  @Override
  public void close() {
    try {
      for (final var database : databases) {
        database.close();
      }
    } catch (final SirixException e) {
      throw new DocumentException(e.getCause());
    }
  }
}

